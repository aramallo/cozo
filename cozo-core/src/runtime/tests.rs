/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use std::collections::BTreeMap;
use std::time::Duration;

use itertools::Itertools;
use log::debug;
use serde_json::json;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::FixedRulePayload;
use crate::fts::{TokenizerCache, TokenizerConfig};
use crate::parse::SourceSpan;
use crate::runtime::callback::CallbackOp;
use crate::runtime::db::Poison;
use crate::{DbInstance, FixedRule, RegularTempStore, ScriptMutability};

#[test]
fn test_limit_offset() {
    let db = DbInstance::default();
    let res = db
        .run_default("?[a] := a in [5,3,1,2,4] :limit 2")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[3], [5]]));
    let res = db
        .run_default("?[a] := a in [5,3,1,2,4] :limit 2 :offset 1")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[1], [3]]));
    let res = db
        .run_default("?[a] := a in [5,3,1,2,4] :limit 2 :offset 4")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[4]]));
    let res = db
        .run_default("?[a] := a in [5,3,1,2,4] :limit 2 :offset 5")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([]));
}

#[test]
fn test_normal_aggr_empty() {
    let db = DbInstance::default();
    let res = db.run_default("?[count(a)] := a in []").unwrap().rows;
    assert_eq!(res, vec![vec![DataValue::from(0)]]);
}

#[test]
fn test_meet_aggr_empty() {
    let db = DbInstance::default();
    let res = db.run_default("?[min(a)] := a in []").unwrap().rows;
    assert_eq!(res, vec![vec![DataValue::Null]]);

    let res = db
        .run_default("?[min(a), count(a)] := a in []")
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![DataValue::Null, DataValue::from(0)]]);
}

#[test]
fn test_layers() {
    let _ = env_logger::builder().is_test(true).try_init();

    let db = DbInstance::default();
    let res = db
        .run_default(
            r#"
        y[a] := a in [1,2,3]
        x[sum(a)] := y[a]
        x[sum(a)] := a in [4,5,6]
        ?[sum(a)] := x[a]
        "#,
        )
        .unwrap()
        .rows;
    assert_eq!(res[0][0], DataValue::from(21.))
}

#[test]
fn test_conditions() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = DbInstance::default();
    db.run_default(
        r#"
        {
            ?[code] <- [['a'],['b'],['c']]
            :create airport {code}
        }
        {
            ?[fr, to, dist] <- [['a', 'b', 1.1], ['a', 'c', 0.5], ['b', 'c', 9.1]]
            :create route {fr, to => dist}
        }
        "#,
    )
    .unwrap();
    debug!("real test begins");
    let res = db
        .run_default(
            r#"
        r[code, dist] := *airport{code}, *route{fr: code, dist};
        ?[dist] := r['a', dist], dist > 0.5, dist <= 1.1;
        "#,
        )
        .unwrap()
        .rows;
    assert_eq!(res[0][0], DataValue::from(1.1))
}

#[test]
fn test_classical() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = DbInstance::default();
    let res = db
        .run_default(
            r#"
parent[] <- [['joseph', 'jakob'],
             ['jakob', 'isaac'],
             ['isaac', 'abraham']]
grandparent[gcld, gp] := parent[gcld, p], parent[p, gp]
?[who] := grandparent[who, 'abraham']
        "#,
        )
        .unwrap()
        .rows;
    println!("{:?}", res);
    assert_eq!(res[0][0], DataValue::from("jakob"))
}

#[test]
fn default_columns() {
    let db = DbInstance::default();

    db.run_default(
        r#"
            :create status {uid: String, ts default now() => quitted: Bool, mood: String}
            "#,
    )
    .unwrap();

    db.run_default(
        r#"
        ?[uid, quitted, mood] <- [['z', true, 'x']]
            :put status {uid => quitted, mood}
        "#,
    )
    .unwrap();
}

#[test]
fn rm_does_not_need_all_keys() {
    let db = DbInstance::default();
    db.run_default(":create status {uid => mood}").unwrap();
    assert!(db
        .run_default("?[uid, mood] <- [[1, 2]] :put status {uid => mood}",)
        .is_ok());
    assert!(db
        .run_default("?[uid, mood] <- [[2]] :put status {uid}",)
        .is_err());
    assert!(db
        .run_default("?[uid, mood] <- [[3, 2]] :rm status {uid => mood}",)
        .is_ok());
    assert!(db.run_default("?[uid] <- [[1]] :rm status {uid}").is_ok());
}

#[test]
fn strict_checks_for_fixed_rules_args() {
    let db = DbInstance::default();
    let res = db.run_default(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[_, _])
        "#,
    );
    println!("{:?}", res);
    assert!(res.is_ok());

    let db = DbInstance::default();
    let res = db.run_default(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[a, b])
        "#,
    );
    assert!(res.is_ok());

    let db = DbInstance::default();
    let res = db.run_default(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[a, a])
        "#,
    );
    assert!(res.is_err());
}

#[test]
fn do_not_unify_underscore() {
    let db = DbInstance::default();
    let res = db
        .run_default(
            r#"
        r1[] <- [[1, 'a'], [2, 'b']]
        r2[] <- [[2, 'B'], [3, 'C']]

        ?[l1, l2] := r1[_ , l1], r2[_ , l2]
        "#,
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 4);

    let res = db.run_default(
        r#"
        ?[_] := _ = 1
        "#,
    );
    assert!(res.is_err());

    let res = db
        .run_default(
            r#"
        ?[x] := x = 1, _ = 1, _ = 2
        "#,
        )
        .unwrap()
        .rows;

    assert_eq!(res.len(), 1);
}

#[test]
fn imperative_script() {
    // let db = DbInstance::default();
    // let res = db
    //     .run_default(
    //         r#"
    //     {:create _test {a}}
    //
    //     %loop
    //         %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z >= 10 }
    //             %then %return _test
    //         %end
    //         { ?[a] := a = rand_uuid_v1(); :put _test {a} }
    //         %debug _test
    //     %end
    // "#,
    //         Default::default(),
    //     )
    //     .unwrap();
    // assert_eq!(res.rows.len(), 10);
    //
    // let res = db
    //     .run_default(
    //         r#"
    //     {?[a] <- [[1], [2], [3]]
    //      :replace _test {a}}
    //
    //     %loop
    //         { ?[a] := *_test[a]; :limit 1; :rm _test {a} }
    //         %debug _test
    //
    //         %if_not _test
    //         %then %break
    //         %end
    //     %end
    //
    //     %return _test
    // "#,
    //         Default::default(),
    //     )
    //     .unwrap();
    // assert_eq!(res.rows.len(), 0);
    //
    // let res = db.run_default(
    //     r#"
    //     {:create _test {a}}
    //
    //     %loop
    //         { ?[a] := a = rand_uuid_v1(); :put _test {a} }
    //
    //         %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z < 10 }
    //             %continue
    //         %end
    //
    //         %return _test
    //         %debug _test
    //     %end
    // "#,
    //     Default::default(),
    // );
    // if let Err(err) = &res {
    //     eprintln!("{err:?}");
    // }
    // assert_eq!(res.unwrap().rows.len(), 10);
    //
    // let res = db
    //     .run_default(
    //         r#"
    //     {?[a] <- [[1], [2], [3]]
    //      :replace _test {a}}
    //     {?[a] <- []
    //      :replace _test2 {a}}
    //     %swap _test _test2
    //     %return _test
    // "#,
    //         Default::default(),
    //     )
    //     .unwrap();
    // assert_eq!(res.rows.len(), 0);
}

#[test]
fn returning_relations() {
    let db = DbInstance::default();
    let res = db
        .run_default(
            r#"
        {:create _xxz {a}}
        {?[a] := a in [5,4,1,2,3] :put _xxz {a}}
        {?[a] := *_xxz[a], a % 2 == 0 :rm _xxz {a}}
        {?[a] := *_xxz[b], a = b * 2}
        "#,
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[2], [6], [10]]));
    let res = db.run_default(
        r#"
        {?[a] := *_xxz[b], a = b * 2}
        "#,
    );
    assert!(res.is_err());
}

#[test]
fn test_trigger() {
    let db = DbInstance::default();
    db.run_default(":create friends {fr: Int, to: Int => data: Any}")
        .unwrap();
    db.run_default(":create friends.rev {to: Int, fr: Int => data: Any}")
        .unwrap();
    db.run_default(
        r#"
        ::set_triggers friends

        on put {
            ?[fr, to, data] := _new[fr, to, data]

            :put friends.rev{ to, fr => data}
        }
        on rm {
            ?[fr, to] := _old[fr, to, data]

            :rm friends.rev{ to, fr }
        }
        "#,
    )
    .unwrap();
    db.run_default(r"?[fr, to, data] <- [[1,2,3]] :put friends {fr, to => data}")
        .unwrap();
    let ret = db
        .export_relations(["friends", "friends.rev"].into_iter())
        .unwrap();
    let frs = ret.get("friends").unwrap();
    assert_eq!(
        vec![DataValue::from(1), DataValue::from(2), DataValue::from(3)],
        frs.rows[0]
    );

    let frs_rev = ret.get("friends.rev").unwrap();
    assert_eq!(
        vec![DataValue::from(2), DataValue::from(1), DataValue::from(3)],
        frs_rev.rows[0]
    );
    db.run_default(r"?[fr, to] <- [[1,2], [2,3]] :rm friends {fr, to}")
        .unwrap();
    let ret = db
        .export_relations(["friends", "friends.rev"].into_iter())
        .unwrap();
    let frs = ret.get("friends").unwrap();
    assert!(frs.rows.is_empty());
}

#[test]
fn test_callback() {
    let db = DbInstance::default();
    let mut collected = vec![];
    let (_id, receiver) = db.register_callback("friends", None);
    db.run_default(":create friends {fr: Int, to: Int => data: Any}")
        .unwrap();
    db.run_default(r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to => data}")
        .unwrap();
    db.run_default(r"?[fr, to, data] <- [[1,2,4],[4,7,6]] :put friends {fr, to => data}")
        .unwrap();
    db.run_default(r"?[fr, to] <- [[1,9],[4,5]] :rm friends {fr, to}")
        .unwrap();
    std::thread::sleep(Duration::from_secs_f64(0.01));
    while let Ok(d) = receiver.try_recv() {
        collected.push(d);
    }
    let collected = collected;
    assert_eq!(collected[0].0, CallbackOp::Put);
    assert_eq!(collected[0].1.rows.len(), 2);
    assert_eq!(collected[0].1.rows[0].len(), 3);
    assert_eq!(collected[0].2.rows.len(), 0);
    assert_eq!(collected[1].0, CallbackOp::Put);
    assert_eq!(collected[1].1.rows.len(), 2);
    assert_eq!(collected[1].1.rows[0].len(), 3);
    assert_eq!(collected[1].2.rows.len(), 1);
    assert_eq!(
        collected[1].2.rows[0],
        vec![DataValue::from(1), DataValue::from(2), DataValue::from(3)]
    );
    assert_eq!(collected[2].0, CallbackOp::Rm);
    assert_eq!(collected[2].1.rows.len(), 2);
    assert_eq!(collected[2].1.rows[0].len(), 2);
    assert_eq!(collected[2].2.rows.len(), 1);
    assert_eq!(collected[2].2.rows[0].len(), 3);
}

#[test]
fn test_update() {
    let db = DbInstance::default();
    db.run_default(":create friends {fr: Int, to: Int => a: Any, b: Any, c: Any}")
        .unwrap();
    db.run_default("?[fr, to, a, b, c] <- [[1,2,3,4,5]] :put friends {fr, to => a, b, c}")
        .unwrap();
    let res = db
        .run_default("?[fr, to, a, b, c] := *friends{fr, to, a, b, c}")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0], json!([1, 2, 3, 4, 5]));
    db.run_default("?[fr, to, b] <- [[1, 2, 100]] :update friends {fr, to => b}")
        .unwrap();
    let res = db
        .run_default("?[fr, to, a, b, c] := *friends{fr, to, a, b, c}")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0], json!([1, 2, 3, 100, 5]));
}

#[test]
fn test_index() {
    let db = DbInstance::default();
    db.run_default(":create friends {fr: Int, to: Int => data: Any}")
        .unwrap();

    db.run_default(r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to, data}")
        .unwrap();

    assert!(db
        .run_default("::index create friends:rev {to, no}")
        .is_err());
    db.run_default("::index create friends:rev {to, data}")
        .unwrap();

    db.run_default(r"?[fr, to, data] <- [[1,2,5],[6,5,7]] :put friends {fr, to => data}")
        .unwrap();
    db.run_default(r"?[fr, to] <- [[4,5]] :rm friends {fr, to}")
        .unwrap();

    let rels_data = db
        .export_relations(["friends", "friends:rev"].into_iter())
        .unwrap();
    assert_eq!(
        rels_data["friends"].clone().into_json()["rows"],
        json!([[1, 2, 5], [6, 5, 7]])
    );
    assert_eq!(
        rels_data["friends:rev"].clone().into_json()["rows"],
        json!([[2, 5, 1], [5, 7, 6]])
    );

    let rels = db.run_default("::relations").unwrap();
    assert_eq!(rels.rows[1][0], DataValue::from("friends:rev"));
    assert_eq!(rels.rows[1][1], DataValue::from(3));
    assert_eq!(rels.rows[1][2], DataValue::from("index"));

    let cols = db.run_default("::columns friends:rev").unwrap();
    assert_eq!(cols.rows.len(), 3);

    let res = db
        .run_default("?[fr, data] := *friends:rev{to: 2, fr, data}")
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));

    let res = db
        .run_default("?[fr, data] := *friends{to: 2, fr, data}")
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));

    let expl = db
        .run_default("::explain { ?[fr, data] := *friends{to: 2, fr, data} }")
        .unwrap();
    let joins = expl.into_json()["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap()[5].clone())
        .collect_vec();
    assert!(joins.contains(&json!(":friends:rev")));
    db.run_default("::index drop friends:rev").unwrap();
}

#[test]
fn test_json_objects() {
    let db = DbInstance::default();
    db.run_default("?[a] := a = {'a': 1}").unwrap();
    db.run_default(
        r"?[a] := a = {
            'a': 1
        }",
    )
    .unwrap();
}

#[test]
fn test_custom_rules() {
    let db = DbInstance::default();
    struct Custom;

    impl FixedRule for Custom {
        fn arity(
            &self,
            _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
            _rule_head: &[Symbol],
            _span: SourceSpan,
        ) -> miette::Result<usize> {
            Ok(1)
        }

        fn run(
            &self,
            payload: FixedRulePayload<'_, '_>,
            out: &'_ mut RegularTempStore,
            _poison: Poison,
        ) -> miette::Result<()> {
            let rel = payload.get_input(0)?;
            let mult = payload.integer_option("mult", Some(2))?;
            for maybe_row in rel.iter()? {
                let row = maybe_row?;
                let mut sum = 0;
                for col in row {
                    let d = col.get_int().unwrap_or(0);
                    sum += d;
                }
                sum *= mult;
                out.put(vec![DataValue::from(sum)])
            }
            Ok(())
        }
    }

    db.register_fixed_rule("SumCols".to_string(), Custom)
        .unwrap();
    let res = db
        .run_default(
            r#"
        rel[] <- [[1,2,3,4],[5,6,7,8]]
        ?[x] <~ SumCols(rel[], mult: 100)
    "#,
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1000], [2600]]));
}

#[test]
fn test_index_short() {
    let db = DbInstance::default();
    db.run_default(":create friends {fr: Int, to: Int => data: Any}")
        .unwrap();

    db.run_default(r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to => data}")
        .unwrap();

    db.run_default("::index create friends:rev {to}").unwrap();

    db.run_default(r"?[fr, to, data] <- [[1,2,5],[6,5,7]] :put friends {fr, to => data}")
        .unwrap();
    db.run_default(r"?[fr, to] <- [[4,5]] :rm friends {fr, to}")
        .unwrap();

    let rels_data = db
        .export_relations(["friends", "friends:rev"].into_iter())
        .unwrap();
    assert_eq!(
        rels_data["friends"].clone().into_json()["rows"],
        json!([[1, 2, 5], [6, 5, 7]])
    );
    assert_eq!(
        rels_data["friends:rev"].clone().into_json()["rows"],
        json!([[2, 1], [5, 6]])
    );

    let rels = db.run_default("::relations").unwrap();
    assert_eq!(rels.rows[1][0], DataValue::from("friends:rev"));
    assert_eq!(rels.rows[1][1], DataValue::from(2));
    assert_eq!(rels.rows[1][2], DataValue::from("index"));

    let cols = db.run_default("::columns friends:rev").unwrap();
    assert_eq!(cols.rows.len(), 2);

    let expl = db
        .run_default("::explain { ?[fr, data] := *friends{to: 2, fr, data} }")
        .unwrap()
        .into_json();

    for row in expl["rows"].as_array().unwrap() {
        println!("{}", row);
    }

    let joins = expl["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap()[5].clone())
        .collect_vec();
    assert!(joins.contains(&json!(":friends:rev")));

    let res = db
        .run_default("?[fr, data] := *friends{to: 2, fr, data}")
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));
}

#[test]
fn test_multi_tx() {
    let db = DbInstance::default();
    let tx = db.multi_transaction(true);
    tx.run_script(":create a {a}", Default::default()).unwrap();
    tx.run_script("?[a] <- [[1]] :put a {a}", Default::default())
        .unwrap();
    assert!(tx.run_script(":create a {a}", Default::default()).is_err());
    tx.run_script("?[a] <- [[2]] :put a {a}", Default::default())
        .unwrap();
    tx.run_script("?[a] <- [[3]] :put a {a}", Default::default())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.run_default("?[a] := *a[a]").unwrap().into_json()["rows"],
        json!([[1], [2], [3]])
    );

    let db = DbInstance::default();
    let tx = db.multi_transaction(true);
    tx.run_script(":create a {a}", Default::default()).unwrap();
    tx.run_script("?[a] <- [[1]] :put a {a}", Default::default())
        .unwrap();
    assert!(tx.run_script(":create a {a}", Default::default()).is_err());
    tx.run_script("?[a] <- [[2]] :put a {a}", Default::default())
        .unwrap();
    tx.run_script("?[a] <- [[3]] :put a {a}", Default::default())
        .unwrap();
    tx.abort().unwrap();
    assert!(db.run_default("?[a] := *a[a]").is_err());
}

#[test]
fn test_vec_types() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(":create a {k: String => v: <F32; 8>}")
        .unwrap();
    db.run_default("?[k, v] <- [['k', [1,2,3,4,5,6,7,8]]] :put a {k => v}")
        .unwrap();
    let res = db.run_default("?[k, v] := *a{k, v}").unwrap();
    assert_eq!(
        json!([1., 2., 3., 4., 5., 6., 7., 8.]),
        res.into_json()["rows"][0][1]
    );
    let res = db
        .run_default("?[v] <- [[vec([1,2,3,4,5,6,7,8])]]")
        .unwrap();
    assert_eq!(
        json!([1., 2., 3., 4., 5., 6., 7., 8.]),
        res.into_json()["rows"][0][0]
    );
    let res = db.run_default("?[v] <- [[rand_vec(5)]]").unwrap();
    assert_eq!(5, res.into_json()["rows"][0][0].as_array().unwrap().len());
    let res = db
        .run_default(r#"
            val[v] <- [[vec([1,2,3,4,5,6,7,8])]]
            ?[x,y,z] := val[v], x=l2_dist(v, v), y=cos_dist(v, v), nv = l2_normalize(v), z=ip_dist(nv, nv)
        "#)
        .unwrap();
    println!("{}", res.into_json());
}

#[test]
fn test_vec_index_insertion() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(
        r"
        ?[k, v, m] <- [['a', [1,2], true],
                       ['b', [2,3], false]]

        :create a {k: String => v: <F32; 2>, m: Bool}
    ",
    )
    .unwrap();
    db.run_default(
        r"
        ::hnsw create a:vec {
            dim: 2,
            m: 50,
            dtype: F32,
            fields: [v],
            distance: L2,
            ef_construction: 20,
            filter: m,
            #extend_candidates: true,
            #keep_pruned_connections: true,
        }",
    )
    .unwrap();
    let res = db
        .run_default("?[k] := *a:vec{layer: 0, fr_k, to_k}, k = fr_k or k = to_k")
        .unwrap();
    assert_eq!(res.rows.len(), 1);
    println!("update!");
    db.run_default(r#"?[k, m] <- [["a", false]] :update a {}"#)
        .unwrap();
    let res = db
        .run_default("?[k] := *a:vec{layer: 0, fr_k, to_k}, k = fr_k or k = to_k")
        .unwrap();
    assert_eq!(res.rows.len(), 0);
    println!("{}", res.into_json());
}

#[test]
fn test_vec_index() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(
        r"
        ?[k, v] <- [['a', [1,2]],
                    ['b', [2,3]],
                    ['bb', [2,3]],
                    ['c', [3,4]],
                    ['x', [0,0.1]],
                    ['a', [112,0]],
                    ['b', [1,1]]]

        :create a {k: String => v: <F32; 2>}
    ",
    )
    .unwrap();
    db.run_default(
        r"
        ::hnsw create a:vec {
            dim: 2,
            m: 50,
            dtype: F32,
            fields: [v],
            distance: L2,
            ef_construction: 20,
            filter: k != 'k1',
            #extend_candidates: true,
            #keep_pruned_connections: true,
        }",
    )
    .unwrap();
    db.run_default(
        r"
        ?[k, v] <- [
                    ['a2', [1,25]],
                    ['b2', [2,34]],
                    ['bb2', [2,33]],
                    ['c2', [2,32]],
                    ['a2', [2,31]],
                    ['b2', [1,10]]
                    ]
        :put a {k => v}
        ",
    )
    .unwrap();

    println!("all links");
    for (_, nrows) in db.export_relations(["a:vec"].iter()).unwrap() {
        let nrows = nrows.rows;
        for row in nrows {
            println!("{} {} -> {} {}", row[0], row[1], row[4], row[7]);
        }
    }

    let res = db
        .run_default(
            r"
        #::explain {
        ?[dist, k, v] := ~a:vec{k, v | query: q, k: 2, ef: 20, bind_distance: dist}, q = vec([200, 34])
        #}
        ",
        )
        .unwrap();
    println!("results");
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{} {} {}", row[0], row[1], row[2]);
    }
}

#[test]
fn test_fts_indexing() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {k: String => v: String}")
        .unwrap();
    db.run_default(
        r"?[k, v] <- [['a', 'hello world!'], ['b', 'the world is round']] :put a {k => v}",
    )
    .unwrap();
    db.run_default(
        r"::fts create a:fts {
            extractor: v,
            tokenizer: Simple,
            filters: [Lowercase, Stemmer('English'), Stopwords('en')]
        }",
    )
    .unwrap();
    db.run_default(
        r"?[k, v] <- [
            ['b', 'the world is square!'],
            ['c', 'see you at the end of the world!'],
            ['d', 'the world is the world and makes the world go around']
        ] :put a {k => v}",
    )
    .unwrap();
    let res = db
        .run_default(
            r"
        ?[word, src_k, offset_from, offset_to, position, total_length] :=
            *a:fts{word, src_k, offset_from, offset_to, position, total_length}
        ",
        )
        .unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
    println!("query");
    let res = db
        .run_default(r"?[k, v, s] := ~a:fts{k, v | query: 'world', k: 2, bind_score: s}")
        .unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
}

#[test]
fn test_lsh_indexing2() {
    for i in 1..10 {
        let f = i as f64 / 10.;
        let db = DbInstance::new("mem", "", "").unwrap();
        db.run_default(r":create a {k: String => v: String}")
            .unwrap();
        db.run_script(
            r"::lsh create a:lsh {extractor: v, tokenizer: NGram, n_gram: 3, target_threshold: $t }",
            BTreeMap::from([("t".into(), f.into())]),
            ScriptMutability::Mutable
        )
            .unwrap();
        db.run_default("?[k, v] <- [['a', 'ewiygfspeoighjsfcfxzdfncalsdf']] :put a {k => v}")
            .unwrap();
        let res = db
            .run_default("?[k] := ~a:lsh{k | query: 'ewiygfspeoighjsfcfxzdfncalsdf', k: 1}")
            .unwrap();
        assert!(res.rows.len() > 0);
    }
}

#[test]
fn test_lsh_indexing3() {
    for i in 1..10 {
        let f = i as f64 / 10.;
        let db = DbInstance::new("mem", "", "").unwrap();
        db.run_default(r":create text {id: String,  => text: String, url: String? default null, dt: Float default now(), dup_for: String? default null }")
            .unwrap();
        db.run_script(
            r"::lsh create text:lsh {
                    extractor: text,
                    # extract_filter: is_null(dup_for),
                    tokenizer: NGram,
                    n_perm: 200,
                    target_threshold: $t,
                    n_gram: 7,
                }",
            BTreeMap::from([("t".into(), f.into())]),
            ScriptMutability::Mutable,
        )
        .unwrap();
        db.run_default(
            "?[id, text] <- [['a', 'This function first generates 32 random bytes using the os.urandom function. It then base64 encodes these bytes using base64.urlsafe_b64encode, removes the padding, and decodes the result to a string.']] :put text {id, text}",
        )
        .unwrap();
        let res = db
            .run_default(
                r#"?[id, dup_for] :=
    ~text:lsh{id: id, dup_for: dup_for, | query: "This function first generates 32 random bytes using the os.urandom function. It then base64 encodes these bytes using base64.urlsafe_b64encode, removes the padding, and decodes the result to a string.", }"#,
            )
            .unwrap();
        assert!(res.rows.len() > 0);
        println!("{}", res.into_json());
    }
}

#[test]
fn filtering() {
    let db = DbInstance::default();
    let res = db
        .run_default(
            r"
        {
            ?[x, y] <- [[1, 2]]
            :create _rel {x => y}
            :returning
        }
        {
            ?[x, y] := x = 1, *_rel{x, y: 3}, y = 2
        }
    ",
        )
        .unwrap();
    assert_eq!(0, res.rows.len());

    let res = db
        .run_default(
            r"
        {
            ?[x, u, y] <- [[1, 0, 2]]
            :create _rel {x, u => y}
            :returning
        }
        {
            ?[x, y] := x = 1, *_rel{x, y: 3}, y = 2
        }
    ",
        )
        .unwrap();
    assert_eq!(0, res.rows.len());
}

#[test]
fn test_lsh_indexing4() {
    for i in 1..10 {
        let f = i as f64 / 10.;
        let db = DbInstance::new("mem", "", "").unwrap();
        db.run_default(r":create a {k: String => v: String}")
            .unwrap();
        db.run_script(
            r"::lsh create a:lsh {extractor: v, tokenizer: NGram, n_gram: 3, target_threshold: $t }",
            BTreeMap::from([("t".into(), f.into())]),
            ScriptMutability::Mutable
        )
            .unwrap();
        db.run_default("?[k, v] <- [['a', 'ewiygfspeoighjsfcfxzdfncalsdf']] :put a {k => v}")
            .unwrap();
        db.run_default("?[k] <- [['a']] :rm a {k}").unwrap();
        let res = db
            .run_default("?[k] := ~a:lsh{k | query: 'ewiygfspeoighjsfcfxzdfncalsdf', k: 1}")
            .unwrap();
        assert!(res.rows.len() == 0);
    }
}

#[test]
fn test_lsh_indexing() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {k: String => v: String}")
        .unwrap();
    db.run_default(
        r"?[k, v] <- [['a', 'hello world!'], ['b', 'the world is round']] :put a {k => v}",
    )
    .unwrap();
    db.run_default(
        r"::lsh create a:lsh {extractor: v, tokenizer: Simple, n_gram: 3, target_threshold: 0.3 }",
    )
    .unwrap();
    db.run_default(
        r"?[k, v] <- [
            ['b', 'the world is square!'],
            ['c', 'see you at the end of the world!'],
            ['d', 'the world is the world and makes the world go around'],
            ['e', 'the world is the world and makes the world not go around']
        ] :put a {k => v}",
    )
    .unwrap();
    let res = db.run_default("::columns a:lsh").unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
    let _res = db
        .run_default(
            r"
        ?[src_k, hash] :=
            *a:lsh{src_k, hash}
        ",
        )
        .unwrap();
    // for row in _res.into_json()["rows"].as_array().unwrap() {
    //     println!("{}", row);
    // }
    let _res = db
        .run_default(
            r"
        ?[k, minhash] :=
            *a:lsh:inv{k, minhash}
        ",
        )
        .unwrap();
    // for row in res.into_json()["rows"].as_array().unwrap() {
    //     println!("{}", row);
    // }
    let res = db
        .run_default(
            r"
            ?[k, v] := ~a:lsh{k, v |
                query: 'see him at the end of the world',
            }
            ",
        )
        .unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
    let res = db.run_default("::indices a").unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
    db.run_default(r"::lsh drop a:lsh").unwrap();
}

#[test]
fn test_insertions() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {k => v: <F32; 1536> default rand_vec(1536)}")
        .unwrap();
    db.run_default(r"?[k] <- [[1]] :put a {k}").unwrap();
    db.run_default(r"?[k, v] := *a{k, v}").unwrap();
    db.run_default(
        r"::hnsw create a:i {
            fields: [v], dim: 1536, ef: 16, filter: k % 3 == 0,
            m: 32
        }",
    )
    .unwrap();
    db.run_default(r"?[count(fr_k)] := *a:i{fr_k}").unwrap();
    db.run_default(r"?[k] <- [[1]] :put a {k}").unwrap();
    db.run_default(r"?[k] := k in int_range(300) :put a {k}")
        .unwrap();
    let res = db
        .run_default(
            r"?[dist, k] := ~a:i{k | query: v, bind_distance: dist, k:10, ef: 50, filter: k % 2 == 0, radius: 245}, *a{k: 96, v}",
        )
        .unwrap();
    println!("results");
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{} {}", row[0], row[1]);
    }
}

#[test]
fn tokenizers() {
    let tokenizers = TokenizerCache::default();
    let tokenizer = tokenizers
        .get(
            "simple",
            &TokenizerConfig {
                name: "Simple".into(),
                args: vec![],
            },
            &[],
        )
        .unwrap();

    // let tokenizer = TextAnalyzer::from(SimpleTokenizer)
    //     .filter(RemoveLongFilter::limit(40))
    //     .filter(LowerCaser)
    //     .filter(Stemmer::new(Language::English));
    let mut token_stream = tokenizer.token_stream("It is closer to Apache Lucene than to Elasticsearch or Apache Solr in the sense it is not an off-the-shelf search engine server, but rather a crate that can be used to build such a search engine.");
    while let Some(token) = token_stream.next() {
        println!("Token {:?}", token.text);
    }

    println!("XXXXXXXXXXXXX");

    let tokenizer = tokenizers
        .get(
            "cangjie",
            &TokenizerConfig {
                name: "Cangjie".into(),
                args: vec![],
            },
            &[],
        )
        .unwrap();

    let mut token_stream = tokenizer.token_stream("这个产品Finchat.io是一个相对比较有特色的文档问答类网站，它集成了750多家公司的经融数据。感觉是把财报等数据借助Embedding都向量化了，然后接入ChatGPT进行对话。");
    while let Some(token) = token_stream.next() {
        println!("Token {:?}", token.text);
    }
}

#[test]
fn multi_index_vec() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(
        r#"
        :create product {
            id
            =>
            name,
            description,
            price,
            name_vec: <F32; 1>,
            description_vec: <F32; 1>
        }
        "#,
    )
    .unwrap();
    db.run_default(
        r#"
        ::hnsw create product:semantic{
            fields: [name_vec, description_vec],
            dim: 1,
            ef: 16,
            m: 32,
        }
        "#,
    )
    .unwrap();
    db.run_default(
        r#"
        ?[id, name, description, price, name_vec, description_vec] <- [[1, "name", "description", 100, [1], [1]]]

        :put product {id => name, description, price, name_vec, description_vec}
        "#,
    ).unwrap();
    let res = db.run_default("::indices product").unwrap();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
}

#[test]
fn ensure_not() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(
        r"
    %ignore_error { :create id_alloc{id: Int => next_id: Int, last_id: Int}}
%ignore_error {
    ?[id, next_id, last_id] <- [[0, 1, 1000]];
    :ensure_not id_alloc{id => next_id, last_id}
}
    ",
    )
    .unwrap();
}

#[test]
fn insertion() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {x => y}").unwrap();
    assert!(db
        .run_default(r"?[x, y] <- [[1, 2]] :insert a {x => y}",)
        .is_ok());
    assert!(db
        .run_default(r"?[x, y] <- [[1, 3]] :insert a {x => y}",)
        .is_err());
}

#[test]
fn deletion() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {x => y}").unwrap();
    assert!(db.run_default(r"?[x] <- [[1]] :delete a {x}").is_err());
    assert!(db
        .run_default(r"?[x, y] <- [[1, 2]] :insert a {x => y}",)
        .is_ok());
    db.run_default(r"?[x] <- [[1]] :delete a {x}").unwrap();
}

#[test]
fn into_payload() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r":create a {x => y}").unwrap();
    db.run_default(r"?[x, y] <- [[1, 2], [3, 4]] :insert a {x => y}")
        .unwrap();

    let mut res = db.run_default(r"?[x, y] := *a[x, y]").unwrap();
    assert_eq!(res.rows.len(), 2);

    let delete = res.clone().into_payload("a", "rm");
    db.run_script(delete.0.as_str(), delete.1, ScriptMutability::Mutable)
        .unwrap();
    assert_eq!(
        db.run_default(r"?[x, y] := *a[x, y]").unwrap().rows.len(),
        0
    );

    db.run_default(r":create b {m => n}").unwrap();
    res.headers = vec!["m".into(), "n".into()];
    let put = res.into_payload("b", "put");
    db.run_script(put.0.as_str(), put.1, ScriptMutability::Mutable)
        .unwrap();
    assert_eq!(
        db.run_default(r"?[m, n] := *b[m, n]").unwrap().rows.len(),
        2
    );
}

#[test]
fn returning() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(":create a {x => y}").unwrap();
    let res = db
        .run_default(r"?[x, y] <- [[1, 2]] :insert a {x => y} ")
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([["OK"]]));
    // for row in res.into_json()["rows"].as_array().unwrap() {
    //     println!("{}", row);
    // }

    let res = db
        .run_default(r"?[x, y] <- [[1, 3], [2, 4]] :returning :put a {x => y} ")
        .unwrap();
    assert_eq!(
        res.into_json()["rows"],
        json!([["inserted", 1, 3], ["inserted", 2, 4], ["replaced", 1, 2]])
    );
    // println!("{:?}", res.headers);
    // for row in res.into_json()["rows"].as_array().unwrap() {
    //     println!("{}", row);
    // }

    let res = db
        .run_default(r"?[x] <- [[1], [4]] :returning :rm a {x} ")
        .unwrap();
    // println!("{:?}", res.headers);
    // for row in res.into_json()["rows"].as_array().unwrap() {
    //     println!("{}", row);
    // }
    assert_eq!(
        res.into_json()["rows"],
        json!([
            ["requested", 1, null],
            ["requested", 4, null],
            ["deleted", 1, 3]
        ])
    );
    db.run_default(r":create todo{id:Uuid default rand_uuid_v1() => label: String, done: Bool}")
        .unwrap();
    let res = db
        .run_default(r"?[label,done] <- [['milk',false]] :put todo{label,done} :returning")
        .unwrap();
    assert_eq!(res.rows[0].len(), 4);
    for title in res.headers.iter() {
        print!("{} ", title);
    }
    println!();
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
}

#[test]
fn parser_corner_case() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_default(r#"?[x] := x = 1 or x = 2"#).unwrap();
    db.run_default(r#"?[C] := C = 1  orx[C] := C = 1"#).unwrap();
    db.run_default(r#"?[C] := C = true, C  inx[C] := C = 1"#)
        .unwrap();
    db.run_default(r#"?[k] := k in int_range(300)"#).unwrap();
    db.run_default(r#"ywcc[a] <- [[1]] noto[A] := ywcc[A] ?[A] := noto[A]"#)
        .unwrap();
}

#[test]
fn as_store_in_imperative_script() {
    let db = DbInstance::new("mem", "", "").unwrap();
    let res = db
        .run_default(
            r#"
    { ?[x, y, z] <- [[1, 2, 3], [4, 5, 6]] } as _store
    { ?[x, y, z] := *_store{x, y, z} }
    "#,
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 2, 3], [4, 5, 6]]));
    let res = db
        .run_default(
            r#"
    {
        ?[y] <- [[1], [2], [3]]
        :create a {x default rand_uuid_v1() => y}
        :returning
    } as _last
    {
        ?[x] := *_last{_kind: 'inserted', x}
    }
    "#,
        )
        .unwrap();
    assert_eq!(3, res.rows.len());
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
    assert!(db
        .run_default(
            r#"
    {
        ?[x, x] := x = 1
    } as _last
    "#
        )
        .is_err());

    let res = db
        .run_default(
            r#"
    {
        x[y] <- [[1], [2], [3]]
        ?[sum(y)] := x[y]
    } as _last
    {
        ?[sum_y] := *_last{sum_y}
    }
    "#,
        )
        .unwrap();
    assert_eq!(1, res.rows.len());
    for row in res.into_json()["rows"].as_array().unwrap() {
        println!("{}", row);
    }
}

#[test]
fn update_shall_not_destroy_values() {
    let db = DbInstance::default();
    db.run_default(r"?[x, y] <- [[1, 2]] :create z {x => y default 0}")
        .unwrap();
    let r = db.run_default(r"?[x, y] := *z {x, y}").unwrap();
    assert_eq!(r.into_json()["rows"], json!([[1, 2]]));
    db.run_default(r"?[x] <- [[1]] :update z {x}").unwrap();
    let r = db.run_default(r"?[x, y] := *z {x, y}").unwrap();
    assert_eq!(r.into_json()["rows"], json!([[1, 2]]));
}

#[test]
fn update_shall_work() {
    let db = DbInstance::default();
    db.run_default(r"?[x, y, z] <- [[1, 2, 3]] :create z {x => y, z}")
        .unwrap();
    let r = db.run_default(r"?[x, y, z] := *z {x, y, z}").unwrap();
    assert_eq!(r.into_json()["rows"], json!([[1, 2, 3]]));
    db.run_default(r"?[x, y] <- [[1, 4]] :update z {x, y}")
        .unwrap();
    let r = db.run_default(r"?[x, y, z] := *z {x, y, z}").unwrap();
    assert_eq!(r.into_json()["rows"], json!([[1, 4, 3]]));
}

#[test]
fn sysop_in_imperatives() {
    let script = r#"
    {
            :create cm_src {
                aid: String =>
                title: String,
                author: String?,
                kind: String,
                url: String,
                domain: String?,
                pub_time: Float?,
                dt: Float default now(),
                weight: Float default 1,
            }
        }
        {
            :create cm_txt {
                tid: String =>
                aid: String,
                tag: String,
                follows_tid: String?,
                dup_for: String?,
                text: String,
                info_amount: Int,
            }
        }
        {
            :create cm_seg {
                sid: String =>
                tid: String,
                tag: String,
                part: Int,
                text: String,
                vec: <F32; 1536>,
            }
        }
        {
            ::hnsw create cm_seg:vec {
                dim: 1536,
                m: 50,
                dtype: F32,
                fields: vec,
                distance: Cosine,
                ef: 100,
            }
        }
        {
            ::lsh create cm_txt:lsh {
                extractor: text,
                extract_filter: is_null(dup_for),
                tokenizer: NGram,
                n_perm: 200,
                target_threshold: 0.5,
                n_gram: 7,
            }
        }
        {::relations}
    "#;
    let db = DbInstance::default();
    db.run_default(script).unwrap();
}

#[test]
fn bad_parse() {
    let db = DbInstance::default();
    db.run_default(
        r"
        :create named_hero_history {
        name: String,
        value: Bool,
        when: Int
    }",
    )
    .unwrap();
    db.run_default(r"
        last_named_hero[first, first, max(hist)] := *named_hero_history[first, first, value, hist], hist <= 1;

        some_named_hero[first, first, value] := last_named_hero[first, first, last], *named_hero_history[first, first, value, last];

        named_hero[first, first, value] := cast[first], value = false, not some_named_hero[first, first, _];
        named_hero[first, first, value] := some_named_hero[first, first, value];
        ?[hero] :=
    ").expect_err("should fail");
}

#[test]
fn puts() {
    let db = DbInstance::default();
    db.run_default(
        r"
            :create cm_txt {
                tid: String =>
                aid: String,
                tag: String,
                follows_tid: String? default null,
                for_qs: [String] default [],
                dup_for: String? default null,
                text: String,
                seg_vecs: [<F32; 1536>],
                seg_pos: [(Int, Int)],
                format: String default 'text',
                info_amount: Int,
            }
    ",
    )
    .unwrap();
    db.run_default(
        r"
        ?[tid, aid, tag, text, info_amount, dup_for, seg_vecs, seg_pos] := dup_for = null,
                tid = 'x', aid = 'y', tag = 'z', text = 'w', info_amount = 12,
                follows_tid = null, for_qs = [], format = 'x',
                seg_vecs = [], seg_pos = [[0, 10]]
        :put cm_txt {tid, aid, tag, text, info_amount, seg_vecs, seg_pos, dup_for}
    ",
    )
    .unwrap();
}

#[test]
fn short_hand() {
    let db = DbInstance::default();
    db.run_default(r":create x {x => y, z}").unwrap();
    db.run_default(r"?[x, y, z] <- [[1, 2, 3]] :put x {}")
        .unwrap();
    let r = db.run_default(r"?[x, y, z] := *x {x, y, z}").unwrap();
    assert_eq!(r.into_json()["rows"], json!([[1, 2, 3]]));
}

#[test]
fn param_shorthand() {
    let db = DbInstance::default();
    db.run_script(
        r"
        ?[] <- [[$x, $y, $z]]
        :create x {}
    ",
        BTreeMap::from([
            ("x".to_string(), DataValue::from(1)),
            ("y".to_string(), DataValue::from(2)),
            ("z".to_string(), DataValue::from(3)),
        ]),
        ScriptMutability::Mutable,
    )
    .unwrap();
    let res = db.run_default(r"?[x, y, z] := *x {x, y, z}");
    assert_eq!(res.unwrap().into_json()["rows"], json!([[1, 2, 3]]));
}

#[test]
fn crashy_imperative() {
    let db = DbInstance::default();
    db.run_default(
        r"
        {:create _test {a}}

        %loop
            %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z >= 10 }
                %then %return _test
            %end
            { ?[a] := a = rand_uuid_v1(); :put _test {a} }
        %end
        ",
    )
    .unwrap();
}

#[test]
fn hnsw_index() {
    let db = DbInstance::default();
    db.run_default(
        r#"
        :create beliefs {
            belief_id: Uuid,
            character_id: Uuid,
            belief: String,
            last_accessed_at: Validity default [floor(now()), true],
            =>
            details: String default "",
            parent_belief_id: Uuid? default null,
            valence: Float default 0,
            aspects: [(String, Float, String, String)] default [],
            belief_embedding: <F32; 768>,
            details_embedding: <F32; 768>,
        }
        "#,
    )
    .unwrap();
    db.run_default(
        r#"
        ::hnsw create beliefs:embedding_space {
            dim: 768,
            m: 50,
            dtype: F32,
            fields: [belief_embedding, details_embedding],
            distance: Cosine,
            ef_construction: 20,
            extend_candidates: false,
            keep_pruned_connections: false,
        }
    "#,
    )
    .unwrap();
    db.run_default(r#"
        ?[belief_id, character_id, belief, belief_embedding, details_embedding] <- [[rand_uuid_v1(), rand_uuid_v1(), "test", rand_vec(768), rand_vec(768)]]
        :put beliefs {}
    "#).unwrap();
    let res = db.run_default(r#"
            ?[belief, valence, dist, character_id, vector] := ~beliefs:embedding_space{ belief, valence, character_id |
                query: rand_vec(768),
                k: 100,
                ef: 20,
                radius: 1.0,
                bind_distance: dist,
                bind_vector: vector
            }

            :order -valence
            :order dist
    "#).unwrap();
    println!("{}", res.into_json()["rows"][0][4]);
}

#[test]
fn fts_drop() {
    let db = DbInstance::default();
    db.run_default(
        r#"
            :create entity {name}
        "#,
    )
    .unwrap();
    db.run_default(
        r#"
        ::fts create entity:fts_index { extractor: name,
            tokenizer: Simple, filters: [Lowercase]
        }
    "#,
    )
    .unwrap();
    db.run_default(
        r#"
        ::fts drop entity:fts_index
    "#,
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// commit_now() — cozo-managed commit-time timestamp column default.
//
// Slice 1 of the archive design. Each script invocation captures cur_vld once
// (at the top of run_script) and threads it through extract_data. A column
// declared with `default commit_now()` should resolve to that value at write
// time, identically for every row in the script, monotonically advancing
// across scripts.
// ---------------------------------------------------------------------------

fn commit_now_get_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .unwrap_or_else(|| panic!("expected integer ts, got {v:?}"))
}

#[test]
fn commit_now_basic_default() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
    let res = db
        .run_default("?[id, ts] := *r{id, ts}")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"].as_array().unwrap().len(), 1);
    assert_eq!(res["rows"][0][0], json!(1));
    let ts = commit_now_get_int(&res["rows"][0][1]);
    assert!(ts > 0, "commit_now should produce a positive ts, got {ts}");
}

#[test]
fn commit_now_uniform_within_script() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    // 50 rows in a single put — they must all share one ts because cur_vld
    // is captured once per script.
    let rows: Vec<String> = (0..50).map(|i| format!("[{i}]")).collect();
    let script = format!("?[id] <- [{}] :put r {{id}}", rows.join(","));
    db.run_default(&script).unwrap();
    // Project (id, ts) — datalog returns set semantics, so projecting only ts
    // would collapse 50 identical timestamps to 1 row and tell us nothing.
    let res = db
        .run_default("?[id, ts] := *r{id, ts}")
        .unwrap()
        .into_json();
    let rows = res["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    let first = commit_now_get_int(&rows[0][1]);
    for r in rows.iter() {
        let ts = commit_now_get_int(&r[1]);
        assert_eq!(
            ts, first,
            "all rows in a single script must share one ts; got {ts} vs {first} (id={})",
            r[0]
        );
    }
}

#[test]
fn commit_now_monotonic_across_scripts() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
    // Sleep a millisecond so the second script's microsecond timestamp is
    // unambiguously later than the first's.
    std::thread::sleep(Duration::from_millis(2));
    db.run_default(r#"?[id] <- [[2]] :put r {id}"#).unwrap();
    let res = db
        .run_default("?[id, ts] := *r{id, ts}")
        .unwrap()
        .into_json();
    let rows = res["rows"].as_array().unwrap();
    let mut by_id: BTreeMap<i64, i64> = BTreeMap::new();
    for r in rows {
        by_id.insert(r[0].as_i64().unwrap(), commit_now_get_int(&r[1]));
    }
    let t1 = by_id[&1];
    let t2 = by_id[&2];
    assert!(
        t2 > t1,
        "second script should have a strictly later ts; got t1={t1} t2={t2}"
    );
}

#[test]
fn commit_now_int_type_is_int() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
    let res = db
        .run_default("?[ts] := *r{ts}")
        .unwrap()
        .into_json();
    let v = &res["rows"][0][0];
    assert!(
        v.is_i64(),
        "ts must serialize as an integer, got {v:?}"
    );
}

#[test]
fn commit_now_user_value_overrides_default() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    // User explicitly supplies ts — default must NOT fire.
    db.run_default(r#"?[id, ts] <- [[1, 999]] :put r {id => ts}"#)
        .unwrap();
    let res = db
        .run_default("?[id, ts] := *r{id, ts}")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0][1], json!(999));
}

#[test]
fn commit_now_on_update_recomputes() {
    let db = DbInstance::default();
    db.run_default(
        r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
    )
    .unwrap();
    db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#)
        .unwrap();
    let res = db
        .run_default("?[ts] := *r{id: 1, ts}")
        .unwrap()
        .into_json();
    let t1 = commit_now_get_int(&res["rows"][0][0]);

    std::thread::sleep(Duration::from_millis(2));

    // Partial update — change `name` only; do NOT bind `ts`. Without our
    // make_update_extractors change, ts would be preserved (latent footgun
    // for the archive watermark). With the change, ts is bumped.
    db.run_default(r#"?[id, name] <- [[1, 'b']] :update r {id => name}"#)
        .unwrap();
    let res = db
        .run_default("?[name, ts] := *r{id: 1, name, ts}")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0][0], json!("b"));
    let t2 = commit_now_get_int(&res["rows"][0][1]);
    assert!(
        t2 > t1,
        "update must bump commit_now ts; got t1={t1} t2={t2}"
    );
}

#[test]
fn commit_now_on_rm_works() {
    let db = DbInstance::default();
    db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    db.run_default(r#"?[id] <- [[1], [2]] :put r {id}"#).unwrap();
    db.run_default(r#"?[id] <- [[1]] :rm r {id}"#).unwrap();
    let res = db
        .run_default("?[id] := *r{id}")
        .unwrap()
        .into_json();
    let rows = res["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], json!(2));
}

#[test]
fn commit_now_outside_default_errors() {
    let db = DbInstance::default();
    // Direct call from a query body must fail loudly, not return a stale value.
    // The Display impl shows only the top-level wrap; walk the cause chain
    // (via Debug) to find op_commit_now's bail message.
    let err = db.run_default("?[x] := x = commit_now()").unwrap_err();
    let err_chain = format!("{err:?}");
    assert!(
        err_chain.contains("commit_now"),
        "error chain should mention commit_now; got: {err_chain}"
    );
}

#[test]
fn commit_now_with_args_errors() {
    let db = DbInstance::default();
    // commit_now is arity 0; supplying an arg must fail (parse- or eval-time).
    let res = db.run_default(
        r#":create r {id: Int => ts: Int default commit_now(1)}"#,
    );
    let err = match res {
        Err(e) => e.to_string(),
        Ok(_) => {
            // Schema may have parsed; the failure surfaces on first put when
            // the default is evaluated.
            db.run_default(r#"?[id] <- [[1]] :put r {id}"#)
                .unwrap_err()
                .to_string()
        }
    };
    assert!(
        err.to_lowercase().contains("arity") || err.contains("commit_now"),
        "error should be about arity or commit_now misuse; got: {err}"
    );
}

#[test]
fn commit_now_incompatible_type_errors() {
    let db = DbInstance::default();
    db.run_default(
        r#":create r {id: Int => ts: String default commit_now()}"#,
    )
    .unwrap();
    let err = db
        .run_default(r#"?[id] <- [[1]] :put r {id}"#)
        .unwrap_err();
    let err_chain = format!("{err:?}").to_lowercase();
    // The Int micros value cannot coerce into String — surface a coercion
    // error rather than silently truncating.
    assert!(
        err_chain.contains("coerc")
            || err_chain.contains("type")
            || err_chain.contains("string"),
        "error should be about type/coercion; got: {err_chain}"
    );
}

#[test]
fn commit_now_with_other_defaults_coexist() {
    let db = DbInstance::default();
    // Mix commit_now() with the existing now() and rand_uuid_v1() defaults
    // to confirm the new extractor variant doesn't interfere with the old
    // DefaultExtractor path for sibling columns.
    db.run_default(
        r#":create r {
            id: Int =>
            uid: Uuid default rand_uuid_v1(),
            wall: Float default now(),
            commit: Int default commit_now()
        }"#,
    )
    .unwrap();
    db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
    let res = db
        .run_default("?[uid, wall, commit] := *r{uid, wall, commit}")
        .unwrap()
        .into_json();
    let row = &res["rows"][0];
    assert!(row[0].is_string(), "uuid serializes as string, got {:?}", row[0]);
    assert!(row[1].is_f64(), "now() is a Float, got {:?}", row[1]);
    assert!(row[2].is_i64(), "commit_now() is an Int, got {:?}", row[2]);
}

#[test]
fn commit_now_shared_across_relations_in_one_script() {
    let db = DbInstance::default();
    db.run_default(r#":create a {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    db.run_default(r#":create b {id: Int => ts: Int default commit_now()}"#)
        .unwrap();
    // A single imperative script writes to two relations. The per-relation
    // commit clocks are each seeded from the same script wall-clock instant, so
    // on their first write both observe the same ts. (After divergent history
    // the two relations' clocks may differ — monotonicity is per-relation.)
    db.run_default(
        r#"
        {?[id] <- [[1]] :put a {id}}
        {?[id] <- [[1]] :put b {id}}
        "#,
    )
    .unwrap();
    let ta = commit_now_get_int(
        &db.run_default("?[ts] := *a{ts}").unwrap().into_json()["rows"][0][0],
    );
    let tb = commit_now_get_int(
        &db.run_default("?[ts] := *b{ts}").unwrap().into_json()["rows"][0][0],
    );
    assert_eq!(
        ta, tb,
        "two relations written in one script must share commit_now ts; got a={ta} b={tb}"
    );
}

// ---------------------------------------------------------------------------
// ::import_parquet — generic Parquet -> relation importer (slice 2).
//
// Tests use arrow::ArrowWriter to create temp Parquet files in tempdirs, then
// run `::import_parquet rel from 'path'` and verify the relation contains the
// expected rows. The `archive` feature must be enabled for the sys op handler
// to do any real work.
// ---------------------------------------------------------------------------

#[cfg(feature = "archive")]
mod import_parquet_tests {
    use super::*;

    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    /// Build a Parquet file at the given path from the supplied (name, type,
    /// array) tuples. Returns the path back for convenience in test bodies.
    fn write_parquet(path: &PathBuf, columns: Vec<(&str, DataType, ArrayRef)>) {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(n, t, _)| Field::new(*n, t.clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, _, a)| a).collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn import_parquet_basic_round_trip() {
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String}"#).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![
                (
                    "id",
                    DataType::Int64,
                    Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                ),
                (
                    "name",
                    DataType::Utf8,
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ),
            ],
        );

        let res = db
            .run_default(&format!(
                "::import_parquet r from '{}'",
                path.to_str().unwrap()
            ))
            .unwrap()
            .into_json();
        // Status row reports the count.
        assert_eq!(res["rows"][0][1], json!(3));

        let res = db
            .run_default("?[id, name] := *r{id, name}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], json!([1, "a"]));
        assert_eq!(rows[2], json!([3, "c"]));
    }

    #[test]
    fn import_parquet_status_row_counts_imported_rows() {
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int}"#).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from((0..50).map(|i| i as i64).collect::<Vec<_>>())),
            )],
        );
        let res = db
            .run_default(&format!(
                "::import_parquet r from '{}'",
                path.to_str().unwrap()
            ))
            .unwrap()
            .into_json();
        // Header[0] is "status", Header[1] is "rows".
        assert_eq!(res["headers"], json!(["status", "rows"]));
        assert_eq!(res["rows"][0][0], json!("OK"));
        assert_eq!(res["rows"][0][1], json!(50));
    }

    #[test]
    fn import_parquet_with_extra_columns_in_file_ignores_them() {
        // Parquet has more columns than the relation; extras should be silently
        // ignored as long as all required columns are present.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String}"#).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![
                (
                    "id",
                    DataType::Int64,
                    Arc::new(Int64Array::from(vec![1i64])),
                ),
                (
                    "name",
                    DataType::Utf8,
                    Arc::new(StringArray::from(vec!["alice"])),
                ),
                (
                    "extra",
                    DataType::Float64,
                    Arc::new(Float64Array::from(vec![3.14])),
                ),
            ],
        );
        db.run_default(&format!(
            "::import_parquet r from '{}'",
            path.to_str().unwrap()
        ))
        .unwrap();
        let res = db
            .run_default("?[id, name] := *r{id, name}")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0], json!([1, "alice"]));
    }

    #[test]
    fn import_parquet_missing_required_column_errors() {
        // Relation declares `name` with no default; Parquet file omits it.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String}"#).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1i64])),
            )],
        );
        let err = db
            .run_default(&format!(
                "::import_parquet r from '{}'",
                path.to_str().unwrap()
            ))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("name") || err.contains("missing"),
            "error should name the missing column; got: {err}"
        );
    }

    #[test]
    fn import_parquet_missing_column_with_default_uses_default() {
        // Relation column `name` has a default; Parquet omits it. The default
        // should fire and the import should succeed.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String default 'unknown'}"#)
            .unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1i64, 2])),
            )],
        );
        db.run_default(&format!(
            "::import_parquet r from '{}'",
            path.to_str().unwrap()
        ))
        .unwrap();
        let res = db
            .run_default("?[id, name] := *r{id, name}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows[0][1], json!("unknown"));
        assert_eq!(rows[1][1], json!("unknown"));
    }

    #[test]
    fn import_parquet_with_commit_now_default_for_missing_column() {
        // The slice 1 + slice 2 integration: relation has `ts default
        // commit_now()`; Parquet doesn't include `ts`; the import should
        // populate `ts` from the script's commit-time stamp.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => ts: Int default commit_now()}"#)
            .unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1i64, 2])),
            )],
        );
        db.run_default(&format!(
            "::import_parquet r from '{}'",
            path.to_str().unwrap()
        ))
        .unwrap();
        let res = db
            .run_default("?[id, ts] := *r{id, ts}")
            .unwrap()
            .into_json();
        let rows = res["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        // Both rows should have the same (positive) ts.
        let t0 = rows[0][1].as_i64().unwrap();
        let t1 = rows[1][1].as_i64().unwrap();
        assert!(t0 > 0, "commit_now ts must be positive");
        assert_eq!(t0, t1, "all rows in one import script must share ts");
    }

    #[test]
    fn import_parquet_overwrites_existing_keys() {
        // Existing put-semantics: Parquet rows whose keys collide with existing
        // rows should overwrite.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String}"#).unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'old'], [2, 'keep']] :put r {id => name}"#)
            .unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![
                (
                    "id",
                    DataType::Int64,
                    Arc::new(Int64Array::from(vec![1i64, 3])),
                ),
                (
                    "name",
                    DataType::Utf8,
                    Arc::new(StringArray::from(vec!["new", "added"])),
                ),
            ],
        );
        db.run_default(&format!(
            "::import_parquet r from '{}'",
            path.to_str().unwrap()
        ))
        .unwrap();

        let res = db
            .run_default("?[id, name] := *r{id, name}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], json!([1, "new"]));
        assert_eq!(rows[1], json!([2, "keep"]));
        assert_eq!(rows[2], json!([3, "added"]));
    }

    #[test]
    fn import_parquet_into_missing_relation_errors() {
        let db = DbInstance::default();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1i64])),
            )],
        );
        let err = db
            .run_default(&format!(
                "::import_parquet nope from '{}'",
                path.to_str().unwrap()
            ))
            .unwrap_err()
            .to_string();
        assert!(
            err.to_lowercase().contains("relation"),
            "error should mention the missing relation; got: {err}"
        );
    }

    #[test]
    fn import_parquet_routes_s3_uri_to_object_store() {
        // s3:// is now wired to the object store (restore path). With no real
        // bucket/credentials in a unit test it must fail trying to *fetch* the
        // object, NOT reject the scheme as unsupported.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int}"#).unwrap();
        let err = db
            .run_default("::import_parquet r from 's3://bucket/key.parquet'")
            .unwrap_err()
            .to_string();
        let lower = err.to_lowercase();
        assert!(
            !lower.contains("not supported"),
            "s3:// should no longer be rejected as an unsupported scheme; got: {err}"
        );
        assert!(
            lower.contains("objectstore")
                || lower.contains("get")
                || lower.contains("s3")
                || lower.contains("bucket")
                || lower.contains("credential")
                || lower.contains("region"),
            "error should indicate an S3 fetch failure; got: {err}"
        );
    }

    #[test]
    fn import_parquet_rejects_index_target() {
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int}"#).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![(
                "id",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1i64])),
            )],
        );
        let err = db
            .run_default(&format!(
                "::import_parquet r:idx from '{}'",
                path.to_str().unwrap()
            ))
            .unwrap_err()
            .to_string();
        // Either the parser rejects the `:idx` form (compound_ident doesn't
        // accept colons) or the runtime check fires. Both are acceptable.
        assert!(
            err.contains("index")
                || err.contains(":")
                || err.to_lowercase().contains("parser"),
            "should refuse to import into an index name; got: {err}"
        );
    }

    #[test]
    fn import_parquet_with_secondary_index_keeps_index_consistent() {
        // Create a relation with a secondary index, import, then verify the
        // index actually has the imported rows by querying through it.
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String}"#).unwrap();
        db.run_default("::index create r:by_name {name}").unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("r.parquet");
        write_parquet(
            &path,
            vec![
                (
                    "id",
                    DataType::Int64,
                    Arc::new(Int64Array::from(vec![1i64, 2])),
                ),
                (
                    "name",
                    DataType::Utf8,
                    Arc::new(StringArray::from(vec!["alice", "bob"])),
                ),
            ],
        );
        db.run_default(&format!(
            "::import_parquet r from '{}'",
            path.to_str().unwrap()
        ))
        .unwrap();

        // Index probe: look up by name.
        let res = db
            .run_default(r#"?[id] := *r:by_name{name: 'alice', id}"#)
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][0], json!(1));
    }
}

// ---------------------------------------------------------------------------
// ::archive — guarded delete with a watermark gate (slice 3).
//
// Slice 3 ships:
//   ::archive_config put '<rel>' '<ts_col>'
//   ::archive_config get [ '<rel>' ]
//   ::archive_config remove '<rel>'
//   ::archive_advance_watermark '<rel>' <ts>
//   ::archive <rel> { <query> }
//
// The replicator (slice 4) doesn't exist yet, so tests advance the watermark
// manually via ::archive_advance_watermark.
// ---------------------------------------------------------------------------

#[cfg(feature = "archive")]
mod archive_tests {
    use super::*;

    fn fresh_db_with_rel() -> DbInstance {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db
    }

    fn ts_of(db: &DbInstance, id: i64) -> i64 {
        let res = db
            .run_default(&format!("?[ts] := *r{{id: {id}, ts}}"))
            .unwrap()
            .into_json();
        res["rows"][0][0].as_i64().unwrap()
    }

    #[test]
    fn archive_config_put_then_get() {
        let db = fresh_db_with_rel();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let res = db.run_default("::archive_config get").unwrap().into_json();
        assert_eq!(
            res["headers"],
            json!([
                "relation",
                "timestamp_column",
                "staging_dir",
                "encryption",
                "kms_key_arn",
                "max_rows_per_segment"
            ])
        );
        // Optional fields default to null.
        assert_eq!(res["rows"][0][0], json!("r"));
        assert_eq!(res["rows"][0][1], json!("ts"));
        assert!(res["rows"][0][2].is_null());
        assert!(res["rows"][0][3].is_null());
        assert!(res["rows"][0][4].is_null());
        assert!(res["rows"][0][5].is_null());
    }

    #[test]
    fn archive_config_put_with_staging_dir() {
        let db = fresh_db_with_rel();
        db.run_default("::archive_config put 'r' 'ts' '/tmp/cozo-archive'")
            .unwrap();
        let res = db
            .run_default("::archive_config get 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][2], json!("/tmp/cozo-archive"));
    }

    #[test]
    fn archive_config_get_filtered() {
        let db = fresh_db_with_rel();
        db.run_default(r#":create s {id: Int => ts: Int default commit_now()}"#)
            .unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        db.run_default("::archive_config put 's' 'ts'").unwrap();
        let res = db
            .run_default("::archive_config get 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 1);
        assert_eq!(res["rows"][0][0], json!("r"));
    }

    #[test]
    fn archive_config_put_unknown_relation_errors() {
        let db = DbInstance::default();
        let err = db
            .run_default("::archive_config put 'nope' 'ts'")
            .unwrap_err()
            .to_string();
        assert!(err.to_lowercase().contains("relation"), "got: {err}");
    }

    #[test]
    fn archive_config_put_unknown_column_errors() {
        let db = fresh_db_with_rel();
        let err = db
            .run_default("::archive_config put 'r' 'nope'")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("nope") || err.to_lowercase().contains("column"),
            "got: {err}"
        );
    }

    #[test]
    fn archive_config_put_non_int_column_errors() {
        let db = fresh_db_with_rel();
        // `name` is String — not a valid timestamp column.
        let err = db
            .run_default("::archive_config put 'r' 'name'")
            .unwrap_err()
            .to_string();
        assert!(
            err.to_lowercase().contains("int") || err.contains("name"),
            "got: {err}"
        );
    }

    #[test]
    fn archive_config_remove_clears_config_and_watermark() {
        let db = fresh_db_with_rel();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        db.run_default("::archive_advance_watermark 'r' 1000").unwrap();
        db.run_default("::archive_config remove 'r'").unwrap();

        let res = db.run_default("::archive_config get").unwrap().into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 0);

        // Watermark gone too — re-advancing without re-config should error.
        let err = db
            .run_default("::archive_advance_watermark 'r' 2000")
            .unwrap_err()
            .to_string();
        assert!(err.to_lowercase().contains("not configured"), "got: {err}");
    }

    #[test]
    fn archive_requires_config() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let err = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap_err()
            .to_string();
        assert!(
            err.to_lowercase().contains("not configured"),
            "should require config first; got: {err}"
        );
    }

    #[test]
    fn archive_requires_watermark() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let err = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("watermark"),
            "should require watermark; got: {err}"
        );
    }

    #[test]
    fn archive_below_watermark_succeeds() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();
        let t = ts_of(&db, 1);
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        // Advance watermark past the rows' timestamps.
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();
        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(2), "archived count");
        assert_eq!(res["rows"][0][2], json!(0), "skipped count");
        assert_eq!(res["rows"][0][3], json!(0), "missing count");

        let remaining = db
            .run_default("?[id] := *r{id}")
            .unwrap()
            .into_json();
        assert_eq!(remaining["rows"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn archive_above_watermark_skips() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#)
            .unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        // Watermark stuck in the past — every row is too new.
        db.run_default("::archive_advance_watermark 'r' 1").unwrap();

        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(0), "archived count");
        assert_eq!(res["rows"][0][2], json!(1), "skipped count");

        // Row still present.
        let remaining = db
            .run_default("?[id] := *r{id}")
            .unwrap()
            .into_json();
        assert_eq!(remaining["rows"][0][0], json!(1));
    }

    #[test]
    fn archive_partial_below_watermark() {
        // Insert two rows with different timestamps. Advance watermark between
        // them. Verify archive picks the earlier one only.
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let t1 = ts_of(&db, 1);
        std::thread::sleep(Duration::from_millis(2));
        db.run_default(r#"?[id, name] <- [[2, 'b']] :put r {id => name}"#).unwrap();
        let t2 = ts_of(&db, 2);
        assert!(t2 > t1);

        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        // Watermark exactly at t1: row 1 (ts == watermark) is eligible, row 2
        // (ts > watermark) is not.
        db.run_default(&format!("::archive_advance_watermark 'r' {t1}"))
            .unwrap();

        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(1), "archived");
        assert_eq!(res["rows"][0][2], json!(1), "skipped");

        let remaining = db
            .run_default("?[id] := *r{id}")
            .unwrap()
            .into_json();
        let ids: Vec<i64> = remaining["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r[0].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn archive_with_predicate_in_query() {
        // The query body is full datalog — let's use it to constrain candidates
        // (e.g., only archive rows with name starting with 'a'). The watermark
        // gate composes on top of that.
        let db = fresh_db_with_rel();
        db.run_default(
            r#"?[id, name] <- [[1, 'alpha'], [2, 'beta'], [3, 'apex']] :put r {id => name}"#,
        )
        .unwrap();
        let t = ts_of(&db, 1);

        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();

        // Pick only ids whose name starts with 'a'.
        let res = db
            .run_default(
                r#"::archive r { ?[id] := *r{id, name}, starts_with(name, 'a') }"#,
            )
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(2), "archived count");

        let remaining = db
            .run_default("?[id, name] := *r{id, name}")
            .unwrap()
            .into_json();
        let rows = remaining["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], json!("beta"));
    }

    #[test]
    fn archive_query_missing_key_column_errors() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let t = ts_of(&db, 1);
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();
        // Query body produces only `name`, not `id` (the key).
        let err = db
            .run_default("::archive r { ?[name] := *r{name} }")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("id") || err.contains("key"),
            "should mention the missing key column; got: {err}"
        );
    }

    #[test]
    fn archive_missing_keys_counted_as_missing() {
        // Query produces a key that doesn't exist in the relation.
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let t = ts_of(&db, 1);
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();
        // Synthesize a key list that includes a non-existent id (99).
        let res = db
            .run_default("::archive r { ?[id] := id in [1, 99] }")
            .unwrap()
            .into_json();
        // 1 archived (id 1, exists, below watermark), 0 skipped, 1 missing (id 99).
        assert_eq!(res["rows"][0][1], json!(1), "archived");
        assert_eq!(res["rows"][0][2], json!(0), "skipped");
        assert_eq!(res["rows"][0][3], json!(1), "missing");
    }

    #[test]
    fn archive_keeps_secondary_index_consistent() {
        let db = fresh_db_with_rel();
        db.run_default("::index create r:by_name {name}").unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let t = ts_of(&db, 1);
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();
        // Archive only id=1.
        db.run_default("::archive r { ?[id] := id = 1 }").unwrap();

        // Index lookup for the archived row should return nothing; for the
        // surviving row, it should still find the id.
        let lookup_a = db
            .run_default(r#"?[id] := *r:by_name{name: 'a', id}"#)
            .unwrap()
            .into_json();
        assert_eq!(lookup_a["rows"].as_array().unwrap().len(), 0);

        let lookup_b = db
            .run_default(r#"?[id] := *r:by_name{name: 'b', id}"#)
            .unwrap()
            .into_json();
        assert_eq!(lookup_b["rows"][0][0], json!(2));
    }

    #[test]
    fn archive_multiple_rounds_advance_watermark() {
        // Round 1: watermark at T1, only the first row (ts <= T1) is archivable.
        // Round 2: watermark advanced to T2, second row becomes archivable.
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let t1 = ts_of(&db, 1);
        std::thread::sleep(Duration::from_millis(2));
        db.run_default(r#"?[id, name] <- [[2, 'b']] :put r {id => name}"#).unwrap();
        let t2 = ts_of(&db, 2);

        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        db.run_default(&format!("::archive_advance_watermark 'r' {t1}")).unwrap();

        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(1), "round 1 archived");
        assert_eq!(res["rows"][0][2], json!(1), "round 1 skipped");

        // Advance and re-archive.
        db.run_default(&format!("::archive_advance_watermark 'r' {t2}")).unwrap();
        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(1), "round 2 archived");
        assert_eq!(res["rows"][0][2], json!(0), "round 2 skipped");

        let remaining = db
            .run_default("?[id] := *r{id}")
            .unwrap()
            .into_json();
        assert_eq!(remaining["rows"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn archive_status_row_shape() {
        let db = fresh_db_with_rel();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        let t = ts_of(&db, 1);
        db.run_default(&format!("::archive_advance_watermark 'r' {}", t + 1))
            .unwrap();
        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(
            res["headers"],
            json!(["status", "archived", "skipped", "missing", "watermark"])
        );
        assert_eq!(res["rows"][0][0], json!("OK"));
    }
}

// ---------------------------------------------------------------------------
// ::replicate_pending — manual-drain replicator with local fs target (slice 4).
//
// Polling model: each call scans the relation, finds rows whose timestamp is
// past the current watermark, writes one Parquet segment, records a manifest
// row, and advances the watermark. Idempotent: a second call with no new rows
// produces no segment.
// ---------------------------------------------------------------------------

#[cfg(feature = "archive")]
mod replicate_tests {
    use super::*;
    use tempfile::tempdir;

    fn ts_of(db: &DbInstance, id: i64) -> i64 {
        db.run_default(&format!("?[ts] := *r{{id: {id}, ts}}"))
            .unwrap()
            .into_json()["rows"][0][0]
            .as_i64()
            .unwrap()
    }

    /// Boilerplate: relation + archive config with a fresh tempdir staging dir.
    fn setup() -> (DbInstance, tempfile::TempDir, String) {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap().to_string();
        db.run_default(&format!(
            "::archive_config put 'r' 'ts' '{dir_str}'"
        ))
        .unwrap();
        (db, dir, dir_str)
    }

    #[test]
    fn replicate_empty_relation_is_noop() {
        let (db, _dir, _) = setup();
        let res = db
            .run_default("::replicate_pending 'r'")
            .unwrap()
            .into_json();
        // Slice 6 shape: [status, rows_replicated, segments_written,
        //                 old_watermark, new_watermark]
        assert_eq!(
            res["headers"],
            json!([
                "status",
                "rows_replicated",
                "segments_written",
                "old_watermark",
                "new_watermark"
            ])
        );
        assert_eq!(res["rows"][0][1], json!(0), "rows_replicated");
        assert_eq!(res["rows"][0][2], json!(0), "segments_written");
        // Watermark unchanged (initial value is i64::MIN reported as old & new).
        assert_eq!(res["rows"][0][3], res["rows"][0][4]);
    }

    #[test]
    fn replicate_writes_segment_and_advances_watermark() {
        let (db, dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();
        let t = ts_of(&db, 1); // both rows in one script -> same ts

        let res = db
            .run_default("::replicate_pending 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(2), "two rows replicated");
        assert_eq!(res["rows"][0][2], json!(1), "single segment under default cap");
        assert_eq!(res["rows"][0][4], json!(t), "watermark advanced to t");

        // Confirm watermark is persisted.
        let res = db
            .run_default(
                "?[ts] := *cozo_archive_watermark{relation: 'r', last_safe_commit_ts: ts}",
            )
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][0], json!(t));

        // Manifest row carries the per-segment details that the sys op no
        // longer returns inline.
        let res = db
            .run_default(
                "?[rel, count, lower, upper, file] := \
                 *cozo_archive_segments{\
                    relation: rel, key_count: count, \
                    lower_commit_ts: lower, upper_commit_ts: upper, \
                    file_path: file}",
            )
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][0], json!("r"));
        assert_eq!(res["rows"][0][1], json!(2));
        assert_eq!(res["rows"][0][2], json!(t));
        assert_eq!(res["rows"][0][3], json!(t));
        let path = res["rows"][0][4].as_str().unwrap().to_string();
        assert!(path.ends_with(".parquet"));
        assert!(std::path::Path::new(&path).exists());

        let _ = dir;
    }

    #[test]
    fn replicate_second_call_is_noop() {
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(1));
        assert_eq!(res["rows"][0][2], json!(1), "first drain wrote one segment");

        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(0), "no new rows");
        assert_eq!(res["rows"][0][2], json!(0), "no new segments");

        // Manifest still has exactly one row.
        let res = db
            .run_default("?[c] := *cozo_archive_segments{segment_id: c}")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn commit_now_is_strictly_monotonic_across_writes() {
        // Each separate :put script must get a strictly greater commit ts than
        // the previous one, even back-to-back within one microsecond. The old
        // wall-clock implementation could repeat a timestamp here, which would
        // let `::archive` delete an un-replicated row at the watermark boundary.
        let (db, _dir, _) = setup();
        let n = 64i64;
        for id in 0..n {
            db.run_default(&format!("?[id, name] <- [[{id}, 'x']] :put r {{id => name}}"))
                .unwrap();
        }
        let res = db.run_default("?[ts] := *r{ts}").unwrap().into_json();
        let rows = res["rows"].as_array().unwrap();
        assert_eq!(rows.len(), n as usize);
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let ts = row[0].as_i64().unwrap();
            assert!(seen.insert(ts), "duplicate commit_now ts {ts}: clock not monotonic");
        }
    }

    #[test]
    fn replicate_is_content_addressed_idempotent() {
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();

        db.run_default("::replicate_pending 'r'").unwrap();
        let seg1 = db
            .run_default("?[s] := *cozo_archive_segments{segment_id: s}")
            .unwrap()
            .into_json();
        assert_eq!(seg1["rows"].as_array().unwrap().len(), 1);
        let id1 = seg1["rows"][0][0].clone();

        // Force the same rows to be due again (reset the watermark) and re-drain.
        // Identical content must reuse the same content-addressed segment_id
        // (an upsert), not append a duplicate manifest row.
        db.run_default("::archive_advance_watermark 'r' 0").unwrap();
        db.run_default("::replicate_pending 'r'").unwrap();

        let seg2 = db
            .run_default("?[s] := *cozo_archive_segments{segment_id: s}")
            .unwrap()
            .into_json();
        assert_eq!(
            seg2["rows"].as_array().unwrap().len(),
            1,
            "re-draining identical content must not create a duplicate segment"
        );
        assert_eq!(
            seg2["rows"][0][0], id1,
            "segment_id must be stable (content-addressed) across re-drains"
        );
    }

    #[test]
    fn replicate_via_timestamp_index_round_trips() {
        // A relation WITH a timestamp index uses the index range-scan path. Prove
        // it replicates the correct rows by restoring the segment and comparing.
        let dir = tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap().to_string();
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int => name: String, ts: Int default commit_now()}"#)
            .unwrap();
        db.run_default("::index create r:ts_idx {ts}").unwrap();
        db.run_default(&format!("::archive_config put 'r' 'ts' '{dir_str}'"))
            .unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b'], [3, 'c']] :put r {id => name}"#)
            .unwrap();

        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(3), "three rows replicated via index");

        let seg = db
            .run_default("?[f] := *cozo_archive_segments{file_path: f}")
            .unwrap()
            .into_json();
        let file = seg["rows"][0][0].as_str().unwrap().to_string();
        assert!(std::path::Path::new(&file).exists());

        // Restore into a fresh relation and confirm the rows survived intact.
        db.run_default(r#":create r2 {id: Int => name: String, ts: Int}"#)
            .unwrap();
        db.run_default(&format!("::import_parquet r2 from '{file}'"))
            .unwrap();
        let res = db
            .run_default("?[id, name] := *r2{id, name}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows, vec![json!([1, "a"]), json!([2, "b"]), json!([3, "c"])]);
    }

    #[test]
    fn replicate_picks_up_new_rows_after_first_drain() {
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::replicate_pending 'r'").unwrap();

        std::thread::sleep(Duration::from_millis(2));
        db.run_default(r#"?[id, name] <- [[2, 'b']] :put r {id => name}"#).unwrap();
        let t2 = ts_of(&db, 2);
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(1), "second drain finds the new row");
        assert_eq!(res["rows"][0][2], json!(1), "second drain wrote one segment");
        assert_eq!(res["rows"][0][4], json!(t2), "watermark advanced to t2");

        // Two segments now (one per drain).
        let res = db
            .run_default("?[c] := *cozo_archive_segments{segment_id: c}")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn replicate_round_trip_via_import_parquet() {
        // The round-trip contract: after replicate, the segment file imported
        // back via ::import_parquet reproduces the original rows (including
        // the commit-time ts). Slice 6: file path comes from the manifest
        // rather than from the sys op response.
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'alice'], [2, 'bob']] :put r {id => name}"#)
            .unwrap();
        let t = ts_of(&db, 1);
        db.run_default("::replicate_pending 'r'").unwrap();

        // Look up the file path from the manifest.
        let res = db
            .run_default(
                "?[file] := *cozo_archive_segments{relation: 'r', file_path: file}",
            )
            .unwrap()
            .into_json();
        let path = res["rows"][0][0].as_str().unwrap().to_string();

        // Fresh mirror relation in the same DB.
        db.run_default(r#":create r2 {id: Int => name: String, ts: Int}"#).unwrap();
        db.run_default(&format!("::import_parquet r2 from '{path}'")).unwrap();

        let res = db
            .run_default("?[id, name, ts] := *r2{id, name, ts}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], json!([1, "alice", t]));
        assert_eq!(rows[1], json!([2, "bob", t]));
    }

    #[test]
    fn replicate_with_archive_workflow_end_to_end() {
        // The user-visible flow: write rows -> replicate -> archive deletes
        // the now-replicated rows from the source relation.
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();
        // Replicate first; this advances the watermark, making rows eligible
        // for archive.
        db.run_default("::replicate_pending 'r'").unwrap();

        let res = db
            .run_default("::archive r { ?[id] := *r{id} }")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(2), "both rows archived");
        assert_eq!(res["rows"][0][2], json!(0), "none skipped");

        // Source relation is empty.
        let res = db
            .run_default("?[id] := *r{id}")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn replicate_requires_staging_dir() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        // Configure WITHOUT staging_dir.
        db.run_default("::archive_config put 'r' 'ts'").unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let err = db.run_default("::replicate_pending 'r'").unwrap_err().to_string();
        assert!(
            err.contains("staging_dir"),
            "error should mention the missing staging_dir; got: {err}"
        );
    }

    #[test]
    fn replicate_unconfigured_relation_errors() {
        let db = DbInstance::default();
        db.run_default(r#":create r {id: Int}"#).unwrap();
        let err = db.run_default("::replicate_pending 'r'").unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("not configured"),
            "got: {err}"
        );
    }

    #[test]
    fn replicate_manifest_records_sha256_and_status() {
        let (db, _dir, _) = setup();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        db.run_default("::replicate_pending 'r'").unwrap();

        let res = db
            .run_default(
                "?[sha, status] := *cozo_archive_segments{sha256: sha, status}",
            )
            .unwrap()
            .into_json();
        let row = &res["rows"][0];
        // sha256 serializes as a base64 string in JSON.
        assert!(row[0].is_string());
        // 32 raw bytes => base64 length 44 with padding (or 43 + '=').
        assert!(
            row[0].as_str().unwrap().len() >= 40,
            "sha looks too short to be a 32-byte hash: {:?}",
            row[0]
        );
        assert_eq!(row[1], json!("uploaded"));
    }

    #[test]
    fn replicate_two_relations_independent_watermarks() {
        // Configuring two relations with separate staging dirs and replicating
        // them independently. Each maintains its own watermark in
        // cozo_archive_watermark.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(
            r#":create s {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let r_dir = dir.path().join("r");
        let s_dir = dir.path().join("s");
        std::fs::create_dir_all(&r_dir).unwrap();
        std::fs::create_dir_all(&s_dir).unwrap();
        db.run_default(&format!(
            "::archive_config put 'r' 'ts' '{}'",
            r_dir.to_str().unwrap()
        ))
        .unwrap();
        db.run_default(&format!(
            "::archive_config put 's' 'ts' '{}'",
            s_dir.to_str().unwrap()
        ))
        .unwrap();

        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        db.run_default(r#"?[id, name] <- [[2, 'b']] :put s {id => name}"#).unwrap();

        let r_res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        let s_res = db.run_default("::replicate_pending 's'").unwrap().into_json();

        // new_watermark is index 4 in the slice 6 response shape.
        let r_wm = r_res["rows"][0][4].as_i64().unwrap();
        let s_wm = s_res["rows"][0][4].as_i64().unwrap();
        assert!(s_wm > r_wm, "s' watermark should be later: r={r_wm} s={s_wm}");

        // r's drain doesn't touch s's watermark.
        let res = db
            .run_default("?[c] := *cozo_archive_segments{segment_id: c}")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 2);
    }

    // -----------------------------------------------------------------------
    // Slice 6: per-segment row caps. Tests below verify that the replicator
    // produces multiple right-sized segments rather than one giant one when
    // the configured cap is small relative to the qualifying-row count.
    // -----------------------------------------------------------------------

    /// Helper: configure r with an explicit max_rows_per_segment.
    /// Skip optional encryption + kms_key_arn slots — pest sees the bare
    /// integer and passes it through to the trailing `expr?` for max_rows.
    fn setup_with_cap(cap: i64) -> (DbInstance, tempfile::TempDir) {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap().to_string();
        db.run_default(&format!(
            "::archive_config put 'r' 'ts' '{dir_str}' {cap}"
        ))
        .unwrap();
        (db, dir)
    }

    #[test]
    fn replicate_splits_distinct_ts_across_segments() {
        // Insert rows across several scripts so each row gets a distinct ts.
        // With cap=2 and 5 distinct-ts rows, we expect 3 segments (2+2+1).
        let (db, _dir) = setup_with_cap(2);
        for i in 1..=5 {
            db.run_default(&format!(r#"?[id, name] <- [[{i}, 'x']] :put r {{id => name}}"#))
                .unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }

        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(5), "rows_replicated");
        assert_eq!(res["rows"][0][2], json!(3), "segments_written");

        // Manifest has 3 segments.
        let res = db
            .run_default(
                "?[c] := *cozo_archive_segments{relation: 'r', segment_id: c}",
            )
            .unwrap()
            .into_json();
        assert_eq!(res["rows"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn replicate_extends_chunk_through_ts_ties() {
        // 5 rows in ONE script means they all share one ts. With cap=2 the
        // chunk would naively close at 2, but the ts-tie extension keeps
        // them all in one segment so the watermark advances safely.
        let (db, _dir) = setup_with_cap(2);
        db.run_default(
            r#"?[id, name] <- [[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd'], [5, 'e']]
               :put r {id => name}"#,
        )
        .unwrap();

        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(5), "all 5 rows replicated");
        assert_eq!(
            res["rows"][0][2],
            json!(1),
            "ts-tie extension keeps them in one segment"
        );

        let res = db
            .run_default(
                "?[count] := *cozo_archive_segments{relation: 'r', key_count: count}",
            )
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][0], json!(5));
    }

    #[test]
    fn replicate_default_cap_applies_when_unset() {
        // No explicit cap → effective cap should be the default (100k).
        // We can't realistically exercise 100k rows in a unit test, so we
        // verify the config row reports null and that the replicator still
        // succeeds for a small input. The default-application is tested via
        // get_config returning None and effective_max_rows_per_segment()
        // applying the const, which is exercised by all the existing tests
        // that don't pass a cap.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap().to_string();
        db.run_default(&format!("::archive_config put 'r' 'ts' '{dir_str}'")).unwrap();

        let res = db.run_default("::archive_config get 'r'").unwrap().into_json();
        // Index 5 = max_rows_per_segment column; null when unset.
        assert!(
            res["rows"][0][5].is_null(),
            "max_rows_per_segment should be null when not specified"
        );

        // 1 row, 1 segment — confirming the default branch runs without error.
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(1));
        assert_eq!(res["rows"][0][2], json!(1));
    }

    #[test]
    fn archive_config_put_with_max_rows_per_segment() {
        // Round-trip the new column through put + get.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default("::archive_config put 'r' 'ts' '/tmp/x' 5000")
            .unwrap();

        let res = db.run_default("::archive_config get 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][5], json!(5000));
    }

    #[test]
    fn archive_config_rejects_zero_max_rows_per_segment() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' '/tmp/x' 0")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("max_rows_per_segment") || err.contains("positive"),
            "got: {err}"
        );
    }

    #[test]
    fn archive_config_rejects_negative_max_rows_per_segment() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' '/tmp/x' (-1)")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("max_rows_per_segment") || err.contains("positive"),
            "got: {err}"
        );
    }

    #[test]
    fn replicate_each_segment_records_distinct_ts_range() {
        // After a multi-segment drain, each manifest row should have a
        // strictly-increasing (lower_ts, upper_ts) compared to the prior.
        let (db, _dir) = setup_with_cap(2);
        for i in 1..=5 {
            db.run_default(&format!(r#"?[id, name] <- [[{i}, 'x']] :put r {{id => name}}"#))
                .unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
        db.run_default("::replicate_pending 'r'").unwrap();

        let res = db
            .run_default(
                "?[lower, upper] := *cozo_archive_segments{\
                    relation: 'r', lower_commit_ts: lower, upper_commit_ts: upper}",
            )
            .unwrap()
            .into_json();
        let mut bounds: Vec<(i64, i64)> = res["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r[0].as_i64().unwrap(), r[1].as_i64().unwrap()))
            .collect();
        bounds.sort();
        // Each upper_ts must be < the next lower_ts (strict separation).
        for w in bounds.windows(2) {
            let prev_upper = w[0].1;
            let next_lower = w[1].0;
            assert!(
                next_lower > prev_upper,
                "segment ranges should not overlap: prev_upper={prev_upper} \
                 next_lower={next_lower}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Slice 5: object_store integration. URI scheme handling, credential
// rejection, encryption config — exercised via `file://` and bare paths.
// Real-S3 smoke tests live in `integration_s3_tests` below and require
// the `integration-s3` feature flag plus AWS_* env vars.
// ---------------------------------------------------------------------------

#[cfg(feature = "archive")]
mod archive_uri_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn config_accepts_file_uri() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let uri = format!("file://{}", dir.path().to_str().unwrap());
        db.run_default(&format!("::archive_config put 'r' 'ts' '{uri}'")).unwrap();
        let res = db
            .run_default("::archive_config get 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][2], json!(uri));
    }

    #[test]
    fn config_accepts_s3_uri_format() {
        // No actual S3 contact — just that the URI parses and is stored.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default("::archive_config put 'r' 'ts' 's3://my-bucket/prefix/'")
            .unwrap();
        let res = db
            .run_default("::archive_config get 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][2], json!("s3://my-bucket/prefix/"));
    }

    #[test]
    fn config_rejects_unknown_uri_scheme() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' 'gs://bucket/key'")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not supported") || err.contains("scheme"),
            "got: {err}"
        );
    }

    #[test]
    fn config_rejects_relative_file_uri() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' 'file://relative/path'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn config_accepts_sse_s3_encryption() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(
            "::archive_config put 'r' 'ts' 's3://b/p/' 'sse-s3'",
        )
        .unwrap();
        let res = db.run_default("::archive_config get 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][3], json!("sse-s3"));
        assert!(res["rows"][0][4].is_null());
    }

    #[test]
    fn config_accepts_sse_kms_with_key_arn() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(
            "::archive_config put 'r' 'ts' 's3://b/p/' 'sse-kms' 'arn:aws:kms:us-east-1:000000000000:key/abc'",
        )
        .unwrap();
        let res = db.run_default("::archive_config get 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][3], json!("sse-kms"));
        assert_eq!(
            res["rows"][0][4],
            json!("arn:aws:kms:us-east-1:000000000000:key/abc")
        );
    }

    #[test]
    fn config_rejects_sse_kms_without_key_arn() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' 's3://b/p/' 'sse-kms'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("kms_key_arn"), "got: {err}");
    }

    #[test]
    fn config_rejects_unknown_encryption_mode() {
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let err = db
            .run_default("::archive_config put 'r' 'ts' 's3://b/p/' 'aes-256-cbc'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown encryption"), "got: {err}");
    }

    #[test]
    fn replicate_via_file_uri_works() {
        // Slice 5 refactored the replicator onto object_store. Verifying
        // that file:// (the LocalFileSystem backend) still works end-to-end.
        // Slice 6: per-segment file path moved from sys op response to manifest.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let uri = format!("file://{}", dir.path().to_str().unwrap());
        db.run_default(&format!("::archive_config put 'r' 'ts' '{uri}'")).unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'a'], [2, 'b']] :put r {id => name}"#)
            .unwrap();
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(2), "rows replicated");
        assert_eq!(res["rows"][0][2], json!(1), "one segment");

        // file_path comes from the manifest in slice 6.
        let res = db
            .run_default(
                "?[file] := *cozo_archive_segments{relation: 'r', file_path: file}",
            )
            .unwrap()
            .into_json();
        let path = res["rows"][0][0].as_str().unwrap();
        assert!(path.starts_with(dir.path().to_str().unwrap()), "got: {path}");
        assert!(std::path::Path::new(path).exists());
    }

    #[test]
    fn replicate_via_bare_path_still_works() {
        // Slice 4 backwards compatibility: bare paths (no scheme) must keep
        // routing to the local filesystem backend.
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        let dir = tempdir().unwrap();
        db.run_default(&format!(
            "::archive_config put 'r' 'ts' '{}'",
            dir.path().to_str().unwrap()
        ))
        .unwrap();
        db.run_default(r#"?[id, name] <- [[1, 'a']] :put r {id => name}"#).unwrap();
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(1));
    }
}

// ---------------------------------------------------------------------------
// Optional: smoke tests against a real S3 / S3-compatible endpoint.
// Enabled via `--features integration-s3`. Requires AWS_* env vars and
// COZO_TEST_S3_BUCKET in the environment (`.env` at repo root supported).
//
// These tests:
//   * skip cleanly if env vars are missing
//   * use a unique prefix per run to avoid collisions
//   * never assume DeleteObject — IAM probe enforces the absence of it
//   * clean up after themselves on success (best effort)
// ---------------------------------------------------------------------------

#[cfg(feature = "integration-s3")]
mod integration_s3_tests {
    use super::*;
    use std::sync::Once;

    static LOAD_ENV: Once = Once::new();

    fn load_env_once() {
        LOAD_ENV.call_once(|| {
            // Best-effort: if there's a .env at repo root, pick it up. If not,
            // fall through to whatever's already in process env.
            let _ = dotenvy::dotenv();
            // The endpoint-URL bridge (AWS_ENDPOINT_URL{_S3} -> AWS_ENDPOINT)
            // happens inside `archive::store::build_object_store`, so it
            // works for production users too — not just tests.
        });
    }

    /// Returns the configured `s3://bucket/prefix/` URI for this test run, or
    /// `None` if env vars are missing — in which case the test should skip.
    fn smoke_uri(test_name: &str) -> Option<String> {
        load_env_once();
        let bucket = std::env::var("COZO_TEST_S3_BUCKET").ok()?;
        // Required AWS creds — if any of these is missing we can't get past
        // the SDK chain, so skip rather than fail.
        std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        std::env::var("AWS_REGION").ok()?;
        let user_prefix = std::env::var("COZO_TEST_S3_PREFIX").unwrap_or_default();
        // Unique per-test-run prefix so parallel test runs / repeated CI
        // invocations don't collide.
        let unique = uuid::Uuid::new_v4();
        let full_prefix = format!(
            "{}{}{test_name}-{unique}/",
            user_prefix.trim_end_matches('/'),
            if user_prefix.is_empty() { "" } else { "/" }
        );
        Some(format!("s3://{bucket}/{full_prefix}"))
    }

    #[test]
    fn smoke_replicate_to_s3_round_trips_via_in_memory_bytes() {
        let Some(uri) = smoke_uri("rt") else {
            eprintln!("skipping: COZO_TEST_S3_BUCKET / AWS_* not set");
            return;
        };

        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => name: String, ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(&format!("::archive_config put 'r' 'ts' '{uri}'"))
            .unwrap();
        db.run_default(
            r#"?[id, name] <- [[1, 'alice'], [2, 'bob']] :put r {id => name}"#,
        )
        .unwrap();

        // Replicate. This exercises: AWS sigv4, optional custom endpoint
        // (Tigris / R2 / MinIO), TLS, IAM probe (DeleteObject must fail),
        // and the actual PUT path.
        let res = db
            .run_default("::replicate_pending 'r'")
            .unwrap()
            .into_json();
        assert_eq!(res["rows"][0][1], json!(2), "rows replicated");

        // The segment's s3:// URI lives in the manifest (the drain summary row
        // carries watermarks, not the path).
        let seg = db
            .run_default(
                "?[count, status, file] := *cozo_archive_segments{\
                    key_count: count, status, file_path: file}",
            )
            .unwrap()
            .into_json();
        assert_eq!(seg["rows"][0][0], json!(2));
        assert_eq!(seg["rows"][0][1], json!("uploaded"));
        let s3_path = seg["rows"][0][2].as_str().unwrap().to_string();
        assert!(
            s3_path.starts_with("s3://"),
            "manifest file_path should be an s3:// URI; got {s3_path}"
        );

        // True round-trip: restore directly from s3:// (4.4 — fetches the bytes
        // via the object store and decodes in memory) and confirm the rows.
        db.run_default(r#":create restored {id: Int => name: String, ts: Int}"#)
            .unwrap();
        db.run_default(&format!("::import_parquet restored from '{s3_path}'"))
            .unwrap();
        let res = db
            .run_default("?[id, name] := *restored{id, name}")
            .unwrap()
            .into_json();
        let mut rows = res["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r[0].as_i64().unwrap());
        assert_eq!(rows, vec![json!([1, "alice"]), json!([2, "bob"])]);
    }

    #[test]
    fn smoke_iam_probe_passes_for_well_scoped_credentials() {
        // Sanity check that the IAM probe doesn't false-positive with a
        // well-scoped IAM role. If this test fails because of "role can
        // DeleteObject", your test credentials are too permissive — narrow
        // the policy.
        let Some(uri) = smoke_uri("iam") else {
            eprintln!("skipping: COZO_TEST_S3_BUCKET / AWS_* not set");
            return;
        };
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(&format!("::archive_config put 'r' 'ts' '{uri}'"))
            .unwrap();
        db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
        // No assertions on the result — just that it succeeds without IAM
        // probe rejecting.
        db.run_default("::replicate_pending 'r'").unwrap();
    }

    #[test]
    fn smoke_no_replication_when_no_new_rows() {
        let Some(uri) = smoke_uri("idem") else {
            eprintln!("skipping: COZO_TEST_S3_BUCKET / AWS_* not set");
            return;
        };
        let db = DbInstance::default();
        db.run_default(
            r#":create r {id: Int => ts: Int default commit_now()}"#,
        )
        .unwrap();
        db.run_default(&format!("::archive_config put 'r' 'ts' '{uri}'"))
            .unwrap();
        db.run_default(r#"?[id] <- [[1]] :put r {id}"#).unwrap();
        db.run_default("::replicate_pending 'r'").unwrap();
        // Second drain should be a no-op (no new rows past the watermark);
        // critically, no extra S3 PUT happens. Response shape is
        // [status, rows_replicated, segments_written, old_watermark, new_watermark].
        let res = db.run_default("::replicate_pending 'r'").unwrap().into_json();
        assert_eq!(res["rows"][0][1], json!(0), "no new rows");
        assert_eq!(res["rows"][0][2], json!(0), "no new segments");
        assert_eq!(
            res["rows"][0][3], res["rows"][0][4],
            "watermark unchanged on a no-op drain"
        );
    }
}
