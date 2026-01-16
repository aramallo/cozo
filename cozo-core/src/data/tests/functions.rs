/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use approx::AbsDiffEq;
use num_traits::FloatConst;
use regex::Regex;
use serde_json::json;

use crate::data::functions::*;
use crate::data::value::{DataValue, JsonData, RegexWrapper};
use crate::DbInstance;

#[test]
fn test_add() {
    assert_eq!(op_add(&[]).unwrap(), DataValue::from(0));
    assert_eq!(op_add(&[DataValue::from(1)]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_add(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(3)
    );
    assert_eq!(
        op_add(&[DataValue::from(1), DataValue::from(2.5)]).unwrap(),
        DataValue::from(3.5)
    );
    assert_eq!(
        op_add(&[DataValue::from(1.5), DataValue::from(2.5)]).unwrap(),
        DataValue::from(4.0)
    );
}

#[test]
fn test_sub() {
    assert_eq!(
        op_sub(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_sub(&[DataValue::from(1), DataValue::from(2.5)]).unwrap(),
        DataValue::from(-1.5)
    );
    assert_eq!(
        op_sub(&[DataValue::from(1.5), DataValue::from(2.5)]).unwrap(),
        DataValue::from(-1.0)
    );
}

#[test]
fn test_mul() {
    assert_eq!(op_mul(&[]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_mul(&[DataValue::from(2), DataValue::from(3)]).unwrap(),
        DataValue::from(6)
    );
    assert_eq!(
        op_mul(&[DataValue::from(0.5), DataValue::from(0.25)]).unwrap(),
        DataValue::from(0.125)
    );
    assert_eq!(
        op_mul(&[DataValue::from(0.5), DataValue::from(3)]).unwrap(),
        DataValue::from(1.5)
    );
}

#[test]
fn test_div() {
    assert_eq!(
        op_div(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_div(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(0.5)
    );
    assert_eq!(
        op_div(&[DataValue::from(7.0), DataValue::from(0.5)]).unwrap(),
        DataValue::from(14.0)
    );
    assert!(op_div(&[DataValue::from(1), DataValue::from(0)]).is_ok());
}

#[test]
fn test_eq_neq() {
    assert_eq!(
        op_eq(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_eq(&[DataValue::from(123), DataValue::from(123)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_neq(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_neq(&[DataValue::from(123), DataValue::from(123.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_eq(&[DataValue::from(123), DataValue::from(123.1)]).unwrap(),
        DataValue::from(false)
    );
}

#[test]
fn test_list() {
    assert_eq!(op_list(&[]).unwrap(), DataValue::List(vec![]));
    assert_eq!(
        op_list(&[DataValue::from(1)]).unwrap(),
        DataValue::List(vec![DataValue::from(1)])
    );
    assert_eq!(
        op_list(&[DataValue::from(1), DataValue::List(vec![])]).unwrap(),
        DataValue::List(vec![DataValue::from(1), DataValue::List(vec![])])
    );
}

#[test]
fn test_is_in() {
    assert_eq!(
        op_is_in(&[
            DataValue::from(1),
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)])
        ])
        .unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_in(&[
            DataValue::from(3),
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)])
        ])
        .unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_in(&[DataValue::from(3), DataValue::List(vec![])]).unwrap(),
        DataValue::from(false)
    );
}

#[test]
fn test_comparators() {
    assert_eq!(
        op_ge(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(2.), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(2), DataValue::from(1.)]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(false)
    );
    assert!(op_ge(&[DataValue::Null, DataValue::from(true)]).is_err());
    assert_eq!(
        op_gt(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_gt(&[DataValue::from(2.), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_gt(&[DataValue::from(2), DataValue::from(1.)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(false)
    );
    assert!(op_gt(&[DataValue::Null, DataValue::from(true)]).is_err());
    assert_eq!(
        op_le(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_le(&[DataValue::from(2.), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_le(&[DataValue::from(2), DataValue::from(1.)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(true)
    );
    assert!(op_le(&[DataValue::Null, DataValue::from(true)]).is_err());
    assert_eq!(
        op_lt(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(2.), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(2), DataValue::from(1.)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(true)
    );
    assert!(op_lt(&[DataValue::Null, DataValue::from(true)]).is_err());
}

#[test]
fn test_max_min() {
    assert_eq!(op_max(&[DataValue::from(1),]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_max(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(4)
    );
    assert_eq!(
        op_max(&[
            DataValue::from(1.0),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(4)
    );
    assert_eq!(
        op_max(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4.0)
        ])
        .unwrap(),
        DataValue::from(4.0)
    );
    assert!(op_max(&[DataValue::from(true)]).is_err());

    assert_eq!(op_min(&[DataValue::from(1),]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_min(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_min(&[
            DataValue::from(1.0),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_min(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4.0)
        ])
        .unwrap(),
        DataValue::from(1)
    );
    assert!(op_max(&[DataValue::from(true)]).is_err());
}

#[test]
fn test_minus() {
    assert_eq!(
        op_minus(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_minus(&[DataValue::from(1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_minus(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::from(f64::NEG_INFINITY)
    );
    assert_eq!(
        op_minus(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(f64::INFINITY)
    );
}

#[test]
fn test_abs() {
    assert_eq!(op_abs(&[DataValue::from(-1)]).unwrap(), DataValue::from(1));
    assert_eq!(op_abs(&[DataValue::from(1)]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_abs(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(1.5)
    );
}

#[test]
fn test_signum() {
    assert_eq!(
        op_signum(&[DataValue::from(0.1)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-0.1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(0.0)]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-0.0)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-3)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(-1)
    );
    assert!(op_signum(&[DataValue::from(f64::NAN)])
        .unwrap()
        .get_float()
        .unwrap()
        .is_nan());
}

#[test]
fn test_floor_ceil() {
    assert_eq!(
        op_floor(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_floor(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-2.0)
    );
    assert_eq!(
        op_floor(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(2.0)
    );
}

#[test]
fn test_round() {
    assert_eq!(
        op_round(&[DataValue::from(0.6)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(0.5)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(2.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-0.6)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-0.5)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-2.0)
    );
}

#[test]
fn test_exp() {
    let n = op_exp(&[DataValue::from(1)]).unwrap().get_float().unwrap();
    assert!(n.abs_diff_eq(&f64::E(), 1E-5));

    let n = op_exp(&[DataValue::from(50.1)])
        .unwrap()
        .get_float()
        .unwrap();
    assert!(n.abs_diff_eq(&(50.1_f64.exp()), 1E-5));
}

#[test]
fn test_exp2() {
    let n = op_exp2(&[DataValue::from(10.)])
        .unwrap()
        .get_float()
        .unwrap();
    assert_eq!(n, 1024.);
}

#[test]
fn test_ln() {
    assert_eq!(
        op_ln(&[DataValue::from(f64::E())]).unwrap(),
        DataValue::from(1.0)
    );
}

#[test]
fn test_log2() {
    assert_eq!(
        op_log2(&[DataValue::from(1024)]).unwrap(),
        DataValue::from(10.)
    );
}

#[test]
fn test_log10() {
    assert_eq!(
        op_log10(&[DataValue::from(1000)]).unwrap(),
        DataValue::from(3.0)
    );
}

#[test]
fn test_trig() {
    assert!(op_sin(&[DataValue::from(f64::PI() / 2.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&1.0, 1e-5));
    assert!(op_cos(&[DataValue::from(f64::PI() / 2.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&0.0, 1e-5));
    assert!(op_tan(&[DataValue::from(f64::PI() / 4.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&1.0, 1e-5));
}

#[test]
fn test_inv_trig() {
    assert!(op_asin(&[DataValue::from(1.0)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 2.), 1e-5));
    assert!(op_acos(&[DataValue::from(0)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 2.), 1e-5));
    assert!(op_atan(&[DataValue::from(1)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 4.), 1e-5));
    assert!(op_atan2(&[DataValue::from(-1), DataValue::from(-1)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(-3. * f64::PI() / 4.), 1e-5));
}

#[test]
fn test_pow() {
    assert_eq!(
        op_pow(&[DataValue::from(2), DataValue::from(10)]).unwrap(),
        DataValue::from(1024.0)
    );
}

#[test]
fn test_mod() {
    assert_eq!(
        op_mod(&[DataValue::from(-10), DataValue::from(7)]).unwrap(),
        DataValue::from(-3)
    );
    assert!(op_mod(&[DataValue::from(5), DataValue::from(0.)]).is_ok());
    assert!(op_mod(&[DataValue::from(5.), DataValue::from(0.)]).is_ok());
    assert!(op_mod(&[DataValue::from(5.), DataValue::from(0)]).is_ok());
    assert!(op_mod(&[DataValue::from(5), DataValue::from(0)]).is_err());
}

#[test]
fn test_boolean() {
    assert_eq!(op_and(&[]).unwrap(), DataValue::from(true));
    assert_eq!(
        op_and(&[DataValue::from(true), DataValue::from(false)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(op_or(&[]).unwrap(), DataValue::from(false));
    assert_eq!(
        op_or(&[DataValue::from(true), DataValue::from(false)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_negate(&[DataValue::from(false)]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_bits() {
    assert_eq!(
        op_bit_and(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b010000].into())
    );
    assert_eq!(
        op_bit_or(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b111101].into())
    );
    assert_eq!(
        op_bit_not(&[DataValue::Bytes([0b00111000].into())]).unwrap(),
        DataValue::Bytes([0b11000111].into())
    );
    assert_eq!(
        op_bit_xor(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b101101].into())
    );
}

#[test]
fn test_pack_bits() {
    assert_eq!(
        op_pack_bits(&[DataValue::List(vec![DataValue::from(true)])]).unwrap(),
        DataValue::Bytes([0b10000000].into())
    )
}

#[test]
fn test_unpack_bits() {
    assert_eq!(
        op_unpack_bits(&[DataValue::Bytes([0b10101010].into())]).unwrap(),
        DataValue::List(
            [true, false, true, false, true, false, true, false]
                .into_iter()
                .map(DataValue::Bool)
                .collect()
        )
    )
}

#[test]
fn test_concat() {
    assert_eq!(
        op_concat(&[DataValue::Str("abc".into()), DataValue::Str("def".into())]).unwrap(),
        DataValue::Str("abcdef".into())
    );

    assert_eq!(
        op_concat(&[
            DataValue::List(vec![DataValue::from(true), DataValue::from(false)]),
            DataValue::List(vec![DataValue::from(true)])
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::from(true),
            DataValue::from(false),
            DataValue::from(true),
        ])
    );
}

#[test]
fn test_str_includes() {
    assert_eq!(
        op_str_includes(&[
            DataValue::Str("abcdef".into()),
            DataValue::Str("bcd".into())
        ])
        .unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_str_includes(&[DataValue::Str("abcdef".into()), DataValue::Str("bd".into())]).unwrap(),
        DataValue::from(false)
    );
}

#[test]
fn test_casings() {
    assert_eq!(
        op_lowercase(&[DataValue::Str("NAÏVE".into())]).unwrap(),
        DataValue::Str("naïve".into())
    );
    assert_eq!(
        op_uppercase(&[DataValue::Str("naïve".into())]).unwrap(),
        DataValue::Str("NAÏVE".into())
    );
}

#[test]
fn test_trim() {
    assert_eq!(
        op_trim(&[DataValue::Str(" a ".into())]).unwrap(),
        DataValue::Str("a".into())
    );
    assert_eq!(
        op_trim_start(&[DataValue::Str(" a ".into())]).unwrap(),
        DataValue::Str("a ".into())
    );
    assert_eq!(
        op_trim_end(&[DataValue::Str(" a ".into())]).unwrap(),
        DataValue::Str(" a".into())
    );
}

#[test]
fn test_starts_ends_with() {
    assert_eq!(
        op_starts_with(&[
            DataValue::Str("abcdef".into()),
            DataValue::Str("abc".into())
        ])
        .unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_starts_with(&[DataValue::Str("abcdef".into()), DataValue::Str("bc".into())]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_ends_with(&[
            DataValue::Str("abcdef".into()),
            DataValue::Str("def".into())
        ])
        .unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_ends_with(&[DataValue::Str("abcdef".into()), DataValue::Str("bc".into())]).unwrap(),
        DataValue::from(false)
    );
}

#[test]
fn test_regex() {
    assert_eq!(
        op_regex_matches(&[
            DataValue::Str("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.e").unwrap()))
        ])
        .unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_regex_matches(&[
            DataValue::Str("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.ef$").unwrap()))
        ])
        .unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_regex_matches(&[
            DataValue::Str("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.e$").unwrap()))
        ])
        .unwrap(),
        DataValue::from(false)
    );

    assert_eq!(
        op_regex_replace(&[
            DataValue::Str("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[be]").unwrap())),
            DataValue::Str("x".into())
        ])
        .unwrap(),
        DataValue::Str("axcdef".into())
    );

    assert_eq!(
        op_regex_replace_all(&[
            DataValue::Str("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[be]").unwrap())),
            DataValue::Str("x".into())
        ])
        .unwrap(),
        DataValue::Str("axcdxf".into())
    );
    assert_eq!(
        op_regex_extract(&[
            DataValue::Str("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[xayef]|(GH)").unwrap()))
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Str("a".into()),
            DataValue::Str("e".into()),
            DataValue::Str("f".into()),
            DataValue::Str("GH".into()),
        ])
    );
    assert_eq!(
        op_regex_extract_first(&[
            DataValue::Str("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[xayef]|(GH)").unwrap()))
        ])
        .unwrap(),
        DataValue::Str("a".into()),
    );
    assert_eq!(
        op_regex_extract(&[
            DataValue::Str("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("xyz").unwrap()))
        ])
        .unwrap(),
        DataValue::List(vec![])
    );

    assert_eq!(
        op_regex_extract_first(&[
            DataValue::Str("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("xyz").unwrap()))
        ])
        .unwrap(),
        DataValue::Null
    );
}

#[test]
fn test_predicates() {
    assert_eq!(
        op_is_null(&[DataValue::Null]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_null(&[DataValue::Bot]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_int(&[DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_int(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_float(&[DataValue::from(1)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_float(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::Null]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_bytes(&[DataValue::Bytes([0b1].into())]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_bytes(&[DataValue::Null]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_list(&[DataValue::List(vec![])]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_list(&[DataValue::Null]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_string(&[DataValue::Str("".into())]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_string(&[DataValue::Null]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_prepend_append() {
    assert_eq!(
        op_prepend(&[
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::Null,
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(1),
            DataValue::from(2),
        ]),
    );
    assert_eq!(
        op_append(&[
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::Null,
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null,
        ]),
    );
}

#[test]
fn test_length() {
    assert_eq!(
        op_length(&[DataValue::Str("abc".into())]).unwrap(),
        DataValue::from(3)
    );
    assert_eq!(
        op_length(&[DataValue::List(vec![])]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_length(&[DataValue::Bytes([].into())]).unwrap(),
        DataValue::from(0)
    );
}

#[test]
fn test_unicode_normalize() {
    assert_eq!(
        op_unicode_normalize(&[DataValue::Str("abc".into()), DataValue::Str("nfc".into())])
            .unwrap(),
        DataValue::Str("abc".into())
    )
}

#[test]
fn test_sort_reverse() {
    assert_eq!(
        op_sorted(&[DataValue::List(vec![
            DataValue::from(2.0),
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null,
        ])])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(2.0),
        ])
    );
    assert_eq!(
        op_reverse(&[DataValue::List(vec![
            DataValue::from(2.0),
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null,
        ])])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(2),
            DataValue::from(1),
            DataValue::from(2.0),
        ])
    )
}

#[test]
fn test_haversine() {
    let d = op_haversine_deg_input(&[
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(180),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&f64::PI(), 1e-5));

    let d = op_haversine_deg_input(&[
        DataValue::from(90),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(123),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&(f64::PI() / 2.), 1e-5));

    let d = op_haversine(&[
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(f64::PI()),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&f64::PI(), 1e-5));
}

#[test]
fn test_deg_rad() {
    assert_eq!(
        op_deg_to_rad(&[DataValue::from(180)]).unwrap(),
        DataValue::from(f64::PI())
    );
    assert_eq!(
        op_rad_to_deg(&[DataValue::from(f64::PI())]).unwrap(),
        DataValue::from(180.0)
    );
}

#[test]
fn test_first_last() {
    assert_eq!(
        op_first(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null,
    );
    assert_eq!(
        op_last(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null,
    );
    assert_eq!(
        op_first(&[DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
        ])])
        .unwrap(),
        DataValue::from(1),
    );
    assert_eq!(
        op_last(&[DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
        ])])
        .unwrap(),
        DataValue::from(2),
    );
}

#[test]
fn test_chunks() {
    assert_eq!(
        op_chunks(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(2),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::List(vec![DataValue::from(3), DataValue::from(4)]),
            DataValue::List(vec![DataValue::from(5)]),
        ])
    );
    assert_eq!(
        op_chunks_exact(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(2),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::List(vec![DataValue::from(3), DataValue::from(4)]),
        ])
    );
    assert_eq!(
        op_windows(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(3),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
            ]),
            DataValue::List(vec![
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
            ]),
            DataValue::List(vec![
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
        ])
    )
}

#[test]
fn test_get() {
    assert!(op_get(&[DataValue::List(vec![]), DataValue::from(0)]).is_err());
    assert_eq!(
        op_get(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
            ]),
            DataValue::from(1)
        ])
        .unwrap(),
        DataValue::from(2)
    );
    assert_eq!(
        op_maybe_get(&[DataValue::List(vec![]), DataValue::from(0)]).unwrap(),
        DataValue::Null
    );
    assert_eq!(
        op_maybe_get(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
            ]),
            DataValue::from(1)
        ])
        .unwrap(),
        DataValue::from(2)
    );
}

#[test]
fn test_slice() {
    assert!(op_slice(&[
        DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
        ]),
        DataValue::from(1),
        DataValue::from(4)
    ])
    .is_err());

    assert!(op_slice(&[
        DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
        ]),
        DataValue::from(1),
        DataValue::from(3)
    ])
    .is_ok());

    assert_eq!(
        op_slice(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
            ]),
            DataValue::from(1),
            DataValue::from(-1)
        ])
        .unwrap(),
        DataValue::List(vec![DataValue::from(2)])
    );
}

#[test]
fn test_chars() {
    assert_eq!(
        op_from_substrings(&[op_chars(&[DataValue::Str("abc".into())]).unwrap()]).unwrap(),
        DataValue::Str("abc".into())
    )
}

#[test]
fn test_encode_decode() {
    assert_eq!(
        op_decode_base64(&[op_encode_base64(&[DataValue::Bytes([1, 2, 3].into())]).unwrap()])
            .unwrap(),
        DataValue::Bytes([1, 2, 3].into())
    )
}

#[test]
fn test_to_string() {
    assert_eq!(
        op_to_string(&[DataValue::from(false)]).unwrap(),
        DataValue::Str("false".into())
    );
}

#[test]
fn test_to_unity() {
    assert_eq!(op_to_unity(&[DataValue::Null]).unwrap(), DataValue::from(0));
    assert_eq!(
        op_to_unity(&[DataValue::from(false)]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_to_unity(&[DataValue::from(true)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_to_unity(&[DataValue::from(10)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_to_unity(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_to_unity(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_to_unity(&[DataValue::Str("0".into())]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_to_unity(&[DataValue::Str("".into())]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_to_unity(&[DataValue::List(vec![])]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_to_unity(&[DataValue::List(vec![DataValue::Null])]).unwrap(),
        DataValue::from(1)
    );
}

#[test]
fn test_to_float() {
    assert_eq!(
        op_to_float(&[DataValue::Null]).unwrap(),
        DataValue::from(0.0)
    );
    assert_eq!(
        op_to_float(&[DataValue::from(false)]).unwrap(),
        DataValue::from(0.0)
    );
    assert_eq!(
        op_to_float(&[DataValue::from(true)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_to_float(&[DataValue::from(1)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_to_float(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(1.0)
    );
    assert!(op_to_float(&[DataValue::Str("NAN".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_nan());
    assert!(op_to_float(&[DataValue::Str("INF".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_infinite());
    assert!(op_to_float(&[DataValue::Str("NEG_INF".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_infinite());
    assert_eq!(
        op_to_float(&[DataValue::Str("3".into())])
            .unwrap()
            .get_float()
            .unwrap(),
        3.
    );
}

#[test]
fn test_rand() {
    let n = op_rand_float(&[]).unwrap().get_float().unwrap();
    assert!(n >= 0.);
    assert!(n <= 1.);
    assert_eq!(
        op_rand_bernoulli(&[DataValue::from(0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_rand_bernoulli(&[DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert!(op_rand_bernoulli(&[DataValue::from(2)]).is_err());
    let n = op_rand_int(&[DataValue::from(100), DataValue::from(200)])
        .unwrap()
        .get_int()
        .unwrap();
    assert!(n >= 100);
    assert!(n <= 200);
    assert_eq!(
        op_rand_choose(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null
    );
    assert_eq!(
        op_rand_choose(&[DataValue::List(vec![DataValue::from(123)])]).unwrap(),
        DataValue::from(123)
    );
}

#[test]
fn test_set_ops() {
    assert_eq!(
        op_union(&[
            DataValue::List([1, 2, 3].into_iter().map(DataValue::from).collect()),
            DataValue::List([2, 3, 4].into_iter().map(DataValue::from).collect()),
            DataValue::List([3, 4, 5].into_iter().map(DataValue::from).collect())
        ])
        .unwrap(),
        DataValue::List([1, 2, 3, 4, 5].into_iter().map(DataValue::from).collect())
    );
    assert_eq!(
        op_intersection(&[
            DataValue::List(
                [1, 2, 3, 4, 5, 6]
                    .into_iter()
                    .map(DataValue::from)
                    .collect(),
            ),
            DataValue::List([2, 3, 4].into_iter().map(DataValue::from).collect()),
            DataValue::List([3, 4, 5].into_iter().map(DataValue::from).collect())
        ])
        .unwrap(),
        DataValue::List([3, 4].into_iter().map(DataValue::from).collect())
    );
    assert_eq!(
        op_difference(&[
            DataValue::List(
                [1, 2, 3, 4, 5, 6]
                    .into_iter()
                    .map(DataValue::from)
                    .collect(),
            ),
            DataValue::List([2, 3, 4].into_iter().map(DataValue::from).collect()),
            DataValue::List([3, 4, 5].into_iter().map(DataValue::from).collect())
        ])
        .unwrap(),
        DataValue::List([1, 6].into_iter().map(DataValue::from).collect())
    );
}

#[test]
fn test_uuid() {
    let v1 = op_rand_uuid_v1(&[]).unwrap();
    let v4 = op_rand_uuid_v4(&[]).unwrap();
    assert!(op_is_uuid(&[v4]).unwrap().get_bool().unwrap());
    assert!(op_uuid_timestamp(&[v1]).unwrap().get_float().is_some());
    assert!(op_to_uuid(&[DataValue::from("")]).is_err());
    assert!(op_to_uuid(&[DataValue::from("f3b4958c-52a1-11e7-802a-010203040506")]).is_ok());
}

#[test]
fn test_now() {
    let now = op_now(&[]).unwrap();
    assert!(matches!(now, DataValue::Num(_)));
    let s = op_format_timestamp(&[now]).unwrap();
    let _dt = op_parse_timestamp(&[s]).unwrap();
}

#[test]
fn test_to_bool() {
    assert_eq!(
        op_to_bool(&[DataValue::Null]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from(true)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from(false)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from(0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from(0.0)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from(1)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from("")]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::from("a")]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_to_bool(&[DataValue::List(vec![])]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_to_bool(&[DataValue::List(vec![DataValue::from(0)])]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_coalesce() {
    let db = DbInstance::default();
    let res = db.run_default("?[a] := a = null ~ 1 ~ 2").unwrap().rows;
    assert_eq!(res[0][0], DataValue::from(1));
    let res = db
        .run_default("?[a] := a = null ~ null ~ null")
        .unwrap()
        .rows;
    assert_eq!(res[0][0], DataValue::Null);
    let res = db.run_default("?[a] := a = 2 ~ null ~ 1").unwrap().rows;
    assert_eq!(res[0][0], DataValue::from(2));
}

#[test]
fn test_range() {
    let db = DbInstance::default();
    let res = db
        .run_default("?[a] := a = int_range(1, 5)")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0][0], json!([1, 2, 3, 4]));
    let res = db
        .run_default("?[a] := a = int_range(5)")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0][0], json!([0, 1, 2, 3, 4]));
    let res = db
        .run_default("?[a] := a = int_range(15, 3, -2)")
        .unwrap()
        .into_json();
    assert_eq!(res["rows"][0][0], json!([15, 13, 11, 9, 7, 5]));
}

#[test]
fn test_date_time_functions() {
    // Test to_local_parts and from_local_parts
    let ts = 1704067200.0; // 2024-01-01 00:00:00 UTC
    let parts = op_to_local_parts(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap();
    if let DataValue::Json(JsonData(json)) = parts {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 1);
        assert_eq!(json["hour"], 0);
        assert_eq!(json["minute"], 0);
        assert_eq!(json["second"], 0);
        assert_eq!(json["dow"], 1); // Monday
        assert_eq!(json["yday"], 1);
    } else {
        panic!("Expected JSON result");
    }

    // Test from_local_parts
    let ts_back = op_from_local_parts(&[
        DataValue::from(2024),
        DataValue::from(1),
        DataValue::from(1),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from("UTC"),
    ]).unwrap();
    assert_eq!(ts_back, DataValue::from(ts));

    // Test year
    assert_eq!(
        op_year(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(2024)
    );

    // Test month
    assert_eq!(
        op_month(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(1)
    );

    // Test day
    assert_eq!(
        op_day(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(1)
    );

    // Test dow
    assert_eq!(
        op_dow(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(1) // Monday
    );

    // Test hour
    assert_eq!(
        op_hour(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(0)
    );

    // Test minute
    assert_eq!(
        op_minute(&[DataValue::from(ts), DataValue::from("UTC")]).unwrap(),
        DataValue::from(0)
    );

    // Test days_in_month
    assert_eq!(
        op_days_in_month(&[DataValue::from(2024), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(29) // Leap year
    );
    assert_eq!(
        op_days_in_month(&[DataValue::from(2023), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(28) // Non-leap year
    );

    // Test start_of_day_local
    let ts_middle = 1704110400.0; // 2024-01-01 12:00:00 UTC
    let start = op_start_of_day_local(&[DataValue::from(ts_middle), DataValue::from("UTC")]).unwrap();
    assert_eq!(start, DataValue::from(ts));
}

#[test]
fn test_interval_functions() {
    // Test interval
    let iv = op_interval(&[DataValue::from(10), DataValue::from(20)]).unwrap();
    assert_eq!(
        iv,
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)])
    );

    // Test interval_len
    assert_eq!(
        op_interval_len(&[iv.clone()]).unwrap(),
        DataValue::from(10)
    );

    // Test interval_intersects
    let iv2 = DataValue::List(vec![DataValue::from(15), DataValue::from(25)]);
    assert_eq!(
        op_interval_intersects(&[iv.clone(), iv2.clone()]).unwrap(),
        DataValue::from(true)
    );

    let iv3 = DataValue::List(vec![DataValue::from(25), DataValue::from(35)]);
    assert_eq!(
        op_interval_intersects(&[iv.clone(), iv3.clone()]).unwrap(),
        DataValue::from(false)
    );

    // Test interval_overlap
    let overlap = op_interval_overlap(&[iv.clone(), iv2.clone()]).unwrap();
    assert_eq!(
        overlap,
        DataValue::List(vec![DataValue::from(15), DataValue::from(20)])
    );

    // Test interval_union
    let union = op_interval_union(&[iv.clone(), iv2.clone()]).unwrap();
    assert_eq!(
        union,
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(10), DataValue::from(25)])
        ])
    );

    // Test interval_minus
    let minus = op_interval_minus(&[iv.clone(), DataValue::List(vec![DataValue::from(12), DataValue::from(18)])]).unwrap();
    assert_eq!(
        minus,
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(10), DataValue::from(12)]),
            DataValue::List(vec![DataValue::from(18), DataValue::from(20)])
        ])
    );

    // Test interval_adjacent
    let iv4 = DataValue::List(vec![DataValue::from(20), DataValue::from(30)]);
    assert_eq!(
        op_interval_adjacent(&[iv.clone(), iv4.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test interval_merge_adjacent
    let intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(20), DataValue::from(30)]),
        DataValue::List(vec![DataValue::from(35), DataValue::from(40)]),
    ]);
    let merged = op_interval_merge_adjacent(&[intervals]).unwrap();
    assert_eq!(
        merged,
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(10), DataValue::from(30)]),
            DataValue::List(vec![DataValue::from(35), DataValue::from(40)])
        ])
    );

    // Test interval_shift
    let shifted = op_interval_shift(&[iv.clone(), DataValue::from(5)]).unwrap();
    assert_eq!(
        shifted,
        DataValue::List(vec![DataValue::from(15), DataValue::from(25)])
    );

    // Test interval_contains
    assert_eq!(
        op_interval_contains(&[iv.clone(), DataValue::from(15)]).unwrap(),
        DataValue::from(true)
    );
    assert_eq!(
        op_interval_contains(&[iv.clone(), DataValue::from(25)]).unwrap(),
        DataValue::from(false)
    );

    // Test interval_contains_interval
    let small_iv = DataValue::List(vec![DataValue::from(12), DataValue::from(18)]);
    assert_eq!(
        op_interval_contains_interval(&[iv.clone(), small_iv]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_allen_interval_algebra() {
    let a = DataValue::List(vec![DataValue::from(10), DataValue::from(20)]);
    let b = DataValue::List(vec![DataValue::from(25), DataValue::from(35)]);
    let c = DataValue::List(vec![DataValue::from(20), DataValue::from(30)]);
    let d = DataValue::List(vec![DataValue::from(15), DataValue::from(25)]);
    let e = DataValue::List(vec![DataValue::from(10), DataValue::from(15)]);
    let f = DataValue::List(vec![DataValue::from(12), DataValue::from(18)]);
    let g = DataValue::List(vec![DataValue::from(15), DataValue::from(20)]);

    // Test allen_before
    assert_eq!(
        op_allen_before(&[a.clone(), b.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_meets
    assert_eq!(
        op_allen_meets(&[a.clone(), c.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_overlaps
    assert_eq!(
        op_allen_overlaps(&[a.clone(), d.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_starts
    assert_eq!(
        op_allen_starts(&[e.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_during
    assert_eq!(
        op_allen_during(&[f.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_finishes
    assert_eq!(
        op_allen_finishes(&[g.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test allen_equals
    assert_eq!(
        op_allen_equals(&[a.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test inverse relations
    assert_eq!(
        op_allen_after(&[b.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_allen_met_by(&[c.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_allen_overlapped_by(&[d.clone(), a.clone()]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_allen_started_by(&[a.clone(), e.clone()]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_allen_contains(&[a.clone(), f.clone()]).unwrap(),
        DataValue::from(true)
    );

    assert_eq!(
        op_allen_finished_by(&[a.clone(), g.clone()]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_utility_functions() {
    // Test normalize_intervals
    let intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(15), DataValue::from(25)]),
        DataValue::List(vec![DataValue::from(30), DataValue::from(40)]),
    ]);
    let normalized = op_normalize_intervals(&[intervals]).unwrap();

    if let DataValue::List(result) = normalized {
        assert_eq!(result.len(), 2); // Should merge first two, keep third separate
    } else {
        panic!("Expected list result");
    }

    // Test intervals_minus
    let main_intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(30)]),
        DataValue::List(vec![DataValue::from(40), DataValue::from(60)]),
    ]);
    let sub_intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(15), DataValue::from(25)]),
    ]);
    let result = op_intervals_minus(&[main_intervals, sub_intervals]).unwrap();

    if let DataValue::List(intervals) = result {
        assert!(intervals.len() >= 2); // Should have multiple intervals after subtraction
    } else {
        panic!("Expected list result");
    }

    // Test nth_weekday_of_month
    let result = op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(1),
        DataValue::from(1), // Monday
        DataValue::from(1), // First Monday
        DataValue::from("UTC"),
    ]).unwrap();

    if let DataValue::Json(JsonData(json)) = result {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 1); // Jan 1, 2024 was a Monday
    } else {
        panic!("Expected JSON result");
    }

    // Test bucket functions
    assert_eq!(
        op_bucket_of(&[DataValue::from(100), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(3) // (100 - 0) / 30 = 3
    );

    assert_eq!(
        op_bucket_start(&[DataValue::from(3), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(90) // 0 + 3 * 30 = 90
    );

    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(100), DataValue::from(30)]).unwrap(),
        DataValue::from(4) // ceiling(100 / 30) = 4
    );
}

#[test]
fn test_time_utility_functions() {
    // Test local_minutes_to_parts
    let base_midnight = 1704067200; // 2024-01-01 00:00:00 UTC
    let result = op_local_minutes_to_parts(&[
        DataValue::from(base_midnight),
        DataValue::from(90), // 1.5 hours
        DataValue::from("UTC"),
    ]).unwrap();

    if let DataValue::Json(JsonData(json)) = result {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 1);
        assert_eq!(json["hour"], 1);
        assert_eq!(json["minute"], 30);
    } else {
        panic!("Expected JSON result");
    }

    // Test parts_to_instant_utc
    let parts = DataValue::Json(JsonData(json!({
        "year": 2024,
        "month": 1,
        "day": 1,
        "hour": 12,
        "minute": 30
    })));
    let result = op_parts_to_instant_utc(&[parts, DataValue::from("UTC")]).unwrap();

    if let DataValue::Num(ts) = result {
        let expected = 1704067200 + 12 * 3600 + 30 * 60; // noon + 30 min
        assert_eq!(ts.get_int(), Some(expected));
    } else {
        panic!("Expected numeric timestamp");
    }
}

#[test]
fn test_enhanced_timestamp_edge_cases() {
    // Test DST transitions with America/New_York
    let dst_start_2024 = 1710054000.0; // 2024-03-10 07:00:00 UTC (2am EST -> 3am EDT)
    let parts = op_to_local_parts(&[DataValue::from(dst_start_2024), DataValue::from("America/New_York")]).unwrap();
    if let DataValue::Json(JsonData(json)) = parts {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 3);
        assert_eq!(json["day"], 10);
        assert_eq!(json["hour"], 3); // DST jump forward
    }

    // Test leap year handling
    let leap_day_2024 = 1709164800.0; // 2024-02-29 00:00:00 UTC
    let parts = op_to_local_parts(&[DataValue::from(leap_day_2024), DataValue::from("UTC")]).unwrap();
    if let DataValue::Json(JsonData(json)) = parts {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 2);
        assert_eq!(json["day"], 29);
    }

    // Test leap year vs non-leap year days_in_month
    assert_eq!(
        op_days_in_month(&[DataValue::from(2024), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(29)
    );
    assert_eq!(
        op_days_in_month(&[DataValue::from(2023), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(28)
    );

    // Test century years (divisible by 100 but not 400)
    assert_eq!(
        op_days_in_month(&[DataValue::from(1900), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(28)
    );
    assert_eq!(
        op_days_in_month(&[DataValue::from(2000), DataValue::from(2), DataValue::from("UTC")]).unwrap(),
        DataValue::from(29)
    );

    // Test year boundaries
    let new_year_2024 = 1704067200.0; // 2024-01-01 00:00:00 UTC
    assert_eq!(
        op_year(&[DataValue::from(new_year_2024), DataValue::from("UTC")]).unwrap(),
        DataValue::from(2024)
    );

    let new_year_eve_2023 = 1704067199.0; // 2023-12-31 23:59:59 UTC
    assert_eq!(
        op_year(&[DataValue::from(new_year_eve_2023), DataValue::from("UTC")]).unwrap(),
        DataValue::from(2023)
    );

    // Test month boundaries for all 12 months
    for month in 1..=12 {
        let result = op_days_in_month(&[DataValue::from(2024), DataValue::from(month), DataValue::from("UTC")]).unwrap();
        if let DataValue::Num(days) = result {
            let expected = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 => 29, // 2024 is leap year
                _ => panic!("Invalid month"),
            };
            assert_eq!(days.get_int(), Some(expected));
        }
    }

    // Test day of week for known dates
    let monday_jan_1_2024 = 1704067200.0; // 2024-01-01 was Monday
    assert_eq!(
        op_dow(&[DataValue::from(monday_jan_1_2024), DataValue::from("UTC")]).unwrap(),
        DataValue::from(1)
    );

    let sunday_jan_7_2024 = 1704585600.0; // 2024-01-07 was Sunday
    assert_eq!(
        op_dow(&[DataValue::from(sunday_jan_7_2024), DataValue::from("UTC")]).unwrap(),
        DataValue::from(7)
    );

    // Test start_of_day_local with different timezones
    // Input: 1704081600 = Jan 1, 2024 04:00:00 UTC = Dec 31, 2023 20:00:00 PST
    // Expected: Start of Dec 31, 2023 in LA = Dec 31, 2023 00:00:00 PST = Dec 31, 2023 08:00:00 UTC = 1704009600
    let ts_pacific = 1704067200.0; // 2024-01-01 00:00:00 UTC
    let start_pacific = op_start_of_day_local(&[DataValue::from(ts_pacific + 14400.0), DataValue::from("America/Los_Angeles")]).unwrap();
    let expected_pacific = ts_pacific - 16.0 * 3600.0; // Dec 31, 2023 08:00:00 UTC (midnight PST)
    assert_eq!(start_pacific, DataValue::from(expected_pacific));

    // Test from_local_parts with invalid dates (should handle gracefully)
    assert!(op_from_local_parts(&[
        DataValue::from(2024),
        DataValue::from(2),
        DataValue::from(30), // February 30th doesn't exist
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from("UTC"),
    ]).is_err());

    // Test invalid timezone handling
    assert!(op_to_local_parts(&[DataValue::from(1704067200.0), DataValue::from("Invalid/Timezone")]).is_err());

    // Test extreme dates
    let unix_epoch = 0.0;
    let parts = op_to_local_parts(&[DataValue::from(unix_epoch), DataValue::from("UTC")]).unwrap();
    if let DataValue::Json(JsonData(json)) = parts {
        assert_eq!(json["year"], 1970);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 1);
        assert_eq!(json["hour"], 0);
        assert_eq!(json["minute"], 0);
        assert_eq!(json["second"], 0);
        assert_eq!(json["dow"], 4); // Thursday
        assert_eq!(json["yday"], 1);
    }

    // Test negative timestamps (before Unix epoch)
    let before_epoch = -86400.0; // 1969-12-31 00:00:00 UTC
    let parts = op_to_local_parts(&[DataValue::from(before_epoch), DataValue::from("UTC")]).unwrap();
    if let DataValue::Json(JsonData(json)) = parts {
        assert_eq!(json["year"], 1969);
        assert_eq!(json["month"], 12);
        assert_eq!(json["day"], 31);
    }

    // Test hour/minute edge cases
    let end_of_day = 1704153599.0; // 2024-01-01 23:59:59 UTC
    assert_eq!(
        op_hour(&[DataValue::from(end_of_day), DataValue::from("UTC")]).unwrap(),
        DataValue::from(23)
    );
    assert_eq!(
        op_minute(&[DataValue::from(end_of_day), DataValue::from("UTC")]).unwrap(),
        DataValue::from(59)
    );
}

#[test]
fn test_interval_edge_cases() {
    // Test zero-length intervals
    let zero_interval = op_interval(&[DataValue::from(10), DataValue::from(10)]);
    assert!(zero_interval.is_err()); // Should reject zero-length intervals

    // Test invalid intervals (end before start)
    let invalid_interval = op_interval(&[DataValue::from(20), DataValue::from(10)]);
    assert!(invalid_interval.is_err());

    // Test minimal valid interval
    let minimal_iv = op_interval(&[DataValue::from(10), DataValue::from(11)]).unwrap();
    assert_eq!(
        op_interval_len(&[minimal_iv.clone()]).unwrap(),
        DataValue::from(1)
    );

    // Test large intervals
    let large_iv = op_interval(&[DataValue::from(0), DataValue::from(1000000)]).unwrap();
    assert_eq!(
        op_interval_len(&[large_iv.clone()]).unwrap(),
        DataValue::from(1000000)
    );

    // Test negative timestamps
    let negative_iv = op_interval(&[DataValue::from(-1000), DataValue::from(-500)]).unwrap();
    assert_eq!(
        op_interval_len(&[negative_iv.clone()]).unwrap(),
        DataValue::from(500)
    );

    // Test interval_intersects edge cases
    let iv1 = DataValue::List(vec![DataValue::from(10), DataValue::from(20)]);
    let iv2 = DataValue::List(vec![DataValue::from(20), DataValue::from(30)]); // Adjacent, not intersecting
    assert_eq!(
        op_interval_intersects(&[iv1.clone(), iv2.clone()]).unwrap(),
        DataValue::from(false)
    );

    let iv3 = DataValue::List(vec![DataValue::from(19), DataValue::from(21)]); // Minimal overlap
    assert_eq!(
        op_interval_intersects(&[iv1.clone(), iv3.clone()]).unwrap(),
        DataValue::from(true)
    );

    // Test interval_overlap with no overlap
    assert_eq!(
        op_interval_overlap(&[iv1.clone(), iv2.clone()]).unwrap(),
        DataValue::Null
    );

    // Test interval_overlap with minimal overlap
    let overlap = op_interval_overlap(&[iv1.clone(), iv3.clone()]).unwrap();
    assert_eq!(
        overlap,
        DataValue::List(vec![DataValue::from(19), DataValue::from(20)])
    );

    // Test interval_union with non-overlapping intervals
    let union = op_interval_union(&[iv1.clone(), iv2.clone()]).unwrap();
    if let DataValue::List(intervals) = union {
        assert_eq!(intervals.len(), 2); // Should return both intervals separately
    }

    // Test interval_union with overlapping intervals
    let union_overlap = op_interval_union(&[iv1.clone(), iv3.clone()]).unwrap();
    if let DataValue::List(intervals) = union_overlap {
        assert_eq!(intervals.len(), 1); // Should merge into one interval
        assert_eq!(
            intervals[0],
            DataValue::List(vec![DataValue::from(10), DataValue::from(21)])
        );
    }

    // Test interval_minus edge cases
    // Complete subtraction (result is empty)
    let complete_minus = op_interval_minus(&[iv1.clone(), DataValue::List(vec![DataValue::from(5), DataValue::from(25)])]).unwrap();
    if let DataValue::List(result) = complete_minus {
        assert_eq!(result.len(), 0);
    }

    // No overlap subtraction (result is original)
    let no_overlap_minus = op_interval_minus(&[iv1.clone(), DataValue::List(vec![DataValue::from(25), DataValue::from(35)])]).unwrap();
    if let DataValue::List(result) = no_overlap_minus {
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], iv1);
    }

    // Edge subtraction (subtract from beginning)
    let edge_minus = op_interval_minus(&[iv1.clone(), DataValue::List(vec![DataValue::from(5), DataValue::from(15)])]).unwrap();
    if let DataValue::List(result) = edge_minus {
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            DataValue::List(vec![DataValue::from(15), DataValue::from(20)])
        );
    }

    // Test interval_adjacent with non-adjacent intervals
    let non_adjacent = DataValue::List(vec![DataValue::from(25), DataValue::from(35)]);
    assert_eq!(
        op_interval_adjacent(&[iv1.clone(), non_adjacent]).unwrap(),
        DataValue::from(false)
    );

    // Test interval_merge_adjacent with overlapping intervals
    let overlapping_intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(15), DataValue::from(25)]),
        DataValue::List(vec![DataValue::from(22), DataValue::from(30)]),
        DataValue::List(vec![DataValue::from(40), DataValue::from(50)]),
    ]);
    let merged = op_interval_merge_adjacent(&[overlapping_intervals]).unwrap();
    if let DataValue::List(result) = merged {
        assert_eq!(result.len(), 2); // Should merge first three, keep last separate
        assert_eq!(
            result[0],
            DataValue::List(vec![DataValue::from(10), DataValue::from(30)])
        );
        assert_eq!(
            result[1],
            DataValue::List(vec![DataValue::from(40), DataValue::from(50)])
        );
    }

    // Test interval_shift with negative and positive offsets
    let shifted_negative = op_interval_shift(&[iv1.clone(), DataValue::from(-5)]).unwrap();
    assert_eq!(
        shifted_negative,
        DataValue::List(vec![DataValue::from(5), DataValue::from(15)])
    );

    let shifted_zero = op_interval_shift(&[iv1.clone(), DataValue::from(0)]).unwrap();
    assert_eq!(shifted_zero, iv1);

    // Test interval_contains boundary conditions
    assert_eq!(
        op_interval_contains(&[iv1.clone(), DataValue::from(10)]).unwrap(),
        DataValue::from(true) // Start is inclusive
    );
    assert_eq!(
        op_interval_contains(&[iv1.clone(), DataValue::from(20)]).unwrap(),
        DataValue::from(false) // End is exclusive
    );
    assert_eq!(
        op_interval_contains(&[iv1.clone(), DataValue::from(9)]).unwrap(),
        DataValue::from(false)
    );
    assert_eq!(
        op_interval_contains(&[iv1.clone(), DataValue::from(21)]).unwrap(),
        DataValue::from(false)
    );

    // Test interval_contains_interval boundary conditions
    let exact_same = DataValue::List(vec![DataValue::from(10), DataValue::from(20)]);
    assert_eq!(
        op_interval_contains_interval(&[iv1.clone(), exact_same]).unwrap(),
        DataValue::from(true)
    );

    let slightly_larger = DataValue::List(vec![DataValue::from(9), DataValue::from(21)]);
    assert_eq!(
        op_interval_contains_interval(&[iv1.clone(), slightly_larger]).unwrap(),
        DataValue::from(false)
    );

    let boundary_start = DataValue::List(vec![DataValue::from(10), DataValue::from(15)]);
    assert_eq!(
        op_interval_contains_interval(&[iv1.clone(), boundary_start]).unwrap(),
        DataValue::from(true)
    );

    let boundary_end = DataValue::List(vec![DataValue::from(15), DataValue::from(20)]);
    assert_eq!(
        op_interval_contains_interval(&[iv1.clone(), boundary_end]).unwrap(),
        DataValue::from(true)
    );

    // Test with very large numbers
    let large_iv1 = DataValue::List(vec![DataValue::from(1000000000), DataValue::from(2000000000)]);
    let large_iv2 = DataValue::List(vec![DataValue::from(1500000000), DataValue::from(2100000000)]);
    assert_eq!(
        op_interval_intersects(&[large_iv1, large_iv2]).unwrap(),
        DataValue::from(true)
    );
}

#[test]
fn test_allen_interval_algebra_edge_cases() {
    // Define test intervals for comprehensive Allen algebra testing
    let a = DataValue::List(vec![DataValue::from(10), DataValue::from(20)]); // [10, 20)
    let b = DataValue::List(vec![DataValue::from(25), DataValue::from(35)]); // [25, 35) - after
    let c = DataValue::List(vec![DataValue::from(20), DataValue::from(30)]); // [20, 30) - meets
    let d = DataValue::List(vec![DataValue::from(15), DataValue::from(25)]); // [15, 25) - overlaps
    let e = DataValue::List(vec![DataValue::from(10), DataValue::from(15)]); // [10, 15) - starts
    let f = DataValue::List(vec![DataValue::from(12), DataValue::from(18)]); // [12, 18) - during
    let g = DataValue::List(vec![DataValue::from(15), DataValue::from(20)]); // [15, 20) - finishes
    let h = DataValue::List(vec![DataValue::from(10), DataValue::from(20)]); // [10, 20) - equals

    // Test allen_before and its edge cases
    assert_eq!(op_allen_before(&[a.clone(), b.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_before(&[a.clone(), c.clone()]).unwrap(), DataValue::from(false)); // meets, not before
    assert_eq!(op_allen_before(&[b.clone(), a.clone()]).unwrap(), DataValue::from(false));

    // Test allen_after (inverse of before)
    assert_eq!(op_allen_after(&[b.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_after(&[a.clone(), b.clone()]).unwrap(), DataValue::from(false));

    // Test allen_meets boundary condition
    assert_eq!(op_allen_meets(&[a.clone(), c.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_meets(&[a.clone(), b.clone()]).unwrap(), DataValue::from(false)); // gap between them

    // Test allen_met_by (inverse of meets)
    assert_eq!(op_allen_met_by(&[c.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_met_by(&[a.clone(), c.clone()]).unwrap(), DataValue::from(false));

    // Test allen_overlaps with various overlap scenarios
    assert_eq!(op_allen_overlaps(&[a.clone(), d.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_overlaps(&[d.clone(), a.clone()]).unwrap(), DataValue::from(false)); // Wrong direction
    assert_eq!(op_allen_overlaps(&[a.clone(), c.clone()]).unwrap(), DataValue::from(false)); // meets, not overlaps

    // Test allen_overlapped_by (inverse of overlaps)
    assert_eq!(op_allen_overlapped_by(&[d.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_overlapped_by(&[a.clone(), d.clone()]).unwrap(), DataValue::from(false));

    // Test allen_starts edge cases
    assert_eq!(op_allen_starts(&[e.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_starts(&[a.clone(), e.clone()]).unwrap(), DataValue::from(false));
    assert_eq!(op_allen_starts(&[f.clone(), a.clone()]).unwrap(), DataValue::from(false)); // during, not starts

    // Test allen_started_by (inverse of starts)
    assert_eq!(op_allen_started_by(&[a.clone(), e.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_started_by(&[e.clone(), a.clone()]).unwrap(), DataValue::from(false));

    // Test allen_during with nested intervals
    assert_eq!(op_allen_during(&[f.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_during(&[a.clone(), f.clone()]).unwrap(), DataValue::from(false));
    assert_eq!(op_allen_during(&[e.clone(), a.clone()]).unwrap(), DataValue::from(false)); // starts, not during
    assert_eq!(op_allen_during(&[g.clone(), a.clone()]).unwrap(), DataValue::from(false)); // finishes, not during

    // Test allen_contains (inverse of during)
    assert_eq!(op_allen_contains(&[a.clone(), f.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_contains(&[f.clone(), a.clone()]).unwrap(), DataValue::from(false));

    // Test allen_finishes edge cases
    assert_eq!(op_allen_finishes(&[g.clone(), a.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_finishes(&[a.clone(), g.clone()]).unwrap(), DataValue::from(false));
    assert_eq!(op_allen_finishes(&[f.clone(), a.clone()]).unwrap(), DataValue::from(false)); // during, not finishes

    // Test allen_finished_by (inverse of finishes)
    assert_eq!(op_allen_finished_by(&[a.clone(), g.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_finished_by(&[g.clone(), a.clone()]).unwrap(), DataValue::from(false));

    // Test allen_equals
    assert_eq!(op_allen_equals(&[a.clone(), h.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_equals(&[a.clone(), b.clone()]).unwrap(), DataValue::from(false));
    assert_eq!(op_allen_equals(&[h.clone(), a.clone()]).unwrap(), DataValue::from(true)); // Symmetric

    // Test edge cases with minimal intervals
    let minimal1 = DataValue::List(vec![DataValue::from(10), DataValue::from(11)]);
    let minimal2 = DataValue::List(vec![DataValue::from(11), DataValue::from(12)]);
    assert_eq!(op_allen_meets(&[minimal1.clone(), minimal2.clone()]).unwrap(), DataValue::from(true));
    assert_eq!(op_allen_before(&[minimal1.clone(), minimal2.clone()]).unwrap(), DataValue::from(false));

    // Test with negative intervals
    let neg1 = DataValue::List(vec![DataValue::from(-20), DataValue::from(-10)]);
    let neg2 = DataValue::List(vec![DataValue::from(-15), DataValue::from(-5)]);
    assert_eq!(op_allen_overlaps(&[neg1.clone(), neg2.clone()]).unwrap(), DataValue::from(true));

    // Test mixed positive/negative intervals
    let mixed1 = DataValue::List(vec![DataValue::from(-10), DataValue::from(10)]);
    let mixed2 = DataValue::List(vec![DataValue::from(5), DataValue::from(15)]);
    assert_eq!(op_allen_overlaps(&[mixed1.clone(), mixed2.clone()]).unwrap(), DataValue::from(true));

    // Test all 13 Allen relations are mutually exclusive
    // For any two intervals, exactly one relation should be true
    let test_intervals = vec![
        (a.clone(), b.clone()),
        (a.clone(), c.clone()),
        (a.clone(), d.clone()),
        (a.clone(), e.clone()),
        (a.clone(), f.clone()),
        (a.clone(), g.clone()),
        (a.clone(), h.clone()),
    ];

    for (iv1, iv2) in test_intervals {
        let relations = vec![
            op_allen_before(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_meets(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_overlaps(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_starts(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_during(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_finishes(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_equals(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_finished_by(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_contains(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_started_by(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_overlapped_by(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_met_by(&[iv1.clone(), iv2.clone()]).unwrap(),
            op_allen_after(&[iv1.clone(), iv2.clone()]).unwrap(),
        ];

        let true_count = relations.iter().filter(|&r| r == &DataValue::from(true)).count();
        assert_eq!(true_count, 1, "Exactly one Allen relation should be true for each pair");
    }

    // Test symmetry for equals relation
    assert_eq!(
        op_allen_equals(&[a.clone(), h.clone()]).unwrap(),
        op_allen_equals(&[h.clone(), a.clone()]).unwrap()
    );

    // Test inverse relations consistency
    assert_eq!(
        op_allen_before(&[a.clone(), b.clone()]).unwrap(),
        op_allen_after(&[b.clone(), a.clone()]).unwrap()
    );

    assert_eq!(
        op_allen_meets(&[a.clone(), c.clone()]).unwrap(),
        op_allen_met_by(&[c.clone(), a.clone()]).unwrap()
    );

    assert_eq!(
        op_allen_overlaps(&[a.clone(), d.clone()]).unwrap(),
        op_allen_overlapped_by(&[d.clone(), a.clone()]).unwrap()
    );

    assert_eq!(
        op_allen_starts(&[e.clone(), a.clone()]).unwrap(),
        op_allen_started_by(&[a.clone(), e.clone()]).unwrap()
    );

    assert_eq!(
        op_allen_during(&[f.clone(), a.clone()]).unwrap(),
        op_allen_contains(&[a.clone(), f.clone()]).unwrap()
    );

    assert_eq!(
        op_allen_finishes(&[g.clone(), a.clone()]).unwrap(),
        op_allen_finished_by(&[a.clone(), g.clone()]).unwrap()
    );
}

#[test]
fn test_advanced_utility_functions_edge_cases() {
    // Test normalize_intervals with various edge cases

    // Empty input
    let empty_intervals = DataValue::List(vec![]);
    let normalized = op_normalize_intervals(&[empty_intervals]).unwrap();
    assert_eq!(normalized, DataValue::List(vec![]));

    // Single interval
    let single_interval = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)])
    ]);
    let normalized = op_normalize_intervals(&[single_interval.clone()]).unwrap();
    assert_eq!(normalized, single_interval);

    // Identical intervals (should merge)
    let identical_intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
    ]);
    let normalized = op_normalize_intervals(&[identical_intervals]).unwrap();
    if let DataValue::List(result) = normalized {
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], DataValue::List(vec![DataValue::from(10), DataValue::from(20)]));
    }

    // Already sorted and non-overlapping
    let sorted_non_overlapping = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(15)]),
        DataValue::List(vec![DataValue::from(20), DataValue::from(25)]),
        DataValue::List(vec![DataValue::from(30), DataValue::from(35)]),
    ]);
    let normalized = op_normalize_intervals(&[sorted_non_overlapping.clone()]).unwrap();
    assert_eq!(normalized, sorted_non_overlapping);

    // Unsorted intervals
    let unsorted_intervals = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(30), DataValue::from(35)]),
        DataValue::List(vec![DataValue::from(10), DataValue::from(15)]),
        DataValue::List(vec![DataValue::from(20), DataValue::from(25)]),
    ]);
    let normalized = op_normalize_intervals(&[unsorted_intervals]).unwrap();
    if let DataValue::List(result) = normalized {
        assert_eq!(result.len(), 3);
        // Should be sorted by start time
        assert_eq!(result[0], DataValue::List(vec![DataValue::from(10), DataValue::from(15)]));
        assert_eq!(result[1], DataValue::List(vec![DataValue::from(20), DataValue::from(25)]));
        assert_eq!(result[2], DataValue::List(vec![DataValue::from(30), DataValue::from(35)]));
    }

    // Complex overlapping scenario
    let complex_overlapping = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(30)]),
        DataValue::List(vec![DataValue::from(20), DataValue::from(40)]),
        DataValue::List(vec![DataValue::from(35), DataValue::from(50)]),
        DataValue::List(vec![DataValue::from(60), DataValue::from(70)]),
        DataValue::List(vec![DataValue::from(65), DataValue::from(75)]),
    ]);
    let normalized = op_normalize_intervals(&[complex_overlapping]).unwrap();
    if let DataValue::List(result) = normalized {
        assert_eq!(result.len(), 2); // Should merge to [10,50) and [60,75)
        assert_eq!(result[0], DataValue::List(vec![DataValue::from(10), DataValue::from(50)]));
        assert_eq!(result[1], DataValue::List(vec![DataValue::from(60), DataValue::from(75)]));
    }

    // Test intervals_minus with edge cases

    // Empty main intervals
    let empty_main = DataValue::List(vec![]);
    let some_subs = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)])
    ]);
    let result = op_intervals_minus(&[empty_main, some_subs]).unwrap();
    assert_eq!(result, DataValue::List(vec![]));

    // Empty subtraction intervals
    let some_main = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)])
    ]);
    let empty_subs = DataValue::List(vec![]);
    let result = op_intervals_minus(&[some_main.clone(), empty_subs]).unwrap();
    assert_eq!(result, some_main);

    // No overlap between main and sub intervals
    let non_overlapping_main = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(30), DataValue::from(40)]),
    ]);
    let non_overlapping_subs = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(50), DataValue::from(60)])
    ]);
    let result = op_intervals_minus(&[non_overlapping_main.clone(), non_overlapping_subs]).unwrap();
    assert_eq!(result, non_overlapping_main);

    // Complete subtraction (all main intervals covered)
    let covered_main = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(10), DataValue::from(20)]),
        DataValue::List(vec![DataValue::from(25), DataValue::from(35)]),
    ]);
    let covering_subs = DataValue::List(vec![
        DataValue::List(vec![DataValue::from(5), DataValue::from(25)]),
        DataValue::List(vec![DataValue::from(20), DataValue::from(40)]),
    ]);
    let result = op_intervals_minus(&[covered_main, covering_subs]).unwrap();
    assert_eq!(result, DataValue::List(vec![]));

    // Test nth_weekday_of_month edge cases

    // Test invalid month
    assert!(op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(13), // Invalid month
        DataValue::from(1),
        DataValue::from(1),
        DataValue::from("UTC"),
    ]).is_err());

    // Test invalid weekday
    assert!(op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(1),
        DataValue::from(8), // Invalid weekday (should be 1-7)
        DataValue::from(1),
        DataValue::from("UTC"),
    ]).is_err());

    // Test invalid n (should be ±1..±5)
    assert!(op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(1),
        DataValue::from(1),
        DataValue::from(6), // Invalid n
        DataValue::from("UTC"),
    ]).is_err());

    // Test last occurrence that doesn't exist
    let result = op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(2), // February
        DataValue::from(1), // Monday
        DataValue::from(-5), // 5th from last (February 2024 only has 4 Mondays)
        DataValue::from("UTC"),
    ]).unwrap();
    assert_eq!(result, DataValue::Null);

    // Test first Monday of various months in 2024
    let months_first_monday = vec![
        (1, 1),   // January 1, 2024 was Monday
        (2, 5),   // February 5, 2024 was first Monday
        (3, 4),   // March 4, 2024 was first Monday
        (4, 1),   // April 1, 2024 was first Monday
        (5, 6),   // May 6, 2024 was first Monday
        (6, 3),   // June 3, 2024 was first Monday
    ];

    for (month, expected_day) in months_first_monday {
        let result = op_nth_weekday_of_month(&[
            DataValue::from(2024),
            DataValue::from(month),
            DataValue::from(1), // Monday
            DataValue::from(1), // First occurrence
            DataValue::from("UTC"),
        ]).unwrap();

        if let DataValue::Json(JsonData(json)) = result {
            assert_eq!(json["year"], 2024);
            assert_eq!(json["month"], month);
            assert_eq!(json["day"], expected_day);
        } else {
            panic!("Expected JSON result for month {}", month);
        }
    }

    // Test last Friday of various months
    let result = op_nth_weekday_of_month(&[
        DataValue::from(2024),
        DataValue::from(1),
        DataValue::from(5), // Friday
        DataValue::from(-1), // Last occurrence
        DataValue::from("UTC"),
    ]).unwrap();

    if let DataValue::Json(JsonData(json)) = result {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 26); // January 26, 2024 was the last Friday
    }

    // Test expand_weekly_days edge cases

    // Empty weekday set
    let empty_wday_set = DataValue::List(vec![]);
    let result = op_expand_weekly_days(&[
        DataValue::from(1704067200), // 2024-01-01 00:00:00 UTC
        DataValue::from(1704672000), // 2024-01-08 00:00:00 UTC
        empty_wday_set,
        DataValue::from("UTC"),
        DataValue::from(480), // 8:00 AM
        DataValue::from(540), // 9:00 AM
    ]).unwrap();
    assert_eq!(result, DataValue::List(vec![]));

    // Test with all weekdays
    let all_weekdays = DataValue::List(vec![
        DataValue::from(1), DataValue::from(2), DataValue::from(3),
        DataValue::from(4), DataValue::from(5), DataValue::from(6), DataValue::from(7)
    ]);
    let result = op_expand_weekly_days(&[
        DataValue::from(1704067200), // 2024-01-01 00:00:00 UTC (Monday)
        DataValue::from(1704672000), // 2024-01-08 00:00:00 UTC (Monday)
        all_weekdays,
        DataValue::from("UTC"),
        DataValue::from(0),   // midnight
        DataValue::from(60),  // 1:00 AM
    ]).unwrap();

    if let DataValue::List(intervals) = result {
        assert_eq!(intervals.len(), 7); // One for each day of the week
    }

    // Test local_minutes_to_parts edge cases

    // Test with negative minutes (previous day)
    let base_midnight = 1704067200; // 2024-01-01 00:00:00 UTC
    let result = op_local_minutes_to_parts(&[
        DataValue::from(base_midnight),
        DataValue::from(-60), // -1 hour (23:00 previous day)
        DataValue::from("UTC"),
    ]).unwrap();

    if let DataValue::Json(JsonData(json)) = result {
        assert_eq!(json["year"], 2023);
        assert_eq!(json["month"], 12);
        assert_eq!(json["day"], 31);
        assert_eq!(json["hour"], 23);
        assert_eq!(json["minute"], 0);
    }

    // Test with minutes beyond 24 hours (next day)
    let result = op_local_minutes_to_parts(&[
        DataValue::from(base_midnight),
        DataValue::from(1500), // 25 hours = 1 day + 1 hour
        DataValue::from("UTC"),
    ]).unwrap();

    if let DataValue::Json(JsonData(json)) = result {
        assert_eq!(json["year"], 2024);
        assert_eq!(json["month"], 1);
        assert_eq!(json["day"], 2);
        assert_eq!(json["hour"], 1);
        assert_eq!(json["minute"], 0);
    }

    // Test parts_to_instant_utc with invalid parts
    let invalid_parts = DataValue::Json(JsonData(json!({
        "year": 2024,
        "month": 2,
        "day": 30, // February 30th doesn't exist
        "hour": 12,
        "minute": 0
    })));
    assert!(op_parts_to_instant_utc(&[invalid_parts, DataValue::from("UTC")]).is_err());

    // Test parts_to_instant_utc with edge time values
    let edge_parts = DataValue::Json(JsonData(json!({
        "year": 2024,
        "month": 12,
        "day": 31,
        "hour": 23,
        "minute": 59
    })));
    let result = op_parts_to_instant_utc(&[edge_parts, DataValue::from("UTC")]).unwrap();

    if let DataValue::Num(ts) = result {
        // Should be just before 2025-01-01 00:00:00 UTC
        let expected = 1735689540; // 2024-12-31 23:59:00 UTC
        assert_eq!(ts.get_int(), Some(expected));
    }
}

#[test]
fn test_bucket_functions_edge_cases() {
    // Test bucket_of with various edge cases

    // Basic bucket calculation
    assert_eq!(
        op_bucket_of(&[DataValue::from(100), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(3) // (100 - 0) / 30 = 3.33 -> floor = 3
    );

    // Test with exact bucket boundary
    assert_eq!(
        op_bucket_of(&[DataValue::from(90), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(3) // (90 - 0) / 30 = 3.0 -> exactly bucket 3
    );

    // Test with negative epoch
    assert_eq!(
        op_bucket_of(&[DataValue::from(50), DataValue::from(30), DataValue::from(-60)]).unwrap(),
        DataValue::from(3) // (50 - (-60)) / 30 = 110 / 30 = 3.67 -> floor = 3
    );

    // Test with timestamp before epoch
    assert_eq!(
        op_bucket_of(&[DataValue::from(-10), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(-1) // (-10 - 0) / 30 = -0.33 -> floor = -1
    );

    // Test with timestamp exactly at epoch
    assert_eq!(
        op_bucket_of(&[DataValue::from(0), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(0) // (0 - 0) / 30 = 0
    );

    // Test bucket_start consistency with bucket_of
    let timestamp = 175;
    let period = 60;
    let epoch = 0;

    let bucket_num = op_bucket_of(&[DataValue::from(timestamp), DataValue::from(period), DataValue::from(epoch)]).unwrap();
    let bucket_start = op_bucket_start(&[bucket_num.clone(), DataValue::from(period), DataValue::from(epoch)]).unwrap();

    // bucket_start should be <= original timestamp
    if let (DataValue::Num(start), DataValue::Num(orig)) = (&bucket_start, &DataValue::from(timestamp)) {
        assert!(start.get_int().unwrap() <= orig.get_int().unwrap());
    }

    // Next bucket start should be > original timestamp
    let next_bucket = DataValue::from(bucket_num.get_int().unwrap() + 1);
    let next_bucket_start = op_bucket_start(&[next_bucket, DataValue::from(period), DataValue::from(epoch)]).unwrap();
    if let DataValue::Num(next_start) = next_bucket_start {
        assert!(next_start.get_int().unwrap() > timestamp);
    }

    // Test bucket_start with negative bucket numbers
    assert_eq!(
        op_bucket_start(&[DataValue::from(-2), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(-60) // 0 + (-2) * 30 = -60
    );

    // Test bucket_start with non-zero epoch
    assert_eq!(
        op_bucket_start(&[DataValue::from(5), DataValue::from(20), DataValue::from(100)]).unwrap(),
        DataValue::from(200) // 100 + 5 * 20 = 200
    );

    // Test ceil_to_bucket edge cases

    // Timestamp exactly on bucket boundary
    assert_eq!(
        op_ceil_to_bucket(&[DataValue::from(90), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(90) // Already on boundary, no change
    );

    // Timestamp slightly above bucket boundary
    assert_eq!(
        op_ceil_to_bucket(&[DataValue::from(91), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(120) // Next bucket starts at 120
    );

    // Timestamp below epoch
    assert_eq!(
        op_ceil_to_bucket(&[DataValue::from(-10), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(0) // Next bucket boundary after -10 is 0
    );

    // Test floor_to_bucket edge cases

    // Timestamp exactly on bucket boundary
    assert_eq!(
        op_floor_to_bucket(&[DataValue::from(90), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(90) // Already on boundary, no change
    );

    // Timestamp slightly above bucket boundary
    assert_eq!(
        op_floor_to_bucket(&[DataValue::from(91), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(90) // Current bucket starts at 90
    );

    // Timestamp below epoch
    assert_eq!(
        op_floor_to_bucket(&[DataValue::from(-10), DataValue::from(30), DataValue::from(0)]).unwrap(),
        DataValue::from(-30) // Previous bucket starts at -30
    );

    // Test duration_in_buckets edge cases

    // Exact multiple of period
    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(120), DataValue::from(30)]).unwrap(),
        DataValue::from(4) // 120 / 30 = 4 exactly
    );

    // Duration not exact multiple
    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(125), DataValue::from(30)]).unwrap(),
        DataValue::from(5) // ceiling(125 / 30) = ceiling(4.17) = 5
    );

    // Zero duration
    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(0), DataValue::from(30)]).unwrap(),
        DataValue::from(0) // 0 duration = 0 buckets
    );

    // Very small duration
    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(1), DataValue::from(30)]).unwrap(),
        DataValue::from(1) // ceiling(1 / 30) = 1
    );

    // Test with larger periods and timestamps

    // Daily buckets (86400 seconds per day)
    let daily_period = 86400;
    let week_duration = 7 * daily_period;

    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(week_duration), DataValue::from(daily_period)]).unwrap(),
        DataValue::from(7) // 7 days exactly
    );

    // Weekly buckets
    let weekly_period = 7 * daily_period;
    let month_duration = 30 * daily_period;

    assert_eq!(
        op_duration_in_buckets(&[DataValue::from(month_duration), DataValue::from(weekly_period)]).unwrap(),
        DataValue::from(5) // ceiling(30 / 7) = 5 weeks
    );

    // Test bucket consistency: bucket_of(bucket_start(k)) should equal k
    for k in -5..=5 {
        let period = 60;
        let epoch = 100;

        let start = op_bucket_start(&[DataValue::from(k), DataValue::from(period), DataValue::from(epoch)]).unwrap();
        let bucket_back = op_bucket_of(&[start, DataValue::from(period), DataValue::from(epoch)]).unwrap();

        assert_eq!(bucket_back.get_int(), Some(k));
    }

    // Test rounding consistency
    let test_cases = vec![
        (100, 30, 0),   // Basic case
        (125, 40, 10),  // With non-zero epoch
        (-50, 25, -100), // Negative timestamp and epoch
        (0, 1, 0),      // Minimal period
    ];

    for (timestamp, period, epoch) in test_cases {
        let floor_bucket = op_floor_to_bucket(&[DataValue::from(timestamp), DataValue::from(period), DataValue::from(epoch)]).unwrap();
        let ceil_bucket = op_ceil_to_bucket(&[DataValue::from(timestamp), DataValue::from(period), DataValue::from(epoch)]).unwrap();

        // floor should be <= timestamp
        assert!(floor_bucket.get_int().unwrap() <= timestamp);

        // ceil should be >= timestamp
        assert!(ceil_bucket.get_int().unwrap() >= timestamp);

        // If timestamp is on bucket boundary, floor == ceil == timestamp
        if (timestamp - epoch) % period == 0 {
            assert_eq!(floor_bucket.get_int(), Some(timestamp));
            assert_eq!(ceil_bucket.get_int(), Some(timestamp));
        } else {
            // Otherwise, ceil should be exactly one period ahead of floor
            assert_eq!(ceil_bucket.get_int().unwrap() - floor_bucket.get_int().unwrap(), period);
        }
    }

    // Test edge cases with very large numbers
    let large_timestamp = 1000000000;
    let large_period = 86400; // 1 day
    let large_epoch = 0;

    let bucket_num = op_bucket_of(&[DataValue::from(large_timestamp), DataValue::from(large_period), DataValue::from(large_epoch)]).unwrap();
    assert!(bucket_num.get_int().unwrap() > 0);

    let start = op_bucket_start(&[bucket_num.clone(), DataValue::from(large_period), DataValue::from(large_epoch)]).unwrap();
    assert!(start.get_int().unwrap() <= large_timestamp);
    assert!(start.get_int().unwrap() + large_period > large_timestamp);

    // Test error conditions

    // Zero period should cause division by zero error
    assert!(op_bucket_of(&[DataValue::from(100), DataValue::from(0), DataValue::from(0)]).is_err());
    assert!(op_duration_in_buckets(&[DataValue::from(100), DataValue::from(0)]).is_err());

    // Negative period should be handled appropriately
    assert!(op_bucket_of(&[DataValue::from(100), DataValue::from(-30), DataValue::from(0)]).is_err());

    // Negative duration should be handled appropriately
    assert!(op_duration_in_buckets(&[DataValue::from(-100), DataValue::from(30)]).is_err());
}

#[test]
fn test_expand_daily() {
    // Test basic daily expansion for a week in January 2025
    // January 1, 2025 00:00:00 UTC = 1735689600000 ms
    // January 8, 2025 00:00:00 UTC = 1736294400000 ms
    let start_ms = 1735689600000_i64;
    let end_ms = 1736294400000_i64;

    // Expand 9:00 AM to 5:00 PM (540 to 1020 minutes from midnight)
    let result = op_expand_daily(&[
        DataValue::from(540),   // h0 = 9:00 AM
        DataValue::from(1020),  // h1 = 5:00 PM
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result {
        // Should have 7 intervals (Jan 1-7)
        assert_eq!(intervals.len(), 7);

        // Check first interval (Jan 1 9:00-17:00 UTC)
        if let DataValue::List(first_iv) = &intervals[0] {
            let iv_start = first_iv[0].get_int().unwrap();
            let iv_end = first_iv[1].get_int().unwrap();
            // Jan 1, 2025 9:00 UTC = 1735722000000 ms
            assert_eq!(iv_start, 1735722000000);
            // Jan 1, 2025 17:00 UTC = 1735750800000 ms
            assert_eq!(iv_end, 1735750800000);
        } else {
            panic!("Expected list for interval");
        }
    } else {
        panic!("Expected list result");
    }

    // Test with timezone (America/Chicago = CST/CDT)
    let result_tz = op_expand_daily(&[
        DataValue::from(540),   // 9:00 AM local
        DataValue::from(1020),  // 5:00 PM local
        DataValue::from("America/Chicago"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result_tz {
        assert!(intervals.len() > 0);
        // In CST (UTC-6), 9:00 AM local = 15:00 UTC
        if let DataValue::List(first_iv) = &intervals[0] {
            let iv_start = first_iv[0].get_int().unwrap();
            // Jan 1, 2025 15:00 UTC = 1735743600000 ms
            assert_eq!(iv_start, 1735743600000);
        }
    }

    // Test empty range (end before start)
    let result_empty = op_expand_daily(&[
        DataValue::from(540),
        DataValue::from(1020),
        DataValue::from("UTC"),
        DataValue::from(end_ms),
        DataValue::from(start_ms), // swapped
    ]).unwrap();

    if let DataValue::List(intervals) = result_empty {
        assert_eq!(intervals.len(), 0);
    }

    // Test partial overlap at start of range
    // Range starts after 9:00 AM on Jan 1
    let late_start_ms = 1735730000000_i64; // Jan 1, 2025 11:13 UTC
    let result_partial = op_expand_daily(&[
        DataValue::from(540),   // 9:00 AM
        DataValue::from(1020),  // 5:00 PM
        DataValue::from("UTC"),
        DataValue::from(late_start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result_partial {
        // Should still include Jan 1 interval since it overlaps
        assert!(intervals.len() >= 7);
    }
}

#[test]
fn test_expand_monthly() {
    // Test monthly expansion for Q1 2025
    // January 1, 2025 00:00:00 UTC = 1735689600000 ms
    // April 1, 2025 00:00:00 UTC = 1743465600000 ms
    let start_ms = 1735689600000_i64;
    let end_ms = 1743465600000_i64;

    // Expand day 15 of each month, 10:00-11:00 AM
    let result = op_expand_monthly(&[
        DataValue::from(15),    // day_of_month
        DataValue::from(600),   // h0 = 10:00 AM
        DataValue::from(660),   // h1 = 11:00 AM
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result {
        // Should have 3 intervals (Jan 15, Feb 15, Mar 15)
        assert_eq!(intervals.len(), 3);

        // Check first interval (Jan 15 10:00-11:00 UTC)
        if let DataValue::List(first_iv) = &intervals[0] {
            let iv_start = first_iv[0].get_int().unwrap();
            let iv_end = first_iv[1].get_int().unwrap();
            // Jan 15, 2025 10:00 UTC = 1736935200000 ms
            assert_eq!(iv_start, 1736935200000);
            // Jan 15, 2025 11:00 UTC = 1736938800000 ms
            assert_eq!(iv_end, 1736938800000);
        }
    } else {
        panic!("Expected list result");
    }

    // Test day 31 clamping for February
    let result_31 = op_expand_monthly(&[
        DataValue::from(31),    // day_of_month = 31
        DataValue::from(840),   // h0 = 14:00
        DataValue::from(900),   // h1 = 15:00
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result_31 {
        // Should have 3 intervals (Jan 31, Feb 28, Mar 31)
        assert_eq!(intervals.len(), 3);

        // Check February interval (clamped to Feb 28, 2025)
        if let DataValue::List(feb_iv) = &intervals[1] {
            let iv_start = feb_iv[0].get_int().unwrap();
            // Feb 28, 2025 14:00 UTC = 1740751200000 ms
            assert_eq!(iv_start, 1740751200000);
        }
    }

    // Test invalid day_of_month
    assert!(op_expand_monthly(&[
        DataValue::from(0),     // Invalid
        DataValue::from(600),
        DataValue::from(660),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());

    assert!(op_expand_monthly(&[
        DataValue::from(32),    // Invalid
        DataValue::from(600),
        DataValue::from(660),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());
}

#[test]
fn test_expand_yearly() {
    // Test yearly expansion for 2024-2027
    // January 1, 2024 00:00:00 UTC = 1704067200000 ms
    // January 1, 2028 00:00:00 UTC = 1830297600000 ms
    let start_ms = 1704067200000_i64;
    let end_ms = 1830297600000_i64;

    // Expand December 25 (Christmas) as all-day event
    let result = op_expand_yearly(&[
        DataValue::from(12),    // month = December
        DataValue::from(25),    // day = 25
        DataValue::from(0),     // h0 = 00:00
        DataValue::from(1440),  // h1 = 24:00
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result {
        // Should have 4 intervals (2024, 2025, 2026, 2027)
        assert_eq!(intervals.len(), 4);

        // Check first interval (Dec 25, 2024 00:00-24:00 UTC)
        if let DataValue::List(first_iv) = &intervals[0] {
            let iv_start = first_iv[0].get_int().unwrap();
            let iv_end = first_iv[1].get_int().unwrap();
            // Dec 25, 2024 00:00 UTC = 1735084800000 ms
            assert_eq!(iv_start, 1735084800000);
            // Dec 26, 2024 00:00 UTC = 1735171200000 ms
            assert_eq!(iv_end, 1735171200000);
        }
    } else {
        panic!("Expected list result");
    }

    // Test leap day (Feb 29) - should only appear in leap years
    let result_leap = op_expand_yearly(&[
        DataValue::from(2),     // month = February
        DataValue::from(29),    // day = 29
        DataValue::from(540),   // h0 = 09:00
        DataValue::from(1020),  // h1 = 17:00
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result_leap {
        // Should have 1 interval (2024 is leap year, 2025-2027 are not)
        assert_eq!(intervals.len(), 1);

        // Check that it's Feb 29, 2024
        if let DataValue::List(leap_iv) = &intervals[0] {
            let iv_start = leap_iv[0].get_int().unwrap();
            // Feb 29, 2024 09:00 UTC = 1709197200000 ms
            assert_eq!(iv_start, 1709197200000);
        }
    }

    // Test July 4th (Independence Day) with timezone
    let result_tz = op_expand_yearly(&[
        DataValue::from(7),     // month = July
        DataValue::from(4),     // day = 4
        DataValue::from(0),     // h0 = 00:00 local
        DataValue::from(1440),  // h1 = 24:00 local
        DataValue::from("America/Chicago"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).unwrap();

    if let DataValue::List(intervals) = result_tz {
        // Should have 4 intervals (2024, 2025, 2026, 2027)
        assert_eq!(intervals.len(), 4);

        // July is in CDT (UTC-5), so midnight local = 05:00 UTC
        if let DataValue::List(first_iv) = &intervals[0] {
            let iv_start = first_iv[0].get_int().unwrap();
            // July 4, 2024 00:00 CDT = July 4, 2024 05:00 UTC = 1720069200000 ms
            assert_eq!(iv_start, 1720069200000);
        }
    }

    // Test invalid month
    assert!(op_expand_yearly(&[
        DataValue::from(0),     // Invalid
        DataValue::from(25),
        DataValue::from(0),
        DataValue::from(1440),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());

    assert!(op_expand_yearly(&[
        DataValue::from(13),    // Invalid
        DataValue::from(25),
        DataValue::from(0),
        DataValue::from(1440),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());

    // Test invalid day
    assert!(op_expand_yearly(&[
        DataValue::from(12),
        DataValue::from(0),     // Invalid
        DataValue::from(0),
        DataValue::from(1440),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());

    assert!(op_expand_yearly(&[
        DataValue::from(12),
        DataValue::from(32),    // Invalid
        DataValue::from(0),
        DataValue::from(1440),
        DataValue::from("UTC"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());

    // Test invalid timezone
    assert!(op_expand_yearly(&[
        DataValue::from(12),
        DataValue::from(25),
        DataValue::from(0),
        DataValue::from(1440),
        DataValue::from("Invalid/Timezone"),
        DataValue::from(start_ms),
        DataValue::from(end_ms),
    ]).is_err());
}
