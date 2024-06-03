use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;
use smartstring::LazyCompact;
use smartstring::SmartString;
use stack_graphs::arena::Handle;
use stack_graphs::graph::Node;

use crate::DataValue;
use crate::Expr;
use crate::fixed_rule::algos::stack_graph::augoor_urn::{AugoorUrn, get_node_byte_range};
use crate::fixed_rule::algos::stack_graph::stack_graph_utils::deserialize_stack_graph;
use crate::FixedRule;
use crate::FixedRulePayload;
use crate::Poison;
use crate::RegularTempStore;
use crate::SourceSpan;
use crate::Symbol;

mod stack_graph_storage_error;
mod augoor_urn;
mod stack_graph_utils;

pub(crate) struct StackGraphQuery;

impl FixedRule for StackGraphQuery {
    /// Returns the row width for the returned relation
    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(1)
    }

    /// The outputs tuples are written to `out`
    /// It must check `poison` periodically for user-initiated termination.
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        // Input parameters
        let graph_rows = payload.get_input(0)?.ensure_min_len(1)?;
        let reference_urn_string = payload.string_option("reference_urn", None)?;
        let reference_urn = AugoorUrn::from_str(reference_urn_string.as_str()).expect("Invalid URN");

        for tuple in graph_rows.iter()?.filter_map(Result::ok) {
            let [repository_id, blob_id, tag, error, graph] = tuple.iter().collect_vec()[..] else {
                // TODO: log error
                break;
            };

            let buffer = graph.get_bytes().unwrap();
            let stack_graph = deserialize_stack_graph(buffer)?;

            stack_graph.iter_nodes().for_each(|handle: Handle<Node>| {
                if let Some(byte_range) = get_node_byte_range(&stack_graph, handle) {
                    let has_urn = reference_urn.node_has_urn(&stack_graph, handle);
                    if has_urn {
                        let found_urn = AugoorUrn::new(String::from(blob_id.get_str().unwrap()), byte_range);
                        out.put(vec![
                            DataValue::from(found_urn.to_string())
                        ]);
                    }
                }
            });
        }

        Ok(())
    }
}
