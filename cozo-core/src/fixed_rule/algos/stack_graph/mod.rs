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
use crate::fixed_rule::algos::stack_graph::stack_graph_info::StackGraphInfo;
use crate::FixedRule;
use crate::FixedRulePayload;
use crate::Poison;
use crate::RegularTempStore;
use crate::SourceSpan;
use crate::Symbol;

mod stack_graph_storage_error;
mod augoor_urn;
mod stack_graph_info;

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
        let repository_param = payload.get_input(0)?.ensure_min_len(1)?;
        let repository_tuples = repository_param.iter()?.filter_map(Result::ok).collect_vec();

        let starting_param = payload.get_input(1)?.ensure_min_len(1)?;
        let starting_tuple = starting_param.iter()?.filter_map(Result::ok).nth(0).ok_or(InvalidTuple)?;

        let reference_urn_string = payload.string_option("reference_urn", None)?;
        let reference_urn = AugoorUrn::from_str(reference_urn_string.as_str()).expect("Invalid URN");

        // Reads input graph
        let stack_graph_info = StackGraphInfo::try_from(starting_tuple)?;
        let stack_graph = stack_graph_info.read_stack_graph()?;

        // Some placeholder code that demonstrates using a stack graph
        // This exists only temporarily to demonstrate that:
        // - stack graphs can be deserialized
        // - the deserialized stack graph works
        // - it is possible to use an Augoor KG URN as a starting point for a query
        stack_graph.iter_nodes().for_each(|handle: Handle<Node>| {
            if let Some(byte_range) = get_node_byte_range(&stack_graph, handle) {
                let has_urn = reference_urn.node_has_urn(&stack_graph, handle);
                if has_urn {
                    let found_urn = AugoorUrn::new(stack_graph_info.blob_id.clone(), byte_range);
                    out.put(vec![
                        DataValue::from(found_urn.to_string())
                    ]);
                }
            }
        });

        Ok(())
    }
}
