use std::collections::BTreeMap;

use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::{
    DataValue, Expr, FixedRule, FixedRulePayload, Poison, RegularTempStore, SourceSpan, Symbol,
};

mod augoor_urn;
mod blobs;
mod error;
mod state;

use augoor_urn::AugoorUrn;
use error::Error;

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
        _poison: Poison,
    ) -> Result<()> {
        use Error as E;

        // Input parameters
        let graph_blobs = payload.get_input(0)?.ensure_min_len(2)?;
        let graph_blobs = graph_blobs
            .iter()?
            .map(|tuple| tuple.map_err(|e| E::Misc(format!("{e:#}")))?.try_into());

        let node_path_blobs = payload.get_input(1)?.ensure_min_len(3)?;
        let node_path_blobs = node_path_blobs
            .iter()?
            .map(|tuple| tuple.map_err(|e| E::Misc(format!("{e:#}")))?.try_into());

        let root_path_blobs = payload.get_input(2)?.ensure_min_len(3)?;
        let root_path_blobs = root_path_blobs
            .iter()?
            .map(|tuple| tuple.map_err(|e| E::Misc(format!("{e:#}")))?.try_into());

        let mut state = state::State::new(graph_blobs, node_path_blobs, root_path_blobs)?;

        let reference_urn_string = payload.string_option("reference_urn", None)?;
        let reference_urn = reference_urn_string
            .parse::<AugoorUrn>()
            .expect("Invalid URN");

        if let Some(definition_urn) = state.get_definition_urn(&reference_urn) {
            out.put(vec![DataValue::from(definition_urn.to_string())])
        }

        Ok(())
    }
}

struct PoisonCancellation(Poison);

impl stack_graphs::CancellationFlag for PoisonCancellation {
    fn check(&self, at: &'static str) -> Result<(), stack_graphs::CancellationError> {
        self.0
            .check()
            .map_err(|_| stack_graphs::CancellationError(at))
    }
}
