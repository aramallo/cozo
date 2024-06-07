use std::collections::BTreeMap;

use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::{
    DataValue, Expr, FixedRule, FixedRulePayload, Poison, RegularTempStore, SourceSpan, Symbol,
};

mod source_pos;
mod blobs;
mod error;
mod query;
mod state;

use source_pos::SourcePos;
use error::Error;
use query::Querier;

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
        use Error as E;

        // Input parameters
        let graph_blobs = payload.get_input(0)?.ensure_min_len(2)?;
        let graph_blobs = graph_blobs
            .iter()?
            .map(|tuple| tuple.map_err(E::tuple_report)?.try_into());

        let node_path_blobs = payload.get_input(1)?.ensure_min_len(3)?;
        let node_path_blobs = node_path_blobs
            .iter()?
            .map(|tuple| tuple.map_err(E::tuple_report)?.try_into());

        let root_path_blobs = payload.get_input(2)?.ensure_min_len(3)?;
        let root_path_blobs = root_path_blobs
            .iter()?
            .map(|tuple| tuple.map_err(E::tuple_report)?.try_into());

        let mut state = state::State::new(graph_blobs, node_path_blobs, root_path_blobs)?;

        let ref_urn = payload.string_option("reference_urn", None)?;
        let ref_urn = ref_urn
            .parse::<SourcePos>()
            .map_err(|e| Error::InvalidSourcePos { got: ref_urn.into(), source: e })?;

        let mut querier = Querier::new(&mut state);
        for def_urn in querier.definitions(&ref_urn, &PoisonCancellation(poison))? {
            out.put(vec![DataValue::from(def_urn.to_string())])
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
