use std::collections::BTreeMap;

use log::debug;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::{
    DataValue, Expr, FixedRule, FixedRulePayload, Poison, RegularTempStore, SourceSpan, Symbol,
};

mod blobs;
mod error;
mod query;
mod source_pos;
mod state;

use error::Error;
use query::Querier;
use source_pos::SourcePos;

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

        debug!("Starting StackGraphQuery fixed rule...");

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

        debug!(" ↳ Initialized state for StackGraphQuery fixed rule");

        let source_pos = payload.string_option("reference", None)?;
        let source_pos = source_pos
            .parse::<SourcePos>()
            .map_err(|e| Error::InvalidSourcePos {
                got: source_pos.into(),
                source: e,
            })?;

        debug!(
            " ↳ Got reference source position \"{source_pos}\" for StackGraphQuery fixed rule..."
        );

        let mut querier = Querier::new(&mut state);
        for def_urn in querier.definitions(&source_pos, &PoisonCancellation(poison))? {
            out.put(vec![DataValue::from(def_urn.to_string())])
        }

        debug!(" ↳ Finished running StackGraphQuery fixed rule");

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

fn pluralize(count: usize, singular: &'static str) -> String {
    // TODO: Irregular pluralization (i.e. not just with “s”, like in “query/queries”)
    format!("{count} {singular}{}", if count == 1 { "" } else { "s" })
}
