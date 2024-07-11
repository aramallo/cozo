use std::collections::BTreeMap;

use log::debug;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::{
    DataValue, Expr, FixedRule, FixedRulePayload, Poison, RegularTempStore, SourceSpan, Symbol,
};

mod error;
mod query;
mod source_pos;
mod state;
mod tuples;

use error::{Error, SourcePosError};
use query::{Querier, ResolutionKind};
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
        Ok(3)
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

        let root_path_blobs = payload.get_input(2)?
            .ensure_min_len(3)?
            .iter()?
            .map(|tuple| tuple.map_err(E::tuple_report)?.try_into());

        let root_path_symbol_stacks_files =
            if let Ok(root_path_symbol_stacks_files) = payload.get_input(3) {
                Some(root_path_symbol_stacks_files
                    .ensure_min_len(2)?
                    .iter()?
                    .map(|tuple| tuple.map_err(E::tuple_report)?.try_into()))
            } else {
                None
            };
        let output_missing_files = root_path_symbol_stacks_files.is_some();
        let mut state = state::State::new(graph_blobs, node_path_blobs, root_path_blobs,
            root_path_symbol_stacks_files.map_or_else::<Box<dyn Iterator<Item = _>>, _, _>(
                || Box::new(std::iter::empty()),
                |files| Box::new(files),
            )
        )?;

        debug!(" ↳ Initialized state for StackGraphQuery fixed rule");

        let references = payload
            .expr_option("references", None)?
            .eval_to_const()
            .map_err(|e| Error::SourcePos(SourcePosError::Other(e)))?;
        let references = references
            .get_slice()
            .ok_or(Error::SourcePos(SourcePosError::InvalidType { expected: "list of strings" }))?
            .iter()
            .map(|d| d.get_str()
                .ok_or(Error::SourcePos(SourcePosError::InvalidType { expected: "string" })))
            .collect::<Result<Vec<_>, _>>()?;
        let source_poss = references
            .into_iter()
            .map(|s| s.parse::<SourcePos>()
                .map_err(|e| Error::SourcePos(
                    SourcePosError::Parse { got: s.into(), source: e })))
            .collect::<Result<Vec<_>, _>>()?;

        let output_missing_files = output_missing_files && payload
            .bool_option("output_missing_files", Some(true))
            .map_err(|_| Error::OutputMissingFiles)?;
        debug!(
            " ↳ {}utputting missing files from StackGraphQuery fixed rule...",
            if output_missing_files { "O" } else { "Not o" },
        );

        debug!(
            " ↳ Got reference source positions {:?} for StackGraphQuery fixed rule...",
            SourcePoss(&source_poss),
        );

        let mut querier = Querier::new(&mut state);
        for resolution in querier.definitions(
            &source_poss,
            output_missing_files,
            &PoisonCancellation(poison),
        )? {
            match resolution.kind {
                ResolutionKind::Definition(definition) => out.put(vec![
                    resolution.reference.to_string().into(),
                    definition.to_string().into(),
                    DataValue::Null,
                ]),
                ResolutionKind::MissingFile(file_id) => out.put(vec![
                    resolution.reference.to_string().into(),
                    DataValue::Null,
                    file_id.as_ref().into(),
                ]),
            }
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

struct SourcePoss<'s>(&'s [SourcePos]);

impl std::fmt::Debug for SourcePoss<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(std::string::ToString::to_string))
            .finish()
    }
}
