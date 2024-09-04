use std::collections::BTreeMap;

use log::debug;
use miette::Result;
use smartstring::{LazyCompact, SmartString};
use stack_graphs::{CancellationError, CancellationFlag};

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

/// A Cozo fixed rule that implements querying a Stack Graph.
///
/// ## Input
///
/// Takes as input a series of relations that contain binary blobs that
/// represent the serialized stack graph and its partial paths (each “blob”
/// field below may optionally be compressed using Zstd, in which case the
/// blob is expected to start with the big-endian magic number `0x28b52ffd`):
///
/// ### Positional parameters
///
/// - graphs:
///   - file path (string);
///   - uncompressed blob size in bytes (unsigned integer);
///   - blob (bytes).
/// - node paths:
///   - file path (string);
///   - start node local ID (unsigned integer);
///   - uncompressed blob size in bytes (unsigned integer);
///   - blob (bytes);
/// - root paths:
///   - file path (string);
///   - symbol stack (string);
///   - uncompressed blob size in bytes (unsigned integer);
///   - blob (bytes);
/// - root paths index (optional):
///   - symbol stack (string);
///   - file path (string);
///
/// The first three parameters are required, and can either contain the entire
/// multi-file stack graph, or a smaller subgraph over a subset of files.
/// Definitions will be found as long as they are in this subgraph.
///
/// The fourth parameter is an index of symbol stacks to file paths over a
/// larger set of files (typically the entire set). It may be omitted, but
/// enables iterative querying, further detailed below.
///
/// ### Named option parameters
///
/// - `references` (list of strings): the references for which definitions are
///   being queried;
/// - `output_missing_files` (boolean, optional): whether or not the output may
///   include any missing file paths (defaults to true if the optional fourth
///   positional parameter is given, otherwise defaults to false).
///
/// Each reference in `references` has the following format:
///
/// ```norust
/// {file path}:{start byte}:{end byte}
/// ```
///
/// Where `file path` is the path of the file where the reference is found, and
/// `start byte` and `end byte` are the UTF-8 or UTF-16 byte offsets of the
/// start and end of the reference within that file (this encoding must match
/// the encoding of the file itself at the time of indexing).
///
/// ## Output
///
/// Returns as output a 3-column relation that contains the input references,
/// any found definitions, and optionally paths of any files missing from the
/// subgraph where missing definitions may still be found:
///
/// - reference (string);
/// - definition (string or null);
/// - missing file path (string or null).
///
/// An output tuple will always contain a reference, and either a definition or
/// a missing file path, never neither and never both. Missing file paths are
/// only returned if the 4th positional parameter (the root paths index) was
/// given.
///
/// ## Iterative querying
///
/// If the input is only a subgraph of the multi-file stack graph, it may be
/// that it does not contain some of the queried definitions. If the 4th
/// positional parameter (the root paths index) was given, the fixed rule can
/// still output the paths of any files missing from the subgraph. This lets
/// the fixed rule be called iteratively with a progressively larger subgraph
/// until all definitions are found.
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

        let root_path_blobs = payload
            .get_input(2)?
            .ensure_min_len(3)?
            .iter()?
            .map(|tuple| tuple.map_err(E::tuple_report)?.try_into());

        let root_path_symbol_stacks_files =
            if let Ok(root_path_symbol_stacks_files) = payload.get_input(3) {
                Some(
                    root_path_symbol_stacks_files
                        .ensure_min_len(2)?
                        .iter()?
                        .map(|tuple| tuple.map_err(E::tuple_report)?.try_into()),
                )
            } else {
                None
            };
        let output_missing_files = root_path_symbol_stacks_files.is_some();
        let mut state = state::State::new(
            graph_blobs,
            node_path_blobs,
            root_path_blobs,
            root_path_symbol_stacks_files.map_or_else::<Box<dyn Iterator<Item = _>>, _, _>(
                || Box::new(std::iter::empty()),
                |files| Box::new(files),
            ),
        )?;

        debug!(" ↳ Initialized state for StackGraphQuery fixed rule");

        let timeout = payload
            .expr_option("timeout", None)?
            .eval_to_const()
            .map_err(|e| Error::SourcePos(SourcePosError::Other(e)))?
            .get_non_neg_int()
            .ok_or(Error::SourcePos(SourcePosError::InvalidType {
                expected: "list of timeout in milliseconds, or 0 if no timeout",
            }))?;
        let max_bytes = payload
            .expr_option("max_bytes", None)?
            .eval_to_const()
            .map_err(|e| Error::SourcePos(SourcePosError::Other(e)))?
            .get_non_neg_int()
            .ok_or(Error::SourcePos(SourcePosError::InvalidType {
                expected: "max amount of usable memory bytes",
            }))?;

        let references = payload
            .expr_option("references", None)?
            .eval_to_const()
            .map_err(|e| Error::SourcePos(SourcePosError::Other(e)))?;
        let references = references
            .get_slice()
            .ok_or(Error::SourcePos(SourcePosError::InvalidType {
                expected: "list of strings",
            }))?
            .iter()
            .map(|d| {
                d.get_str()
                    .ok_or(Error::SourcePos(SourcePosError::InvalidType {
                        expected: "string",
                    }))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let source_poss = references
            .into_iter()
            .map(|s| {
                s.parse::<SourcePos>().map_err(|e| {
                    Error::SourcePos(SourcePosError::Parse {
                        got: s.into(),
                        source: e,
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let output_missing_files = output_missing_files
            && payload
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
        let cancellation_flag = PoisonCancellation(poison);

        for resolution in
            querier.definitions(&source_poss, output_missing_files, &cancellation_flag)?
        {
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

impl CancellationFlag for PoisonCancellation {
    fn check(&self, at: &'static str) -> Result<(), CancellationError> {
        self.0.check().map_err(|_| CancellationError(at))
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
