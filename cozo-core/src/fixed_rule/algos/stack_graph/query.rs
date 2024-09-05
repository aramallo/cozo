use log::debug;
use stack_graphs::{
    stitching::{ForwardCandidates as _, ForwardPartialPathStitcher, StitcherConfig},
    CancellationFlag,
};

use super::{
    error::Result,
    pluralize,
    state::{node_byte_range, FileId, State},
    Error, SourcePos,
};

/// Adapted from the [SQLite implementation].
///
/// [SQLite implementation]: https://github.com/github/stack-graphs/blob/3c4d1a6/tree-sitter-stack-graphs/src/cli/query.rs#L153
pub(super) struct Querier<'state> {
    db: &'state mut State,
    // TODO: Stats? Reporting?
}

pub(super) enum ResolutionKind {
    Definition(SourcePos),
    MissingFile(FileId),
}

pub(super) struct Resolution {
    pub(super) reference: SourcePos,
    pub(super) kind: ResolutionKind,
}

impl<'state> Querier<'state> {
    pub fn new(db: &'state mut State) -> Self {
        Self { db }
    }

    pub fn definitions(
        &mut self,
        ref_source_poss: &[SourcePos],
        output_missing_files: bool,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<Vec<Resolution>> {
        let mut resolutions = Vec::new();

        let prev_missing_files = std::mem::replace(
            &mut self.db.missing_files,
            output_missing_files.then(Vec::new),
        );

        for ref_source_pos in ref_source_poss {
            debug!("Finding definitions for reference at \"{ref_source_pos}\"...");

            let nodes = self.db.load_nodes(ref_source_pos)?.collect::<Vec<_>>();

            if nodes.is_empty() {
                return Err(Error::Query(ref_source_pos.clone()));
            }

            debug!(" ↳ Found {}", pluralize(nodes.len(), "reference"));

            cancellation_flag.check("before stitching")?;
            let mut all_paths = vec![];
            let config = StitcherConfig::default()
                // Always detect similar paths: we don't know the language
                // configurations for the data in the database
                .with_detect_similar_paths(true)
                .with_collect_stats(true);
            ForwardPartialPathStitcher::find_all_complete_partial_paths(
                self.db,
                nodes,
                config,
                cancellation_flag,
                |_g, _ps, path| all_paths.push(path.clone()),
            )?;

            debug!(" ↳ Found {}", pluralize(all_paths.len(), "total path"));

            let (graph, partials, _) = self.db.get_graph_partials_and_db();
            let mut actual_paths = vec![];
            for path in &all_paths {
                cancellation_flag.check("shadowing")?;

                if all_paths
                    .iter()
                    .all(|other_path| !other_path.shadows(partials, path))
                {
                    actual_paths.push(path.clone());
                }
            }

            debug!(" ↳ Found {}", pluralize(actual_paths.len(), "actual path"));

            resolutions.extend(actual_paths.into_iter().filter_map(|path| {
                // TODO: Bail?
                let file = graph[path.end_node].file()?; // Def. nodes should be in a file
                let byte_range = node_byte_range(graph, path.end_node)?; // Def. nodes should have source info
                Some(Resolution {
                    reference: ref_source_pos.clone(),
                    kind: ResolutionKind::Definition(SourcePos {
                        file_id: graph[file].name().into(),
                        byte_range,
                    }),
                })
            }));

            if let Some(missing_files) = self.db.missing_files.as_mut() {
                resolutions.extend(missing_files.drain(..).map(|file| Resolution {
                    reference: ref_source_pos.clone(),
                    kind: ResolutionKind::MissingFile(file.clone()),
                }))
            }
        }

        self.db.missing_files = prev_missing_files;

        Ok(resolutions)
    }
}
