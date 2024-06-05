use stack_graphs::{
    stitching::{ForwardCandidates as _, ForwardPartialPathStitcher, StitcherConfig},
    CancellationFlag,
};

use crate::fixed_rule::stack_graph::state::node_byte_range;

use super::{
    augoor_urn::AugoorUrn,
    error::{Error, Result},
    state::State,
};

/// Adapted from the [SQLite implementation].
/// [SQLite implementation]: https://github.com/github/stack-graphs/blob/3c4d1a6/tree-sitter-stack-graphs/src/cli/query.rs#L153
pub(super) struct Querier<'state> {
    db: &'state mut State,
    // TODO: Stats? Reporting?
}

impl<'state> Querier<'state> {
    pub fn new(db: &'state mut State) -> Self {
        Self { db }
    }

    pub fn definitions(
        &mut self,
        augoor_urn: &AugoorUrn,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<Vec<AugoorUrn>> {
        let node = self
            .db
            .load_node(augoor_urn)?
            .ok_or_else(|| Error::Misc(format!("no node found for reference {augoor_urn}")))?;

        let mut all_paths = vec![];
        let config = StitcherConfig::default()
            // always detect similar paths, we don't know the language configurations for the data in the database
            .with_detect_similar_paths(true)
            .with_collect_stats(true);
        ForwardPartialPathStitcher::find_all_complete_partial_paths(
            self.db,
            std::iter::once(node),
            config,
            cancellation_flag,
            |_g, _ps, path| all_paths.push(path.clone()),
        )?;

        let (graph, partials, _) = self.db.get_graph_partials_and_db();
        let mut actual_paths = vec![];
        for path in &all_paths {
            if let Err(err) = cancellation_flag.check("shadowing") {
                // self.reporter.failed(&log_path, "query timed out", None);
                return Err(err.into());
            }

            if all_paths
                .iter()
                .all(|other_path| !other_path.shadows(partials, path))
            {
                actual_paths.push(path.clone());
            }
        }

        Ok(actual_paths
            .into_iter()
            .filter_map(|path| {
                // TOOD: Bail?
                let file = graph[path.end_node].file()?; // Def. nodes should be in a file
                let file_id = graph[file].name();
                let byte_range = node_byte_range(graph, path.end_node)?; // Def. nodes should have source info
                Some(AugoorUrn {
                    file_id: file_id.into(),
                    byte_range,
                })
            })
            .collect())
    }
}
