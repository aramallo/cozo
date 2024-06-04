use std::{fmt::Display, str::FromStr};
use std::ops::Range;
use stack_graphs::arena::Handle;
use stack_graphs::graph::{Node, StackGraph};

fn lsp_position_to_byte_offset(position: &lsp_positions::Position) -> u32 {
    let line_start = position.containing_line.start;
    let line_offset = position.column.utf8_offset;
    (line_start + line_offset) as u32
}

pub fn get_node_byte_range(
    stack_graph: &StackGraph,
    stack_graph_node: Handle<Node>,
) -> Option<Range<u32>> {
    let source_info = stack_graph.source_info(stack_graph_node)?;
    let span = &source_info.span;

    let start = lsp_position_to_byte_offset(&span.start);
    let end = lsp_position_to_byte_offset(&span.end);

    if start == 0 && end == 0 {
        None
    } else {
        Some(start..end)
    }
}

#[derive(Clone, Debug)]
pub struct AugoorUrn {
    pub blob_id: String,
    pub byte_range: Range<u32>,
}

impl AugoorUrn {
    pub fn new(blob_id: String, byte_range: Range<u32>) -> Self {
        Self {
            blob_id,
            byte_range
        }
    }

    pub fn node_has_urn(
        &self,
        stack_graph: &StackGraph,
        stack_graph_node: Handle<Node>,
    ) -> bool {
        if let Some(byte_range) = get_node_byte_range(stack_graph, stack_graph_node) {
            byte_range == self.byte_range
        } else {
            false
        }
    }
}

impl FromStr for AugoorUrn {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();

        if parts.len() != 5 {
            return Err("Invalid URN format".to_string());
        } else if parts[0] != "urn" || parts[1] != "augr" {
            return Err("Invalid URN scheme".to_string());
        }

        let blob_id = parts[2].to_string();
        let start_byte = parts[3].parse::<u32>().map_err(|_| "Invalid URN start_byte".to_string())?;
        let end_byte = parts[4].parse::<u32>().map_err(|_| "Invalid URN end_byte".to_string())?;

        Ok(AugoorUrn {
            blob_id,
            byte_range: start_byte..end_byte,
        })
    }
}

impl Display for AugoorUrn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "urn:augr:{}:{}:{}", self.blob_id, self.byte_range.start, self.byte_range.end)
    }
}
