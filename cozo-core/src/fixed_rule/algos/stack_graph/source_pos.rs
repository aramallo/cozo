use std::{fmt::Display, ops::Range, str::FromStr};

#[derive(Clone, Debug)]
pub struct SourcePos {
    pub file_id: String,
    pub byte_range: Range<u32>, // TODO: Line/column instead?
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("missing colon before {which} byte offset")]
    MissingColon { which: String },
    #[error("invalid {which} byte offset")]
    InvalidByteOffset { which: String, source: std::num::ParseIntError },
}

impl FromStr for SourcePos {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        let mut rev_bytes = s.bytes().rev();
        let pos_colon_1 = rev_bytes
            .position(|b| b == b':')
            .ok_or_else(|| ParseError::MissingColon { which: "end".into() })?;
        let end_byte = &s[s.len() - pos_colon_1..];
        let pos_colon_2 = rev_bytes
            .position(|b| b == b':')
            .ok_or_else(|| ParseError::MissingColon { which: "start".into() })?;
        let start_byte = &s[s.len() - pos_colon_1 - 1 - pos_colon_2..s.len() - pos_colon_1 - 1];

        let file_id = &s[..s.len() - pos_colon_1 - 2 - pos_colon_2];

        let start_byte = start_byte
            .parse::<u32>()
            .map_err(|e| ParseError::InvalidByteOffset { which: "start".into(), source: e })?;
        let end_byte = end_byte
            .parse::<u32>()
            .map_err(|e| ParseError::InvalidByteOffset { which: "end".into(), source: e })?;

        Ok(SourcePos {
            file_id: file_id.to_string(),
            byte_range: start_byte..end_byte,
        })
    }
}

impl Display for SourcePos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.file_id, self.byte_range.start, self.byte_range.end
        )
    }
}
