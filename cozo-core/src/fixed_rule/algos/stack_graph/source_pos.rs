use std::{fmt::Display, ops::Range, str::FromStr};

#[derive(Clone, Debug)]
pub struct SourcePos {
    pub file_id: String,
    pub byte_range: Range<u32>, // TODO: Line/column instead?
}

impl FromStr for SourcePos {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let mut rev_bytes = s.bytes().rev();
        let pos_colon_1 = rev_bytes
            .position(|b| b == b':')
            .ok_or("expected colon before end byte")?;
        let end_byte = &s[s.len() - pos_colon_1..];
        let pos_colon_2 = rev_bytes
            .position(|b| b == b':')
            .ok_or("expected colon before start byte")?;
        let start_byte = &s[s.len() - pos_colon_1 - 1 - pos_colon_2..s.len() - pos_colon_1 - 1];

        let file_id = &s[..s.len() - pos_colon_1 - 2 - pos_colon_2];

        let start_byte = start_byte
            .parse::<u32>()
            .map_err(|_| "Invalid URN start_byte".to_string())?;
        let end_byte = end_byte
            .parse::<u32>()
            .map_err(|_| "Invalid URN end_byte".to_string())?;

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
