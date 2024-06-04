use std::{fmt::Display, ops::Range, str::FromStr};

#[derive(Clone, Debug)]
pub struct AugoorUrn {
    pub file_id: String,
    pub byte_range: Range<u32>,
}

impl AugoorUrn {
    pub fn new(file_id: String, byte_range: Range<u32>) -> Self {
        Self {
            file_id,
            byte_range
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

        let file_id = parts[2].to_string();
        let start_byte = parts[3].parse::<u32>().map_err(|_| "Invalid URN start_byte".to_string())?;
        let end_byte = parts[4].parse::<u32>().map_err(|_| "Invalid URN end_byte".to_string())?;

        Ok(AugoorUrn {
            file_id,
            byte_range: start_byte..end_byte,
        })
    }
}

impl Display for AugoorUrn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "urn:augr:{}:{}:{}", self.file_id, self.byte_range.start, self.byte_range.end)
    }
}
