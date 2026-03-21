//! Row encoding/decoding for storage in B+tree values.

use dust_types::{DustError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
    Real(f64),
    Blob(Vec<u8>),
}

impl std::fmt::Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Null => f.write_str("NULL"),
            Datum::Integer(n) => write!(f, "{n}"),
            Datum::Text(s) => f.write_str(s),
            Datum::Boolean(b) => write!(f, "{b}"),
            Datum::Real(r) => write!(f, "{r}"),
            Datum::Blob(b) => write!(f, "<blob {} bytes>", b.len()),
        }
    }
}

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_TEXT: u8 = 2;
const TAG_BOOLEAN: u8 = 3;
const TAG_REAL: u8 = 4;
const TAG_BLOB: u8 = 5;

/// Encode a row of datums into bytes.
///
/// Format: column_count(u16) + for each column: tag(u8) + data
pub fn encode_row(columns: &[Datum]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());

    for datum in columns {
        match datum {
            Datum::Null => buf.push(TAG_NULL),
            Datum::Integer(n) => {
                buf.push(TAG_INTEGER);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Datum::Text(s) => {
                buf.push(TAG_TEXT);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Datum::Boolean(b) => {
                buf.push(TAG_BOOLEAN);
                buf.push(if *b { 1 } else { 0 });
            }
            Datum::Real(r) => {
                buf.push(TAG_REAL);
                buf.extend_from_slice(&r.to_le_bytes());
            }
            Datum::Blob(data) => {
                buf.push(TAG_BLOB);
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
        }
    }

    buf
}

/// Decode a row of datums from bytes.
pub fn decode_row(data: &[u8]) -> Result<Vec<Datum>> {
    if data.len() < 2 {
        return Err(DustError::InvalidInput("row data too short".to_string()));
    }

    let col_count = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
    let mut offset = 2;
    let mut columns = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        if offset >= data.len() {
            return Err(DustError::InvalidInput("row data truncated".to_string()));
        }

        let tag = data[offset];
        offset += 1;

        let datum = match tag {
            TAG_NULL => Datum::Null,
            TAG_INTEGER => {
                if offset + 8 > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (integer)".to_string(),
                    ));
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Datum::Integer(n)
            }
            TAG_TEXT => {
                if offset + 4 > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (text length)".to_string(),
                    ));
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (text body)".to_string(),
                    ));
                }
                let s = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
                offset += len;
                Datum::Text(s)
            }
            TAG_BOOLEAN => {
                if offset >= data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (boolean)".to_string(),
                    ));
                }
                let b = data[offset] != 0;
                offset += 1;
                Datum::Boolean(b)
            }
            TAG_REAL => {
                if offset + 8 > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (real)".to_string(),
                    ));
                }
                let r = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Datum::Real(r)
            }
            TAG_BLOB => {
                if offset + 4 > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (blob length)".to_string(),
                    ));
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(DustError::InvalidInput(
                        "row data truncated (blob body)".to_string(),
                    ));
                }
                let blob = data[offset..offset + len].to_vec();
                offset += len;
                Datum::Blob(blob)
            }
            other => {
                return Err(DustError::InvalidInput(format!(
                    "unknown datum tag: {other}"
                )));
            }
        };

        columns.push(datum);
    }

    Ok(columns)
}

/// Encode a u64 key in big-endian for B+tree sort order.
pub fn encode_key_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a u64 key from big-endian bytes.
pub fn decode_key_u64(data: &[u8]) -> u64 {
    u64::from_be_bytes(data[0..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let row = vec![
            Datum::Null,
            Datum::Integer(42),
            Datum::Text("hello world".to_string()),
            Datum::Boolean(true),
            Datum::Real(3.14),
            Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ];

        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();

        assert_eq!(decoded.len(), 6);
        assert_eq!(decoded[0], Datum::Null);
        assert_eq!(decoded[1], Datum::Integer(42));
        assert_eq!(decoded[2], Datum::Text("hello world".to_string()));
        assert_eq!(decoded[3], Datum::Boolean(true));
        assert_eq!(decoded[5], Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));

        // Real needs approximate comparison
        match &decoded[4] {
            Datum::Real(r) => assert!((r - 3.14).abs() < f64::EPSILON),
            other => panic!("expected Real, got {other:?}"),
        }
    }

    #[test]
    fn key_u64_preserves_order() {
        let a = encode_key_u64(100);
        let b = encode_key_u64(200);
        let c = encode_key_u64(300);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn empty_row() {
        let row: Vec<Datum> = vec![];
        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_invalid_data_errors() {
        assert!(decode_row(&[]).is_err());
        assert!(decode_row(&[1, 0, 99]).is_err()); // 1 column, unknown tag
    }
}
