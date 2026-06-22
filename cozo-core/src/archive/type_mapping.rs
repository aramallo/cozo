/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Mapping from Arrow array values to cozo `DataValue`.
//!
//! Pure functions, separated from I/O so the conversion logic is unit-testable
//! without touching files or schemas.
//!
//! Mapping policy: produce the most natural `DataValue` for each Arrow type;
//! let cozo's existing `NullableColType::coerce` handle the final coercion to
//! the column's declared type. This means:
//!
//! - Numeric widening / narrowing (e.g. INT32 -> Int) is fine; coerce decides.
//! - Booleans, strings, bytes pass through directly.
//! - UUIDs are encoded as `FIXED_LEN_BYTE_ARRAY(16)` in Parquet; cozo's
//!   coerce path accepts UUIDs as bytes-shaped values.
//! - Lists become `DataValue::List`, recursing on the element type.
//! - Nulls become `DataValue::Null`.
//!
//! Anything not handled here returns an error rather than silently producing
//! a wrong value.

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use miette::{bail, IntoDiagnostic, Result};
use uuid::Uuid;

use crate::data::value::{DataValue, UuidWrapper};

/// Convert the value at `row_idx` in `array` to a cozo `DataValue`. Does not
/// perform schema-level coercion to a declared column type — the caller is
/// expected to invoke `NullableColType::coerce` afterwards.
pub(crate) fn arrow_value_to_data(array: &ArrayRef, row_idx: usize) -> Result<DataValue> {
    if array.is_null(row_idx) {
        return Ok(DataValue::Null);
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let a = downcast::<BooleanArray>(array, "Boolean")?;
            Ok(DataValue::from(a.value(row_idx)))
        }
        ArrowDataType::Int8 => Ok(DataValue::from(
            downcast::<Int8Array>(array, "Int8")?.value(row_idx) as i64,
        )),
        ArrowDataType::Int16 => Ok(DataValue::from(
            downcast::<Int16Array>(array, "Int16")?.value(row_idx) as i64,
        )),
        ArrowDataType::Int32 => Ok(DataValue::from(
            downcast::<Int32Array>(array, "Int32")?.value(row_idx) as i64,
        )),
        ArrowDataType::Int64 => Ok(DataValue::from(
            downcast::<Int64Array>(array, "Int64")?.value(row_idx),
        )),
        ArrowDataType::UInt8 => Ok(DataValue::from(
            downcast::<UInt8Array>(array, "UInt8")?.value(row_idx) as i64,
        )),
        ArrowDataType::UInt16 => Ok(DataValue::from(
            downcast::<UInt16Array>(array, "UInt16")?.value(row_idx) as i64,
        )),
        ArrowDataType::UInt32 => Ok(DataValue::from(
            downcast::<UInt32Array>(array, "UInt32")?.value(row_idx) as i64,
        )),
        ArrowDataType::UInt64 => {
            // Best effort — values >= 2^63 will be truncated. Round-tripping
            // cozo data won't hit this since cozo Int is i64.
            let v = downcast::<UInt64Array>(array, "UInt64")?.value(row_idx);
            Ok(DataValue::from(v as i64))
        }
        ArrowDataType::Float32 => Ok(DataValue::from(
            downcast::<Float32Array>(array, "Float32")?.value(row_idx) as f64,
        )),
        ArrowDataType::Float64 => Ok(DataValue::from(
            downcast::<Float64Array>(array, "Float64")?.value(row_idx),
        )),
        ArrowDataType::Utf8 => Ok(DataValue::from(
            downcast::<StringArray>(array, "Utf8")?.value(row_idx),
        )),
        ArrowDataType::LargeUtf8 => Ok(DataValue::from(
            downcast::<LargeStringArray>(array, "LargeUtf8")?.value(row_idx),
        )),
        ArrowDataType::Binary => {
            let bytes = downcast::<BinaryArray>(array, "Binary")?
                .value(row_idx)
                .to_vec();
            Ok(DataValue::Bytes(bytes))
        }
        ArrowDataType::LargeBinary => {
            let bytes = downcast::<LargeBinaryArray>(array, "LargeBinary")?
                .value(row_idx)
                .to_vec();
            Ok(DataValue::Bytes(bytes))
        }
        ArrowDataType::FixedSizeBinary(len) => {
            let arr = downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?;
            let bytes = arr.value(row_idx);
            // 16-byte fixed binary is treated as a UUID. Other widths fall
            // through to raw Bytes.
            if *len == 16 {
                let uuid = Uuid::from_slice(bytes).into_diagnostic()?;
                Ok(DataValue::Uuid(UuidWrapper(uuid)))
            } else {
                Ok(DataValue::Bytes(bytes.to_vec()))
            }
        }
        ArrowDataType::Timestamp(unit, _tz) => {
            // Normalise all timestamps to microseconds since epoch as Int.
            // This matches commit_now()'s output and is the format the archive
            // pipeline standardises on.
            let micros: i64 = match unit {
                TimeUnit::Second => {
                    downcast::<TimestampSecondArray>(array, "TimestampSec")?.value(row_idx)
                        * 1_000_000
                }
                TimeUnit::Millisecond => {
                    downcast::<TimestampMillisecondArray>(array, "TimestampMs")?.value(row_idx)
                        * 1_000
                }
                TimeUnit::Microsecond => {
                    downcast::<TimestampMicrosecondArray>(array, "TimestampUs")?.value(row_idx)
                }
                TimeUnit::Nanosecond => {
                    downcast::<TimestampNanosecondArray>(array, "TimestampNs")?.value(row_idx)
                        / 1_000
                }
            };
            Ok(DataValue::from(micros))
        }
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            let arr = downcast::<ListArray>(array, "List")?;
            let inner = arr.value(row_idx);
            let mut out = Vec::with_capacity(inner.len());
            for i in 0..inner.len() {
                out.push(arrow_value_to_data(&inner, i)?);
            }
            Ok(DataValue::List(out))
        }
        other => bail!(
            "unsupported Arrow type for Parquet import: {:?}; \
            supported types are: Bool, Int8/16/32/64, UInt8/16/32/64, \
            Float32/64, Utf8, LargeUtf8, Binary, LargeBinary, \
            FixedSizeBinary, Timestamp, List, LargeList",
            other
        ),
    }
}

fn downcast<'a, A: Array + 'static>(array: &'a ArrayRef, label: &str) -> Result<&'a A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        miette::miette!(
            "internal: failed to downcast Arrow array to {} (got {:?})",
            label,
            array.data_type()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Int32Type};

    fn bool_array(values: &[Option<bool>]) -> ArrayRef {
        Arc::new(BooleanArray::from(values.to_vec()))
    }

    #[test]
    fn maps_bool() {
        let arr = bool_array(&[Some(true), Some(false), None]);
        assert_eq!(arrow_value_to_data(&arr, 0).unwrap(), DataValue::from(true));
        assert_eq!(arrow_value_to_data(&arr, 1).unwrap(), DataValue::from(false));
        assert_eq!(arrow_value_to_data(&arr, 2).unwrap(), DataValue::Null);
    }

    #[test]
    fn maps_int_widths_to_int64() {
        let i32a: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3]));
        let i64a: ArrayRef = Arc::new(Int64Array::from(vec![100i64, -200, 300]));
        assert_eq!(arrow_value_to_data(&i32a, 1).unwrap(), DataValue::from(-2i64));
        assert_eq!(arrow_value_to_data(&i64a, 0).unwrap(), DataValue::from(100i64));
    }

    #[test]
    fn maps_float() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.5, -2.25]));
        assert_eq!(arrow_value_to_data(&arr, 0).unwrap(), DataValue::from(1.5f64));
    }

    #[test]
    fn maps_string() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["alice", "bob"]));
        assert_eq!(arrow_value_to_data(&arr, 1).unwrap(), DataValue::from("bob"));
    }

    #[test]
    fn maps_binary() {
        let arr: ArrayRef = Arc::new(BinaryArray::from(vec![b"abc".as_ref(), b"de".as_ref()]));
        assert_eq!(
            arrow_value_to_data(&arr, 0).unwrap(),
            DataValue::Bytes(b"abc".to_vec())
        );
    }

    #[test]
    fn maps_fixed_binary_16_as_uuid() {
        let uuid = Uuid::new_v4();
        let bytes = uuid.as_bytes().to_vec();
        let arr: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![bytes].into_iter()).unwrap(),
        );
        match arrow_value_to_data(&arr, 0).unwrap() {
            DataValue::Uuid(w) => assert_eq!(w.0, uuid),
            other => panic!("expected Uuid, got {other:?}"),
        }
    }

    #[test]
    fn maps_fixed_binary_other_lens_as_bytes() {
        let arr: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3, 4]].into_iter()).unwrap(),
        );
        match arrow_value_to_data(&arr, 0).unwrap() {
            DataValue::Bytes(b) => assert_eq!(b, vec![1, 2, 3, 4]),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn maps_timestamp_micros_unchanged() {
        let arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_000_000i64, 5]));
        assert_eq!(
            arrow_value_to_data(&arr, 0).unwrap(),
            DataValue::from(1_000_000i64)
        );
        assert_eq!(arrow_value_to_data(&arr, 1).unwrap(), DataValue::from(5i64));
    }

    #[test]
    fn maps_list_of_int() {
        // ListArray<Int32> with two rows: [1,2,3] and [4,5]
        let data = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
        ]);
        let arr: ArrayRef = Arc::new(data);
        match arrow_value_to_data(&arr, 0).unwrap() {
            DataValue::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], DataValue::from(1i64));
                assert_eq!(l[2], DataValue::from(3i64));
            }
            other => panic!("expected List, got {other:?}"),
        }
        match arrow_value_to_data(&arr, 1).unwrap() {
            DataValue::List(l) => assert_eq!(l.len(), 2),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_type_errors_clearly() {
        // Decimal128 is not in our supported set.
        use arrow::array::Decimal128Array;
        let arr: ArrayRef = Arc::new(
            Decimal128Array::from(vec![123i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let err = arrow_value_to_data(&arr, 0).unwrap_err().to_string();
        assert!(
            err.contains("unsupported Arrow type"),
            "error should be the unsupported-type message; got: {err}"
        );
    }

    // Silence dead-code warnings for unused imports introduced for future tests.
    #[allow(dead_code)]
    fn _unused_imports_keepalive() {
        let _ = (Buffer::from_vec(vec![0u8]), Field::new("x", DataType::Int32, true));
    }
}
