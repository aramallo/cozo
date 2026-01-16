/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::mem;
use std::ops::{Div, Rem};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use itertools::Itertools;
#[cfg(target_arch = "wasm32")]
use js_sys::Date;
use miette::{bail, ensure, miette, IntoDiagnostic, Result};
use num_traits::FloatConst;
use rand::prelude::*;
use serde_json::{json, Value};
use smartstring::SmartString;
use unicode_normalization::UnicodeNormalization;
use uuid::v1::Timestamp;

use crate::data::expr::Op;
use crate::data::json::JsonValue;
use crate::data::relation::VecElementType;
use crate::data::value::{
    DataValue, JsonData, Num, RegexWrapper, UuidWrapper, Validity, ValidityTs, Vector,
};

macro_rules! define_op {
    ($name:ident, $min_arity:expr, $vararg:expr) => {
        pub(crate) const $name: Op = Op {
            name: stringify!($name),
            min_arity: $min_arity,
            vararg: $vararg,
            inner: ::casey::lower!($name),
        };
    };
}

fn ensure_same_value_type(a: &DataValue, b: &DataValue) -> Result<()> {
    use DataValue::*;
    if !matches!(
        (a, b),
        (Null, Null)
            | (Bool(_), Bool(_))
            | (Num(_), Num(_))
            | (Str(_), Str(_))
            | (Bytes(_), Bytes(_))
            | (Regex(_), Regex(_))
            | (List(_), List(_))
            | (Set(_), Set(_))
            | (Bot, Bot)
    ) {
        bail!(
            "comparison can only be done between the same datatypes, got {:?} and {:?}",
            a,
            b
        )
    }
    Ok(())
}

define_op!(OP_LIST, 0, true);
pub(crate) fn op_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(args.to_vec()))
}

define_op!(OP_JSON, 1, false);
pub(crate) fn op_json(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Json(JsonData(to_json(&args[0]))))
}

define_op!(OP_SET_JSON_PATH, 3, false);
pub(crate) fn op_set_json_path(args: &[DataValue]) -> Result<DataValue> {
    let mut result = to_json(&args[0]);
    let path = args[1]
        .get_slice()
        .ok_or_else(|| miette!("json path must be a string"))?;
    let pointer = get_json_path(&mut result, path)?;
    let new_val = to_json(&args[2]);
    *pointer = new_val;
    Ok(DataValue::Json(JsonData(result)))
}

fn get_json_path_immutable<'a>(
    mut pointer: &'a JsonValue,
    path: &[DataValue],
) -> Result<&'a JsonValue> {
    for key in path {
        match pointer {
            JsonValue::Object(obj) => {
                let key = val2str(key);
                let entry = obj
                    .get(&key)
                    .ok_or_else(|| miette!("json path does not exist"))?;
                pointer = entry;
            }
            JsonValue::Array(arr) => {
                let key = key
                    .get_int()
                    .ok_or_else(|| miette!("json path must be a string or a number"))?
                    as usize;

                let val = arr
                    .get(key)
                    .ok_or_else(|| miette!("json path does not exist"))?;
                pointer = val;
            }
            _ => {
                bail!("json path does not exist")
            }
        }
    }
    Ok(pointer)
}

fn get_json_path<'a>(
    mut pointer: &'a mut JsonValue,
    path: &[DataValue],
) -> Result<&'a mut JsonValue> {
    for key in path {
        match pointer {
            JsonValue::Object(obj) => {
                let key = val2str(key);
                let entry = obj.entry(key).or_insert(json!({}));
                pointer = entry;
            }
            JsonValue::Array(arr) => {
                let key = key
                    .get_int()
                    .ok_or_else(|| miette!("json path must be a string or a number"))?
                    as usize;
                if arr.len() <= key + 1 {
                    arr.resize_with(key + 1, || JsonValue::Null);
                }

                let val = arr.get_mut(key).unwrap();
                pointer = val;
            }
            _ => {
                bail!("json path does not exist")
            }
        }
    }
    Ok(pointer)
}

define_op!(OP_REMOVE_JSON_PATH, 2, false);
pub(crate) fn op_remove_json_path(args: &[DataValue]) -> Result<DataValue> {
    let mut result = to_json(&args[0]);
    let path = args[1]
        .get_slice()
        .ok_or_else(|| miette!("json path must be a string"))?;
    let (last, path) = path
        .split_last()
        .ok_or_else(|| miette!("json path must not be empty"))?;
    let pointer = get_json_path(&mut result, path)?;
    match pointer {
        JsonValue::Object(obj) => {
            let key = val2str(last);
            obj.remove(&key);
        }
        JsonValue::Array(arr) => {
            let key = last
                .get_int()
                .ok_or_else(|| miette!("json path must be a string or a number"))?
                as usize;
            arr.remove(key);
        }
        _ => {
            bail!("json path does not exist")
        }
    }
    Ok(DataValue::Json(JsonData(result)))
}

define_op!(OP_JSON_OBJECT, 0, true);
pub(crate) fn op_json_object(args: &[DataValue]) -> Result<DataValue> {
    ensure!(
        args.len() % 2 == 0,
        "json_object requires an even number of arguments"
    );
    let mut obj = serde_json::Map::with_capacity(args.len() / 2);
    for pair in args.chunks_exact(2) {
        let key = val2str(&pair[0]);
        let value = to_json(&pair[1]);
        obj.insert(key.to_string(), value);
    }
    Ok(DataValue::Json(JsonData(Value::Object(obj))))
}

fn to_json(d: &DataValue) -> JsonValue {
    match d {
        DataValue::Null => {
            json!(null)
        }
        DataValue::Bool(b) => {
            json!(b)
        }
        DataValue::Num(n) => match n {
            Num::Int(i) => {
                json!(i)
            }
            Num::Float(f) => {
                json!(f)
            }
        },
        DataValue::Str(s) => {
            json!(s)
        }
        DataValue::Bytes(b) => {
            json!(b)
        }
        DataValue::Uuid(u) => {
            json!(u.0.as_bytes())
        }
        DataValue::Regex(r) => {
            json!(r.0.as_str())
        }
        DataValue::List(l) => {
            let mut arr = Vec::with_capacity(l.len());
            for el in l {
                arr.push(to_json(el));
            }
            arr.into()
        }
        DataValue::Set(l) => {
            let mut arr = Vec::with_capacity(l.len());
            for el in l {
                arr.push(to_json(el));
            }
            arr.into()
        }
        DataValue::Vec(v) => {
            let mut arr = Vec::with_capacity(v.len());
            match v {
                Vector::F32(a) => {
                    for el in a {
                        arr.push(json!(el));
                    }
                }
                Vector::F64(a) => {
                    for el in a {
                        arr.push(json!(el));
                    }
                }
            }
            arr.into()
        }
        DataValue::Json(j) => j.0.clone(),
        DataValue::Validity(vld) => {
            json!([vld.timestamp.0, vld.is_assert.0])
        }
        DataValue::Bot => {
            json!(null)
        }
    }
}

define_op!(OP_PARSE_JSON, 1, false);
pub(crate) fn op_parse_json(args: &[DataValue]) -> Result<DataValue> {
    match args[0].get_str() {
        Some(s) => {
            let value = serde_json::from_str(s).into_diagnostic()?;
            Ok(DataValue::Json(JsonData(value)))
        }
        None => bail!("parse_json requires a string argument"),
    }
}

define_op!(OP_DUMP_JSON, 1, false);
pub(crate) fn op_dump_json(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Json(j) => Ok(DataValue::Str(j.0.to_string().into())),
        _ => bail!("dump_json requires a json argument"),
    }
}

define_op!(OP_COALESCE, 0, true);
pub(crate) fn op_coalesce(args: &[DataValue]) -> Result<DataValue> {
    for val in args {
        if *val != DataValue::Null {
            return Ok(val.clone());
        }
    }
    Ok(DataValue::Null)
}

define_op!(OP_EQ, 2, false);
pub(crate) fn op_eq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(f)), DataValue::Num(Num::Int(i)))
        | (DataValue::Num(Num::Int(i)), DataValue::Num(Num::Float(f))) => *i as f64 == *f,
        (a, b) => a == b,
    }))
}

define_op!(OP_IS_UUID, 1, false);
pub(crate) fn op_is_uuid(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Uuid(_))))
}

define_op!(OP_IS_JSON, 1, false);
pub(crate) fn op_is_json(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Json(_))))
}

define_op!(OP_JSON_TO_SCALAR, 1, false);
pub(crate) fn op_json_to_scalar(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Json(JsonData(j)) => json2val(j.clone()),
        d => d.clone(),
    })
}

define_op!(OP_IS_IN, 2, false);
pub(crate) fn op_is_in(args: &[DataValue]) -> Result<DataValue> {
    let left = &args[0];
    let right = args[1]
        .get_slice()
        .ok_or_else(|| miette!("right hand side of 'is_in' must be a list"))?;
    Ok(DataValue::from(right.contains(left)))
}

define_op!(OP_NEQ, 2, false);
pub(crate) fn op_neq(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(f)), DataValue::Num(Num::Int(i)))
        | (DataValue::Num(Num::Int(i)), DataValue::Num(Num::Float(f))) => *i as f64 != *f,
        (a, b) => a != b,
    }))
}

define_op!(OP_GT, 2, false);
pub(crate) fn op_gt(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l > *r as f64,
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => *l as f64 > *r,
        (a, b) => a > b,
    }))
}

define_op!(OP_GE, 2, false);
pub(crate) fn op_ge(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l >= *r as f64,
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => *l as f64 >= *r,
        (a, b) => a >= b,
    }))
}

define_op!(OP_LT, 2, false);
pub(crate) fn op_lt(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l < (*r as f64),
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => (*l as f64) < *r,
        (a, b) => a < b,
    }))
}

define_op!(OP_LE, 2, false);
pub(crate) fn op_le(args: &[DataValue]) -> Result<DataValue> {
    ensure_same_value_type(&args[0], &args[1])?;
    Ok(DataValue::from(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Float(l)), DataValue::Num(Num::Int(r))) => *l <= (*r as f64),
        (DataValue::Num(Num::Int(l)), DataValue::Num(Num::Float(r))) => (*l as f64) <= *r,
        (a, b) => a <= b,
    }))
}

define_op!(OP_ADD, 0, true);
pub(crate) fn op_add(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 0i64;
    let mut f_accum = 0.0f64;
    for arg in args {
        match arg {
            DataValue::Num(Num::Int(i)) => i_accum += i,
            DataValue::Num(Num::Float(f)) => f_accum += f,
            DataValue::Vec(_) => return add_vecs(args),
            _ => bail!("addition requires numbers"),
        }
    }
    if f_accum == 0.0f64 {
        Ok(DataValue::Num(Num::Int(i_accum)))
    } else {
        Ok(DataValue::Num(Num::Float(i_accum as f64 + f_accum)))
    }
}

fn add_vecs(args: &[DataValue]) -> Result<DataValue> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    let (last, first) = args.split_last().unwrap();
    let first = add_vecs(first)?;
    match (first, last) {
        (DataValue::Vec(a), DataValue::Vec(b)) => {
            if a.len() != b.len() {
                bail!("can only add vectors of the same length");
            }
            match (a, b) {
                (Vector::F32(a), Vector::F32(b)) => Ok(DataValue::Vec(Vector::F32(a + b))),
                (Vector::F64(a), Vector::F64(b)) => Ok(DataValue::Vec(Vector::F64(a + b))),
                (Vector::F32(a), Vector::F64(b)) => {
                    let a = a.mapv(|x| x as f64);
                    Ok(DataValue::Vec(Vector::F64(a + b)))
                }
                (Vector::F64(a), Vector::F32(b)) => {
                    let b = b.mapv(|x| x as f64);
                    Ok(DataValue::Vec(Vector::F64(a + b)))
                }
            }
        }
        (DataValue::Vec(a), b) => {
            let f = b
                .get_float()
                .ok_or_else(|| miette!("can only add numbers to vectors"))?;
            match a {
                Vector::F32(mut v) => {
                    v += f as f32;
                    Ok(DataValue::Vec(Vector::F32(v)))
                }
                Vector::F64(mut v) => {
                    v += f;
                    Ok(DataValue::Vec(Vector::F64(v)))
                }
            }
        }
        (a, DataValue::Vec(b)) => {
            let f = a
                .get_float()
                .ok_or_else(|| miette!("can only add numbers to vectors"))?;
            match b {
                Vector::F32(v) => Ok(DataValue::Vec(Vector::F32(v + f as f32))),
                Vector::F64(v) => Ok(DataValue::Vec(Vector::F64(v + f))),
            }
        }
        _ => bail!("addition requires numbers"),
    }
}

define_op!(OP_MAX, 1, true);
pub(crate) fn op_max(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Num(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Num(a)), DataValue::Num(b)) => Ok(Some(DataValue::Num(a.max(*b)))),
            _ => bail!("'max can only be applied to numbers'"),
        })?;
    match res {
        None => Ok(DataValue::Num(Num::Float(f64::NEG_INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_MIN, 1, true);
pub(crate) fn op_min(args: &[DataValue]) -> Result<DataValue> {
    let res = args
        .iter()
        .try_fold(None, |accum, nxt| match (accum, nxt) {
            (None, d @ DataValue::Num(_)) => Ok(Some(d.clone())),
            (Some(DataValue::Num(a)), DataValue::Num(b)) => Ok(Some(DataValue::Num(a.min(*b)))),
            _ => bail!("'min' can only be applied to numbers"),
        })?;
    match res {
        None => Ok(DataValue::Num(Num::Float(f64::INFINITY))),
        Some(v) => Ok(v),
    }
}

define_op!(OP_SUB, 2, false);
pub(crate) fn op_sub(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Int(*a - *b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(*a - *b))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64) - b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a - (*b as f64)))
        }
        (DataValue::Vec(a), DataValue::Vec(b)) => match (a, b) {
            (Vector::F32(a), Vector::F32(b)) => DataValue::Vec(Vector::F32(a - b)),
            (Vector::F64(a), Vector::F64(b)) => DataValue::Vec(Vector::F64(a - b)),
            (Vector::F32(a), Vector::F64(b)) => {
                let a = a.mapv(|x| x as f64);
                DataValue::Vec(Vector::F64(a - b))
            }
            (Vector::F64(a), Vector::F32(b)) => {
                let b = b.mapv(|x| x as f64);
                DataValue::Vec(Vector::F64(a - b))
            }
        },
        (DataValue::Vec(a), b) => {
            let b = b
                .get_float()
                .ok_or_else(|| miette!("can only subtract numbers from vectors"))?;
            match a.clone() {
                Vector::F32(mut v) => {
                    v -= b as f32;
                    DataValue::Vec(Vector::F32(v))
                }
                Vector::F64(mut v) => {
                    v -= b;
                    DataValue::Vec(Vector::F64(v))
                }
            }
        }
        (a, DataValue::Vec(b)) => {
            let a = a
                .get_float()
                .ok_or_else(|| miette!("can only subtract vectors from numbers"))?;
            match b.clone() {
                Vector::F32(mut v) => {
                    v -= a as f32;
                    DataValue::Vec(Vector::F32(-v))
                }
                Vector::F64(mut v) => {
                    v -= a;
                    DataValue::Vec(Vector::F64(-v))
                }
            }
        }
        _ => bail!("subtraction requires numbers"),
    })
}

define_op!(OP_MUL, 0, true);
pub(crate) fn op_mul(args: &[DataValue]) -> Result<DataValue> {
    let mut i_accum = 1i64;
    let mut f_accum = 1.0f64;
    for arg in args {
        match arg {
            DataValue::Num(Num::Int(i)) => i_accum *= i,
            DataValue::Num(Num::Float(f)) => f_accum *= f,
            DataValue::Vec(_) => return mul_vecs(args),
            _ => bail!("multiplication requires numbers"),
        }
    }
    if f_accum == 1.0f64 {
        Ok(DataValue::Num(Num::Int(i_accum)))
    } else {
        Ok(DataValue::Num(Num::Float(i_accum as f64 * f_accum)))
    }
}

fn mul_vecs(args: &[DataValue]) -> Result<DataValue> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    let (last, first) = args.split_last().unwrap();
    let first = add_vecs(first)?;
    match (first, last) {
        (DataValue::Vec(a), DataValue::Vec(b)) => {
            if a.len() != b.len() {
                bail!("can only add vectors of the same length");
            }
            match (a, b) {
                (Vector::F32(a), Vector::F32(b)) => Ok(DataValue::Vec(Vector::F32(a * b))),
                (Vector::F64(a), Vector::F64(b)) => Ok(DataValue::Vec(Vector::F64(a * b))),
                (Vector::F32(a), Vector::F64(b)) => {
                    let a = a.mapv(|x| x as f64);
                    Ok(DataValue::Vec(Vector::F64(a * b)))
                }
                (Vector::F64(a), Vector::F32(b)) => {
                    let b = b.mapv(|x| x as f64);
                    Ok(DataValue::Vec(Vector::F64(a * b)))
                }
            }
        }
        (DataValue::Vec(a), b) => {
            let f = b
                .get_float()
                .ok_or_else(|| miette!("can only add numbers to vectors"))?;
            match a {
                Vector::F32(mut v) => {
                    v *= f as f32;
                    Ok(DataValue::Vec(Vector::F32(v)))
                }
                Vector::F64(mut v) => {
                    v *= f;
                    Ok(DataValue::Vec(Vector::F64(v)))
                }
            }
        }
        (a, DataValue::Vec(b)) => {
            let f = a
                .get_float()
                .ok_or_else(|| miette!("can only add numbers to vectors"))?;
            match b {
                Vector::F32(v) => Ok(DataValue::Vec(Vector::F32(v * f as f32))),
                Vector::F64(v) => Ok(DataValue::Vec(Vector::F64(v * f))),
            }
        }
        _ => bail!("addition requires numbers"),
    }
}

define_op!(OP_DIV, 2, false);
pub(crate) fn op_div(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float((*a as f64) / (*b as f64)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(*a / *b))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64) / b))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a / (*b as f64)))
        }
        (DataValue::Vec(a), DataValue::Vec(b)) => match (a, b) {
            (Vector::F32(a), Vector::F32(b)) => DataValue::Vec(Vector::F32(a / b)),
            (Vector::F64(a), Vector::F64(b)) => DataValue::Vec(Vector::F64(a / b)),
            (Vector::F32(a), Vector::F64(b)) => {
                let a = a.mapv(|x| x as f64);
                DataValue::Vec(Vector::F64(a / b))
            }
            (Vector::F64(a), Vector::F32(b)) => {
                let b = b.mapv(|x| x as f64);
                DataValue::Vec(Vector::F64(a / b))
            }
        },
        (DataValue::Vec(a), b) => {
            let b = b
                .get_float()
                .ok_or_else(|| miette!("can only subtract numbers from vectors"))?;
            match a.clone() {
                Vector::F32(mut v) => {
                    v /= b as f32;
                    DataValue::Vec(Vector::F32(v))
                }
                Vector::F64(mut v) => {
                    v /= b;
                    DataValue::Vec(Vector::F64(v))
                }
            }
        }
        (a, DataValue::Vec(b)) => {
            let a = a
                .get_float()
                .ok_or_else(|| miette!("can only subtract vectors from numbers"))?;
            match b {
                Vector::F32(v) => DataValue::Vec(Vector::F32(a as f32 / v)),
                Vector::F64(v) => DataValue::Vec(Vector::F64(a / v)),
            }
        }
        _ => bail!("division requires numbers"),
    })
}

define_op!(OP_MINUS, 1, false);
pub(crate) fn op_minus(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(-(*i))),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(-(*f))),
        DataValue::Vec(Vector::F64(v)) => DataValue::Vec(Vector::F64(0. - v)),
        DataValue::Vec(Vector::F32(v)) => DataValue::Vec(Vector::F32(0. - v)),
        _ => bail!("minus can only be applied to numbers"),
    })
}

define_op!(OP_ABS, 1, false);
pub(crate) fn op_abs(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(i.abs())),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.abs())),
        DataValue::Vec(Vector::F64(v)) => DataValue::Vec(Vector::F64(v.mapv(|x| x.abs()))),
        DataValue::Vec(Vector::F32(v)) => DataValue::Vec(Vector::F32(v.mapv(|x| x.abs()))),
        _ => bail!("'abs' requires numbers"),
    })
}

define_op!(OP_SIGNUM, 1, false);
pub(crate) fn op_signum(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(i.signum())),
        DataValue::Num(Num::Float(f)) => {
            if f.signum() < 0. {
                DataValue::from(-1)
            } else if *f == 0. {
                DataValue::from(0)
            } else if *f > 0. {
                DataValue::from(1)
            } else {
                DataValue::from(f64::NAN)
            }
        }
        _ => bail!("'signum' requires numbers"),
    })
}

define_op!(OP_FLOOR, 1, false);
pub(crate) fn op_floor(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.floor())),
        _ => bail!("'floor' requires numbers"),
    })
}

define_op!(OP_CEIL, 1, false);
pub(crate) fn op_ceil(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.ceil())),
        _ => bail!("'ceil' requires numbers"),
    })
}

define_op!(OP_ROUND, 1, false);
pub(crate) fn op_round(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(Num::Int(i)) => DataValue::Num(Num::Int(*i)),
        DataValue::Num(Num::Float(f)) => DataValue::Num(Num::Float(f.round())),
        _ => bail!("'round' requires numbers"),
    })
}

define_op!(OP_EXP, 1, false);
pub(crate) fn op_exp(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.exp()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.exp()))));
        }
        _ => bail!("'exp' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.exp())))
}

define_op!(OP_EXP2, 1, false);
pub(crate) fn op_exp2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.exp2()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.exp2()))));
        }
        _ => bail!("'exp2' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.exp2())))
}

define_op!(OP_LN, 1, false);
pub(crate) fn op_ln(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.ln()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.ln()))));
        }
        _ => bail!("'ln' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.ln())))
}

define_op!(OP_LOG2, 1, false);
pub(crate) fn op_log2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.log2()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.log2()))));
        }
        _ => bail!("'log2' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.log2())))
}

define_op!(OP_LOG10, 1, false);
pub(crate) fn op_log10(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.log10()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.log10()))));
        }
        _ => bail!("'log10' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.log10())))
}

define_op!(OP_SIN, 1, false);
pub(crate) fn op_sin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.sin()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.sin()))));
        }
        _ => bail!("'sin' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.sin())))
}

define_op!(OP_COS, 1, false);
pub(crate) fn op_cos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.cos()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.cos()))));
        }
        _ => bail!("'cos' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.cos())))
}

define_op!(OP_TAN, 1, false);
pub(crate) fn op_tan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.tan()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.tan()))));
        }
        _ => bail!("'tan' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.tan())))
}

define_op!(OP_ASIN, 1, false);
pub(crate) fn op_asin(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.asin()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.asin()))));
        }
        _ => bail!("'asin' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.asin())))
}

define_op!(OP_ACOS, 1, false);
pub(crate) fn op_acos(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.acos()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.acos()))));
        }
        _ => bail!("'acos' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.acos())))
}

define_op!(OP_ATAN, 1, false);
pub(crate) fn op_atan(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.atan()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.atan()))));
        }
        _ => bail!("'atan' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.atan())))
}

define_op!(OP_ATAN2, 2, false);
pub(crate) fn op_atan2(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atan2' requires numbers"),
    };
    let b = match &args[1] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'atan2' requires numbers"),
    };

    Ok(DataValue::Num(Num::Float(a.atan2(b))))
}

define_op!(OP_SINH, 1, false);
pub(crate) fn op_sinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.sinh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.sinh()))));
        }
        _ => bail!("'sinh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.sinh())))
}

define_op!(OP_COSH, 1, false);
pub(crate) fn op_cosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.cosh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.cosh()))));
        }
        _ => bail!("'cosh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.cosh())))
}

define_op!(OP_TANH, 1, false);
pub(crate) fn op_tanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.tanh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.tanh()))));
        }
        _ => bail!("'tanh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.tanh())))
}

define_op!(OP_ASINH, 1, false);
pub(crate) fn op_asinh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.asinh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.asinh()))));
        }
        _ => bail!("'asinh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.asinh())))
}

define_op!(OP_ACOSH, 1, false);
pub(crate) fn op_acosh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.acosh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.acosh()))));
        }
        _ => bail!("'acosh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.acosh())))
}

define_op!(OP_ATANH, 1, false);
pub(crate) fn op_atanh(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.atanh()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.atanh()))));
        }
        _ => bail!("'atanh' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.atanh())))
}

define_op!(OP_SQRT, 1, false);
pub(crate) fn op_sqrt(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.sqrt()))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.sqrt()))));
        }
        _ => bail!("'sqrt' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.sqrt())))
}

define_op!(OP_POW, 2, false);
pub(crate) fn op_pow(args: &[DataValue]) -> Result<DataValue> {
    let a = match &args[0] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        DataValue::Vec(Vector::F32(v)) => {
            let b = args[1]
                .get_float()
                .ok_or_else(|| miette!("'pow' requires numbers"))?;
            return Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x.powf(b as f32)))));
        }
        DataValue::Vec(Vector::F64(v)) => {
            let b = args[1]
                .get_float()
                .ok_or_else(|| miette!("'pow' requires numbers"))?;
            return Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x.powf(b)))));
        }
        _ => bail!("'pow' requires numbers"),
    };
    let b = match &args[1] {
        DataValue::Num(Num::Int(i)) => *i as f64,
        DataValue::Num(Num::Float(f)) => *f,
        _ => bail!("'pow' requires numbers"),
    };
    Ok(DataValue::Num(Num::Float(a.powf(b))))
}

define_op!(OP_MOD, 2, false);
pub(crate) fn op_mod(args: &[DataValue]) -> Result<DataValue> {
    Ok(match (&args[0], &args[1]) {
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Int(b))) => {
            if *b == 0 {
                bail!("'mod' requires non-zero divisor")
            }
            DataValue::Num(Num::Int(a.rem(b)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float(a.rem(*b)))
        }
        (DataValue::Num(Num::Int(a)), DataValue::Num(Num::Float(b))) => {
            DataValue::Num(Num::Float((*a as f64).rem(b)))
        }
        (DataValue::Num(Num::Float(a)), DataValue::Num(Num::Int(b))) => {
            DataValue::Num(Num::Float(a.rem(*b as f64)))
        }
        _ => bail!("'mod' requires numbers"),
    })
}

define_op!(OP_AND, 0, true);
pub(crate) fn op_and(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if !arg
            .get_bool()
            .ok_or_else(|| miette!("'and' requires booleans"))?
        {
            return Ok(DataValue::from(false));
        }
    }
    Ok(DataValue::from(true))
}

define_op!(OP_OR, 0, true);
pub(crate) fn op_or(args: &[DataValue]) -> Result<DataValue> {
    for arg in args {
        if arg
            .get_bool()
            .ok_or_else(|| miette!("'or' requires booleans"))?
        {
            return Ok(DataValue::from(true));
        }
    }
    Ok(DataValue::from(false))
}

define_op!(OP_NEGATE, 1, false);
pub(crate) fn op_negate(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bool(b) = &args[0] {
        Ok(DataValue::from(!*b))
    } else {
        bail!("'negate' requires booleans");
    }
}

define_op!(OP_BIT_AND, 2, false);
pub(crate) fn op_bit_and(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_and' must have the same lengths"
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l &= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_and' requires bytes"),
    }
}

define_op!(OP_BIT_OR, 2, false);
pub(crate) fn op_bit_or(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_or' must have the same lengths",
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l |= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_or' requires bytes"),
    }
}

define_op!(OP_BIT_NOT, 1, false);
pub(crate) fn op_bit_not(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(arg) => {
            let mut ret = arg.clone();
            for l in ret.iter_mut() {
                *l = !*l;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_not' requires bytes"),
    }
}

define_op!(OP_BIT_XOR, 2, false);
pub(crate) fn op_bit_xor(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Bytes(left), DataValue::Bytes(right)) => {
            ensure!(
                left.len() == right.len(),
                "operands of 'bit_xor' must have the same lengths"
            );
            let mut ret = left.clone();
            for (l, r) in ret.iter_mut().zip(right.iter()) {
                *l ^= *r;
            }
            Ok(DataValue::Bytes(ret))
        }
        _ => bail!("'bit_xor' requires bytes"),
    }
}

define_op!(OP_UNPACK_BITS, 1, false);
pub(crate) fn op_unpack_bits(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::Bytes(bs) = &args[0] {
        let mut ret = vec![false; bs.len() * 8];
        for (chunk, byte) in bs.iter().enumerate() {
            ret[chunk * 8] = (*byte & 0b10000000) != 0;
            ret[chunk * 8 + 1] = (*byte & 0b01000000) != 0;
            ret[chunk * 8 + 2] = (*byte & 0b00100000) != 0;
            ret[chunk * 8 + 3] = (*byte & 0b00010000) != 0;
            ret[chunk * 8 + 4] = (*byte & 0b00001000) != 0;
            ret[chunk * 8 + 5] = (*byte & 0b00000100) != 0;
            ret[chunk * 8 + 6] = (*byte & 0b00000010) != 0;
            ret[chunk * 8 + 7] = (*byte & 0b00000001) != 0;
        }
        Ok(DataValue::List(
            ret.into_iter().map(DataValue::Bool).collect_vec(),
        ))
    } else {
        bail!("'unpack_bits' requires bytes")
    }
}

define_op!(OP_PACK_BITS, 1, false);
pub(crate) fn op_pack_bits(args: &[DataValue]) -> Result<DataValue> {
    if let DataValue::List(v) = &args[0] {
        let l = (v.len() as f64 / 8.).ceil() as usize;
        let mut res = vec![0u8; l];
        for (i, b) in v.iter().enumerate() {
            match b {
                DataValue::Bool(b) => {
                    if *b {
                        let chunk = i.div(&8);
                        let idx = i % 8;
                        let target = res.get_mut(chunk).unwrap();
                        match idx {
                            0 => *target |= 0b10000000,
                            1 => *target |= 0b01000000,
                            2 => *target |= 0b00100000,
                            3 => *target |= 0b00010000,
                            4 => *target |= 0b00001000,
                            5 => *target |= 0b00000100,
                            6 => *target |= 0b00000010,
                            7 => *target |= 0b00000001,
                            _ => unreachable!(),
                        }
                    }
                }
                _ => bail!("'pack_bits' requires list of booleans"),
            }
        }
        Ok(DataValue::Bytes(res))
    } else if let DataValue::Set(v) = &args[0] {
        let l = v.iter().cloned().collect_vec();
        op_pack_bits(&[DataValue::List(l)])
    } else {
        bail!("'pack_bits' requires list of booleans")
    }
}

define_op!(OP_CONCAT, 1, true);
pub(crate) fn op_concat(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(_) => {
            let mut ret: String = Default::default();
            for arg in args {
                if let DataValue::Str(s) = arg {
                    ret += s;
                } else {
                    bail!("'concat' requires strings, or lists");
                }
            }
            Ok(DataValue::from(ret))
        }
        DataValue::List(_) | DataValue::Set(_) => {
            let mut ret = vec![];
            for arg in args {
                if let DataValue::List(l) = arg {
                    ret.extend_from_slice(l);
                } else if let DataValue::Set(s) = arg {
                    ret.extend(s.iter().cloned());
                } else {
                    bail!("'concat' requires strings, or lists");
                }
            }
            Ok(DataValue::List(ret))
        }
        DataValue::Json(_) => {
            let mut ret = json!(null);
            for arg in args {
                if let DataValue::Json(j) = arg {
                    ret = deep_merge_json(ret, j.0.clone());
                } else {
                    bail!("'concat' requires strings, lists, or JSON objects");
                }
            }
            Ok(DataValue::Json(JsonData(ret)))
        }
        _ => bail!("'concat' requires strings, lists, or JSON objects"),
    }
}

fn deep_merge_json(value1: JsonValue, value2: JsonValue) -> JsonValue {
    match (value1, value2) {
        (JsonValue::Object(mut obj1), JsonValue::Object(obj2)) => {
            for (key, value2) in obj2 {
                let value1 = obj1.remove(&key);
                obj1.insert(key, deep_merge_json(value1.unwrap_or(Value::Null), value2));
            }
            JsonValue::Object(obj1)
        }
        (JsonValue::Array(mut arr1), JsonValue::Array(arr2)) => {
            arr1.extend(arr2);
            JsonValue::Array(arr1)
        }
        (_, value2) => value2,
    }
}

define_op!(OP_STR_INCLUDES, 2, false);
pub(crate) fn op_str_includes(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(l), DataValue::Str(r)) => Ok(DataValue::from(l.find(r as &str).is_some())),
        _ => bail!("'str_includes' requires strings"),
    }
}

define_op!(OP_LOWERCASE, 1, false);
pub(crate) fn op_lowercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::from(s.to_lowercase())),
        _ => bail!("'lowercase' requires strings"),
    }
}

define_op!(OP_UPPERCASE, 1, false);
pub(crate) fn op_uppercase(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::from(s.to_uppercase())),
        _ => bail!("'uppercase' requires strings"),
    }
}

define_op!(OP_TRIM, 1, false);
pub(crate) fn op_trim(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::from(s.trim())),
        _ => bail!("'trim' requires strings"),
    }
}

define_op!(OP_TRIM_START, 1, false);
pub(crate) fn op_trim_start(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::from(s.trim_start())),
        v => bail!("'trim_start' requires strings, got {}", v),
    }
}

define_op!(OP_TRIM_END, 1, false);
pub(crate) fn op_trim_end(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => Ok(DataValue::from(s.trim_end())),
        _ => bail!("'trim_end' requires strings"),
    }
}

define_op!(OP_STARTS_WITH, 2, false);
pub(crate) fn op_starts_with(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(l), DataValue::Str(r)) => Ok(DataValue::from(l.starts_with(r as &str))),
        (DataValue::Bytes(l), DataValue::Bytes(r)) => {
            Ok(DataValue::from(l.starts_with(r as &[u8])))
        }
        _ => bail!("'starts_with' requires strings or bytes"),
    }
}

define_op!(OP_ENDS_WITH, 2, false);
pub(crate) fn op_ends_with(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(l), DataValue::Str(r)) => Ok(DataValue::from(l.ends_with(r as &str))),
        (DataValue::Bytes(l), DataValue::Bytes(r)) => Ok(DataValue::from(l.ends_with(r as &[u8]))),
        _ => bail!("'ends_with' requires strings or bytes"),
    }
}

define_op!(OP_REGEX, 1, false);
pub(crate) fn op_regex(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        r @ DataValue::Regex(_) => r.clone(),
        DataValue::Str(s) => {
            DataValue::Regex(RegexWrapper(regex::Regex::new(s).map_err(|err| {
                miette!("The string cannot be interpreted as regex: {}", err)
            })?))
        }
        _ => bail!("'regex' requires strings"),
    })
}

define_op!(OP_REGEX_MATCHES, 2, false);
pub(crate) fn op_regex_matches(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => Ok(DataValue::from(r.0.is_match(s))),
        _ => bail!("'regex_matches' requires strings"),
    }
}

define_op!(OP_REGEX_REPLACE, 3, false);
pub(crate) fn op_regex_replace(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1], &args[2]) {
        (DataValue::Str(s), DataValue::Regex(r), DataValue::Str(rp)) => {
            Ok(DataValue::Str(r.0.replace(s, rp as &str).into()))
        }
        _ => bail!("'regex_replace' requires strings"),
    }
}

define_op!(OP_REGEX_REPLACE_ALL, 3, false);
pub(crate) fn op_regex_replace_all(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1], &args[2]) {
        (DataValue::Str(s), DataValue::Regex(r), DataValue::Str(rp)) => {
            Ok(DataValue::Str(r.0.replace_all(s, rp as &str).into()))
        }
        _ => bail!("'regex_replace' requires strings"),
    }
}

define_op!(OP_REGEX_EXTRACT, 2, false);
pub(crate) fn op_regex_extract(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => {
            let found =
                r.0.find_iter(s)
                    .map(|v| DataValue::from(v.as_str()))
                    .collect_vec();
            Ok(DataValue::List(found))
        }
        _ => bail!("'regex_extract' requires strings"),
    }
}

define_op!(OP_REGEX_EXTRACT_FIRST, 2, false);
pub(crate) fn op_regex_extract_first(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Regex(r)) => {
            let found = r.0.find(s).map(|v| DataValue::from(v.as_str()));
            Ok(found.unwrap_or(DataValue::Null))
        }
        _ => bail!("'regex_extract_first' requires strings"),
    }
}

define_op!(OP_T2S, 1, false);
fn op_t2s(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Str(s) => DataValue::Str(fast2s::convert(s).into()),
        d => d.clone(),
    })
}

define_op!(OP_IS_NULL, 1, false);
pub(crate) fn op_is_null(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Null)))
}

define_op!(OP_IS_INT, 1, false);
pub(crate) fn op_is_int(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(
        args[0],
        DataValue::Num(Num::Int(_))
    )))
}

define_op!(OP_IS_FLOAT, 1, false);
pub(crate) fn op_is_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(
        args[0],
        DataValue::Num(Num::Float(_))
    )))
}

define_op!(OP_IS_NUM, 1, false);
pub(crate) fn op_is_num(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(
        args[0],
        DataValue::Num(Num::Int(_)) | DataValue::Num(Num::Float(_))
    )))
}

define_op!(OP_IS_FINITE, 1, false);
pub(crate) fn op_is_finite(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Num(Num::Int(_)) => true,
        DataValue::Num(Num::Float(f)) => f.is_finite(),
        _ => false,
    }))
}

define_op!(OP_IS_INFINITE, 1, false);
pub(crate) fn op_is_infinite(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Num(Num::Float(f)) => f.is_infinite(),
        _ => false,
    }))
}

define_op!(OP_IS_NAN, 1, false);
pub(crate) fn op_is_nan(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Num(Num::Float(f)) => f.is_nan(),
        _ => false,
    }))
}

define_op!(OP_IS_STRING, 1, false);
pub(crate) fn op_is_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Str(_))))
}

define_op!(OP_IS_LIST, 1, false);
pub(crate) fn op_is_list(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(
        args[0],
        DataValue::List(_) | DataValue::Set(_)
    )))
}

define_op!(OP_IS_VEC, 1, false);
pub(crate) fn op_is_vec(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Vec(_))))
}

define_op!(OP_APPEND, 2, false);
pub(crate) fn op_append(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => {
            let mut l = l.clone();
            l.push(args[1].clone());
            Ok(DataValue::List(l))
        }
        DataValue::Set(l) => {
            let mut l = l.iter().cloned().collect_vec();
            l.push(args[1].clone());
            Ok(DataValue::List(l))
        }
        _ => bail!("'append' requires first argument to be a list"),
    }
}

define_op!(OP_PREPEND, 2, false);
pub(crate) fn op_prepend(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(pl) => {
            let mut l = vec![args[1].clone()];
            l.extend_from_slice(pl);
            Ok(DataValue::List(l))
        }
        DataValue::Set(pl) => {
            let mut l = vec![args[1].clone()];
            l.extend(pl.iter().cloned());
            Ok(DataValue::List(l))
        }
        _ => bail!("'prepend' requires first argument to be a list"),
    }
}

define_op!(OP_IS_BYTES, 1, false);
pub(crate) fn op_is_bytes(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(matches!(args[0], DataValue::Bytes(_))))
}

define_op!(OP_LENGTH, 1, false);
pub(crate) fn op_length(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Set(s) => s.len() as i64,
        DataValue::List(l) => l.len() as i64,
        DataValue::Str(s) => s.chars().count() as i64,
        DataValue::Bytes(b) => b.len() as i64,
        DataValue::Vec(v) => v.len() as i64,
        _ => bail!("'length' requires lists"),
    }))
}

define_op!(OP_UNICODE_NORMALIZE, 2, false);
pub(crate) fn op_unicode_normalize(args: &[DataValue]) -> Result<DataValue> {
    match (&args[0], &args[1]) {
        (DataValue::Str(s), DataValue::Str(n)) => Ok(DataValue::Str(match n as &str {
            "nfc" => s.nfc().collect(),
            "nfd" => s.nfd().collect(),
            "nfkc" => s.nfkc().collect(),
            "nfkd" => s.nfkd().collect(),
            u => bail!("unknown normalization {} for 'unicode_normalize'", u),
        })),
        _ => bail!("'unicode_normalize' requires strings"),
    }
}

define_op!(OP_SORTED, 1, false);
pub(crate) fn op_sorted(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'sort' requires lists"))?
        .to_vec();
    arg.sort();
    Ok(DataValue::List(arg))
}

define_op!(OP_REVERSE, 1, false);
pub(crate) fn op_reverse(args: &[DataValue]) -> Result<DataValue> {
    let mut arg = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'reverse' requires lists"))?
        .to_vec();
    arg.reverse();
    Ok(DataValue::List(arg))
}

define_op!(OP_HAVERSINE, 4, false);
pub(crate) fn op_haversine(args: &[DataValue]) -> Result<DataValue> {
    let miette = || miette!("'haversine' requires numbers");
    let lat1 = args[0].get_float().ok_or_else(miette)?;
    let lon1 = args[1].get_float().ok_or_else(miette)?;
    let lat2 = args[2].get_float().ok_or_else(miette)?;
    let lon2 = args[3].get_float().ok_or_else(miette)?;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_HAVERSINE_DEG_INPUT, 4, false);
pub(crate) fn op_haversine_deg_input(args: &[DataValue]) -> Result<DataValue> {
    let miette = || miette!("'haversine_deg_input' requires numbers");
    let lat1 = args[0].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lon1 = args[1].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lat2 = args[2].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let lon2 = args[3].get_float().ok_or_else(miette)? * f64::PI() / 180.;
    let ret = 2.
        * f64::asin(f64::sqrt(
            f64::sin((lat1 - lat2) / 2.).powi(2)
                + f64::cos(lat1) * f64::cos(lat2) * f64::sin((lon1 - lon2) / 2.).powi(2),
        ));
    Ok(DataValue::from(ret))
}

define_op!(OP_DEG_TO_RAD, 1, false);
pub(crate) fn op_deg_to_rad(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| miette!("'deg_to_rad' requires numbers"))?;
    Ok(DataValue::from(x * f64::PI() / 180.))
}

define_op!(OP_RAD_TO_DEG, 1, false);
pub(crate) fn op_rad_to_deg(args: &[DataValue]) -> Result<DataValue> {
    let x = args[0]
        .get_float()
        .ok_or_else(|| miette!("'rad_to_deg' requires numbers"))?;
    Ok(DataValue::from(x * 180. / f64::PI()))
}

define_op!(OP_FIRST, 1, false);
pub(crate) fn op_first(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_slice()
        .ok_or_else(|| miette!("'first' requires lists"))?
        .first()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_LAST, 1, false);
pub(crate) fn op_last(args: &[DataValue]) -> Result<DataValue> {
    Ok(args[0]
        .get_slice()
        .ok_or_else(|| miette!("'last' requires lists"))?
        .last()
        .cloned()
        .unwrap_or(DataValue::Null))
}

define_op!(OP_CHUNKS, 2, false);
pub(crate) fn op_chunks(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_slice()
        .ok_or_else(|| miette!("first argument of 'chunks' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'chunks' must be an integer"))?;
    ensure!(n > 0, "second argument to 'chunks' must be positive");
    let res = arg
        .chunks(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_CHUNKS_EXACT, 2, false);
pub(crate) fn op_chunks_exact(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_slice()
        .ok_or_else(|| miette!("first argument of 'chunks_exact' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'chunks_exact' must be an integer"))?;
    ensure!(n > 0, "second argument to 'chunks_exact' must be positive");
    let res = arg
        .chunks_exact(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

define_op!(OP_WINDOWS, 2, false);
pub(crate) fn op_windows(args: &[DataValue]) -> Result<DataValue> {
    let arg = args[0]
        .get_slice()
        .ok_or_else(|| miette!("first argument of 'windows' must be a list"))?;
    let n = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument of 'windows' must be an integer"))?;
    ensure!(n > 0, "second argument to 'windows' must be positive");
    let res = arg
        .windows(n as usize)
        .map(|el| DataValue::List(el.to_vec()))
        .collect_vec();
    Ok(DataValue::List(res))
}

fn get_index(mut i: i64, total: usize, is_upper: bool) -> Result<usize> {
    if i < 0 {
        i += total as i64;
    }
    Ok(if i >= 0 {
        let i = i as usize;
        if i > total || (!is_upper && i == total) {
            bail!("index {} out of bound", i)
        } else {
            i
        }
    } else {
        bail!("index {} out of bound", i)
    })
}

define_op!(OP_GET, 2, true);
pub(crate) fn op_get(args: &[DataValue]) -> Result<DataValue> {
    match get_impl(args) {
        Ok(res) => Ok(res),
        Err(err) => {
            if let Some(default) = args.get(2) {
                Ok(default.clone())
            } else {
                Err(err)
            }
        }
    }
}

fn get_impl(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => {
            let n = args[1]
                .get_int()
                .ok_or_else(|| miette!("second argument to 'get' mut be an integer"))?;
            let idx = get_index(n, l.len(), false)?;
            Ok(l[idx].clone())
        }
        DataValue::Json(json) => {
            let res = match &args[1] {
                DataValue::Str(s) => json
                    .get(s as &str)
                    .ok_or_else(|| miette!("key '{}' not found in json", s))?
                    .clone(),
                DataValue::Num(i) => {
                    let i = i
                        .get_int()
                        .ok_or_else(|| miette!("index '{}' not found in json", i))?;
                    json.get(i as usize)
                        .ok_or_else(|| miette!("index '{}' not found in json", i))?
                        .clone()
                }
                DataValue::List(l) => get_json_path_immutable(json, l)?.clone(),
                _ => bail!("second argument to 'get' mut be a string or integer"),
            };
            let res = json2val(res);
            Ok(res)
        }
        _ => bail!("first argument to 'get' mut be a list or json"),
    }
}

fn json2val(res: Value) -> DataValue {
    match res {
        Value::Null => DataValue::Null,
        Value::Bool(b) => DataValue::Bool(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DataValue::from(i)
            } else if let Some(f) = n.as_f64() {
                DataValue::from(f)
            } else {
                DataValue::Null
            }
        }
        Value::String(s) => DataValue::Str(SmartString::from(s)),
        Value::Array(arr) => DataValue::Json(JsonData(json!(arr))),
        Value::Object(obj) => DataValue::Json(JsonData(json!(obj))),
    }
}

define_op!(OP_MAYBE_GET, 2, false);
pub(crate) fn op_maybe_get(args: &[DataValue]) -> Result<DataValue> {
    match get_impl(args) {
        Ok(res) => Ok(res),
        Err(_) => Ok(DataValue::Null),
    }
}

define_op!(OP_SLICE, 3, false);
pub(crate) fn op_slice(args: &[DataValue]) -> Result<DataValue> {
    let l = args[0]
        .get_slice()
        .ok_or_else(|| miette!("first argument to 'slice' mut be a list"))?;
    let m = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument to 'slice' mut be an integer"))?;
    let n = args[2]
        .get_int()
        .ok_or_else(|| miette!("third argument to 'slice' mut be an integer"))?;
    let m = get_index(m, l.len(), false)?;
    let n = get_index(n, l.len(), true)?;
    Ok(DataValue::List(l[m..n].to_vec()))
}

define_op!(OP_CHARS, 1, false);
pub(crate) fn op_chars(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::List(
        args[0]
            .get_str()
            .ok_or_else(|| miette!("'chars' requires strings"))?
            .chars()
            .map(|c| {
                let mut s = SmartString::new();
                s.push(c);
                DataValue::Str(s)
            })
            .collect_vec(),
    ))
}

define_op!(OP_SLICE_STRING, 3, false);
pub(crate) fn op_slice_string(args: &[DataValue]) -> Result<DataValue> {
    let s = args[0]
        .get_str()
        .ok_or_else(|| miette!("first argument to 'slice_string' mut be a string"))?;
    let m = args[1]
        .get_int()
        .ok_or_else(|| miette!("second argument to 'slice_string' mut be an integer"))?;
    ensure!(
        m >= 0,
        "second argument to 'slice_string' mut be a positive integer"
    );
    let n = args[2]
        .get_int()
        .ok_or_else(|| miette!("third argument to 'slice_string' mut be an integer"))?;
    ensure!(n >= m, "third argument to 'slice_string' mut be a positive integer greater than the second argument");
    Ok(DataValue::Str(
        s.chars().skip(m as usize).take((n - m) as usize).collect(),
    ))
}

define_op!(OP_FROM_SUBSTRINGS, 1, false);
pub(crate) fn op_from_substrings(args: &[DataValue]) -> Result<DataValue> {
    let mut ret = String::new();
    match &args[0] {
        DataValue::List(ss) => {
            for arg in ss {
                if let DataValue::Str(s) = arg {
                    ret.push_str(s);
                } else {
                    bail!("'from_substring' requires a list of strings")
                }
            }
        }
        DataValue::Set(ss) => {
            for arg in ss {
                if let DataValue::Str(s) = arg {
                    ret.push_str(s);
                } else {
                    bail!("'from_substring' requires a list of strings")
                }
            }
        }
        _ => bail!("'from_substring' requires a list of strings"),
    }
    Ok(DataValue::from(ret))
}

define_op!(OP_ENCODE_BASE64, 1, false);
pub(crate) fn op_encode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bytes(b) => {
            let s = STANDARD.encode(b);
            Ok(DataValue::from(s))
        }
        _ => bail!("'encode_base64' requires bytes"),
    }
}

define_op!(OP_DECODE_BASE64, 1, false);
pub(crate) fn op_decode_base64(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Str(s) => {
            let b = STANDARD
                .decode(s)
                .map_err(|_| miette!("Data is not properly encoded"))?;
            Ok(DataValue::Bytes(b))
        }
        _ => bail!("'decode_base64' requires strings"),
    }
}

define_op!(OP_TO_BOOL, 1, false);
pub(crate) fn op_to_bool(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Null => false,
        DataValue::Bool(b) => *b,
        DataValue::Num(n) => n.get_int() != Some(0),
        DataValue::Str(s) => !s.is_empty(),
        DataValue::Bytes(b) => !b.is_empty(),
        DataValue::Uuid(u) => !u.0.is_nil(),
        DataValue::Regex(r) => !r.0.as_str().is_empty(),
        DataValue::List(l) => !l.is_empty(),
        DataValue::Set(s) => !s.is_empty(),
        DataValue::Vec(_) => true,
        DataValue::Validity(vld) => vld.is_assert.0,
        DataValue::Bot => false,
        DataValue::Json(json) => match &json.0 {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_i64() != Some(0),
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
        },
    }))
}

define_op!(OP_TO_UNITY, 1, false);
pub(crate) fn op_to_unity(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::from(match &args[0] {
        DataValue::Null => 0,
        DataValue::Bool(b) => *b as i64,
        DataValue::Num(n) => (n.get_float() != 0.) as i64,
        DataValue::Str(s) => i64::from(!s.is_empty()),
        DataValue::Bytes(b) => i64::from(!b.is_empty()),
        DataValue::Uuid(u) => i64::from(!u.0.is_nil()),
        DataValue::Regex(r) => i64::from(!r.0.as_str().is_empty()),
        DataValue::List(l) => i64::from(!l.is_empty()),
        DataValue::Set(s) => i64::from(!s.is_empty()),
        DataValue::Vec(_) => 1,
        DataValue::Validity(vld) => i64::from(vld.is_assert.0),
        DataValue::Bot => 0,
        DataValue::Json(json) => match &json.0 {
            Value::Null => 0,
            Value::Bool(b) => *b as i64,
            Value::Number(n) => (n.as_i64() != Some(0)) as i64,
            Value::String(s) => !s.is_empty() as i64,
            Value::Array(a) => !a.is_empty() as i64,
            Value::Object(o) => !o.is_empty() as i64,
        },
    }))
}

define_op!(OP_TO_INT, 1, false);
pub(crate) fn op_to_int(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(n) => match n.get_int() {
            None => {
                let f = n.get_float();
                DataValue::Num(Num::Int(f as i64))
            }
            Some(i) => DataValue::Num(Num::Int(i)),
        },
        DataValue::Null => DataValue::from(0),
        DataValue::Bool(b) => DataValue::from(if *b { 1 } else { 0 }),
        DataValue::Str(t) => {
            let s = t as &str;
            i64::from_str(s)
                .map_err(|_| miette!("The string cannot be interpreted as int"))?
                .into()
        }
        DataValue::Validity(vld) => DataValue::Num(Num::Int(vld.timestamp.0 .0)),
        v => bail!("'to_int' does not recognize {:?}", v),
    })
}

define_op!(OP_TO_FLOAT, 1, false);
pub(crate) fn op_to_float(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Num(n) => n.get_float().into(),
        DataValue::Null => DataValue::from(0.0),
        DataValue::Bool(b) => DataValue::from(if *b { 1.0 } else { 0.0 }),
        DataValue::Str(t) => match t as &str {
            "PI" => f64::PI().into(),
            "E" => f64::E().into(),
            "NAN" => f64::NAN.into(),
            "INF" => f64::INFINITY.into(),
            "NEG_INF" => f64::NEG_INFINITY.into(),
            s => f64::from_str(s)
                .map_err(|_| miette!("The string cannot be interpreted as float"))?
                .into(),
        },
        v => bail!("'to_float' does not recognize {:?}", v),
    })
}

define_op!(OP_TO_STRING, 1, false);
pub(crate) fn op_to_string(args: &[DataValue]) -> Result<DataValue> {
    Ok(DataValue::Str(val2str(&args[0]).into()))
}

fn val2str(arg: &DataValue) -> String {
    match arg {
        DataValue::Str(s) => s.to_string(),
        DataValue::Json(JsonData(JsonValue::String(s))) => s.clone(),
        v => {
            let jv = to_json(v);
            jv.to_string()
        }
    }
}

define_op!(OP_VEC, 1, true);
pub(crate) fn op_vec(args: &[DataValue]) -> Result<DataValue> {
    let t = match args.get(1) {
        Some(DataValue::Str(s)) => match s as &str {
            "F32" | "Float" => VecElementType::F32,
            "F64" | "Double" => VecElementType::F64,
            _ => bail!("'vec' does not recognize type {}", s),
        },
        None => VecElementType::F32,
        _ => bail!("'vec' requires a string as second argument"),
    };

    match &args[0] {
        DataValue::Json(j) => match t {
            VecElementType::F32 => {
                let mut res_arr = ndarray::Array1::zeros(j.0.as_array().unwrap().len());
                for (mut row, el) in res_arr
                    .axis_iter_mut(ndarray::Axis(0))
                    .zip(j.0.as_array().unwrap().iter())
                {
                    let f = el
                        .as_f64()
                        .ok_or_else(|| miette!("'vec' requires a list of numbers"))?;
                    row.fill(f as f32);
                }
                Ok(DataValue::Vec(Vector::F32(res_arr)))
            }
            VecElementType::F64 => {
                let mut res_arr = ndarray::Array1::zeros(j.0.as_array().unwrap().len());
                for (mut row, el) in res_arr
                    .axis_iter_mut(ndarray::Axis(0))
                    .zip(j.0.as_array().unwrap().iter())
                {
                    let f = el
                        .as_f64()
                        .ok_or_else(|| miette!("'vec' requires a list of numbers"))?;
                    row.fill(f);
                }
                Ok(DataValue::Vec(Vector::F64(res_arr)))
            }
        },
        DataValue::List(l) => match t {
            VecElementType::F32 => {
                let mut res_arr = ndarray::Array1::zeros(l.len());
                for (mut row, el) in res_arr.axis_iter_mut(ndarray::Axis(0)).zip(l.iter()) {
                    let f = el
                        .get_float()
                        .ok_or_else(|| miette!("'vec' requires a list of numbers"))?;
                    row.fill(f as f32);
                }
                Ok(DataValue::Vec(Vector::F32(res_arr)))
            }
            VecElementType::F64 => {
                let mut res_arr = ndarray::Array1::zeros(l.len());
                for (mut row, el) in res_arr.axis_iter_mut(ndarray::Axis(0)).zip(l.iter()) {
                    let f = el
                        .get_float()
                        .ok_or_else(|| miette!("'vec' requires a list of numbers"))?;
                    row.fill(f);
                }
                Ok(DataValue::Vec(Vector::F64(res_arr)))
            }
        },
        DataValue::Vec(v) => match (t, v) {
            (VecElementType::F32, Vector::F32(v)) => Ok(DataValue::Vec(Vector::F32(v.clone()))),
            (VecElementType::F64, Vector::F64(v)) => Ok(DataValue::Vec(Vector::F64(v.clone()))),
            (VecElementType::F32, Vector::F64(v)) => {
                Ok(DataValue::Vec(Vector::F32(v.mapv(|x| x as f32))))
            }
            (VecElementType::F64, Vector::F32(v)) => {
                Ok(DataValue::Vec(Vector::F64(v.mapv(|x| x as f64))))
            }
        },
        DataValue::Str(s) => {
            let bytes = STANDARD
                .decode(s)
                .map_err(|_| miette!("Data is not base64 encoded"))?;
            match t {
                VecElementType::F32 => {
                    let f32_count = bytes.len() / mem::size_of::<f32>();
                    let arr = unsafe {
                        ndarray::ArrayView1::from_shape_ptr(
                            ndarray::Dim([f32_count]),
                            bytes.as_ptr() as *const f32,
                        )
                    };
                    Ok(DataValue::Vec(Vector::F32(arr.to_owned())))
                }
                VecElementType::F64 => {
                    let f64_count = bytes.len() / mem::size_of::<f64>();
                    let arr = unsafe {
                        ndarray::ArrayView1::from_shape_ptr(
                            ndarray::Dim([f64_count]),
                            bytes.as_ptr() as *const f64,
                        )
                    };
                    Ok(DataValue::Vec(Vector::F64(arr.to_owned())))
                }
            }
        }
        _ => bail!("'vec' requires a list or a vector"),
    }
}

define_op!(OP_RAND_VEC, 1, true);
pub(crate) fn op_rand_vec(args: &[DataValue]) -> Result<DataValue> {
    let len = args[0]
        .get_int()
        .ok_or_else(|| miette!("'rand_vec' requires an integer"))? as usize;
    let t = match args.get(1) {
        Some(DataValue::Str(s)) => match s as &str {
            "F32" | "Float" => VecElementType::F32,
            "F64" | "Double" => VecElementType::F64,
            _ => bail!("'vec' does not recognize type {}", s),
        },
        None => VecElementType::F32,
        _ => bail!("'vec' requires a string as second argument"),
    };

    let mut rng = thread_rng();
    match t {
        VecElementType::F32 => {
            let mut res_arr = ndarray::Array1::zeros(len);
            for mut row in res_arr.axis_iter_mut(ndarray::Axis(0)) {
                row.fill(rng.gen::<f64>() as f32);
            }
            Ok(DataValue::Vec(Vector::F32(res_arr)))
        }
        VecElementType::F64 => {
            let mut res_arr = ndarray::Array1::zeros(len);
            for mut row in res_arr.axis_iter_mut(ndarray::Axis(0)) {
                row.fill(rng.gen::<f64>());
            }
            Ok(DataValue::Vec(Vector::F64(res_arr)))
        }
    }
}

define_op!(OP_L2_NORMALIZE, 1, false);
pub(crate) fn op_l2_normalize(args: &[DataValue]) -> Result<DataValue> {
    let a = &args[0];
    match a {
        DataValue::Vec(Vector::F32(a)) => {
            let norm = a.dot(a).sqrt();
            Ok(DataValue::Vec(Vector::F32(a / norm)))
        }
        DataValue::Vec(Vector::F64(a)) => {
            let norm = a.dot(a).sqrt();
            Ok(DataValue::Vec(Vector::F64(a / norm)))
        }
        _ => bail!("'l2_normalize' requires a vector"),
    }
}

define_op!(OP_L2_DIST, 2, false);
pub(crate) fn op_l2_dist(args: &[DataValue]) -> Result<DataValue> {
    let a = &args[0];
    let b = &args[1];
    match (a, b) {
        (DataValue::Vec(Vector::F32(a)), DataValue::Vec(Vector::F32(b))) => {
            if a.len() != b.len() {
                bail!("'l2_dist' requires two vectors of the same length");
            }
            let diff = a - b;
            Ok(DataValue::from(diff.dot(&diff) as f64))
        }
        (DataValue::Vec(Vector::F64(a)), DataValue::Vec(Vector::F64(b))) => {
            if a.len() != b.len() {
                bail!("'l2_dist' requires two vectors of the same length");
            }
            let diff = a - b;
            Ok(DataValue::from(diff.dot(&diff)))
        }
        _ => bail!("'l2_dist' requires two vectors of the same type"),
    }
}

define_op!(OP_IP_DIST, 2, false);
pub(crate) fn op_ip_dist(args: &[DataValue]) -> Result<DataValue> {
    let a = &args[0];
    let b = &args[1];
    match (a, b) {
        (DataValue::Vec(Vector::F32(a)), DataValue::Vec(Vector::F32(b))) => {
            if a.len() != b.len() {
                bail!("'ip_dist' requires two vectors of the same length");
            }
            let dot = a.dot(b);
            Ok(DataValue::from(1. - dot as f64))
        }
        (DataValue::Vec(Vector::F64(a)), DataValue::Vec(Vector::F64(b))) => {
            if a.len() != b.len() {
                bail!("'ip_dist' requires two vectors of the same length");
            }
            let dot = a.dot(b);
            Ok(DataValue::from(1. - dot))
        }
        _ => bail!("'ip_dist' requires two vectors of the same type"),
    }
}

define_op!(OP_COS_DIST, 2, false);
pub(crate) fn op_cos_dist(args: &[DataValue]) -> Result<DataValue> {
    let a = &args[0];
    let b = &args[1];
    match (a, b) {
        (DataValue::Vec(Vector::F32(a)), DataValue::Vec(Vector::F32(b))) => {
            if a.len() != b.len() {
                bail!("'cos_dist' requires two vectors of the same length");
            }
            let a_norm = a.dot(a) as f64;
            let b_norm = b.dot(b) as f64;
            let dot = a.dot(b) as f64;
            Ok(DataValue::from(1. - dot / (a_norm * b_norm).sqrt()))
        }
        (DataValue::Vec(Vector::F64(a)), DataValue::Vec(Vector::F64(b))) => {
            if a.len() != b.len() {
                bail!("'cos_dist' requires two vectors of the same length");
            }
            let a_norm = a.dot(a);
            let b_norm = b.dot(b);
            let dot = a.dot(b);
            Ok(DataValue::from(1. - dot / (a_norm * b_norm).sqrt()))
        }
        _ => bail!("'cos_dist' requires two vectors of the same type"),
    }
}

define_op!(OP_INT_RANGE, 1, true);
pub(crate) fn op_int_range(args: &[DataValue]) -> Result<DataValue> {
    let [start, end] = match args.len() {
        1 => {
            let end = args[0]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for end"))?;
            [0, end]
        }
        2 => {
            let start = args[0]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for start"))?;
            let end = args[1]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for end"))?;
            [start, end]
        }
        3 => {
            let start = args[0]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for start"))?;
            let end = args[1]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for end"))?;
            let step = args[2]
                .get_int()
                .ok_or_else(|| miette!("'int_range' requires integer argument for step"))?;
            let mut current = start;
            let mut result = vec![];
            if step > 0 {
                while current < end {
                    result.push(DataValue::from(current));
                    current += step;
                }
            } else {
                while current > end {
                    result.push(DataValue::from(current));
                    current += step;
                }
            }
            return Ok(DataValue::List(result));
        }
        _ => bail!("'int_range' requires 1 to 3 argument"),
    };
    Ok(DataValue::List((start..end).map(DataValue::from).collect()))
}

define_op!(OP_RAND_FLOAT, 0, false);
pub(crate) fn op_rand_float(_args: &[DataValue]) -> Result<DataValue> {
    Ok(thread_rng().gen::<f64>().into())
}

define_op!(OP_RAND_BERNOULLI, 1, false);
pub(crate) fn op_rand_bernoulli(args: &[DataValue]) -> Result<DataValue> {
    let prob = match &args[0] {
        DataValue::Num(n) => {
            let f = n.get_float();
            ensure!(
                (0. ..=1.).contains(&f),
                "'rand_bernoulli' requires number between 0. and 1."
            );
            f
        }
        _ => bail!("'rand_bernoulli' requires number between 0. and 1."),
    };
    Ok(DataValue::from(thread_rng().gen_bool(prob)))
}

define_op!(OP_RAND_INT, 2, false);
pub(crate) fn op_rand_int(args: &[DataValue]) -> Result<DataValue> {
    let lower = &args[0]
        .get_int()
        .ok_or_else(|| miette!("'rand_int' requires integers"))?;
    let upper = &args[1]
        .get_int()
        .ok_or_else(|| miette!("'rand_int' requires integers"))?;
    Ok(thread_rng().gen_range(*lower..=*upper).into())
}

define_op!(OP_RAND_CHOOSE, 1, false);
pub(crate) fn op_rand_choose(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::List(l) => Ok(l
            .choose(&mut thread_rng())
            .cloned()
            .unwrap_or(DataValue::Null)),
        DataValue::Set(l) => Ok(l
            .iter()
            .collect_vec()
            .choose(&mut thread_rng())
            .cloned()
            .cloned()
            .unwrap_or(DataValue::Null)),
        _ => bail!("'rand_choice' requires lists"),
    }
}

define_op!(OP_ASSERT, 1, true);
pub(crate) fn op_assert(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        DataValue::Bool(true) => Ok(DataValue::from(true)),
        _ => bail!("assertion failed: {:?}", args),
    }
}

define_op!(OP_UNION, 1, true);
pub(crate) fn op_union(args: &[DataValue]) -> Result<DataValue> {
    let mut ret = BTreeSet::new();
    for arg in args {
        match arg {
            DataValue::List(l) => {
                for el in l {
                    ret.insert(el.clone());
                }
            }
            DataValue::Set(s) => {
                for el in s {
                    ret.insert(el.clone());
                }
            }
            _ => bail!("'union' requires lists"),
        }
    }
    Ok(DataValue::List(ret.into_iter().collect()))
}

define_op!(OP_DIFFERENCE, 2, true);
pub(crate) fn op_difference(args: &[DataValue]) -> Result<DataValue> {
    let mut start: BTreeSet<_> = match &args[0] {
        DataValue::List(l) => l.iter().cloned().collect(),
        DataValue::Set(s) => s.iter().cloned().collect(),
        _ => bail!("'difference' requires lists"),
    };
    for arg in &args[1..] {
        match arg {
            DataValue::List(l) => {
                for el in l {
                    start.remove(el);
                }
            }
            DataValue::Set(s) => {
                for el in s {
                    start.remove(el);
                }
            }
            _ => bail!("'difference' requires lists"),
        }
    }
    Ok(DataValue::List(start.into_iter().collect()))
}

define_op!(OP_INTERSECTION, 1, true);
pub(crate) fn op_intersection(args: &[DataValue]) -> Result<DataValue> {
    let mut start: BTreeSet<_> = match &args[0] {
        DataValue::List(l) => l.iter().cloned().collect(),
        DataValue::Set(s) => s.iter().cloned().collect(),
        _ => bail!("'intersection' requires lists"),
    };
    for arg in &args[1..] {
        match arg {
            DataValue::List(l) => {
                let other: BTreeSet<_> = l.iter().cloned().collect();
                start = start.intersection(&other).cloned().collect();
            }
            DataValue::Set(s) => start = start.intersection(s).cloned().collect(),
            _ => bail!("'intersection' requires lists"),
        }
    }
    Ok(DataValue::List(start.into_iter().collect()))
}

define_op!(OP_TO_UUID, 1, false);
pub(crate) fn op_to_uuid(args: &[DataValue]) -> Result<DataValue> {
    match &args[0] {
        d @ DataValue::Uuid(_u) => Ok(d.clone()),
        DataValue::Str(s) => {
            let id = uuid::Uuid::try_parse(s).map_err(|_| miette!("invalid UUID"))?;
            Ok(DataValue::uuid(id))
        }
        _ => bail!("'to_uuid' requires a string"),
    }
}

define_op!(OP_NOW, 0, false);
#[cfg(target_arch = "wasm32")]
pub(crate) fn op_now(_args: &[DataValue]) -> Result<DataValue> {
    let d: f64 = Date::now() / 1000.;
    Ok(DataValue::from(d))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn op_now(_args: &[DataValue]) -> Result<DataValue> {
    let now = SystemTime::now();
    Ok(DataValue::from(
        now.duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
    ))
}

pub fn current_validity() -> ValidityTs {
    #[cfg(not(target_arch = "wasm32"))]
    let ts_micros = {
        let now = SystemTime::now();
        now.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64
    };
    #[cfg(target_arch = "wasm32")]
    let ts_micros = { (Date::now() * 1000.) as i64 };

    ValidityTs(Reverse(ts_micros))
}

pub(crate) const MAX_VALIDITY_TS: ValidityTs = ValidityTs(Reverse(i64::MAX));
pub(crate) const TERMINAL_VALIDITY: Validity = Validity {
    timestamp: ValidityTs(Reverse(i64::MIN)),
    is_assert: Reverse(false),
};

define_op!(OP_FORMAT_TIMESTAMP, 1, true);
pub(crate) fn op_format_timestamp(args: &[DataValue]) -> Result<DataValue> {
    let dt = {
        let millis = match &args[0] {
            DataValue::Validity(vld) => vld.timestamp.0 .0 / 1000,
            v => {
                let f = v
                    .get_float()
                    .ok_or_else(|| miette!("'format_timestamp' expects a number"))?;
                (f * 1000.) as i64
            }
        };
        Utc.timestamp_millis_opt(millis)
            .latest()
            .ok_or_else(|| miette!("bad time: {}", &args[0]))?
    };
    match args.get(1) {
        Some(tz_v) => {
            let tz_s = tz_v.get_str().ok_or_else(|| {
                miette!("'format_timestamp' timezone specification requires a string")
            })?;
            let tz = chrono_tz::Tz::from_str(tz_s)
                .map_err(|_| miette!("bad timezone specification: {}", tz_s))?;
            let dt_tz = dt.with_timezone(&tz);
            let s = SmartString::from(dt_tz.to_rfc3339());
            Ok(DataValue::Str(s))
        }
        None => {
            let s = SmartString::from(dt.to_rfc3339());
            Ok(DataValue::Str(s))
        }
    }
}

define_op!(OP_PARSE_TIMESTAMP, 1, false);
pub(crate) fn op_parse_timestamp(args: &[DataValue]) -> Result<DataValue> {
    let s = args[0]
        .get_str()
        .ok_or_else(|| miette!("'parse_timestamp' expects a string"))?;
    let dt = DateTime::parse_from_rfc3339(s).map_err(|_| miette!("bad datetime: {}", s))?;
    let st: SystemTime = dt.into();
    Ok(DataValue::from(
        st.duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
    ))
}

pub(crate) fn str2vld(s: &str) -> Result<ValidityTs> {
    let dt = DateTime::parse_from_rfc3339(s).map_err(|_| miette!("bad datetime: {}", s))?;
    let st: SystemTime = dt.into();
    let microseconds = st.duration_since(UNIX_EPOCH).unwrap().as_micros();
    Ok(ValidityTs(Reverse(microseconds as i64)))
}

define_op!(OP_RAND_UUID_V1, 0, false);
pub(crate) fn op_rand_uuid_v1(_args: &[DataValue]) -> Result<DataValue> {
    let mut rng = rand::thread_rng();
    let uuid_ctx = uuid::v1::Context::new(rng.gen());
    #[cfg(target_arch = "wasm32")]
    let ts = {
        let since_epoch: f64 = Date::now();
        let seconds = since_epoch.floor();
        let fractional = (since_epoch - seconds) * 1.0e9;
        Timestamp::from_unix(uuid_ctx, seconds as u64, fractional as u32)
    };
    #[cfg(not(target_arch = "wasm32"))]
    let ts = {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        Timestamp::from_unix(uuid_ctx, since_epoch.as_secs(), since_epoch.subsec_nanos())
    };
    let mut rand_vals = [0u8; 6];
    rng.fill(&mut rand_vals);
    let id = uuid::Uuid::new_v1(ts, &rand_vals);
    Ok(DataValue::uuid(id))
}

define_op!(OP_RAND_UUID_V4, 0, false);
pub(crate) fn op_rand_uuid_v4(_args: &[DataValue]) -> Result<DataValue> {
    let id = uuid::Uuid::new_v4();
    Ok(DataValue::uuid(id))
}

define_op!(OP_UUID_TIMESTAMP, 1, false);
pub(crate) fn op_uuid_timestamp(args: &[DataValue]) -> Result<DataValue> {
    Ok(match &args[0] {
        DataValue::Uuid(UuidWrapper(id)) => match id.get_timestamp() {
            None => DataValue::Null,
            Some(t) => {
                let (s, subs) = t.to_unix();
                let s = (s as f64) + (subs as f64 / 10_000_000.);
                s.into()
            }
        },
        _ => bail!("not an UUID"),
    })
}

define_op!(OP_VALIDITY, 1, true);
pub(crate) fn op_validity(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_int()
        .ok_or_else(|| miette!("'validity' expects an integer"))?;
    let is_assert = if args.len() == 1 {
        true
    } else {
        args[1]
            .get_bool()
            .ok_or_else(|| miette!("'validity' expects a boolean as second argument"))?
    };
    Ok(DataValue::Validity(Validity {
        timestamp: ValidityTs(Reverse(ts)),
        is_assert: Reverse(is_assert),
    }))
}

define_op!(OP_TO_LOCAL_PARTS, 2, false);
pub(crate) fn op_to_local_parts(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'to_local_parts' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'to_local_parts' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    let weekday = match dt_tz.weekday() {
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
        Weekday::Sun => 7,
    };

    let result = json!({
        "year": dt_tz.year(),
        "month": dt_tz.month() as i32,
        "day": dt_tz.day() as i32,
        "hour": dt_tz.hour() as i32,
        "minute": dt_tz.minute() as i32,
        "second": dt_tz.second() as i32,
        "dow": weekday,
        "yday": dt_tz.ordinal() as i32,
    });

    Ok(DataValue::Json(JsonData(result)))
}

define_op!(OP_FROM_LOCAL_PARTS, 7, false);
pub(crate) fn op_from_local_parts(args: &[DataValue]) -> Result<DataValue> {
    let year = args[0]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects year as integer"))?;
    let month = args[1]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects month as integer"))?;
    let day = args[2]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects day as integer"))?;
    let hour = args[3]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects hour as integer"))?;
    let minute = args[4]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects minute as integer"))?;
    let second = args[5]
        .get_int()
        .ok_or_else(|| miette!("'from_local_parts' expects second as integer"))?;
    let tz_str = args[6]
        .get_str()
        .ok_or_else(|| miette!("'from_local_parts' expects timezone string as last argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = tz.with_ymd_and_hms(year as i32, month as u32, day as u32, hour as u32, minute as u32, second as u32)
        .single()
        .ok_or_else(|| miette!("Invalid date/time parts"))?;

    Ok(DataValue::from(dt.timestamp() as f64))
}

define_op!(OP_YEAR, 2, false);
pub(crate) fn op_year(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'year' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'year' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    Ok(DataValue::from(dt_tz.year() as i64))
}

define_op!(OP_MONTH, 2, false);
pub(crate) fn op_month(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'month' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'month' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    Ok(DataValue::from(dt_tz.month() as i64))
}

define_op!(OP_DAY, 2, false);
pub(crate) fn op_day(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'day' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'day' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    Ok(DataValue::from(dt_tz.day() as i64))
}

define_op!(OP_DOW, 2, false);
pub(crate) fn op_dow(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'dow' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'dow' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    let weekday = match dt_tz.weekday() {
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
        Weekday::Sun => 7,
    };

    Ok(DataValue::from(weekday as i64))
}

define_op!(OP_HOUR, 2, false);
pub(crate) fn op_hour(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'hour' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'hour' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    Ok(DataValue::from(dt_tz.hour() as i64))
}

define_op!(OP_MINUTE, 2, false);
pub(crate) fn op_minute(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'minute' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'minute' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    Ok(DataValue::from(dt_tz.minute() as i64))
}

define_op!(OP_DAYS_IN_MONTH, 3, false);
pub(crate) fn op_days_in_month(args: &[DataValue]) -> Result<DataValue> {
    let year = args[0]
        .get_int()
        .ok_or_else(|| miette!("'days_in_month' expects year as integer"))?;
    let month = args[1]
        .get_int()
        .ok_or_else(|| miette!("'days_in_month' expects month as integer"))?;
    let _tz_str = args[2]
        .get_str()
        .ok_or_else(|| miette!("'days_in_month' expects timezone string as third argument"))?;

    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if is_leap { 29 } else { 28 }
        }
        _ => bail!("Invalid month: {}", month),
    };

    Ok(DataValue::from(days as i64))
}

define_op!(OP_START_OF_DAY_LOCAL, 2, false);
pub(crate) fn op_start_of_day_local(args: &[DataValue]) -> Result<DataValue> {
    let ts = args[0]
        .get_float()
        .ok_or_else(|| miette!("'start_of_day_local' expects a number as first argument"))?;
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'start_of_day_local' expects a timezone string as second argument"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    let dt = Utc.timestamp_opt(ts as i64, ((ts.fract() * 1_000_000_000.0) as u32))
        .single()
        .ok_or_else(|| miette!("Invalid timestamp"))?;
    let dt_tz = dt.with_timezone(&tz);

    let start_of_day = dt_tz.date_naive().and_hms_opt(0, 0, 0)
        .ok_or_else(|| miette!("Failed to create start of day"))?;
    let start_dt = tz.from_local_datetime(&start_of_day)
        .single()
        .ok_or_else(|| miette!("Failed to convert to timezone"))?;

    Ok(DataValue::from(start_dt.timestamp() as f64))
}

define_op!(OP_INTERVAL, 2, false);
pub(crate) fn op_interval(args: &[DataValue]) -> Result<DataValue> {
    let s = args[0]
        .get_int()
        .ok_or_else(|| miette!("'interval' expects start as integer"))?;
    let e = args[1]
        .get_int()
        .ok_or_else(|| miette!("'interval' expects end as integer"))?;

    if s >= e {
        bail!("'interval' expects start < end, got {} >= {}", s, e);
    }

    Ok(DataValue::List(vec![DataValue::from(s), DataValue::from(e)]))
}

define_op!(OP_INTERVAL_LEN, 1, false);
pub(crate) fn op_interval_len(args: &[DataValue]) -> Result<DataValue> {
    let iv = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_len' expects an interval (list)"))?;

    if iv.len() != 2 {
        bail!("'interval_len' expects interval with exactly 2 elements");
    }

    let s = iv[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let e = iv[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(e - s))
}

define_op!(OP_INTERVAL_INTERSECTS, 2, false);
pub(crate) fn op_interval_intersects(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_intersects' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_intersects' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_intersects' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    let intersects = as_ < be && bs < ae;
    Ok(DataValue::from(intersects))
}

define_op!(OP_INTERVAL_OVERLAP, 2, false);
pub(crate) fn op_interval_overlap(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_overlap' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_overlap' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_overlap' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    let s = as_.max(bs);
    let e = ae.min(be);

    if s < e {
        Ok(DataValue::List(vec![DataValue::from(s), DataValue::from(e)]))
    } else {
        Ok(DataValue::Null)
    }
}

define_op!(OP_INTERVAL_UNION, 2, false);
pub(crate) fn op_interval_union(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_union' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_union' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_union' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    // Check if intervals are overlapping (not just adjacent)
    if ae > bs && as_ < be {
        // Can merge into single interval
        let s = as_.min(bs);
        let e = ae.max(be);
        Ok(DataValue::List(vec![
            DataValue::List(vec![DataValue::from(s), DataValue::from(e)])
        ]))
    } else {
        // Return two separate intervals, sorted
        if as_ < bs {
            Ok(DataValue::List(vec![
                DataValue::List(vec![DataValue::from(as_), DataValue::from(ae)]),
                DataValue::List(vec![DataValue::from(bs), DataValue::from(be)])
            ]))
        } else {
            Ok(DataValue::List(vec![
                DataValue::List(vec![DataValue::from(bs), DataValue::from(be)]),
                DataValue::List(vec![DataValue::from(as_), DataValue::from(ae)])
            ]))
        }
    }
}

define_op!(OP_INTERVAL_MINUS, 2, false);
pub(crate) fn op_interval_minus(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_minus' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_minus' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_minus' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    // No intersection, return original interval
    if ae <= bs || be <= as_ {
        Ok(DataValue::List(vec![
            DataValue::List(vec![DataValue::from(as_), DataValue::from(ae)])
        ]))
    }
    // b completely contains a
    else if bs <= as_ && be >= ae {
        Ok(DataValue::List(vec![]))
    }
    // b cuts from the left
    else if bs <= as_ && be < ae {
        Ok(DataValue::List(vec![
            DataValue::List(vec![DataValue::from(be), DataValue::from(ae)])
        ]))
    }
    // b cuts from the right
    else if bs > as_ && be >= ae {
        Ok(DataValue::List(vec![
            DataValue::List(vec![DataValue::from(as_), DataValue::from(bs)])
        ]))
    }
    // b cuts in the middle
    else {
        Ok(DataValue::List(vec![
            DataValue::List(vec![DataValue::from(as_), DataValue::from(bs)]),
            DataValue::List(vec![DataValue::from(be), DataValue::from(ae)])
        ]))
    }
}

define_op!(OP_INTERVAL_ADJACENT, 2, false);
pub(crate) fn op_interval_adjacent(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_adjacent' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_adjacent' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_adjacent' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    let adjacent = ae == bs || be == as_;
    Ok(DataValue::from(adjacent))
}

define_op!(OP_INTERVAL_MERGE_ADJACENT, 1, false);
pub(crate) fn op_interval_merge_adjacent(args: &[DataValue]) -> Result<DataValue> {
    let intervals = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_merge_adjacent' expects a list of intervals"))?;

    if intervals.is_empty() {
        return Ok(DataValue::List(vec![]));
    }

    // Extract and validate all intervals
    let mut ivs: Vec<(i64, i64)> = vec![];
    for iv in intervals {
        let iv_list = iv.get_slice()
            .ok_or_else(|| miette!("each element must be an interval (list)"))?;
        if iv_list.len() != 2 {
            bail!("each interval must have exactly 2 elements");
        }
        let s = iv_list[0].get_int()
            .ok_or_else(|| miette!("interval start must be integer"))?;
        let e = iv_list[1].get_int()
            .ok_or_else(|| miette!("interval end must be integer"))?;
        ivs.push((s, e));
    }

    // Sort by start time
    ivs.sort_by_key(|&(s, _)| s);

    // Merge adjacent/overlapping intervals
    let mut result = vec![];
    let mut current = ivs[0];

    for &(s, e) in &ivs[1..] {
        if current.1 >= s {
            // Adjacent or overlapping - merge
            current.1 = current.1.max(e);
        } else {
            // Not adjacent - save current and start new
            result.push(DataValue::List(vec![
                DataValue::from(current.0),
                DataValue::from(current.1)
            ]));
            current = (s, e);
        }
    }

    // Add the last interval
    result.push(DataValue::List(vec![
        DataValue::from(current.0),
        DataValue::from(current.1)
    ]));

    Ok(DataValue::List(result))
}

define_op!(OP_INTERVAL_SHIFT, 2, false);
pub(crate) fn op_interval_shift(args: &[DataValue]) -> Result<DataValue> {
    let iv = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_shift' expects an interval (list)"))?;
    let d = args[1]
        .get_int()
        .ok_or_else(|| miette!("'interval_shift' expects shift amount as integer"))?;

    if iv.len() != 2 {
        bail!("'interval_shift' expects interval with exactly 2 elements");
    }

    let s = iv[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let e = iv[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::List(vec![
        DataValue::from(s + d),
        DataValue::from(e + d)
    ]))
}

define_op!(OP_INTERVAL_CONTAINS, 2, false);
pub(crate) fn op_interval_contains(args: &[DataValue]) -> Result<DataValue> {
    let iv = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_contains' expects an interval (list)"))?;
    let t = args[1]
        .get_int()
        .ok_or_else(|| miette!("'interval_contains' expects time as integer"))?;

    if iv.len() != 2 {
        bail!("'interval_contains' expects interval with exactly 2 elements");
    }

    let s = iv[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let e = iv[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(s <= t && t < e))
}

define_op!(OP_INTERVAL_CONTAINS_INTERVAL, 2, false);
pub(crate) fn op_interval_contains_interval(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'interval_contains_interval' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'interval_contains_interval' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'interval_contains_interval' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(as_ <= bs && be <= ae))
}

define_op!(OP_ALLEN_BEFORE, 2, false);
pub(crate) fn op_allen_before(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_before' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_before' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_before' expects intervals with exactly 2 elements");
    }

    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;

    Ok(DataValue::from(ae < bs))
}

define_op!(OP_ALLEN_MEETS, 2, false);
pub(crate) fn op_allen_meets(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_meets' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_meets' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_meets' expects intervals with exactly 2 elements");
    }

    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;

    Ok(DataValue::from(ae == bs))
}

define_op!(OP_ALLEN_OVERLAPS, 2, false);
pub(crate) fn op_allen_overlaps(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_overlaps' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_overlaps' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_overlaps' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(as_ < bs && bs < ae && ae < be))
}

define_op!(OP_ALLEN_STARTS, 2, false);
pub(crate) fn op_allen_starts(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_starts' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_starts' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_starts' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(as_ == bs && ae < be))
}

define_op!(OP_ALLEN_DURING, 2, false);
pub(crate) fn op_allen_during(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_during' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_during' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_during' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(bs < as_ && ae < be))
}

define_op!(OP_ALLEN_FINISHES, 2, false);
pub(crate) fn op_allen_finishes(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_finishes' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_finishes' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_finishes' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(as_ > bs && ae == be))
}

define_op!(OP_ALLEN_EQUALS, 2, false);
pub(crate) fn op_allen_equals(args: &[DataValue]) -> Result<DataValue> {
    let a = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'allen_equals' expects first interval as list"))?;
    let b = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'allen_equals' expects second interval as list"))?;

    if a.len() != 2 || b.len() != 2 {
        bail!("'allen_equals' expects intervals with exactly 2 elements");
    }

    let as_ = a[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let ae = a[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;
    let bs = b[0].get_int().ok_or_else(|| miette!("interval start must be integer"))?;
    let be = b[1].get_int().ok_or_else(|| miette!("interval end must be integer"))?;

    Ok(DataValue::from(as_ == bs && ae == be))
}

define_op!(OP_ALLEN_AFTER, 2, false);
pub(crate) fn op_allen_after(args: &[DataValue]) -> Result<DataValue> {
    // a after b is equivalent to b before a
    op_allen_before(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_ALLEN_MET_BY, 2, false);
pub(crate) fn op_allen_met_by(args: &[DataValue]) -> Result<DataValue> {
    // a met by b is equivalent to b meets a
    op_allen_meets(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_ALLEN_OVERLAPPED_BY, 2, false);
pub(crate) fn op_allen_overlapped_by(args: &[DataValue]) -> Result<DataValue> {
    // a overlapped by b is equivalent to b overlaps a
    op_allen_overlaps(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_ALLEN_STARTED_BY, 2, false);
pub(crate) fn op_allen_started_by(args: &[DataValue]) -> Result<DataValue> {
    // a started by b is equivalent to b starts a
    op_allen_starts(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_ALLEN_CONTAINS, 2, false);
pub(crate) fn op_allen_contains(args: &[DataValue]) -> Result<DataValue> {
    // a contains b is equivalent to b during a
    op_allen_during(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_ALLEN_FINISHED_BY, 2, false);
pub(crate) fn op_allen_finished_by(args: &[DataValue]) -> Result<DataValue> {
    // a finished by b is equivalent to b finishes a
    op_allen_finishes(&[args[1].clone(), args[0].clone()])
}

define_op!(OP_EXPAND_WEEKLY_DAYS, 6, false);
pub(crate) fn op_expand_weekly_days(args: &[DataValue]) -> Result<DataValue> {
    let start_ts = args[0]
        .get_int()
        .ok_or_else(|| miette!("'expand_weekly_days' expects start timestamp as integer"))?;
    let end_ts = args[1]
        .get_int()
        .ok_or_else(|| miette!("'expand_weekly_days' expects end timestamp as integer"))?;

    let by_wday_slice = args[2]
        .get_slice()
        .ok_or_else(|| miette!("'expand_weekly_days' expects by_wday as list"))?;
    let by_wday: Result<Vec<i64>, _> = by_wday_slice.iter()
        .map(|v| v.get_int().ok_or_else(|| miette!("weekday must be integer")))
        .collect();
    let by_wday = by_wday?;

    let tz_str = args[3]
        .get_str()
        .ok_or_else(|| miette!("'expand_weekly_days' expects timezone string"))?;
    let start_min = args[4]
        .get_int()
        .ok_or_else(|| miette!("'expand_weekly_days' expects start_min as integer"))?;
    let end_min = args[5]
        .get_int()
        .ok_or_else(|| miette!("'expand_weekly_days' expects end_min as integer"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Convert timestamps to dates in the target timezone
    let start_dt = Utc.timestamp_opt(start_ts, 0)
        .single()
        .ok_or_else(|| miette!("Invalid start timestamp"))?
        .with_timezone(&tz);
    let end_dt = Utc.timestamp_opt(end_ts, 0)
        .single()
        .ok_or_else(|| miette!("Invalid end timestamp"))?
        .with_timezone(&tz);

    let mut intervals = Vec::new();
    let mut current_date = start_dt.date_naive();
    let end_date = end_dt.date_naive();

    // Iterate through each day in the range
    while current_date < end_date {
        // Get weekday (1 = Monday, 7 = Sunday)
        let weekday = match current_date.weekday() {
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
            Weekday::Sun => 7,
        };

        // Check if this weekday is in the requested list
        if by_wday.contains(&weekday) {
            // Create start time for this day
            let start_hour = (start_min / 60) as u32;
            let start_minute = (start_min % 60) as u32;
            let day_start = current_date.and_hms_opt(start_hour, start_minute, 0)
                .ok_or_else(|| miette!("Invalid start time"))?;
            let day_start_utc = tz.from_local_datetime(&day_start)
                .single()
                .ok_or_else(|| miette!("Ambiguous start time in timezone"))?;

            // Create end time for this day
            let end_hour = (end_min / 60) as u32;
            let end_minute = (end_min % 60) as u32;
            let day_end = current_date.and_hms_opt(end_hour, end_minute, 0)
                .ok_or_else(|| miette!("Invalid end time"))?;
            let day_end_utc = tz.from_local_datetime(&day_end)
                .single()
                .ok_or_else(|| miette!("Ambiguous end time in timezone"))?;

            intervals.push(DataValue::List(vec![
                DataValue::from(day_start_utc.timestamp()),
                DataValue::from(day_end_utc.timestamp())
            ]));
        }

        current_date = current_date.succ_opt()
            .ok_or_else(|| miette!("Date overflow"))?;
    }

    Ok(DataValue::List(intervals))
}

define_op!(OP_EXPAND_MONTHLY_SETPOS, 7, false);
pub(crate) fn op_expand_monthly_setpos(args: &[DataValue]) -> Result<DataValue> {
    let h0 = args[0]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects start hour as integer"))?;
    let h1 = args[1]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects end hour as integer"))?;

    let by_wday_slice = args[2]
        .get_slice()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects by_wday as list"))?;
    let by_wday: Result<Vec<i64>, _> = by_wday_slice.iter()
        .map(|v| v.get_int().ok_or_else(|| miette!("weekday must be integer")))
        .collect();
    let by_wday = by_wday?;

    let by_setpos_slice = args[3]
        .get_slice()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects by_setpos as list"))?;
    let by_setpos: Result<Vec<i64>, _> = by_setpos_slice.iter()
        .map(|v| v.get_int().ok_or_else(|| miette!("setpos must be integer")))
        .collect();
    let by_setpos = by_setpos?;

    let tz_str = args[4]
        .get_str()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects timezone string"))?;
    let start_min = args[5]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects start_min as integer"))?;
    let end_min = args[6]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly_setpos' expects end_min as integer"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // For a sample month (January 2024)
    let year = 2024;
    let month = 1;
    let mut intervals = Vec::new();

    // Find all occurrences of the specified weekdays in the month
    let first_day = NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or_else(|| miette!("Failed to create first day of month"))?;
    let last_day = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }.ok_or_else(|| miette!("Failed to create last day of month"))?
    .pred_opt()
    .ok_or_else(|| miette!("Failed to get previous day"))?;

    for &wday in &by_wday {
        if wday < 1 || wday > 7 {
            bail!("Weekday must be 1-7, got {}", wday);
        }

        // Find all dates in the month that match this weekday
        let mut matching_dates = Vec::new();
        let mut current_date = first_day;

        while current_date <= last_day {
            let current_wday = match current_date.weekday() {
                Weekday::Mon => 1,
                Weekday::Tue => 2,
                Weekday::Wed => 3,
                Weekday::Thu => 4,
                Weekday::Fri => 5,
                Weekday::Sat => 6,
                Weekday::Sun => 7,
            };

            if current_wday == wday {
                matching_dates.push(current_date);
            }

            current_date = current_date.succ_opt()
                .ok_or_else(|| miette!("Failed to increment date"))?;
        }

        // Apply setpos filtering
        for &setpos in &by_setpos {
            let date_opt = if setpos > 0 {
                matching_dates.get((setpos - 1) as usize)
            } else if setpos < 0 {
                let idx = (matching_dates.len() as i64 + setpos) as usize;
                matching_dates.get(idx)
            } else {
                bail!("Setpos cannot be 0");
            };

            if let Some(date) = date_opt {
                // Create start time
                let start_dt = date.and_hms_opt(h0 as u32, start_min as u32, 0)
                    .ok_or_else(|| miette!("Invalid start time"))?;
                let start_utc = tz.from_local_datetime(&start_dt)
                    .single()
                    .ok_or_else(|| miette!("Ambiguous start time in timezone"))?;

                // Create end time
                let end_dt = date.and_hms_opt(h1 as u32, end_min as u32, 0)
                    .ok_or_else(|| miette!("Invalid end time"))?;
                let end_utc = tz.from_local_datetime(&end_dt)
                    .single()
                    .ok_or_else(|| miette!("Ambiguous end time in timezone"))?;

                intervals.push(DataValue::List(vec![
                    DataValue::from(start_utc.timestamp()),
                    DataValue::from(end_utc.timestamp())
                ]));
            }
        }
    }

    Ok(DataValue::List(intervals))
}

define_op!(OP_NORMALIZE_INTERVALS, 1, false);
pub(crate) fn op_normalize_intervals(args: &[DataValue]) -> Result<DataValue> {
    let intervals = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'normalize_intervals' expects a list of intervals"))?;

    if intervals.is_empty() {
        return Ok(DataValue::List(vec![]));
    }

    // Extract and validate all intervals
    let mut ivs: Vec<(i64, i64)> = vec![];
    for iv in intervals {
        let iv_list = iv.get_slice()
            .ok_or_else(|| miette!("each element must be an interval (list)"))?;
        if iv_list.len() != 2 {
            bail!("each interval must have exactly 2 elements");
        }
        let s = iv_list[0].get_int()
            .ok_or_else(|| miette!("interval start must be integer"))?;
        let e = iv_list[1].get_int()
            .ok_or_else(|| miette!("interval end must be integer"))?;
        if s < e {
            ivs.push((s, e));
        }
    }

    if ivs.is_empty() {
        return Ok(DataValue::List(vec![]));
    }

    // Sort by start time
    ivs.sort_by_key(|&(s, _)| s);

    // Merge overlapping and adjacent intervals
    let mut result = vec![];
    let mut current = ivs[0];

    for &(s, e) in &ivs[1..] {
        if current.1 >= s {
            // Overlapping or adjacent - merge
            current.1 = current.1.max(e);
        } else {
            // Not overlapping - save current and start new
            result.push(DataValue::List(vec![
                DataValue::from(current.0),
                DataValue::from(current.1)
            ]));
            current = (s, e);
        }
    }

    // Add the last interval
    result.push(DataValue::List(vec![
        DataValue::from(current.0),
        DataValue::from(current.1)
    ]));

    Ok(DataValue::List(result))
}

define_op!(OP_INTERVALS_MINUS, 2, false);
pub(crate) fn op_intervals_minus(args: &[DataValue]) -> Result<DataValue> {
    let intervals = args[0]
        .get_slice()
        .ok_or_else(|| miette!("'intervals_minus' expects first argument as list of intervals"))?;
    let subs = args[1]
        .get_slice()
        .ok_or_else(|| miette!("'intervals_minus' expects second argument as list of intervals"))?;

    // Parse input intervals
    let mut ivs: Vec<(i64, i64)> = vec![];
    for iv in intervals {
        let iv_list = iv.get_slice()
            .ok_or_else(|| miette!("each element must be an interval (list)"))?;
        if iv_list.len() != 2 {
            bail!("each interval must have exactly 2 elements");
        }
        let s = iv_list[0].get_int()
            .ok_or_else(|| miette!("interval start must be integer"))?;
        let e = iv_list[1].get_int()
            .ok_or_else(|| miette!("interval end must be integer"))?;
        if s < e {
            ivs.push((s, e));
        }
    }

    // Parse subtraction intervals
    let mut sub_ivs: Vec<(i64, i64)> = vec![];
    for iv in subs {
        let iv_list = iv.get_slice()
            .ok_or_else(|| miette!("each element must be an interval (list)"))?;
        if iv_list.len() != 2 {
            bail!("each interval must have exactly 2 elements");
        }
        let s = iv_list[0].get_int()
            .ok_or_else(|| miette!("interval start must be integer"))?;
        let e = iv_list[1].get_int()
            .ok_or_else(|| miette!("interval end must be integer"))?;
        if s < e {
            sub_ivs.push((s, e));
        }
    }

    // Apply all subtractions
    let mut result_ivs = ivs;
    for (sub_s, sub_e) in sub_ivs {
        let mut new_result = Vec::new();
        for (iv_s, iv_e) in result_ivs {
            // Subtract sub from iv
            if sub_e <= iv_s || sub_s >= iv_e {
                // No intersection
                new_result.push((iv_s, iv_e));
            } else if sub_s <= iv_s && sub_e >= iv_e {
                // Sub completely contains iv - remove iv
            } else if sub_s <= iv_s && sub_e < iv_e {
                // Sub cuts from the left
                new_result.push((sub_e, iv_e));
            } else if sub_s > iv_s && sub_e >= iv_e {
                // Sub cuts from the right
                new_result.push((iv_s, sub_s));
            } else {
                // Sub cuts in the middle
                new_result.push((iv_s, sub_s));
                new_result.push((sub_e, iv_e));
            }
        }
        result_ivs = new_result;
    }

    // Convert back to DataValue format
    let result: Vec<DataValue> = result_ivs.into_iter()
        .map(|(s, e)| DataValue::List(vec![DataValue::from(s), DataValue::from(e)]))
        .collect();

    Ok(DataValue::List(result))
}

define_op!(OP_NTH_WEEKDAY_OF_MONTH, 5, false);
pub(crate) fn op_nth_weekday_of_month(args: &[DataValue]) -> Result<DataValue> {
    let year = args[0]
        .get_int()
        .ok_or_else(|| miette!("'nth_weekday_of_month' expects year as integer"))?;
    let month = args[1]
        .get_int()
        .ok_or_else(|| miette!("'nth_weekday_of_month' expects month as integer"))?;
    let weekday = args[2]
        .get_int()
        .ok_or_else(|| miette!("'nth_weekday_of_month' expects weekday as integer"))?;
    let n = args[3]
        .get_int()
        .ok_or_else(|| miette!("'nth_weekday_of_month' expects n as integer"))?;
    let _tz_str = args[4]
        .get_str()
        .ok_or_else(|| miette!("'nth_weekday_of_month' expects timezone string"))?;

    if weekday < 1 || weekday > 7 {
        bail!("Weekday must be 1-7, got {}", weekday);
    }
    if n == 0 {
        bail!("n cannot be 0");
    }
    if n < -5 || n > 5 {
        bail!("n must be ±1..±5, got {}", n);
    }

    // Create first and last day of month
    let first_day = NaiveDate::from_ymd_opt(year as i32, month as u32, 1)
        .ok_or_else(|| miette!("Invalid year/month: {}/{}", year, month))?;
    let last_day = if month == 12 {
        NaiveDate::from_ymd_opt((year + 1) as i32, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year as i32, (month + 1) as u32, 1)
    }.ok_or_else(|| miette!("Failed to create end of month"))?
    .pred_opt()
    .ok_or_else(|| miette!("Failed to get previous day"))?;

    // Find all dates in the month that match this weekday
    let mut matching_dates = Vec::new();
    let mut current_date = first_day;

    while current_date <= last_day {
        let current_wday = match current_date.weekday() {
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
            Weekday::Sun => 7,
        };

        if current_wday == weekday {
            matching_dates.push(current_date);
        }

        current_date = current_date.succ_opt()
            .ok_or_else(|| miette!("Failed to increment date"))?;
    }

    // Get the nth occurrence
    let target_date = if n > 0 {
        matching_dates.get((n - 1) as usize)
    } else {
        let idx = (matching_dates.len() as i64 + n) as usize;
        matching_dates.get(idx)
    };

    if let Some(date) = target_date {
        let result = json!({
            "year": date.year(),
            "month": date.month() as i32,
            "day": date.day() as i32,
        });
        Ok(DataValue::Json(JsonData(result)))
    } else {
        Ok(DataValue::Null)
    }
}

define_op!(OP_LOCAL_MINUTES_TO_PARTS, 3, false);
pub(crate) fn op_local_minutes_to_parts(args: &[DataValue]) -> Result<DataValue> {
    let base_local_midnight_utc = args[0]
        .get_int()
        .ok_or_else(|| miette!("'local_minutes_to_parts' expects base_local_midnight_utc as integer"))?;
    let minutes = args[1]
        .get_int()
        .ok_or_else(|| miette!("'local_minutes_to_parts' expects minutes as integer"))?;
    let tz_str = args[2]
        .get_str()
        .ok_or_else(|| miette!("'local_minutes_to_parts' expects timezone string"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Convert base midnight to local timezone
    let base_dt = Utc.timestamp_opt(base_local_midnight_utc, 0)
        .single()
        .ok_or_else(|| miette!("Invalid base timestamp"))?;
    let base_local = base_dt.with_timezone(&tz);

    // Add the minutes
    let target_local = base_local + Duration::minutes(minutes);

    let result = json!({
        "year": target_local.year(),
        "month": target_local.month() as i32,
        "day": target_local.day() as i32,
        "hour": target_local.hour() as i32,
        "minute": target_local.minute() as i32,
    });

    Ok(DataValue::Json(JsonData(result)))
}

define_op!(OP_PARTS_TO_INSTANT_UTC, 2, false);
pub(crate) fn op_parts_to_instant_utc(args: &[DataValue]) -> Result<DataValue> {
    let parts_json = match &args[0] {
        DataValue::Json(JsonData(json)) => json,
        _ => bail!("'parts_to_instant_utc' expects parts as JSON object"),
    };
    let tz_str = args[1]
        .get_str()
        .ok_or_else(|| miette!("'parts_to_instant_utc' expects timezone string"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Extract parts from JSON
    let year = parts_json.get("year")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| miette!("Missing or invalid year in parts"))?;
    let month = parts_json.get("month")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| miette!("Missing or invalid month in parts"))?;
    let day = parts_json.get("day")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| miette!("Missing or invalid day in parts"))?;
    let hour = parts_json.get("hour")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| miette!("Missing or invalid hour in parts"))?;
    let minute = parts_json.get("minute")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| miette!("Missing or invalid minute in parts"))?;

    // Create local datetime and convert to UTC
    let dt = tz.with_ymd_and_hms(year as i32, month as u32, day as u32, hour as u32, minute as u32, 0)
        .single()
        .ok_or_else(|| miette!("Invalid date/time parts or ambiguous due to DST"))?;

    Ok(DataValue::from(dt.timestamp()))
}

define_op!(OP_BUCKET_OF, 3, false);
pub(crate) fn op_bucket_of(args: &[DataValue]) -> Result<DataValue> {
    let t = args[0]
        .get_int()
        .ok_or_else(|| miette!("'bucket_of' expects timestamp as integer"))?;
    let period = args[1]
        .get_int()
        .ok_or_else(|| miette!("'bucket_of' expects period as integer"))?;
    let epoch0 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'bucket_of' expects epoch0 as integer"))?;

    if period <= 0 {
        bail!("Period must be positive, got {}", period);
    }

    let bucket = (t - epoch0).div_euclid(period);
    Ok(DataValue::from(bucket))
}

define_op!(OP_BUCKET_START, 3, false);
pub(crate) fn op_bucket_start(args: &[DataValue]) -> Result<DataValue> {
    let k = args[0]
        .get_int()
        .ok_or_else(|| miette!("'bucket_start' expects bucket number as integer"))?;
    let period = args[1]
        .get_int()
        .ok_or_else(|| miette!("'bucket_start' expects period as integer"))?;
    let epoch0 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'bucket_start' expects epoch0 as integer"))?;

    if period <= 0 {
        bail!("Period must be positive, got {}", period);
    }

    let start = epoch0 + k * period;
    Ok(DataValue::from(start))
}

define_op!(OP_CEIL_TO_BUCKET, 3, false);
pub(crate) fn op_ceil_to_bucket(args: &[DataValue]) -> Result<DataValue> {
    let t = args[0]
        .get_int()
        .ok_or_else(|| miette!("'ceil_to_bucket' expects timestamp as integer"))?;
    let period = args[1]
        .get_int()
        .ok_or_else(|| miette!("'ceil_to_bucket' expects period as integer"))?;
    let epoch0 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'ceil_to_bucket' expects epoch0 as integer"))?;

    if period <= 0 {
        bail!("Period must be positive, got {}", period);
    }

    let offset = t - epoch0;
    let bucket = if offset >= 0 {
        (offset + period - 1) / period
    } else {
        offset / period
    };
    let ceiling = epoch0 + bucket * period;
    Ok(DataValue::from(ceiling))
}

define_op!(OP_FLOOR_TO_BUCKET, 3, false);
pub(crate) fn op_floor_to_bucket(args: &[DataValue]) -> Result<DataValue> {
    let t = args[0]
        .get_int()
        .ok_or_else(|| miette!("'floor_to_bucket' expects timestamp as integer"))?;
    let period = args[1]
        .get_int()
        .ok_or_else(|| miette!("'floor_to_bucket' expects period as integer"))?;
    let epoch0 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'floor_to_bucket' expects epoch0 as integer"))?;

    if period <= 0 {
        bail!("Period must be positive, got {}", period);
    }

    let offset = t - epoch0;
    let bucket = if offset >= 0 {
        offset / period
    } else {
        (offset - period + 1) / period
    };
    let floor = epoch0 + bucket * period;
    Ok(DataValue::from(floor))
}

define_op!(OP_DURATION_IN_BUCKETS, 2, false);
pub(crate) fn op_duration_in_buckets(args: &[DataValue]) -> Result<DataValue> {
    let d = args[0]
        .get_int()
        .ok_or_else(|| miette!("'duration_in_buckets' expects duration as integer"))?;
    let period = args[1]
        .get_int()
        .ok_or_else(|| miette!("'duration_in_buckets' expects period as integer"))?;

    if period <= 0 {
        bail!("Period must be positive, got {}", period);
    }

    if d < 0 {
        bail!("Duration must be non-negative, got {}", d);
    }

    let buckets = (d + period - 1) / period; // Ceiling division for positive duration
    Ok(DataValue::from(buckets))
}

define_op!(OP_EXPAND_DAILY, 5, false);
pub(crate) fn op_expand_daily(args: &[DataValue]) -> Result<DataValue> {
    let h0 = args[0]
        .get_int()
        .ok_or_else(|| miette!("'expand_daily' expects h0 (start minutes from midnight) as integer"))?;
    let h1 = args[1]
        .get_int()
        .ok_or_else(|| miette!("'expand_daily' expects h1 (end minutes from midnight) as integer"))?;
    let tz_str = args[2]
        .get_str()
        .ok_or_else(|| miette!("'expand_daily' expects timezone string"))?;
    let start_ms = args[3]
        .get_int()
        .ok_or_else(|| miette!("'expand_daily' expects start timestamp in milliseconds"))?;
    let end_ms = args[4]
        .get_int()
        .ok_or_else(|| miette!("'expand_daily' expects end timestamp in milliseconds"))?;

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Convert milliseconds to seconds for chrono
    let start_dt = DateTime::from_timestamp(start_ms / 1000, ((start_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid start timestamp"))?
        .with_timezone(&tz);
    let end_dt = DateTime::from_timestamp(end_ms / 1000, ((end_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid end timestamp"))?
        .with_timezone(&tz);

    let mut intervals = Vec::new();

    // Start from the date of start_dt
    let mut current_date = start_dt.date_naive();
    let end_date = end_dt.date_naive();

    // Convert h0, h1 from minutes to hours and minutes
    let h0_hour = (h0 / 60) as u32;
    let h0_min = (h0 % 60) as u32;

    while current_date <= end_date {
        // Create start time for this day
        if let Some(start_time) = current_date.and_hms_opt(h0_hour, h0_min, 0) {
            // Handle end time - if h1 >= 1440 (24:00), use next day's midnight
            let end_time_opt = if h1 >= 1440 {
                current_date.succ_opt()
                    .and_then(|next_day| next_day.and_hms_opt(0, 0, 0))
            } else {
                let h1_hour = (h1 / 60) as u32;
                let h1_min = (h1 % 60) as u32;
                current_date.and_hms_opt(h1_hour, h1_min, 0)
            };

            if let Some(end_time) = end_time_opt {
                // Convert to timezone-aware datetime, handling DST
                let interval_start = tz.from_local_datetime(&start_time)
                    .earliest()
                    .or_else(|| tz.from_local_datetime(&start_time).latest());
                let interval_end = tz.from_local_datetime(&end_time)
                    .earliest()
                    .or_else(|| tz.from_local_datetime(&end_time).latest());

                if let (Some(iv_start), Some(iv_end)) = (interval_start, interval_end) {
                    let iv_start_ms = iv_start.timestamp() * 1000 + (iv_start.timestamp_subsec_millis() as i64);
                    let iv_end_ms = iv_end.timestamp() * 1000 + (iv_end.timestamp_subsec_millis() as i64);

                    // Only include intervals that overlap with [start_ms, end_ms]
                    if iv_end_ms > start_ms && iv_start_ms < end_ms {
                        intervals.push(DataValue::List(vec![
                            DataValue::from(iv_start_ms),
                            DataValue::from(iv_end_ms),
                        ]));
                    }
                }
            }
        }

        current_date = current_date.succ_opt()
            .ok_or_else(|| miette!("Failed to increment date"))?;
    }

    Ok(DataValue::List(intervals))
}

define_op!(OP_EXPAND_MONTHLY, 6, false);
pub(crate) fn op_expand_monthly(args: &[DataValue]) -> Result<DataValue> {
    let day_of_month = args[0]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly' expects day_of_month as integer"))?;
    let h0 = args[1]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly' expects h0 (start minutes from midnight) as integer"))?;
    let h1 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly' expects h1 (end minutes from midnight) as integer"))?;
    let tz_str = args[3]
        .get_str()
        .ok_or_else(|| miette!("'expand_monthly' expects timezone string"))?;
    let start_ms = args[4]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly' expects start timestamp in milliseconds"))?;
    let end_ms = args[5]
        .get_int()
        .ok_or_else(|| miette!("'expand_monthly' expects end timestamp in milliseconds"))?;

    if day_of_month < 1 || day_of_month > 31 {
        bail!("day_of_month must be 1-31, got {}", day_of_month);
    }

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Convert milliseconds to seconds for chrono
    let start_dt = DateTime::from_timestamp(start_ms / 1000, ((start_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid start timestamp"))?
        .with_timezone(&tz);
    let end_dt = DateTime::from_timestamp(end_ms / 1000, ((end_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid end timestamp"))?
        .with_timezone(&tz);

    let mut intervals = Vec::new();

    // Convert h0 from minutes to hours and minutes
    let h0_hour = (h0 / 60) as u32;
    let h0_min = (h0 % 60) as u32;

    // Start from the month of start_dt
    let mut current_year = start_dt.year();
    let mut current_month = start_dt.month();
    let end_year = end_dt.year();
    let end_month = end_dt.month();

    while (current_year, current_month) <= (end_year, end_month) {
        // Calculate the actual day for this month (clamp to last day if needed)
        let days_in_month = days_in_month_helper(current_year, current_month);
        let actual_day = (day_of_month as u32).min(days_in_month);

        if let Some(target_date) = NaiveDate::from_ymd_opt(current_year, current_month, actual_day) {
            if let Some(start_time) = target_date.and_hms_opt(h0_hour, h0_min, 0) {
                // Handle end time - if h1 >= 1440 (24:00), use next day's midnight
                let end_time_opt = if h1 >= 1440 {
                    target_date.succ_opt()
                        .and_then(|next_day| next_day.and_hms_opt(0, 0, 0))
                } else {
                    let h1_hour = (h1 / 60) as u32;
                    let h1_min = (h1 % 60) as u32;
                    target_date.and_hms_opt(h1_hour, h1_min, 0)
                };

                if let Some(end_time) = end_time_opt {
                    // Convert to timezone-aware datetime, handling DST
                    let interval_start = tz.from_local_datetime(&start_time)
                        .earliest()
                        .or_else(|| tz.from_local_datetime(&start_time).latest());
                    let interval_end = tz.from_local_datetime(&end_time)
                        .earliest()
                        .or_else(|| tz.from_local_datetime(&end_time).latest());

                    if let (Some(iv_start), Some(iv_end)) = (interval_start, interval_end) {
                        let iv_start_ms = iv_start.timestamp() * 1000 + (iv_start.timestamp_subsec_millis() as i64);
                        let iv_end_ms = iv_end.timestamp() * 1000 + (iv_end.timestamp_subsec_millis() as i64);

                        // Only include intervals that overlap with [start_ms, end_ms]
                        if iv_end_ms > start_ms && iv_start_ms < end_ms {
                            intervals.push(DataValue::List(vec![
                                DataValue::from(iv_start_ms),
                                DataValue::from(iv_end_ms),
                            ]));
                        }
                    }
                }
            }
        }

        // Move to next month
        if current_month == 12 {
            current_year += 1;
            current_month = 1;
        } else {
            current_month += 1;
        }
    }

    Ok(DataValue::List(intervals))
}

define_op!(OP_EXPAND_YEARLY, 7, false);
pub(crate) fn op_expand_yearly(args: &[DataValue]) -> Result<DataValue> {
    let month = args[0]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects month as integer"))?;
    let day = args[1]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects day as integer"))?;
    let h0 = args[2]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects h0 (start minutes from midnight) as integer"))?;
    let h1 = args[3]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects h1 (end minutes from midnight) as integer"))?;
    let tz_str = args[4]
        .get_str()
        .ok_or_else(|| miette!("'expand_yearly' expects timezone string"))?;
    let start_ms = args[5]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects start timestamp in milliseconds"))?;
    let end_ms = args[6]
        .get_int()
        .ok_or_else(|| miette!("'expand_yearly' expects end timestamp in milliseconds"))?;

    if month < 1 || month > 12 {
        bail!("month must be 1-12, got {}", month);
    }
    if day < 1 || day > 31 {
        bail!("day must be 1-31, got {}", day);
    }

    let tz = chrono_tz::Tz::from_str(tz_str)
        .map_err(|_| miette!("Invalid timezone: {}", tz_str))?;

    // Convert milliseconds to seconds for chrono
    let start_dt = DateTime::from_timestamp(start_ms / 1000, ((start_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid start timestamp"))?
        .with_timezone(&tz);
    let end_dt = DateTime::from_timestamp(end_ms / 1000, ((end_ms % 1000) * 1_000_000) as u32)
        .ok_or_else(|| miette!("Invalid end timestamp"))?
        .with_timezone(&tz);

    let mut intervals = Vec::new();

    // Convert h0 from minutes to hours and minutes
    let h0_hour = (h0 / 60) as u32;
    let h0_min = (h0 % 60) as u32;

    let month_u32 = month as u32;

    // Iterate over each year in the range
    for current_year in start_dt.year()..=end_dt.year() {
        // Calculate the actual day for this year/month (clamp to last day if needed)
        let days_in_target_month = days_in_month_helper(current_year, month_u32);
        let actual_day = (day as u32).min(days_in_target_month);

        // For Feb 29, skip non-leap years entirely (don't clamp to Feb 28)
        if month == 2 && day == 29 && !is_leap_year(current_year) {
            continue;
        }

        if let Some(target_date) = NaiveDate::from_ymd_opt(current_year, month_u32, actual_day) {
            if let Some(start_time) = target_date.and_hms_opt(h0_hour, h0_min, 0) {
                // Handle end time - if h1 >= 1440 (24:00), use next day's midnight
                let end_time_opt = if h1 >= 1440 {
                    target_date.succ_opt()
                        .and_then(|next_day| next_day.and_hms_opt(0, 0, 0))
                } else {
                    let h1_hour = (h1 / 60) as u32;
                    let h1_min = (h1 % 60) as u32;
                    target_date.and_hms_opt(h1_hour, h1_min, 0)
                };

                if let Some(end_time) = end_time_opt {
                    // Convert to timezone-aware datetime, handling DST
                    let interval_start = tz.from_local_datetime(&start_time)
                        .earliest()
                        .or_else(|| tz.from_local_datetime(&start_time).latest());
                    let interval_end = tz.from_local_datetime(&end_time)
                        .earliest()
                        .or_else(|| tz.from_local_datetime(&end_time).latest());

                    if let (Some(iv_start), Some(iv_end)) = (interval_start, interval_end) {
                        let iv_start_ms = iv_start.timestamp() * 1000 + (iv_start.timestamp_subsec_millis() as i64);
                        let iv_end_ms = iv_end.timestamp() * 1000 + (iv_end.timestamp_subsec_millis() as i64);

                        // Only include intervals that overlap with [start_ms, end_ms]
                        if iv_end_ms > start_ms && iv_start_ms < end_ms {
                            intervals.push(DataValue::List(vec![
                                DataValue::from(iv_start_ms),
                                DataValue::from(iv_end_ms),
                            ]));
                        }
                    }
                }
            }
        }
    }

    Ok(DataValue::List(intervals))
}

// Helper function to get days in a month
fn days_in_month_helper(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 30, // Should not happen
    }
}

// Helper function to check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}
