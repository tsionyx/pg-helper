//! The most common Postgres data types.
//! `https://www.postgresql.org/docs/14/datatype.html`

use std::{any::Any, fmt};

use itertools::Itertools as _;

pub struct CommaSeparatedValues {
    values: Vec<(DbType, Box<dyn Any>)>,
}

impl CommaSeparatedValues {
    pub fn with_values(values: Vec<(DbType, Box<dyn Any>)>) -> Self {
        Self { values }
    }
}

impl fmt::Display for CommaSeparatedValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut has_items = false;
        for (db_type, val) in &self.values {
            let value = db_type.escape_val(val.as_ref()).ok_or(fmt::Error)?;
            if has_items {
                write!(f, ", {}", value)?;
            } else {
                write!(f, "{}", value)?;
            }

            has_items = true;
        }

        Ok(())
    }
}

pub trait StructType: fmt::Debug {
    fn name(&self) -> String;
    fn fields(&self) -> Vec<(String, DbType)>;

    fn as_vec(&self, val: &dyn Any) -> Option<Vec<Box<dyn Any>>>;
    fn as_nullable_vec(&self, val: &dyn Any) -> Option<Nullable<Vec<Box<dyn Any>>>>;

    fn csv(&self, val: &dyn Any) -> Option<CommaSeparatedValues> {
        let values = self.as_vec(val)?;
        let values_with_fields = self
            .fields()
            .into_iter()
            .map(|(_, ty)| ty)
            .zip(values)
            .collect();
        Some(CommaSeparatedValues::with_values(values_with_fields))
    }

    fn nullable_csv(&self, val: &dyn Any) -> Option<Nullable<CommaSeparatedValues>> {
        let values = self.as_nullable_vec(val)?;
        Some(values.map(|values| {
            let values_with_fields = self
                .fields()
                .into_iter()
                .map(|(_, ty)| ty)
                .zip(values)
                .collect();
            CommaSeparatedValues::with_values(values_with_fields)
        }))
    }
}

#[derive(Debug)]
pub enum DbType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Uuid,
    Float,
    Double,
    Date,
    Json,
    Char(Option<u8>),
    VarChar(Option<u8>),
    String,
    CustomStruct(Box<dyn StructType>),
}

impl DbType {
    pub fn escape_val(&self, val: &dyn Any) -> Option<String> {
        match self {
            Self::Boolean => {
                let val = val.downcast_ref::<bool>()?;
                Some(self.format(val))
            }
            Self::Int16 => {
                let val = val.downcast_ref::<i16>()?;
                Some(self.format(val))
            }
            Self::Int32 => {
                let val = val.downcast_ref::<i32>()?;
                Some(self.format(val))
            }
            Self::Int64 => {
                let val = val.downcast_ref::<i64>()?;
                Some(self.format(val))
            }
            Self::Uuid => {
                let val = val.downcast_ref::<uuid::Uuid>()?;
                Some(self.format(val))
            }
            Self::Float => {
                let val = val.downcast_ref::<f32>()?;
                Some(self.format(val))
            }
            Self::Double => {
                let val = val.downcast_ref::<f64>()?;
                Some(self.format(val))
            }
            Self::Date => {
                todo!()
            }
            Self::Json => {
                todo!()
            }
            Self::Char(size) => {
                let size: usize = size.unwrap_or(1).into();
                if size == 1 {
                    if let Some(val) = val.downcast_ref::<char>() {
                        return Some(self.format(val));
                    }
                }
                let val = val.downcast_ref::<String>()?;
                if val.len() > size {
                    return None;
                }
                Some(self.format(val))
            }
            Self::VarChar(size) => {
                let val = val.downcast_ref::<String>()?;
                if let Some(size) = size {
                    if val.len() > usize::from(*size) {
                        return None;
                    }
                }
                Some(self.format(val))
            }
            Self::String => {
                let val = val.downcast_ref::<String>()?;
                Some(self.format(val))
            }
            Self::CustomStruct(ty) => {
                let val = ty.csv(val)?;
                Some(self.format(&val))
            }
        }
    }

    pub fn escape_nullable_val(&self, val: &dyn Any) -> Option<String> {
        if let Some(not_null) = self.escape_val(val) {
            return Some(not_null);
        }

        match self {
            Self::Boolean => {
                let val = val.downcast_ref::<Nullable<bool>>()?;
                Some(self.format_opt(val))
            }
            Self::Int16 => {
                let val = val.downcast_ref::<Nullable<i16>>()?;
                Some(self.format_opt(val))
            }
            Self::Int32 => {
                let val = val.downcast_ref::<Nullable<i32>>()?;
                Some(self.format_opt(val))
            }
            Self::Int64 => {
                let val = val.downcast_ref::<Nullable<i64>>()?;
                Some(self.format_opt(val))
            }
            Self::Uuid => {
                let val = val.downcast_ref::<Nullable<uuid::Uuid>>()?;
                Some(self.format_opt(val))
            }
            Self::Float => {
                let val = val.downcast_ref::<Nullable<f32>>()?;
                Some(self.format_opt(val))
            }
            Self::Double => {
                let val = val.downcast_ref::<Nullable<f64>>()?;
                Some(self.format_opt(val))
            }
            Self::Date => {
                todo!()
            }
            Self::Json => {
                todo!()
            }
            Self::Char(size) => {
                let size: usize = size.unwrap_or(1).into();
                if size == 1 {
                    if let Some(val) = val.downcast_ref::<Nullable<char>>() {
                        return Some(self.format_opt(val));
                    }
                }
                let val = val.downcast_ref::<Nullable<String>>()?;
                let value_len = val.as_opt().map(|s| s.len()).unwrap_or(0);
                if value_len > size {
                    return None;
                }
                Some(self.format_opt(val))
            }
            Self::VarChar(size) => {
                let val = val.downcast_ref::<Nullable<String>>()?;
                if let Some(size) = size {
                    let value_len = val.as_opt().map(|s| s.len()).unwrap_or(0);
                    if value_len > usize::from(*size) {
                        return None;
                    }
                }
                Some(self.format_opt(val))
            }
            Self::String => {
                let val = val.downcast_ref::<Nullable<String>>()?;
                Some(self.format_opt(val))
            }
            Self::CustomStruct(ty) => {
                let val = ty.nullable_csv(val)?;
                Some(self.format_opt(&val))
            }
        }
    }

    pub fn create_sql(&self) -> Option<String> {
        match self {
            Self::Boolean
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Uuid
            | Self::Float
            | Self::Double
            | Self::Date
            | Self::Json
            | Self::Char(_)
            | Self::VarChar(_)
            | Self::String => None,
            Self::CustomStruct(ty) => {
                let fields = ty.fields();
                let fields = fields
                    .iter()
                    .map(|(f_name, f_type)| format!("{} {}", f_name, f_type))
                    .join(", ");
                Some(format!("CREATE TYPE {} AS ({})", ty.name(), fields))
            }
        }
    }

    fn format<V: fmt::Display>(&self, val: &V) -> String {
        match self {
            Self::Boolean
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Float
            | Self::Double => format!("{}", val),
            Self::Uuid | Self::Char(_) | Self::VarChar(_) | Self::String => format!("'{}'", val),
            Self::Date => {
                todo!()
            }
            Self::Json => {
                todo!()
            }
            Self::CustomStruct(ty) => {
                format!("ROW({})::{}", val, ty.name())
            }
        }
    }

    fn format_opt<V: fmt::Display>(&self, val: &Nullable<V>) -> String {
        if let Nullable::Val(val) = val {
            self.format(val)
        } else {
            "NULL".into()
        }
    }
}

pub enum Nullable<T> {
    Val(T),
    Null,
}

impl<T> From<Option<T>> for Nullable<T> {
    fn from(x: Option<T>) -> Self {
        if let Some(val) = x {
            Self::Val(val)
        } else {
            Self::Null
        }
    }
}

impl<T> Nullable<T> {
    pub fn map<U, F>(self, f: F) -> Nullable<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Self::Val(x) => Nullable::Val(f(x)),
            Self::Null => Nullable::Null,
        }
    }

    pub const fn as_opt(&self) -> Option<&T> {
        match self {
            Self::Val(x) => Some(x),
            Self::Null => None,
        }
    }
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => write!(f, "boolean"),
            Self::Int16 => write!(f, "smallint"),
            Self::Int32 => write!(f, "integer"),
            Self::Int64 => write!(f, "bigint"),
            Self::Uuid => write!(f, "uuid"),
            Self::Float => write!(f, "real"),
            Self::Double => write!(f, "double precision"),
            Self::Date => write!(f, "date"),
            Self::Json => write!(f, "json"),
            Self::Char(n) => {
                if let Some(n) = *n {
                    write!(f, "char({})", n)
                } else {
                    write!(f, "char")
                }
            }
            Self::VarChar(n) => {
                if let Some(n) = *n {
                    write!(f, "varchar({})", n)
                } else {
                    write!(f, "varchar")
                }
            }
            Self::String => write!(f, "text"),
            Self::CustomStruct(ty) => write!(f, "{}", ty.name()),
        }
    }
}
