//! The most common Postgres data types.
//! `https://www.postgresql.org/docs/14/datatype.html`

use std::{any::Any, fmt};

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
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
        }
    }

    pub fn escape_nullable_val(&self, val: &dyn Any) -> Option<String> {
        if let Some(not_null) = self.escape_val(val) {
            return Some(not_null);
        }

        match self {
            Self::Boolean => {
                let val = val.downcast_ref::<Option<bool>>()?;
                Some(self.format_opt(val))
            }
            Self::Int16 => {
                let val = val.downcast_ref::<Option<i16>>()?;
                Some(self.format_opt(val))
            }
            Self::Int32 => {
                let val = val.downcast_ref::<Option<i32>>()?;
                Some(self.format_opt(val))
            }
            Self::Int64 => {
                let val = val.downcast_ref::<Option<i64>>()?;
                Some(self.format_opt(val))
            }
            Self::Uuid => {
                let val = val.downcast_ref::<Option<uuid::Uuid>>()?;
                Some(self.format_opt(val))
            }
            Self::Float => {
                let val = val.downcast_ref::<Option<f32>>()?;
                Some(self.format_opt(val))
            }
            Self::Double => {
                let val = val.downcast_ref::<Option<f64>>()?;
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
                    if let Some(val) = val.downcast_ref::<Option<char>>() {
                        return Some(self.format_opt(val));
                    }
                }
                let val = val.downcast_ref::<Option<String>>()?;
                let value_len = val.as_ref().map(|s| s.len()).unwrap_or(0);
                if value_len > size {
                    return None;
                }
                Some(self.format_opt(val))
            }
            Self::VarChar(size) => {
                let val = val.downcast_ref::<Option<String>>()?;
                if let Some(size) = size {
                    let value_len = val.as_ref().map(|s| s.len()).unwrap_or(0);
                    if value_len > usize::from(*size) {
                        return None;
                    }
                }
                Some(self.format_opt(val))
            }
            Self::String => {
                let val = val.downcast_ref::<Option<String>>()?;
                Some(self.format_opt(val))
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
        }
    }

    fn format_opt<V: fmt::Display>(&self, val: &Option<V>) -> String {
        if let Some(val) = val {
            self.format(val)
        } else {
            "NULL".into()
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
        }
    }
}
