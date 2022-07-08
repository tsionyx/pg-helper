use std::{
    any::Any,
    fmt::{self, Debug, Display},
};

use crate::types::DbType;

pub struct ColumnBuilder {
    name: String,
    db_type: DbType,
    nullable: bool,
    unique: bool,
    primary_key: bool,
    foreign_key: Option<(String, String)>,
}

impl ColumnBuilder {
    pub fn new(name: impl AsRef<str>, db_type: DbType) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            db_type,
            nullable: false,
            unique: false,
            primary_key: false,
            foreign_key: None,
        }
    }

    pub const fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub const fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub const fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.unique = true;
        self
    }

    pub fn foreign_key(mut self, table_name: impl AsRef<str>, column: impl AsRef<str>) -> Self {
        self.foreign_key = Some((table_name.as_ref().to_owned(), column.as_ref().to_owned()));
        self
    }

    pub fn finish(self) -> Column {
        Column {
            name: self.name,
            db_type: self.db_type,
            nullable: self.nullable,
            unique: self.unique,
            primary_key: self.primary_key,
            foreign_key: self.foreign_key,
        }
    }
}

#[derive(Debug)]
pub struct Column {
    name: String,
    db_type: DbType,
    nullable: bool,
    unique: bool,
    primary_key: bool,
    foreign_key: Option<(String, String)>,
}

impl Column {
    pub fn new(name: impl AsRef<str>, db_type: DbType) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            db_type,
            nullable: false,
            unique: false,
            primary_key: false,
            foreign_key: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn escape_val(&self, val: &dyn Any) -> Result<String, Error> {
        // TODO: include value into Error
        if self.nullable {
            self.db_type
                .escape_nullable_val(val)
                .ok_or_else(|| Error::BadValueForNullable {
                    column_name: self.name().into(),
                    column_type: self.db_type().to_string(),
                })
        } else {
            self.db_type.escape_val(val).ok_or_else(|| Error::BadValue {
                column_name: self.name().into(),
                column_type: self.db_type().to_string(),
            })
        }
    }
}

#[derive(Debug)]
pub enum Error {
    BadValue {
        column_name: String,
        column_type: String,
    },
    BadValueForNullable {
        column_name: String,
        column_type: String,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadValue {
                column_name,
                column_type,
            } => {
                write!(
                    f,
                    "The value cannot be inserted into column '{}' of type '{} NOT NULL'",
                    column_name, column_type
                )
            }
            Self::BadValueForNullable {
                column_name,
                column_type,
            } => {
                write!(
                    f,
                    "The value cannot be inserted into column '{}' of type '{}'",
                    column_name, column_type
                )
            }
        }
    }
}

impl std::error::Error for Error {}

impl Column {
    pub const fn db_type(&self) -> &DbType {
        &self.db_type
    }

    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub const fn is_unique(&self) -> bool {
        self.nullable
    }

    pub const fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    pub fn foreign_key(&self) -> Option<(String, String)> {
        self.foreign_key.clone()
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nullable = if self.nullable { " NULL" } else { " NOT NULL" };
        let unique = if self.unique { " UNIQUE" } else { "" };
        let primary_key = if self.primary_key { " PRIMARY KEY" } else { "" };
        let foreign_key = if let Some((ref_table, ref_column)) = &self.foreign_key {
            format!(" REFERENCES {}({})", ref_table, ref_column)
        } else {
            "".into()
        };
        write!(
            f,
            "{} {}{}{}{}{}",
            self.name, self.db_type, nullable, unique, primary_key, foreign_key
        )
    }
}
