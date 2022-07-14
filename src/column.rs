use std::fmt::{self, Debug, Display};

use postgres_types::{Kind, Type as DbType};

use crate::type_helpers::TypeNameAndCreate;

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

    pub(crate) fn type_create_sql(&self) -> Vec<TypeNameAndCreate> {
        TypeNameAndCreate::from_type(self.db_type())
    }
}

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

        let type_desc = match self.db_type.kind() {
            Kind::Array(inner) => format!("{}[]", inner),
            _ => self.db_type.to_string(),
        };

        write!(
            f,
            "{} {}{}{}{}{}",
            self.name, type_desc, nullable, unique, primary_key, foreign_key
        )
    }
}
