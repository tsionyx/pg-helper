use std::fmt::{self, Debug, Display};

use itertools::Itertools;
use postgres_types::{Field, Kind, Type as DbType};

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

    pub(crate) fn type_create_sql(&self) -> Option<String> {
        let type_defs = type_definition(self.db_type());
        if type_defs.is_empty() {
            None
        } else {
            Some(type_defs.into_iter().unique().join("; "))
        }
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
        write!(
            f,
            "{} {}{}{}{}{}",
            self.name, self.db_type, nullable, unique, primary_key, foreign_key
        )
    }
}

/// Construct _CREATE_ statement for a type if it is not a standard type.
/// Returns `Vec` of statements to include all the nested types also.
fn type_definition(ty: &DbType) -> Vec<String> {
    match ty.kind() {
        Kind::Simple | Kind::Pseudo => vec![],
        Kind::Array(inner) => type_definition(inner),
        Kind::Range(inner) => {
            // TODO: check for the range itself whether it is a standard type
            type_definition(inner)
        }
        Kind::Domain(inner) => {
            let mut so_far = type_definition(inner);
            so_far.push(format!("CREATE DOMAIN \"{}\" AS {}", ty, inner));
            so_far
        }
        Kind::Enum(fields) => {
            let fields = fields.iter().map(|f| format!("'{}'", f)).join(", ");
            vec![format!("CREATE TYPE \"{}\" AS ENUM ({})", ty, fields)]
        }
        Kind::Composite(fields) => {
            let mut so_far: Vec<_> = fields
                .iter()
                .flat_map(|f| type_definition(f.type_()))
                .collect();

            let fields = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), f.type_()))
                .join(", ");
            so_far.push(format!("CREATE TYPE {} AS ({})", ty.name(), fields));
            so_far
        }
        other_kind => {
            unimplemented!("Unhandled type kind: {:?}", other_kind)
        }
    }
}

pub fn struct_type(name: impl AsRef<str>, fields: &[(impl AsRef<str>, DbType)]) -> DbType {
    let fields = fields
        .iter()
        .map(|(name, type_)| Field::new(name.as_ref().to_owned(), type_.clone()))
        .collect();
    DbType::new(
        name.as_ref().to_owned(),
        0,
        Kind::Composite(fields),
        "public".into(),
    )
}
