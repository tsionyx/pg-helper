use std::fmt::{self, Debug, Display};

use postgres_types::{Kind, Type as DbType};

use crate::type_helpers::ObjectAndCreateSql;

pub struct ColumnBuilder {
    name: String,
    db_type: DbType,
    nullable: bool,
    unique: bool,
    primary_key: bool,
    foreign_key: Option<(String, String)>,
    index: Option<IndexMethod>,
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
            index: None,
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

    pub fn index(self) -> Self {
        self.index_with(IndexMethod::default())
    }

    pub fn index_with(mut self, method: IndexMethod) -> Self {
        self.index = Some(method);
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
            index: self.index,
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
    index: Option<IndexMethod>,
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
            index: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn create_types_sql(&self) -> Vec<ObjectAndCreateSql> {
        ObjectAndCreateSql::from_type(self.db_type())
    }

    pub(crate) fn create_index_sql(&self, table_name: &str) -> Option<ObjectAndCreateSql> {
        self.index.map(|im| {
            let idx = Index {
                table_name: table_name.to_string(),
                column_name: self.name.clone(),
                method: im,
            };
            ObjectAndCreateSql::new(&self.name, idx.to_string())
        })
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

    pub fn get_index(&self) -> Option<IndexMethod> {
        self.index
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

#[derive(Debug)]
pub struct Index {
    table_name: String,
    column_name: String,
    method: IndexMethod,
}

impl Index {
    fn generate_name(&self) -> String {
        format!("{}_idx_{}", self.column_name, self.table_name)
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.generate_name();
        write!(
            f,
            "CREATE INDEX IF NOT EXISTS {} ON {} USING {} ({})",
            name, self.table_name, self.method, self.column_name
        )
    }
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum IndexMethod {
    BTree,
    Hash,
}

impl Default for IndexMethod {
    fn default() -> Self {
        Self::BTree
    }
}

impl Display for IndexMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let desc = match self {
            IndexMethod::BTree => "btree",
            IndexMethod::Hash => "hash",
        };
        write!(f, "{}", desc)
    }
}
