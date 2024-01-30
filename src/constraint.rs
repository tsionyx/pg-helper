use super::column::Column;

pub trait Constraint {
    fn as_sql(&self) -> String {
        format!("CONSTRAINT {} {}", self.name(), self.body())
    }

    fn name(&self) -> &str;

    fn body(&self) -> String;
}

#[derive(Debug)]
pub struct CheckConstraint {
    name: String,
    condition: String,
}

impl CheckConstraint {
    pub fn new(name: impl AsRef<str>, condition: impl AsRef<str>) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            condition: condition.as_ref().to_owned(),
        }
    }
}

impl Constraint for CheckConstraint {
    fn name(&self) -> &str {
        &self.name
    }

    fn body(&self) -> String {
        format!("CHECK ({})", self.condition)
    }
}

#[derive(Debug)]
pub struct PrimaryKeyConstraint {
    name: String,
    columns: Vec<String>,
}

impl PrimaryKeyConstraint {
    pub fn new(name: impl AsRef<str>, columns: &[&Column]) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            columns: columns.iter().map(|col| col.name().to_owned()).collect(),
        }
    }
}

impl Constraint for PrimaryKeyConstraint {
    fn name(&self) -> &str {
        &self.name
    }

    fn body(&self) -> String {
        format!("PRIMARY KEY ({})", self.columns.join(", "))
    }
}

#[derive(Debug)]
pub struct ForeignKeyConstraint {
    name: String,
    target_table: String,
    column_pairs: Vec<(String, String)>,
}

impl ForeignKeyConstraint {
    pub fn new(
        name: impl AsRef<str>,
        target_table: impl AsRef<str>,
        column_pairs: &[(&Column, &Column)],
    ) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            target_table: target_table.as_ref().to_owned(),
            column_pairs: column_pairs
                .iter()
                .map(|(src, dest)| (src.name().to_owned(), dest.name().to_owned()))
                .collect(),
        }
    }
}

impl Constraint for ForeignKeyConstraint {
    fn name(&self) -> &str {
        &self.name
    }

    fn body(&self) -> String {
        let (src, dest): (Vec<_>, Vec<_>) = self
            .column_pairs
            .iter()
            .map(|x| (x.0.as_str(), x.1.as_str()))
            .unzip();
        format!(
            "FOREIGN KEY ({}) REFERENCES {} ({})",
            src.join(", "),
            self.target_table,
            dest.join(", ")
        )
    }
}

#[derive(Debug)]
pub struct UniqueConstraint {
    name: String,
    columns: Vec<String>,
    // TODO
    with_nulls_non_distinct: bool,
}

impl UniqueConstraint {
    pub fn new(name: impl AsRef<str>, columns: &[&Column]) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            columns: columns.iter().map(|col| col.name().to_owned()).collect(),
            with_nulls_non_distinct: false,
        }
    }
}

impl Constraint for UniqueConstraint {
    fn name(&self) -> &str {
        &self.name
    }

    fn body(&self) -> String {
        if self.with_nulls_non_distinct {
            format!("UNIQUE NULLS NOT DISTINCT ({})", self.columns.join(", "))
        } else {
            format!("UNIQUE ({})", self.columns.join(", "))
        }
    }
}
