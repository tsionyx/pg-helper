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
pub struct UniqueConstraint {
    name: String,
    columns: Vec<String>,
}

impl UniqueConstraint {
    pub fn new(name: impl AsRef<str>, columns: &[&Column]) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            columns: columns.iter().map(|col| col.name().to_owned()).collect(),
        }
    }
}

impl Constraint for UniqueConstraint {
    fn name(&self) -> &str {
        &self.name
    }

    fn body(&self) -> String {
        format!("UNIQUE ({})", self.columns.join(", "))
    }
}
