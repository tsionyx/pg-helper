use std::fmt;

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

impl fmt::Display for CheckConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CONSTRAINT {} CHECK ({})", self.name, self.condition)
    }
}
