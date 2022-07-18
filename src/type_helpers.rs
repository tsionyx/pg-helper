use itertools::Itertools;
use postgres_types::{Field, Kind, Type};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ObjectAndCreateSql {
    name: String,
    create_sql: String,
}

impl ObjectAndCreateSql {
    pub(crate) fn new(name: impl AsRef<str>, create_sql: impl AsRef<str>) -> Self {
        Self {
            name: name.as_ref().to_owned(),
            create_sql: create_sql.as_ref().to_owned(),
        }
    }

    /// Construct _CREATE_ statement for a type if it is not a standard type.
    /// Returns `Vec` of statements to include all the nested types also.
    pub(crate) fn from_type(ty: &Type) -> Vec<Self> {
        match ty.kind() {
            Kind::Simple | Kind::Pseudo => vec![],
            Kind::Array(inner) => Self::from_type(inner),
            Kind::Range(inner) => {
                // TODO: check for the range itself whether it is a standard type
                Self::from_type(inner)
            }
            Kind::Domain(inner) => {
                let mut prev_defs = Self::from_type(inner);
                let def = Self::new(ty.name(), format!("CREATE DOMAIN \"{}\" AS {}", ty, inner));
                prev_defs.push(def);
                prev_defs
            }
            Kind::Enum(fields) => {
                let fields = fields.iter().map(|f| format!("'{}'", f)).join(", ");
                let def = Self::new(
                    ty.name(),
                    format!("CREATE TYPE \"{}\" AS ENUM ({})", ty, fields),
                );
                vec![def]
            }
            Kind::Composite(fields) => {
                let mut prev_defs: Vec<_> = fields
                    .iter()
                    .flat_map(|f| Self::from_type(f.type_()))
                    .collect();

                let fields = fields
                    .iter()
                    .map(|f| format!("{} {}", f.name(), f.type_()))
                    .join(", ");
                let def = Self::new(
                    ty.name(),
                    format!("CREATE TYPE {} AS ({})", ty.name(), fields),
                );
                prev_defs.push(def);
                prev_defs
            }
            other_kind => {
                unimplemented!("Unhandled type kind: {:?}", other_kind)
            }
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_sql(&self) -> &str {
        &self.create_sql
    }
}

pub fn struct_type(name: impl AsRef<str>, fields: &[(impl AsRef<str>, Type)]) -> Type {
    let fields = fields
        .iter()
        .map(|(name, type_)| Field::new(name.as_ref().to_owned(), type_.clone()))
        .collect();
    Type::new(
        name.as_ref().to_owned(),
        0,
        Kind::Composite(fields),
        "public".into(),
    )
}

pub fn array_type(of: Type) -> Type {
    let plural = format!("{}s", of.name());
    Type::new(plural, 0, Kind::Array(of), "public".into())
}
