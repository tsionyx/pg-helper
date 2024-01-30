use std::{borrow::Cow, error::Error, fmt::Debug};

use postgres_types::{
    private::BytesMut, to_sql_checked, FromSql, IsNull, Kind, ToSql, Type as DbType,
};

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum Serial<T> {
    Default,
    Value(T),
}

impl<T: Default + Clone> Serial<T> {
    pub fn value_or_default(&self) -> Cow<T> {
        match self {
            Serial::Default => Cow::Owned(T::default()),
            Serial::Value(val) => Cow::Borrowed(val),
        }
    }
}

impl<T> ToSql for Serial<T>
where
    T: ToSql + Default + Clone,
{
    fn to_sql(
        &self,
        ty: &DbType,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.value_or_default().to_sql(ty, out)
    }

    fn accepts(ty: &DbType) -> bool
    where
        Self: Sized,
    {
        T::accepts(ty)
    }

    to_sql_checked!();
}

macro_rules! serial_from {
    ($t:ty, $f:ident, $expected:path, $sql_type:literal) => {
        impl<'a> FromSql<'a> for Serial<$t> {
            fn from_sql(_: &DbType, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
                postgres_protocol::types::$f(raw).map(Self::Value)
            }

            fn accepts(ty: &DbType) -> bool {
                matches!(*ty, $expected)
            }
        }

        impl Serial<$t> {
            pub fn sql_type() -> DbType {
                DbType::new($sql_type.into(), 0, Kind::Simple, "public".into())
            }
        }
    };
}

serial_from!(i16, int2_from_sql, DbType::INT2, "serial2");
serial_from!(i32, int4_from_sql, DbType::INT4, "serial4");
serial_from!(i64, int8_from_sql, DbType::INT8, "serial8");
