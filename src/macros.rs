#[macro_export]
macro_rules! count {
    () => (0_usize);
    ( $x:tt $($xs:tt)* ) => (1_usize + $crate::count!($($xs)*));
}

#[macro_export]
macro_rules! gen_table {
    (
        $(#[$outer:meta])*
        $struct_vis:vis struct $TableName:ident ($sql_name:literal) {
            $(
                $(#[$inner:ident $($args:tt)*])*
                $field:ident: $field_ty:ty = $sql_ty:expr $(;[$($prop:ident($($prop_arg:expr),*)),+ $(,)?])?
            ),+ $(,)?
            $(=> constraints = [$($constraint:expr),+ $(,)?])?
        }
    ) => {
        $(#[$outer])*
        $struct_vis struct $TableName {
            $(
                $(#[$inner $($args)*])*
                $field: $field_ty,
            )+
        }

        impl $crate::Table< {$crate::count!($($field)+)} > for $TableName {
            fn name() -> &'static str {
                $sql_name
            }

            fn columns() -> [$crate::Column; $crate::count!($($field)+)] {
                [
                    $(
                        // $field
                        $crate::ColumnBuilder::new(
                            stringify!($field), $sql_ty)
                        $($(.$prop($($prop_arg),*))+)?
                        .finish(),
                    )+
                ]
            }

            $(
                fn constraints() -> Option<Vec<Box<dyn $crate::Constraint>>> {
                    Some(vec![
                        $(Box::new($constraint)),+
                    ])
                }
            )?
        }

        impl $crate::InsertableValues< {$crate::count!($($field)+)} > for $TableName {
            fn values(&self) -> [&(dyn postgres_types::ToSql + Sync); $crate::count!($($field)+)] {
                [$(&self.$field,)+]
            }
        }

        impl TryFrom<tokio_postgres::Row> for $TableName {
            type Error = tokio_postgres::Error;

            fn try_from(value: tokio_postgres::Row) -> Result<Self, Self::Error> {
                $(
                    let $field = value.try_get(stringify!($field))?;
                )+

                Ok(Self { $($field,)+ })
            }
        }
    };
}

#[macro_export]
macro_rules! primary_key_with_indices {
    ($name:expr => [$($idx:literal),+ $(,)?]) => {
         $crate::primary_key_with_indices!($name => Self[$($idx),+])
    };
    ($name:expr => $table:ident [$($idx:literal),+ $(,)?]) => {
         $crate::PrimaryKeyConstraint::new($name, &[$(&$table::columns()[$idx]),+])
    };
}

#[macro_export]
macro_rules! foreign_key_with_indices {
    ($name:expr => $dst_table:ident [$($src_idx:literal => $dst_idx:literal),+ $(,)?]) => {
         $crate::foreign_key_with_indices!($name => (Self => $dst_table) [$($src_idx => $dst_idx),+])
    };
    ($name:expr => ($src_table:ident => $dst_table:ident) [$($src_idx:literal => $dst_idx:literal),+ $(,)?]) => {
         $crate::ForeignKeyConstraint::new($name, $dst_table::name(),
            &[$( (&$src_table::columns()[$src_idx], &$dst_table::columns()[$dst_idx]) ),+])
    };
}

#[macro_export]
macro_rules! unique_with_indices {
    ($name:expr => [$($idx:literal),+ $(,)?]) => {
         $crate::unique_with_indices!($name => Self[$($idx),+])
    };
    ($name:expr => $table:ident [$($idx:literal),+ $(,)?]) => {
         $crate::UniqueConstraint::new($name, &[$(&$table::columns()[$idx]),+])
    };
}

#[test]
fn constraints_are_compiled() {
    use crate::Table as _;

    gen_table!(
        pub struct Bar("bar") {
            x: i16 = postgres::types::Type::INT2,
            y: i16 = postgres::types::Type::INT2,
            z: i16 = postgres::types::Type::INT2,
            => constraints = [
                primary_key_with_indices!("pk" => [0]),
            ]
        }
    );
    assert_eq!(
        Bar::create_table_sql(),
        "CREATE TABLE IF NOT EXISTS bar \
               (x int2 NOT NULL, y int2 NOT NULL, z int2 NOT NULL, \
               CONSTRAINT pk PRIMARY KEY (x));"
    );

    gen_table!(
        pub struct Foo("foo") {
            x: i16 = postgres::types::Type::INT2,
            y: i16 = postgres::types::Type::INT2,
            => constraints = [
                primary_key_with_indices!("pk" => [0]),
                foreign_key_with_indices!("fk" => Bar [0=>1, 1=>2]),
            ]
        }
    );

    assert_eq!(
        Foo::create_table_sql(),
        "CREATE TABLE IF NOT EXISTS foo \
               (x int2 NOT NULL, y int2 NOT NULL, \
               CONSTRAINT pk PRIMARY KEY (x), \
               CONSTRAINT fk FOREIGN KEY (x, y) REFERENCES bar (y, z));"
    );
}
