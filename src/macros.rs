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
