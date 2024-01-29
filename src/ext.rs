use crate::table::Table;

use log::{debug, info};
use postgres::{Client, Error, Row};
use postgres_types::ToSql;

pub trait PgTableExtension {
    fn create_table<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>;
    fn create_types<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>;
    fn create_indices<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>;

    fn insert_row<T, const N: usize>(&mut self, row: &T) -> Result<u64, Error>
    where
        T: Table<N>;
    fn insert_rows<T, const N: usize>(&mut self, rows: &[T]) -> Result<u64, Error>
    where
        T: Table<N>;

    fn select_all<T, const N: usize>(&mut self) -> Result<Vec<T>, Error>
    where
        T: Table<N> + TryFrom<Row, Error = Error>;
    fn select<T, const N: usize>(
        &mut self,
        condition: impl Into<Option<String>>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<T>, Error>
    where
        T: Table<N> + TryFrom<Row, Error = Error>;
}

pub(super) fn query_type_existence(type_name: &str) -> String {
    format!(
        "SELECT oid FROM pg_catalog.pg_type where typname = '{}'",
        type_name
    )
}

impl PgTableExtension for Client {
    fn create_table<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>,
    {
        self.create_types::<T, N>()?;

        info!("Creating the table {}...", T::name());
        let query = T::create_table_sql();
        debug!("CREATE for table {}: {}", T::name(), query);
        self.batch_execute(&query)?;

        self.create_indices::<T, N>()
    }

    fn create_types<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>,
    {
        let create_types = T::create_types_sql();

        if create_types.is_empty() {
            debug!("Skip the types for a table {:?}...", T::name());
        } else {
            info!("Creating the types for a table {:?}...", T::name());
            for ty_query in create_types {
                let type_name = ty_query.name();
                let res = self.query(&query_type_existence(type_name), &[])?;
                if res.is_empty() {
                    let sql = ty_query.create_sql();
                    info!("Not found type {:?}. Creating it with {:?}", type_name, sql);
                    self.execute(sql, &[])?;
                }
            }
            info!("Types for table {} created", T::name());
        }
        Ok(())
    }

    fn create_indices<T, const N: usize>(&mut self) -> Result<(), Error>
    where
        T: Table<N>,
    {
        let create_indices = T::create_indices_sql();

        if create_indices.is_empty() {
            debug!("Skip the indices for a table {:?}...", T::name());
        } else {
            info!("Creating the indices for a table {:?}...", T::name());
            for idx_query in create_indices {
                let col_name = idx_query.name();
                info!(
                    "Creating the index {:?} for a table {:?}...",
                    col_name,
                    T::name()
                );
                let sql = idx_query.create_sql();
                debug!("Full index query: {:?}", sql);
                self.execute(sql, &[])?;
            }
            info!("Indices for table {} created", T::name());
        }
        Ok(())
    }

    fn insert_row<T, const N: usize>(&mut self, row: &T) -> Result<u64, Error>
    where
        T: Table<N>,
    {
        let query = T::insert_sql();
        self.execute(&query, &row.values())
    }

    fn insert_rows<T, const N: usize>(&mut self, rows: &[T]) -> Result<u64, Error>
    where
        T: Table<N>,
    {
        let query = T::insert_many_sql(rows.len());
        let params: Vec<_> = rows.iter().flat_map(|row| row.values()).collect();
        self.execute(&query, &params)
    }

    fn select_all<T, const N: usize>(&mut self) -> Result<Vec<T>, Error>
    where
        T: Table<N> + TryFrom<Row, Error = Error>,
    {
        self.select(None, &[])
    }

    // TODO: make it lazy iterator
    fn select<T, const N: usize>(
        &mut self,
        condition: impl Into<Option<String>>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<T>, Error>
    where
        T: Table<N> + TryFrom<Row, Error = Error>,
    {
        let name = T::name();
        let query = format!("SELECT * FROM {}", name);
        let query = if let Some(condition) = condition.into() {
            format!("{} WHERE {}", query, condition)
        } else {
            query
        };

        let rows = self.query(&query, params)?;
        rows.into_iter().map(T::try_from).collect()
    }
}

/// These tests are conflicting with each other since they changing
/// the external entities (table in database), so you should run them **in 1 thread**.
///
/// Also, they are only truly run when you set the DATABASE_URL environment variable
/// to point to your instance of Postgres server:
///
/// ```shell
/// export DATABASE_URL="postgresql://USER_NAME:PASSWORD@HOST_NAME/DB_NAME"
/// cargo t -- --test-threads=1
/// ```
#[cfg(test)]
mod tests {
    use std::{marker::PhantomData, sync::Once};

    use super::*;
    use crate::{Column, ColumnBuilder};

    use postgres_types::Type;
    use uuid::Uuid;

    static INIT: Once = Once::new();

    /// Setup function that is only run once, even if called multiple times.
    fn setup() {
        INIT.call_once(|| {
            if let Err(err) = env_logger::try_init() {
                info!("logger probably initialized before: {}", err);
            }
        });
    }

    fn get_client() -> Option<Client> {
        setup();
        let db_url = std::env::var("DATABASE_URL").ok()?;
        let client = Client::connect(&db_url, postgres::NoTls).unwrap();
        Some(client)
    }

    struct Roundtrip<T, const N: usize>
    where
        T: Table<N>,
    {
        _phantom: PhantomData<T>,
    }

    impl<T, const N: usize> Roundtrip<T, N>
    where
        T: Table<N>,
    {
        fn new() -> Self {
            Self {
                _phantom: PhantomData,
            }
        }

        fn drop_table() {
            if let Some(mut client) = get_client() {
                client
                    .execute(&format!("DROP TABLE {}", T::name()), &[])
                    .unwrap();

                for ty in T::create_types_sql() {
                    let type_name = ty.name();

                    // TODO: correctly remove complex types
                    client
                        .execute(&format!("DROP TYPE {}", type_name), &[])
                        .unwrap();
                }
            }
        }
    }

    impl<T, const N: usize> Roundtrip<T, N>
    where
        T: Table<N> + PartialEq + std::fmt::Debug + TryFrom<Row, Error = Error> + Sync,
    {
        fn run(&self, items: &[T]) {
            if let Some(mut client) = get_client() {
                client.create_table::<T, N>().unwrap();

                let inserted = if items.is_empty() {
                    0
                } else if items.len() == 1 {
                    client.insert_row(&items[0]).unwrap()
                } else {
                    client.insert_rows(items).unwrap()
                };
                assert_eq!(inserted as usize, items.len());

                let from_db_items: Vec<T> = client.select_all().unwrap();
                assert_eq!(from_db_items, items);
            }
        }
    }

    impl<T, const N: usize> Drop for Roundtrip<T, N>
    where
        T: Table<N>,
    {
        fn drop(&mut self) {
            Self::drop_table();
        }
    }

    mod simple_table {
        use super::*;

        #[derive(Debug, PartialEq)]
        struct User {
            user_id: Uuid,
        }

        impl Table<1> for User {
            fn name() -> &'static str {
                "users"
            }

            fn columns() -> [Column; 1] {
                [ColumnBuilder::new("user_id", Type::UUID)
                    .primary_key()
                    .finish()]
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 1] {
                [&self.user_id]
            }
        }

        #[derive(Debug, PartialEq)]
        struct Buy {
            buy_id: Uuid,
            customer_id: Uuid,
            has_discount: Option<bool>,
            total_price: Option<f32>,
            details: Option<String>,
        }

        impl Table<5> for Buy {
            fn name() -> &'static str {
                "buys"
            }

            fn columns() -> [Column; 5] {
                [
                    ColumnBuilder::new("buy_id", Type::UUID)
                        .primary_key()
                        .finish(),
                    ColumnBuilder::new("customer_id", Type::UUID)
                        .foreign_key(User::name(), "user_id")
                        .finish(),
                    ColumnBuilder::new("has_discount", Type::BOOL)
                        .nullable()
                        .finish(),
                    ColumnBuilder::new("total_price", Type::FLOAT4)
                        .nullable()
                        .finish(),
                    ColumnBuilder::new("details", Type::VARCHAR)
                        .nullable()
                        .finish(),
                ]
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 5] {
                [
                    &self.buy_id,
                    &self.customer_id,
                    &self.has_discount,
                    &self.total_price,
                    &self.details,
                ]
            }
        }

        impl TryFrom<Row> for Buy {
            type Error = Error;

            fn try_from(value: Row) -> Result<Self, Self::Error> {
                let buy_id = value.try_get("buy_id")?;
                let customer_id = value.try_get("customer_id")?;
                let has_discount = value.try_get("has_discount")?;
                let total_price = value.try_get("total_price")?;
                let details = value.try_get("details")?;
                Ok(Self {
                    buy_id,
                    customer_id,
                    has_discount,
                    total_price,
                    details,
                })
            }
        }

        #[test]
        fn insert_single() {
            let user_id = Uuid::new_v4();
            let b = Buy {
                buy_id: Uuid::new_v4(),
                customer_id: user_id,
                has_discount: None,
                total_price: Some(14.56),
                details: None,
            };

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&[b]);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }

        #[test]
        fn insert_with_all_values() {
            let user_id = Uuid::new_v4();
            let b = Buy {
                buy_id: Uuid::new_v4(),
                customer_id: user_id,
                has_discount: Some(true),
                total_price: Some(18899.9),
                details: Some("the delivery should be performed".into()),
            };

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&[b]);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }

        #[test]
        fn insert_both() {
            let user_id = Uuid::new_v4();
            let buys = vec![
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: user_id,
                    has_discount: None,
                    total_price: Some(14.56),
                    details: None,
                },
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: user_id,
                    has_discount: Some(true),
                    total_price: Some(18899.9),
                    details: Some("the delivery should be performed".into()),
                },
            ];

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&buys);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }
    }

    mod simple_table_with_macro_ {
        use super::*;

        crate::gen_table! {
            #[derive(Debug, PartialEq)]
            struct User("users") {
                user_id: Uuid = Type::UUID; [primary_key()],
            }
        }

        crate::gen_table! {
            #[derive(Debug, PartialEq)]
            struct Buy("buys") {
                buy_id: Uuid = Type::UUID; [primary_key()],
                customer_id: Uuid = Type::UUID; [foreign_key(User::name(), "user_id")],
                has_discount: Option<bool> = Type::BOOL; [nullable()],
                total_price: Option<f32> = Type::FLOAT4; [nullable()],
                details: Option<String> = Type::VARCHAR; [nullable()],
            }
        }

        #[test]
        fn insert_single() {
            let user_id = Uuid::new_v4();
            let b = Buy {
                buy_id: Uuid::new_v4(),
                customer_id: user_id,
                has_discount: None,
                total_price: Some(14.56),
                details: None,
            };

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&[b]);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }

        #[test]
        fn insert_with_all_values() {
            let user_id = Uuid::new_v4();
            let b = Buy {
                buy_id: Uuid::new_v4(),
                customer_id: user_id,
                has_discount: Some(true),
                total_price: Some(18899.9),
                details: Some("the delivery should be performed".into()),
            };

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&[b]);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }

        #[test]
        fn insert_both() {
            let user_id = Uuid::new_v4();
            let buys = vec![
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: user_id,
                    has_discount: None,
                    total_price: Some(14.56),
                    details: None,
                },
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: user_id,
                    has_discount: Some(true),
                    total_price: Some(18899.9),
                    details: Some("the delivery should be performed".into()),
                },
            ];

            if let Some(mut client) = get_client() {
                client.create_table::<User, 1>().unwrap();
                client.insert_row(&User { user_id }).unwrap();
                Roundtrip::<_, 5>::new().run(&buys);
                client
                    .execute(&format!("DROP TABLE {}", User::name()), &[])
                    .unwrap();
            }
        }
    }

    mod table_with_complex_fields {
        use super::*;
        use crate::struct_type;
        use postgres_types::FromSql;

        #[derive(Debug, Copy, Clone, PartialEq, ToSql, FromSql)]
        #[postgres(name = "point2d")]
        struct Point {
            x: i16,
            y: i16,
        }

        #[derive(Debug, PartialEq)]
        struct Image {
            point_top_left: Point,
            point_bottom_right: Point,
            center: Option<Point>,
        }

        impl Table<3> for Image {
            fn name() -> &'static str {
                "images"
            }

            fn columns() -> [Column; 3] {
                let point_type = struct_type("point2d", &[("x", Type::INT2), ("y", Type::INT2)]);
                [
                    Column::new("top_left", point_type.clone()),
                    Column::new("bottom_right", point_type.clone()),
                    ColumnBuilder::new("center", point_type).nullable().finish(),
                ]
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 3] {
                [&self.point_top_left, &self.point_bottom_right, &self.center]
            }
        }

        impl TryFrom<Row> for Image {
            type Error = Error;

            fn try_from(value: Row) -> Result<Self, Self::Error> {
                let point_top_left = value.try_get("top_left")?;
                let point_bottom_right = value.try_get("bottom_right")?;
                let center = value.try_get("center")?;
                Ok(Self {
                    point_top_left,
                    point_bottom_right,
                    center,
                })
            }
        }

        #[test]
        fn insert_simple() {
            let im = Image {
                point_top_left: Point { x: 5, y: 8 },
                point_bottom_right: Point { x: 215, y: 160 },
                center: None,
            };

            Roundtrip::<_, 3>::new().run(&[im]);
        }

        #[test]
        fn insert_with_center() {
            let im = Image {
                point_top_left: Point { x: 5, y: 8 },
                point_bottom_right: Point { x: 215, y: 160 },
                center: Some(Point { x: 100, y: 80 }),
            };

            Roundtrip::<_, 3>::new().run(&[im]);
        }

        #[test]
        fn insert_both() {
            let images = vec![
                Image {
                    point_top_left: Point { x: 5, y: 8 },
                    point_bottom_right: Point { x: 215, y: 160 },
                    center: None,
                },
                Image {
                    point_top_left: Point { x: 5, y: 8 },
                    point_bottom_right: Point { x: 215, y: 160 },
                    center: Some(Point { x: 100, y: 80 }),
                },
            ];

            Roundtrip::<_, 3>::new().run(&images);
        }
    }

    mod table_with_heterogeneous_struct {
        use super::*;
        use crate::struct_type;
        use postgres_types::FromSql;

        #[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
        #[postgres(name = "with_label")]
        struct IntWithLabel {
            val: i16,
            label: String,
        }

        #[derive(Debug, PartialEq)]
        struct SingleValuedTable {
            val: IntWithLabel,
        }

        impl Table<1> for SingleValuedTable {
            fn name() -> &'static str {
                "tbl"
            }

            fn columns() -> [Column; 1] {
                let int_with_label_type = struct_type(
                    "with_label",
                    &[("val", Type::INT2), ("label", Type::VARCHAR)],
                );
                [Column::new("val", int_with_label_type)]
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 1] {
                [&self.val]
            }
        }

        impl TryFrom<Row> for SingleValuedTable {
            type Error = Error;

            fn try_from(value: Row) -> Result<Self, Self::Error> {
                let val = value.try_get("val")?;
                Ok(Self { val })
            }
        }

        #[test]
        fn insert_simple() {
            let x = SingleValuedTable {
                val: IntWithLabel {
                    val: 0,
                    label: "foo-bar".to_string(),
                },
            };

            Roundtrip::new().run(&[x]);
        }
    }

    mod table_with_array_of_structs {
        use super::*;
        use crate::{array_type, struct_type};
        use postgres_types::FromSql;

        #[derive(Debug, Copy, Clone, PartialEq, ToSql, FromSql)]
        #[postgres(name = "point2d")]
        struct Point {
            x: i16,
            y: i16,
        }

        #[derive(Debug, PartialEq)]
        struct Figure {
            name: String,
            polygon: Vec<Point>,
        }

        impl Table<2> for Figure {
            fn name() -> &'static str {
                "figures"
            }

            fn columns() -> [Column; 2] {
                let point_type = struct_type("point2d", &[("x", Type::INT2), ("y", Type::INT2)]);
                [
                    Column::new("name", Type::VARCHAR),
                    Column::new("polygon", array_type(point_type)),
                ]
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 2] {
                [&self.name, &self.polygon]
            }
        }

        impl TryFrom<Row> for Figure {
            type Error = Error;

            fn try_from(value: Row) -> Result<Self, Self::Error> {
                let name = value.try_get("name")?;
                let polygon = value.try_get("polygon")?;
                Ok(Self { name, polygon })
            }
        }

        #[test]
        fn insert() {
            let fig = Figure {
                name: "trapezoid".into(),
                polygon: vec![
                    Point { x: 0, y: 0 },
                    Point { x: 2, y: 4 },
                    Point { x: 3, y: 4 },
                    Point { x: 6, y: 0 },
                ],
            };

            Roundtrip::new().run(&[fig]);
        }
    }
}
