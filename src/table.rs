use std::fmt::Write as _;

use itertools::Itertools as _;
use postgres_types::ToSql;

use crate::{column::Column, constraint::Constraint, type_helpers::ObjectAndCreateSql};

pub trait Table<const N: usize> {
    fn name() -> &'static str;

    fn columns() -> [Column; N];

    fn constraints() -> Option<Vec<Box<dyn Constraint>>> {
        None
    }

    fn create_indices_sql() -> Vec<ObjectAndCreateSql> {
        Self::columns()
            .iter()
            .filter_map(|col| col.create_index_sql(Self::name()))
            .collect()
    }

    fn create_types_sql() -> Vec<ObjectAndCreateSql> {
        Self::columns()
            .iter()
            .flat_map(|col| col.create_types_sql())
            .unique()
            .collect()
    }

    fn create_table_sql() -> String {
        let columns: String = Self::columns().iter().map(|col| col.to_string()).join(", ");
        let mut query = columns;

        if let Some(constraints) = Self::constraints() {
            let constraints = constraints
                .iter()
                .map(|constraint| constraint.as_sql())
                .join(", ");
            if !constraints.is_empty() {
                write!(query, ", {}", constraints).unwrap();
            }
        }

        format!("CREATE TABLE IF NOT EXISTS {} ({});", Self::name(), query)
    }

    fn values(&self) -> [&(dyn ToSql + Sync); N];

    fn insert_sql() -> String {
        Self::insert_many_sql(1)
    }

    fn insert_many_sql(rows_number: usize) -> String {
        if rows_number == 0 {
            return String::new();
        }
        let columns_names = Self::columns().iter().map(|c| c.name()).join(", ");
        let placeholder_values = (0..rows_number)
            .map(|row_idx| {
                let row_placeholders = (1..=N)
                    .map(|x| {
                        let abs_index = N * row_idx + x;
                        format!("${}", abs_index)
                    })
                    .join(", ");
                format!("({})", row_placeholders)
            })
            .join(", ");

        format!(
            "INSERT INTO {} ({}) VALUES {};",
            Self::name(),
            columns_names,
            placeholder_values,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnBuilder;

    use postgres_types::Type;
    use uuid::Uuid;

    mod simple {
        use super::*;

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
                        .foreign_key("users", "user_id")
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

        #[test]
        fn create_types() {
            assert!(Buy::create_types_sql().is_empty());
        }

        #[test]
        fn create_table() {
            assert_eq!(
                Buy::create_table_sql(),
                "CREATE TABLE IF NOT EXISTS buys (\
                buy_id uuid NOT NULL UNIQUE PRIMARY KEY, \
                customer_id uuid NOT NULL REFERENCES users(user_id), \
                has_discount bool NULL, \
                total_price float4 NULL, \
                details varchar NULL\
            );"
            );
        }

        #[test]
        fn insert_single() {
            assert_eq!(
                Buy::insert_sql(),
                "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
                VALUES ($1, $2, $3, $4, $5);"
            );
        }

        #[test]
        fn insert_both() {
            let buys = vec![
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: Uuid::new_v4(),
                    has_discount: None,
                    total_price: Some(14.56),
                    details: None,
                },
                Buy {
                    buy_id: Uuid::new_v4(),
                    customer_id: Uuid::new_v4(),
                    has_discount: Some(true),
                    total_price: Some(18899.9),
                    details: Some("the delivery should be performed".into()),
                },
            ];

            assert_eq!(
                Buy::insert_many_sql(buys.len()),
                "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
                VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10);"
            );
        }
    }

    mod with_complex_fields {
        use super::*;
        use crate::struct_type;

        #[derive(Debug, Copy, Clone, ToSql)]
        #[postgres(name = "point2d")]
        struct Point {
            x: i16,
            y: i16,
        }

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

        #[test]
        fn create_types() {
            assert_eq!(
                Image::create_types_sql(),
                [ObjectAndCreateSql::new(
                    "point2d",
                    "CREATE TYPE point2d AS (x int2, y int2)"
                )]
            );
        }

        #[test]
        fn create_table() {
            assert_eq!(
                Image::create_table_sql(),
                "CREATE TABLE IF NOT EXISTS images (\
                top_left point2d NOT NULL, \
                bottom_right point2d NOT NULL, \
                center point2d NULL\
            );"
            );
        }

        #[test]
        fn insert_single() {
            assert_eq!(
                Image::insert_sql(),
                "INSERT INTO images (top_left, bottom_right, center) \
                VALUES ($1, $2, $3);"
            );
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

            assert_eq!(
                Image::insert_many_sql(images.len()),
                "INSERT INTO images (top_left, bottom_right, center) \
                VALUES ($1, $2, $3), ($4, $5, $6);"
            );
        }
    }

    mod with_heterogeneous_struct {
        use super::*;
        use crate::struct_type;

        #[derive(Debug, Clone, ToSql)]
        #[postgres(name = "with_label")]
        struct IntWithLabel {
            val: i16,
            label: String,
        }

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

        #[test]
        fn create_types() {
            assert_eq!(
                SingleValuedTable::create_types_sql(),
                [ObjectAndCreateSql::new(
                    "with_label",
                    "CREATE TYPE with_label AS (val int2, label varchar)"
                )]
            );
        }

        #[test]
        fn create_table() {
            assert_eq!(
                SingleValuedTable::create_table_sql(),
                "CREATE TABLE IF NOT EXISTS tbl (\
                val with_label NOT NULL\
            );"
            );
        }

        #[test]
        fn insert_single() {
            assert_eq!(
                SingleValuedTable::insert_sql(),
                "INSERT INTO tbl (val) VALUES ($1);"
            );
        }
    }

    mod with_constraints {
        use super::*;
        use crate::{CheckConstraint, UniqueConstraint};

        struct ConstrainedTable {
            key1: i16,
            key2: i16,
            label: i16,
        }

        impl Table<3> for ConstrainedTable {
            fn name() -> &'static str {
                "constrained"
            }

            fn columns() -> [Column; 3] {
                [
                    Column::new("key1", Type::INT2),
                    Column::new("key2", Type::INT2),
                    Column::new("label", Type::INT2),
                ]
            }

            fn constraints() -> Option<Vec<Box<dyn Constraint>>> {
                let cols = Self::columns();
                Some(vec![
                    Box::new(UniqueConstraint::new("combined_key", &[&cols[0], &cols[1]])),
                    Box::new(CheckConstraint::new("label_percent", "label <= 100")),
                ])
            }

            fn values(&self) -> [&(dyn ToSql + Sync); 3] {
                [&self.key1, &self.key2, &self.label]
            }
        }

        #[test]
        fn create_table() {
            assert_eq!(
                ConstrainedTable::create_table_sql(),
                "CREATE TABLE IF NOT EXISTS constrained (\
                key1 int2 NOT NULL, \
                key2 int2 NOT NULL, \
                label int2 NOT NULL, \
                CONSTRAINT combined_key UNIQUE (key1, key2), \
                CONSTRAINT label_percent CHECK (label <= 100)\
            );"
            );
        }
    }
}
