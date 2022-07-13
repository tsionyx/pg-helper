use std::fmt::Write as _;

use itertools::Itertools as _;
use postgres_types::ToSql;

use crate::{column::Column, constraint::CheckConstraint};

pub trait Table<const N: usize, const CONSTRAINTS_N: usize = 0> {
    fn name() -> &'static str;

    fn columns() -> [Column; N];

    fn constraints() -> [CheckConstraint; CONSTRAINTS_N];

    fn create_types_sql() -> Option<String> {
        let types = Self::columns()
            .iter()
            .filter_map(|col| col.type_create_sql())
            .unique()
            .join("; ");

        if types.is_empty() {
            None
        } else {
            Some(format!("{};", types))
        }
    }

    fn create_table_sql() -> String {
        let columns: String = Self::columns().iter().map(|col| col.to_string()).join(", ");
        let mut sql = columns;

        let constraints = Self::constraints()
            .iter()
            .map(CheckConstraint::to_string)
            .join(",");
        if !constraints.is_empty() {
            write!(sql, ", {}", constraints).unwrap();
        }

        format!("CREATE TABLE IF NOT EXISTS {} ({});", Self::name(), sql)
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

        fn constraints() -> [CheckConstraint; 0] {
            []
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
        assert!(Buy::create_types_sql().is_none());
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

        // let b = Buy {
        //     buy_id: Uuid::new_v4(),
        //     customer_id: Uuid::new_v4(),
        //     has_discount: None,
        //     total_price: Some(14.56),
        //     details: None,
        // };
        //
        // assert_eq!(
        //     b.insert_sql(),
        //     format!(
        //         "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
        //         VALUES (\
        //             '{}', \
        //             '{}', \
        //             NULL, \
        //             14.56, \
        //             NULL\
        //         );",
        //         b.buy_id, b.customer_id
        //     )
        // );
    }

    #[test]
    #[ignore = "without the real values it is just a repetition of the previous test"]
    fn insert_with_all_values() {
        // let b = Buy {
        //     buy_id: Uuid::new_v4(),
        //     customer_id: Uuid::new_v4(),
        //     has_discount: Some(true),
        //     total_price: Some(18899.9),
        //     details: Some("the delivery should be performed".into()),
        // };
        //
        // assert_eq!(
        //     b.insert_sql(),
        //     format!(
        //         "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
        //             VALUES (\
        //                 '{}', \
        //                 '{}', \
        //                 true, \
        //                 18899.9, \
        //                 'the delivery should be performed'\
        //             );",
        //         b.buy_id, b.customer_id
        //     )
        // );
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
        // assert_eq!(
        //     Buy::insert_many_sql(&buys),
        //     format!(
        //         "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
        //         VALUES (\
        //             '{}', \
        //             '{}', \
        //             NULL, \
        //             14.56, \
        //             NULL\
        //         ), (\
        //             '{}', \
        //             '{}', \
        //             true, \
        //             18899.9, \
        //             'the delivery should be performed'\
        //         );",
        //         buys[0].buy_id, buys[0].customer_id, buys[1].buy_id, buys[1].customer_id
        //     )
        // );
    }
}

#[cfg(test)]
mod tests_custom {
    use super::*;
    use crate::{struct_type, ColumnBuilder};

    use postgres_types::{ToSql, Type};

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
                ColumnBuilder::new("top_left", point_type.clone()).finish(),
                ColumnBuilder::new("bottom_right", point_type.clone()).finish(),
                ColumnBuilder::new("center", point_type).nullable().finish(),
            ]
        }

        fn constraints() -> [CheckConstraint; 0] {
            []
        }

        fn values(&self) -> [&(dyn ToSql + Sync); 3] {
            [&self.point_top_left, &self.point_bottom_right, &self.center]
        }
    }

    #[test]
    fn create_types() {
        assert_eq!(
            Image::create_types_sql().unwrap(),
            "CREATE TYPE point2d AS (\
                x int2, \
                y int2\
            );"
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
    fn insert_simple() {
        assert_eq!(
            Image::insert_sql(),
            "INSERT INTO images (top_left, bottom_right, center) \
                VALUES ($1, $2, $3);"
        );
        // let im = Image {
        //     point_top_left: Point { x: 5, y: 8 },
        //     point_bottom_right: Point { x: 215, y: 160 },
        //     center: None,
        // };
        //
        // assert_eq!(
        //     im.insert_sql().unwrap(),
        //     format!(
        //         "INSERT INTO images (top_left, bottom_right, center) \
        //         VALUES (\
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d, \
        //             NULL\
        //         );",
        //         im.point_top_left.x,
        //         im.point_top_left.y,
        //         im.point_bottom_right.x,
        //         im.point_bottom_right.y
        //     )
        // );
    }

    #[test]
    #[ignore = "without the real values it is just a repetition of the previous test"]
    fn insert_with_center() {
        // let im = Image {
        //     point_top_left: Point { x: 5, y: 8 },
        //     point_bottom_right: Point { x: 215, y: 160 },
        //     center: Some(Point { x: 100, y: 80 }),
        // };
        //
        // assert_eq!(
        //     im.insert_sql().unwrap(),
        //     format!(
        //         "INSERT INTO images (top_left, bottom_right, center) \
        //         VALUES (\
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d\
        //         );",
        //         im.point_top_left.x,
        //         im.point_top_left.y,
        //         im.point_bottom_right.x,
        //         im.point_bottom_right.y,
        //         im.center.unwrap().x,
        //         im.center.unwrap().y
        //     )
        // );
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
        // assert_eq!(
        //     Image::insert_many_sql(&images),
        //     format!(
        //         "INSERT INTO images (top_left, bottom_right, center) \
        //         VALUES (\
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d, \
        //             NULL\
        //         ), (\
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d, \
        //             ROW({}, {})::point2d\
        //         );",
        //         images[0].point_top_left.x,
        //         images[0].point_top_left.y,
        //         images[0].point_bottom_right.x,
        //         images[0].point_bottom_right.y,
        //         images[1].point_top_left.x,
        //         images[1].point_top_left.y,
        //         images[1].point_bottom_right.x,
        //         images[1].point_bottom_right.y,
        //         images[1].center.unwrap().x,
        //         images[1].center.unwrap().y
        //     )
        // );
    }
}

#[cfg(test)]
mod tests_heterogeneous {
    use super::*;
    use crate::{struct_type, ColumnBuilder};

    use postgres_types::{ToSql, Type};

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
            [ColumnBuilder::new("val", int_with_label_type).finish()]
        }

        fn constraints() -> [CheckConstraint; 0] {
            []
        }

        fn values(&self) -> [&(dyn ToSql + Sync); 1] {
            [&self.val]
        }
    }

    #[test]
    fn create_types() {
        assert_eq!(
            SingleValuedTable::create_types_sql().unwrap(),
            "CREATE TYPE with_label AS (\
                val int2, \
                label varchar\
            );"
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
    fn insert_simple() {
        assert_eq!(
            SingleValuedTable::insert_sql(),
            "INSERT INTO tbl (val) VALUES ($1);"
        );

        // let x = SingleValuedTable {
        //     val: IntWithLabel {
        //         val: 0,
        //         label: "foo-bar".to_string(),
        //     },
        // };
        //
        // assert_eq!(
        //     x.insert_sql().unwrap(),
        //     format!(
        //         "INSERT INTO tbl (val) \
        //         VALUES (\
        //             ROW(0, 'foo-bar')::with_label\
        //         );",
        //     )
        // );
    }
}
