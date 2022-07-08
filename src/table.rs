use std::{any::Any as SqlValue, fmt::Write as _};

use itertools::Itertools as _;

use crate::{
    column::{self, Column},
    constraint::CheckConstraint,
};

pub trait Table<const N: usize, const CONSTRAINTS_N: usize = 0> {
    fn name() -> &'static str;

    fn columns() -> [Column; N];

    fn constraints() -> [CheckConstraint; CONSTRAINTS_N];

    fn create_types_sql() -> Option<String> {
        let types = Self::columns()
            .iter()
            .filter_map(|col| col.db_type().create_sql())
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

    // TODO: use `dyn crate::value::SqlValue` instead
    //  the main stop point here is the trait upcasting coercion,
    //  the cast `dyn SqlValue -> dyn Any` cannot be performed for now.
    //  See the <https://github.com/rust-lang/rust/issues/65991> for details.
    fn values(&self) -> [Box<dyn SqlValue>; N];

    fn insert_values_row(&self) -> Result<String, column::Error> {
        let columns = Self::columns();

        let values = self.values();
        let values: Result<Vec<_>, _> = columns
            .iter()
            .zip(values)
            .map(|(col, val)| col.escape_val(val.as_ref()))
            .collect();

        Ok(values?.join(", "))
    }

    fn insert_sql(&self) -> Result<String, column::Error> {
        let columns_names = Self::columns().iter().map(|c| c.name()).join(", ");
        let values = self.insert_values_row()?;

        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({});",
            Self::name(),
            columns_names,
            values,
        ))
    }

    fn insert_many_sql(rows: &[Self]) -> Result<String, column::Error>
    where
        Self: Sized,
    {
        let columns_names = Self::columns().iter().map(|c| c.name()).join(", ");
        let many_rows: Result<Vec<_>, _> = rows
            .iter()
            .map(|row| row.insert_values_row().map(|sql| format!("({})", sql)))
            .collect();

        let many_rows = many_rows?.join(", ");

        Ok(format!(
            "INSERT INTO {} ({}) VALUES {};",
            Self::name(),
            columns_names,
            many_rows,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnBuilder, DbType};
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
                ColumnBuilder::new("buy_id", DbType::Uuid)
                    .primary_key()
                    .finish(),
                ColumnBuilder::new("customer_id", DbType::Uuid)
                    .foreign_key("users", "user_id")
                    .finish(),
                ColumnBuilder::new("has_discount", DbType::Boolean)
                    .nullable()
                    .finish(),
                ColumnBuilder::new("total_price", DbType::Float)
                    .nullable()
                    .finish(),
                ColumnBuilder::new("details", DbType::VarChar(None))
                    .nullable()
                    .finish(),
            ]
        }

        fn constraints() -> [CheckConstraint; 0] {
            []
        }

        fn values(&self) -> [Box<dyn SqlValue>; 5] {
            [
                Box::new(self.buy_id),
                Box::new(self.customer_id),
                Box::new(self.has_discount),
                Box::new(self.total_price),
                Box::new(self.details.clone()),
            ]
        }
    }

    #[test]
    fn create_types() {
        assert!(Buy::create_types_sql().is_none(),);
    }

    #[test]
    fn create_table() {
        assert_eq!(
            Buy::create_table_sql(),
            "CREATE TABLE IF NOT EXISTS buys (\
                buy_id uuid NOT NULL UNIQUE PRIMARY KEY, \
                customer_id uuid NOT NULL REFERENCES users(user_id), \
                has_discount boolean NULL, \
                total_price real NULL, \
                details varchar NULL\
            );"
        );
    }

    #[test]
    fn insert_single() {
        let b = Buy {
            buy_id: Uuid::new_v4(),
            customer_id: Uuid::new_v4(),
            has_discount: None,
            total_price: Some(14.56),
            details: None,
        };

        assert_eq!(
            b.insert_sql().unwrap(),
            format!(
                "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
                VALUES (\
                    '{}', \
                    '{}', \
                    NULL, \
                    14.56, \
                    NULL\
                );",
                b.buy_id, b.customer_id
            )
        );
    }

    #[test]
    fn insert_with_all_values() {
        let b = Buy {
            buy_id: Uuid::new_v4(),
            customer_id: Uuid::new_v4(),
            has_discount: Some(true),
            total_price: Some(18899.9),
            details: Some("the delivery should be performed".into()),
        };

        assert_eq!(
            b.insert_sql().unwrap(),
            format!(
                "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
                    VALUES (\
                        '{}', \
                        '{}', \
                        true, \
                        18899.9, \
                        'the delivery should be performed'\
                    );",
                b.buy_id, b.customer_id
            )
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
            Buy::insert_many_sql(&buys).unwrap(),
            format!(
                "INSERT INTO buys (buy_id, customer_id, has_discount, total_price, details) \
                VALUES (\
                    '{}', \
                    '{}', \
                    NULL, \
                    14.56, \
                    NULL\
                ), (\
                    '{}', \
                    '{}', \
                    true, \
                    18899.9, \
                    'the delivery should be performed'\
                );",
                buys[0].buy_id, buys[0].customer_id, buys[1].buy_id, buys[1].customer_id
            )
        );
    }
}

#[cfg(test)]
mod tests_custom {
    use super::*;
    use crate::{types::StructType, ColumnBuilder, DbType};

    #[derive(Debug, Copy, Clone)]
    struct PointType;

    // TODO: combine those types together

    #[derive(Debug, Copy, Clone)]
    struct Point {
        x: i16,
        y: i16,
    }

    impl Point {
        fn as_tuple(self) -> (i16, i16) {
            (self.x, self.y)
        }
    }

    impl StructType for PointType {
        fn name(&self) -> String {
            "point2d".into()
        }

        fn fields(&self) -> Vec<(String, DbType)> {
            vec![("x".into(), DbType::Int16), ("y".into(), DbType::Int16)]
        }

        fn as_vec(&self, val: &dyn SqlValue) -> Option<Vec<Box<dyn std::any::Any>>> {
            let (x, y) = val.downcast_ref::<(i16, i16)>()?;
            Some(vec![Box::new(*x), Box::new(*y)])
        }

        fn as_nullable_vec(
            &self,
            val: &dyn SqlValue,
        ) -> Option<Option<Vec<Box<dyn std::any::Any>>>> {
            let value = val.downcast_ref::<Option<(i16, i16)>>()?;
            Some(value.and_then(|val| self.as_vec(&val)))
        }
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
            [
                ColumnBuilder::new("top_left", DbType::CustomStruct(Box::new(PointType))).finish(),
                ColumnBuilder::new("bottom_right", DbType::CustomStruct(Box::new(PointType)))
                    .finish(),
                ColumnBuilder::new("center", DbType::CustomStruct(Box::new(PointType)))
                    .nullable()
                    .finish(),
            ]
        }

        fn constraints() -> [CheckConstraint; 0] {
            []
        }

        fn values(&self) -> [Box<dyn SqlValue>; 3] {
            [
                Box::new(self.point_top_left.as_tuple()),
                Box::new(self.point_bottom_right.as_tuple()),
                Box::new(self.center.map(Point::as_tuple)),
            ]
        }
    }

    #[test]
    fn create_types() {
        assert_eq!(
            Image::create_types_sql().unwrap(),
            "CREATE TYPE point2d AS (\
                x smallint, \
                y smallint\
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
        let im = Image {
            point_top_left: Point { x: 5, y: 8 },
            point_bottom_right: Point { x: 215, y: 160 },
            center: None,
        };

        assert_eq!(
            im.insert_sql().unwrap(),
            format!(
                "INSERT INTO images (top_left, bottom_right, center) \
                VALUES (\
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d, \
                    NULL\
                );",
                im.point_top_left.x,
                im.point_top_left.y,
                im.point_bottom_right.x,
                im.point_bottom_right.y
            )
        );
    }

    #[test]
    fn insert_with_center() {
        let im = Image {
            point_top_left: Point { x: 5, y: 8 },
            point_bottom_right: Point { x: 215, y: 160 },
            center: Some(Point { x: 100, y: 80 }),
        };

        assert_eq!(
            im.insert_sql().unwrap(),
            format!(
                "INSERT INTO images (top_left, bottom_right, center) \
                VALUES (\
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d\
                );",
                im.point_top_left.x,
                im.point_top_left.y,
                im.point_bottom_right.x,
                im.point_bottom_right.y,
                im.center.unwrap().x,
                im.center.unwrap().y
            )
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
            Image::insert_many_sql(&images).unwrap(),
            format!(
                "INSERT INTO images (top_left, bottom_right, center) \
                VALUES (\
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d, \
                    NULL\
                ), (\
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d, \
                    ROW({}, {})::point2d\
                );",
                images[0].point_top_left.x,
                images[0].point_top_left.y,
                images[0].point_bottom_right.x,
                images[0].point_bottom_right.y,
                images[1].point_top_left.x,
                images[1].point_top_left.y,
                images[1].point_bottom_right.x,
                images[1].point_bottom_right.y,
                images[1].center.unwrap().x,
                images[1].center.unwrap().y
            )
        );
    }
}

#[cfg(test)]
mod tests_heterogeneous {
    use super::*;
    use crate::{types::StructType, ColumnBuilder, DbType};

    #[derive(Debug, Copy, Clone)]
    struct IntWithLabelType;

    // TODO: combine those types together

    #[derive(Debug)]
    struct IntWithLabel {
        val: i16,
        label: String,
    }

    impl IntWithLabel {
        fn clone_tuple(&self) -> (i16, String) {
            (self.val, self.label.clone())
        }
    }

    impl StructType for IntWithLabelType {
        fn name(&self) -> String {
            "with_label".into()
        }

        fn fields(&self) -> Vec<(String, DbType)> {
            vec![
                ("val".into(), DbType::Int16),
                ("label".into(), DbType::VarChar(None)),
            ]
        }

        fn as_vec(&self, val: &dyn SqlValue) -> Option<Vec<Box<dyn std::any::Any>>> {
            let (v, l) = val.downcast_ref::<(i16, String)>()?;
            Some(vec![Box::new(*v), Box::new(l.clone())])
        }

        fn as_nullable_vec(
            &self,
            val: &dyn SqlValue,
        ) -> Option<Option<Vec<Box<dyn std::any::Any>>>> {
            let value = val.downcast_ref::<Option<(i16, String)>>()?;
            Some(value.as_ref().and_then(|val| self.as_vec(val)))
        }
    }

    struct SingleValuedTable {
        val: IntWithLabel,
    }

    impl Table<1> for SingleValuedTable {
        fn name() -> &'static str {
            "tbl"
        }

        fn columns() -> [Column; 1] {
            [
                ColumnBuilder::new("val", DbType::CustomStruct(Box::new(IntWithLabelType)))
                    .finish(),
            ]
        }

        fn constraints() -> [CheckConstraint; 0] {
            []
        }

        fn values(&self) -> [Box<dyn SqlValue>; 1] {
            [Box::new(self.val.clone_tuple())]
        }
    }

    #[test]
    fn create_types() {
        assert_eq!(
            SingleValuedTable::create_types_sql().unwrap(),
            "CREATE TYPE with_label AS (\
                val smallint, \
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
        let x = SingleValuedTable {
            val: IntWithLabel {
                val: 0,
                label: "foo-bar".to_string(),
            },
        };

        assert_eq!(
            x.insert_sql().unwrap(),
            format!(
                "INSERT INTO tbl (val) \
                VALUES (\
                    ROW(0, 'foo-bar')::with_label\
                );",
            )
        );
    }
}
