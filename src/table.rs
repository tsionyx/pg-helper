use std::fmt::Write as _;

use itertools::Itertools as _;

use crate::{
    column::{self, Column},
    constraint::CheckConstraint,
    value::SqlValue,
};

pub trait Table<const N: usize, const CONSTRAINTS_N: usize = 0> {
    fn name() -> &'static str;

    fn columns() -> [Column; N];

    fn constraints() -> [CheckConstraint; CONSTRAINTS_N];

    fn create_table_sql() -> String {
        let columns: String = Self::columns().iter().map(|col| col.to_string()).join(",");
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

    fn values(&self) -> [Box<dyn SqlValue>; N];

    fn insert_sql(&self) -> Result<String, column::Error<Box<dyn SqlValue>>> {
        let columns = Self::columns();
        let columns_names = columns.iter().map(|c| c.name()).join(",");

        let values = self.values();
        let values: Result<Vec<_>, _> = columns
            .iter()
            .zip(values)
            .map(|(col, val)| col.escape_val(val.as_ref()))
            .collect();

        let values = values?.join(",");

        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({});",
            Self::name(),
            columns_names,
            values,
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
    fn create_table() {
        assert_eq!(
            Buy::create_table_sql(),
            "CREATE TABLE IF NOT EXISTS buys (\
                buy_id uuid NOT NULL UNIQUE PRIMARY KEY,\
                customer_id uuid NOT NULL REFERENCES users(user_id),\
                has_discount boolean NULL,\
                total_price real NULL,\
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
                "INSERT INTO buys (buy_id,customer_id,has_discount,total_price,details) \
                VALUES (\
                    '{}',\
                    '{}',\
                    NULL,\
                    14.56,\
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
                "INSERT INTO buys (buy_id,customer_id,has_discount,total_price,details) \
                    VALUES (\
                        '{}',\
                        '{}',\
                        true,\
                        18899.9,\
                        'the delivery should be performed'\
                    );",
                b.buy_id, b.customer_id
            )
        );
    }
}
