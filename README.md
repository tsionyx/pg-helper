# Postgres Helper

Provides some useful functions to ease the CREAT-ion of tables,
INSERT-ing and SELECT-ing the rows as instances of `Table` trait:


```rust
use pg_helper::{array_type, struct_type, Column, ColumnBuilder, PgTableExtension, Table};
use postgres::{types::{FromSql, ToSql, Type}, Client, Error, Row};


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
            ColumnBuilder::new("name", Type::VARCHAR).index().finish(),
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

fn main() {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let mut client = Client::connect(&db_url, postgres::NoTls).unwrap();

    client.create_table::<Figure, 2>().unwrap();

    let fig = Figure {
        name: "trapezoid".into(),
        polygon: vec![
            Point { x: 0, y: 0 },
            Point { x: 2, y: 4 },
            Point { x: 3, y: 4 },
            Point { x: 6, y: 0 },
        ],
    };
    client.insert_row(&fig).unwrap();
    println!("Figure inserted!");

    let items: Vec<Figure> = client.select_all().unwrap();
    println!("{} figures selected!", items.len());
    assert_eq!(items[0], fig);
}
```


One of the main advantages is the automatic creation of table along with the
all the indices and even underlying types!

Also, it becomes very convenient to forget about the details of SQL ser/de,
once you fit all the requirements for your `Table` (see in the example above)
and just use the instances as always.
