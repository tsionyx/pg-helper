use pg_helper::{array_type, gen_table, struct_type, PgTableExtension};
use postgres::{
    types::{FromSql, ToSql, Type},
    Client,
};

#[derive(Debug, Copy, Clone, PartialEq, ToSql, FromSql)]
#[postgres(name = "point2d")]
struct Point {
    x: i16,
    y: i16,
}

gen_table!(
    #[derive(Debug, PartialEq)]
    struct Figure("figures") {
        name: String = Type::VARCHAR; [index()],
        polygon: Vec<Point> = array_type(struct_type("point2d", &[("x", Type::INT2), ("y", Type::INT2)])),
    }
);

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
