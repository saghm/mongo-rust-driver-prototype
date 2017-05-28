use std::path::PathBuf;

use mongodb::{Connector, ThreadedClient};
use mongodb::db::ThreadedDatabase;

#[test]
fn ssl_connect_and_insert() {
    let mut test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_path.push("tests");
    test_path.push("ssl");

    let client = Connector::new()
        .connect_with_ssl("mongodb://127.0.0.1:27018",
                          test_path.join("ca.pem").to_str().unwrap(),
                          test_path.join("client.crt").to_str().unwrap(),
                          test_path.join("client.key").to_str().unwrap(),
                          false)
        .unwrap();
    let db = client.db("test");
    let coll = db.collection("stuff");

    let doc = doc! { "x" => 1 };

    coll.insert_one(doc, None).unwrap();
}
