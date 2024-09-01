#![allow(warnings)]

use clap::Parser;
use color_eyre::eyre;
use dialoguer::Confirm;
use futures::TryStreamExt;
use mongodb::{
    bson::{
        self, doc,
        spec::{BinarySubtype, ElementType},
        Document, RawBsonRef, RawDocument, RawDocumentBuf,
    },
    options::{ClientOptions, ServerApi, ServerApiVersion},
    Client, Cursor,
};
use pretty_assertions::Comparison;
use serde::Deserialize;
// use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Options {
    #[arg(long = "uri", help = "MongoDB connection URI")]
    pub connection_uri: String,
    #[arg(long = "database", aliases = ["db"], help = "MongoDB database name")]
    pub database_name: Option<String>,
    #[arg(long = "collection", aliases = ["col"], help = "MongoDB collection name")]
    pub collection_name: Option<String>,
    #[arg(long = "confirm", help = "Confirm changes interactively")]
    pub confirm: Option<bool>,
}

// fn fix_document_new<'a>(
//     doc: bson::RawBsonRef<'a>,
//     // new_doc: &mut RawDocumentBuf,
//     confirm: bool,
// ) -> eyre::Result<bson::RawBson> {
// }

fn fix_string(
    doc: &RawDocument,
    key: &str,
    elem: &bson::raw::RawElement,
    start: usize,
    confirm: bool,
) -> eyre::Result<String> {
    let bytes = doc.as_bytes();

    let key_start = start + 4 + 1;
    let raw_key = &bytes[key_start..key_start + key.len()];
    assert_eq!(key, String::from_utf8_lossy(raw_key).to_string());

    let value_start = key_start + key.len();
    let raw_value = &bytes[value_start + 4 + 1..value_start + elem.len()];
    let old_value_utf8 = String::from_utf8_lossy(raw_value).to_string();
    // println!("{key: >20} => {:#02x?}", raw_value);
    // println!(
    //     "{key: >20} => [utf8]{:?}",
    //     String::from_utf8_lossy(raw_value)
    // );
    let value_utf16 =
        String::from_utf16_lossy(&raw_value.into_iter().map(|v| *v as u16).collect::<Vec<_>>());
    let new_value_utf8_bytes = value_utf16.as_bytes();
    let new_value_utf8 = String::from_utf8_lossy(new_value_utf8_bytes).to_string();
    // println!("{key: >20} => [utf16]{:?}", value_utf16);
    // println!(
    //     "{key: >20} => [utf8]{:?}",
    //     String::from_utf8_lossy(value_utf8)
    // );

    let hex_id = doc
        .get_object_id("_id")
        .ok()
        .map(bson::oid::ObjectId::to_hex);

    let prompt = format!(
        "[{}][{key}] {old_value_utf8:?} => {new_value_utf8:?}",
        hex_id.as_deref().unwrap_or(""),
    );
    let prompt = format!(
        "[{}][{key}] {}",
        hex_id.as_deref().unwrap_or(""),
        Comparison::new(&old_value_utf8, &new_value_utf8)
    );
    let confirmation = if confirm {
        Confirm::new().with_prompt(&prompt).interact().unwrap()
    } else {
        true
    };

    Ok(if confirmation {
        println!("{}", &prompt);

        new_value_utf8
        // new_doc.append(key, bson::raw::RawBson::String(new_value_utf8.to_string()));
    } else {
        old_value_utf8
        // new_doc.append(key, bson::raw::RawBson::String(old_value_utf8.to_string()));
    })
}

fn fix_document(
    doc: &RawDocument,
    new_doc: &mut RawDocumentBuf,
    confirm: bool,
) -> eyre::Result<()> {
    let mut start = 0;
    for elem in doc.iter_elements() {
        let elem = elem?;
        let key = elem.key();
        let value = elem.value();

        match elem.element_type() {
            ElementType::EmbeddedDocument => {
                let subdoc = doc.get_document(key)?;
                // let mut new_subdoc = doc.get_document(key)?.to_raw_document_buf();
                let mut new_subdoc = RawDocumentBuf::new();
                fix_document(subdoc, &mut new_subdoc, confirm)?;
                new_doc.append(key, new_subdoc);
            }
            ElementType::Array => {
                // let array = doc.get_arr(key)?;
                // let array = doc.get_document(key)?;
                let array = doc.get_array(key)?;
                // let array = value?.to_raw_bson();
                // let array = RawDocument::try_from(value);
                // let mut new_array = RawDocumentBuf::new();
                let mut new_array = bson::raw::RawArrayBuf::new();
                // for elem in value?.iter() {
                // }
                // fix_document(array, &mut new_subdoc, confirm)?;
                // RawDocumentBuf::from(array);
                // let mut new_array = doc.get_array(key)?.to_raw_array_buf();
                // for idx in 0..array.len() {
                // TODO: fix array types
                // for (idx, item) in array.into_iter().enumerate() {
                for item in array.into_iter() {
                    match item? {
                        // bson::raw::RawBsonRef::String() => {
                        //     new_array.push(bson::raw::RawBson::String(fix_string(
                        //         doc, key, &elem, start, confirm,
                        //     )?));
                        // }
                        bson::raw::RawBsonRef::Document(subdoc) => {
                            let mut new_subdoc = RawDocumentBuf::new();
                            fix_document(subdoc, &mut new_subdoc, confirm)?;
                            new_array.push(new_subdoc);
                        }
                        other => {
                            new_array.push(other.to_raw_bson());
                        }
                    }
                }
                // }
                // new_array.push(fix_document_new(item?, confirm)?);
                //     fix_document(item?.to_raw_bson(), &mut new_subdoc, confirm)?;
                // }

                new_doc.append(key, new_array);
            }
            ElementType::String => {
                if let Err(bson::raw::Error {
                    kind: bson::raw::ErrorKind::Utf8EncodingError(err),
                    ..
                }) = value
                {
                    new_doc.append(
                        key,
                        bson::raw::RawBson::String(fix_string(doc, key, &elem, start, confirm)?),
                    );
                } else {
                    new_doc.append(key, value?.to_raw_bson());
                }
            }
            other => {
                new_doc.append(key, value?.to_raw_bson());
            }
        }
        start += 1 + key.len() + 1 + elem.len();
    }
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let options = Options::parse();

    println!("{:?}", options);

    let client = Client::with_uri_str(&options.connection_uri).await?;

    // Send a ping to confirm a successful connection
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await?;
    println!("connected to {}", options.connection_uri);

    let Some(database_name) = options.database_name else {
        println!("no database specified");
        return Ok(());
    };

    let Some(collection_name) = options.collection_name else {
        println!("no collection specified");
        return Ok(());
    };

    let db = client.database(&database_name);
    let collection = db.collection::<RawDocumentBuf>(&collection_name);

    let mut cursor = collection.find(doc! {}).await?;
    while let Some(raw_doc) = cursor.try_next().await? {
        let mut new_raw_doc = bson::raw::RawDocumentBuf::new();
        // let mut new_raw_doc = raw_doc.clone();

        println!(
            "{}",
            raw_doc
                .get_object_id("_id")
                .ok()
                .map(bson::oid::ObjectId::to_hex)
                .as_deref()
                .unwrap_or("")
        );

        // todo: compute diff
        fix_document(
            &*raw_doc,
            &mut new_raw_doc,
            options.confirm.unwrap_or(false),
        )?;

        let doc = raw_doc.to_document();
        let fixed_doc = new_raw_doc.to_document();

        match (doc, fixed_doc) {
            (Ok(doc), Ok(fixed_doc)) => {
                if doc != fixed_doc {
                    print!("{}", Comparison::new(&doc, &fixed_doc));
                }
            }
            (Err(doc), Ok(fixed_doc)) => {
                // fine
            }
            (doc, fixed_doc) => {
                println!("{:?}", doc);
                println!("{:?}", fixed_doc);
            }
        }

        // continue;
        // for kvp in &raw_doc {
        //     match &kvp {
        //         Ok((k, v)) => {
        //             // println!("k: {:?} v: {:?}", k, v);
        //         }
        //         Err(bson::raw::Error {
        //             kind: bson::raw::ErrorKind::Utf8EncodingError(err),
        //             ..
        //         }) => {
        //             println!("err: {:?}", err);
        //         }
        //         Err(err) => {
        //             println!("other error: {:?}", err);
        //         }
        //     }
        //     // println!("{:?}", kvp); // prints Ok(RawBsonRef) or Err(...)
        // }
        // println!("{:?}", doc.des);
        // println!("{:?}", doc);
    }
    // println!("db: {:?}", db);
    Ok(())
}
