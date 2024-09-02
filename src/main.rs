use clap::Parser;
use color_eyre::eyre;
use dialoguer::Confirm;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use mongodb::{bson, Client};
use pretty_assertions::Comparison;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Options {
    #[arg(long = "uri", help = "MongoDB connection URI")]
    pub connection_uri: String,
    #[arg(long = "database", aliases = ["db"], help = "MongoDB database name")]
    pub database_name: Option<String>,
    #[arg(long = "collection", help = "MongoDB collection names")]
    pub collection_names: Vec<String>,
    #[arg(long = "confirm", help = "Confirm changes interactively")]
    pub confirm: Option<bool>,
    #[arg(
        long = "dry-run",
        default_value = "false",
        help = "Run in dry run mode"
    )]
    pub dry_run: bool,
}

fn fix_string(
    doc: &bson::RawDocument,
    key: &str,
    elem: &bson::raw::RawElement,
    start: usize,
    confirm: bool,
) -> eyre::Result<(bool, String)> {
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

    // let prompt = format!(
    //     "[{}][{key}] {old_value_utf8:?} => {new_value_utf8:?}",
    //     hex_id.as_deref().unwrap_or(""),
    // );
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
        (true, new_value_utf8)
    } else {
        (false, old_value_utf8)
    })
}

fn fix_document(
    doc: &bson::RawDocument,
    new_doc: &mut bson::RawDocumentBuf,
    confirm: bool,
) -> eyre::Result<bool> {
    let mut changed = false;
    let mut start = 0;
    for elem in doc.iter_elements() {
        let elem = elem?;
        let key = elem.key();
        let value = elem.value();

        match elem.element_type() {
            bson::spec::ElementType::EmbeddedDocument => {
                let subdoc = doc.get_document(key)?;
                let mut new_subdoc = bson::RawDocumentBuf::new();
                fix_document(subdoc, &mut new_subdoc, confirm)?;
                new_doc.append(key, new_subdoc);
            }
            bson::spec::ElementType::Array => {
                let array = doc.get_array(key)?;
                let mut new_array = bson::raw::RawArrayBuf::new();
                for item in array.into_iter() {
                    match item? {
                        bson::raw::RawBsonRef::Document(subdoc) => {
                            let mut new_subdoc = bson::RawDocumentBuf::new();
                            fix_document(subdoc, &mut new_subdoc, confirm)?;
                            new_array.push(new_subdoc);
                        }
                        bson::raw::RawBsonRef::String(value) => {
                            // this is not good enough yet
                            new_array.push(bson::RawBson::String(
                                String::from_utf8_lossy(value.as_bytes()).to_string(),
                            ));
                        }
                        other => {
                            new_array.push(other.to_raw_bson());
                        }
                    }
                }
                new_doc.append(key, new_array);
            }
            bson::spec::ElementType::String => {
                if let Err(bson::raw::Error {
                    kind: bson::raw::ErrorKind::Utf8EncodingError(_err),
                    ..
                }) = value
                {
                    let (fixed, value) = fix_string(doc, key, &elem, start, confirm)?;
                    new_doc.append(key, bson::raw::RawBson::String(value));
                    if fixed {
                        changed = true;
                    }
                } else {
                    new_doc.append(key, value?.to_raw_bson());
                }
            }
            _other => {
                new_doc.append(key, value?.to_raw_bson());
            }
        }
        start += 1 + key.len() + 1 + elem.len();
    }
    Ok(changed)
}

async fn fix_collection(
    collection: mongodb::Collection<bson::RawDocumentBuf>,
    confirm: bool,
    dry_run: bool,
) -> eyre::Result<()> {
    let mut cursor = collection.find(bson::doc! {}).await?;
    while let Some(raw_doc) = cursor.try_next().await? {
        let mut new_raw_doc = bson::raw::RawDocumentBuf::new();

        let id = raw_doc.get_object_id("_id").ok();

        println!(
            "collection = {: <20} id = {: <30}",
            collection.name(),
            id.map(bson::oid::ObjectId::to_hex).as_deref().unwrap_or("")
        );

        let changed = fix_document(&*raw_doc, &mut new_raw_doc, confirm)?;

        let doc = raw_doc.to_document();
        let fixed_doc = new_raw_doc.clone().to_document();

        match (&doc, &fixed_doc) {
            (Ok(doc), Ok(fixed_doc)) => {
                // print!("{}", Comparison::new(&doc, &fixed_doc));
                if doc != fixed_doc {
                    print!("{}", Comparison::new(&doc, &fixed_doc));
                }
            }
            (Err(_doc), Ok(_fixed_doc)) => {
                // fine
            }
            (doc, fixed_doc) => {
                println!("{:?}", doc);
                println!("{:?}", fixed_doc);
            }
        }

        if !dry_run && changed {
            // replace the document
            if let Ok(id) = raw_doc.get_object_id("_id") {
                collection
                    .find_one_and_replace(bson::doc! {"_id": id}, new_raw_doc)
                    .await?;
                println!(
                    "collection = {: <20} id = {: <30} REPLACED",
                    collection.name(),
                    id.to_hex()
                );
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let options = Options::parse();

    let client = Client::with_uri_str(&options.connection_uri).await?;

    // Send a ping to confirm a successful connection
    client
        .database("admin")
        .run_command(bson::doc! { "ping": 1 })
        .await?;
    println!("connected to {}", options.connection_uri);

    let Some(database_name) = options.database_name else {
        eprintln!("no database specified");
        return Ok(());
    };

    let db = client.database(&database_name);

    let collection_names: Vec<String> = if !options.collection_names.is_empty() {
        options.collection_names
    } else {
        db.list_collection_names().await?
    };

    let confirm = options.confirm.unwrap_or(false);

    stream::iter(collection_names.into_iter())
        .map(|col| {
            let db_clone = db.clone();
            async move {
                let collection = db_clone.collection::<bson::RawDocumentBuf>(&col);
                fix_collection(collection, confirm, options.dry_run).await
            }
        })
        .buffered(1)
        .collect::<Vec<_>>()
        .await;

    Ok(())
}
