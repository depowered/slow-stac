use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use roxmltree::Node;
use stac::{Item, ItemCollection};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MapError {
    #[error("Unable to clone request")]
    Clone,
}

pub enum Sentinel2Level2A {
    B01,
    B02,
    B03,
    B04,
    B05,
    B06,
    B07,
    B08,
    B09,
    B10,
    B11,
    B12,
    SCL,
    SNW,
    CLD,
    TCI,
}

pub async fn try_download(
    client: Client,
    id: &str,
    product: Sentinel2Level2A,
    output_dir: PathBuf,
) -> Result<PathBuf> {
    // Retrieve the associated STAC item
    println!("Retrieve STAC item");
    let item = fetch_stac_item(id).await?;

    println!("Extract S3 directory");
    let s3_dir =
        extract_s3_dir(&item).ok_or(anyhow!("Failed to extract S3Directory from STAC item"))?;
    println!("{s3_dir:#?}");

    // Retrieve and parse the associated manifest
    println!("Fetch manifest");
    let manifest = fetch_manifest(&client, &s3_dir).await?;

    println!("Parse manifest");
    let data_objects = parse_manifest(&manifest)?;

    println!("{data_objects:#?}");

    Ok(PathBuf::new())
}

pub async fn get_s3_client() -> Client {
    let url = "https://eodata.dataspace.copernicus.eu";

    let base_config = aws_config::from_env()
        .profile_name("copernicus")
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .endpoint_url(url)
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

async fn search_stac_item(id: &str) -> Result<Item> {
    const SEARCH_API: &str = "https://catalogue.dataspace.copernicus.eu/stac/search";
    const COLLECTION: &str = "SENTINEL-2";

    let item_collection: ItemCollection = reqwest::Client::new()
        .post(SEARCH_API)
        .json(&serde_json::json!({"collections": vec![COLLECTION], "ids": vec![id]}))
        .send()
        .await?
        .json()
        .await?;

    let item = item_collection.items[0].clone();

    Ok(item)
}

async fn fetch_stac_item(id: &str) -> Result<Item> {
    let url = format!(
        "https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-2/items/{id}",
    );
    let item = reqwest::get(url).await?.json::<Item>().await?;
    Ok(item)
}

#[derive(Debug)]
struct S3Directory {
    bucket: String,
    key: String,
}

fn extract_s3_dir(item: &Item) -> Option<S3Directory> {
    let s3_dir = item
        .assets
        .get("PRODUCT")?
        .additional_fields
        .get("alternate")?
        .get("s3")?
        .get("href")?
        .as_str()?;

    let parts: Vec<&str> = s3_dir.split("/").collect();
    let bucket = parts[1].to_owned();
    let key = parts[2..].join("/");

    Some(S3Directory { bucket, key })
}

fn strip_x_id_get_object_param_from_uri(
    req: HttpRequest,
) -> std::result::Result<HttpRequest, MapError> {
    let mut r = req.try_clone().ok_or(MapError::Clone)?;
    let _ = r.set_uri(r.uri().replace("x-id=GetObject", ""));
    Ok(r)
}

async fn fetch_manifest(client: &Client, s3_dir: &S3Directory) -> Result<String> {
    let object = client
        .get_object()
        .bucket(&s3_dir.bucket)
        .key(format!("{}/manifest.safe", &s3_dir.key))
        .customize()
        .map_request(strip_x_id_get_object_param_from_uri)
        .send()
        .await?;

    let data = object.body.collect().await?.to_vec();
    let string = String::from_utf8(data)?;

    Ok(string)
}

#[derive(Debug, PartialEq, Eq)]
struct DataObjectInfo {
    id: String,
    filesize: u64,
    relative_href: String,
    checksum_algorithm: String,
    checksum: String,
}

fn parse_manifest(manifest: &str) -> Result<Vec<DataObjectInfo>> {
    let mut data_objects: Vec<DataObjectInfo> = vec![];
    let doc = roxmltree::Document::parse(manifest)?;

    let data_object_section = doc
        .descendants()
        .filter(|n| n.has_tag_name("dataObjectSection"))
        .next()
        .ok_or(anyhow!("Unable to locate 'dataObjectSection' tag"))?;

    for data_object in data_object_section.children() {
        if let Some(info) = parse_data_object(data_object) {
            data_objects.push(info);
        }
    }
    Ok(data_objects)
}

fn extract_id(data_object: Node) -> Option<String> {
    Some(data_object.attribute("ID")?.to_string())
}

fn extract_filesize(data_object: Node) -> Option<u64> {
    let byte_stream = data_object
        .children()
        .filter(|n| n.has_tag_name("byteStream"))
        .next()?;
    let filesize: u64 = byte_stream.attribute("size")?.parse().ok()?;
    Some(filesize)
}

fn extract_relative_href(data_object: Node) -> Option<String> {
    let file_location = data_object
        .descendants()
        .filter(|n| n.has_tag_name("fileLocation"))
        .next()?;
    let relative_href = file_location.attribute("href")?.to_string();
    Some(relative_href)
}

fn extract_checksum_algorithm(data_object: Node) -> Option<String> {
    let checksum = data_object
        .descendants()
        .filter(|n| n.has_tag_name("checksum"))
        .next()?;
    let checksum_algorithm = checksum.attribute("checksumName")?.to_string();
    Some(checksum_algorithm)
}

fn extract_checksum(data_object: Node) -> Option<String> {
    let checksum = data_object
        .descendants()
        .filter(|n| n.has_tag_name("checksum"))
        .next()?;
    let checksum = checksum.text()?.to_string();
    Some(checksum)
}

fn parse_data_object(data_object: Node) -> Option<DataObjectInfo> {
    let id = extract_id(data_object)?;
    let filesize = extract_filesize(data_object)?;
    let relative_href = extract_relative_href(data_object)?;
    let checksum_algorithm = extract_checksum_algorithm(data_object)?;
    let checksum = extract_checksum(data_object)?;

    Some(DataObjectInfo {
        id,
        filesize,
        relative_href,
        checksum_algorithm,
        checksum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA_OBJECT_CONTENT: &str = r#"
<dataObject ID="IMG_DATA_Band_TCI_10m_Tile1_Data">
    <byteStream mimeType="application/octet-stream" size="135297067">
        <fileLocation href="./GRANULE/L2A_T08VPH_A046314_20240504T195929/IMG_DATA/R10m/T08VPH_20240504T195901_TCI_10m.jp2" locatorType="URL"/>
        <checksum checksumName="SHA3-256">D54088291C554975B475E31D078EEEEBDB31CB9AFF959AD18D642EBDDD20F623</checksum>
    </byteStream>
</dataObject>"#;

    #[test]
    fn test_extract_data_object_id() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let data_object = doc.root_element();

        assert_eq!(
            extract_id(data_object),
            Some("IMG_DATA_Band_TCI_10m_Tile1_Data".to_string())
        );
    }

    #[test]
    fn test_extract_data_object_filesize() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let data_object = doc.root_element();

        assert_eq!(extract_filesize(data_object), Some(135297067_u64));
    }

    #[test]
    fn test_extract_data_object_relative_href() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let data_object = doc.root_element();

        assert_eq!(extract_relative_href(data_object), Some("./GRANULE/L2A_T08VPH_A046314_20240504T195929/IMG_DATA/R10m/T08VPH_20240504T195901_TCI_10m.jp2".to_string()));
    }

    #[test]
    fn test_extract_data_object_checksum_algorithm() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let data_object = doc.root_element();

        assert_eq!(
            extract_checksum_algorithm(data_object),
            Some("SHA3-256".to_string())
        );
    }

    #[test]
    fn test_extract_data_object_checksum() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let data_object = doc.root_element();

        assert_eq!(
            extract_checksum(data_object),
            Some("D54088291C554975B475E31D078EEEEBDB31CB9AFF959AD18D642EBDDD20F623".to_string())
        );
    }

    #[test]
    fn test_parse_data_object() {
        let doc = roxmltree::Document::parse(DATA_OBJECT_CONTENT).unwrap();
        let node = doc.root_element();

        let expected = DataObjectInfo {
            id: "IMG_DATA_Band_TCI_10m_Tile1_Data".to_string(),
            filesize: 135297067_u64,
            relative_href: "./GRANULE/L2A_T08VPH_A046314_20240504T195929/IMG_DATA/R10m/T08VPH_20240504T195901_TCI_10m.jp2".to_string(),
            checksum_algorithm: "SHA3-256".to_string(),
            checksum: "D54088291C554975B475E31D078EEEEBDB31CB9AFF959AD18D642EBDDD20F623".to_string(),
        };

        assert_eq!(parse_data_object(node).unwrap(), expected);
    }
}
