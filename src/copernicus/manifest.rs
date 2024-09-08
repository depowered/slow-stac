use crate::s3::S3ObjOps;
use anyhow::{anyhow, Result};
use roxmltree::Node;
use stac::Item;

pub struct Manifest {
    pub bucket: String,
    pub prefix: String,
    content: String,
}

impl Manifest {
    pub async fn fetch(provider: &impl S3ObjOps, id: &str) -> anyhow::Result<Self> {
        // Get the STAC Item corresponding to the provided id
        let url = format!(
            "https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-2/items/{id}",
        );
        let item = reqwest::get(url).await?.json::<Item>().await?;

        // Extract the bucket and directory key from the STAC Item
        let (bucket, prefix) = extract_bucket_and_prefix(&item)
            .ok_or(anyhow!("Error extracting bucket and directory key"))?;

        let key = format!("{}/manifest.safe", &prefix);

        let object = provider.get_object(&bucket, &key).await?;

        let data = object.body.collect().await?.to_vec();
        let content = String::from_utf8(data)?;

        Ok(Manifest {
            bucket,
            prefix,
            content,
        })
    }

    pub fn parse(self: &Self) -> Result<Vec<DataObject>> {
        let mut data_objects: Vec<DataObject> = vec![];
        let doc = roxmltree::Document::parse(&self.content)?;

        let data_object_section = doc
            .descendants()
            .filter(|n| n.has_tag_name("dataObjectSection"))
            .next()
            .ok_or(anyhow!("Unable to locate 'dataObjectSection' tag"))?;

        for data_object in data_object_section.children() {
            if let Some(d) = DataObject::new(data_object) {
                data_objects.push(d);
            }
        }
        Ok(data_objects)
    }
}

fn extract_bucket_and_prefix(item: &Item) -> Option<(String, String)> {
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
    let prefix = parts[2..].join("/");

    Some((bucket, prefix))
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DataObject {
    pub id: String,
    pub filesize: u64,
    pub relative_href: String,
    pub checksum_algorithm: String,
    pub checksum: String,
}

impl DataObject {
    fn new(data_object: Node) -> Option<Self> {
        let id = Self::extract_id(data_object)?;
        let filesize = Self::extract_filesize(data_object)?;
        let relative_href = Self::extract_relative_href(data_object)?;
        let checksum_algorithm = Self::extract_checksum_algorithm(data_object)?;
        let checksum = Self::extract_checksum(data_object)?;

        Some(Self {
            id,
            filesize,
            relative_href,
            checksum_algorithm,
            checksum,
        })
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
        let relative_href = file_location
            .attribute("href")?
            .strip_prefix("./")?
            .to_string();
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
}
