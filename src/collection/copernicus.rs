pub mod sentinel2level2 {
    use super::manifest;
    use crate::download_plan::DownloadPlan;
    use crate::{Client, CollectionKind};
    use crate::{DownloadTask, Result};
    use crate::{Error, ImageSelection};
    use std::path::{Path, PathBuf};

    pub async fn get_stac_item(client: &Client, id: &str) -> Result<stac::Item> {
        let url = format!(
            "https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-2/items/{id}"
        );
        let item = client.web_get(&url).await?.json::<stac::Item>().await?;
        Ok(item)
    }

    pub async fn create_download_plan(
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: PathBuf,
    ) -> Result<DownloadPlan> {
        let ids_to_download = image_selection
            .ids_to_download()
            .ok_or(Error::NoIdsToDownload)?;
        let products_to_download = image_selection
            .products_to_download()
            .ok_or(Error::NoProductsSelected)?;

        let mut tasks: Vec<DownloadTask> = vec![];

        for id in ids_to_download {
            let item = get_stac_item(client, &id).await?;

            // fetch manifest
            let (bucket, prefix) = manifest::extract_bucket_and_prefix(&item)
                .ok_or(Error::S3UrlParseError(String::from("")))?;
            let manifest_key = format!("{}/{}", prefix, "manifest.xml");

            let manifest_content = client
                .s3_get_object(&bucket, &manifest_key, None)
                .await?
                .body
                .collect()
                .await?
                .to_vec();

            let manifest = manifest::Manifest{
                bucket,
                prefix,
                content: String::from_utf8(manifest_content)?,
            };
            let data_objects = manifest.parse()?;

            let filtered_data_objects = manifest::filter_data_objects(&products_to_download, &data_objects)?;

            // Create a DownloadTask for each filtered_data_object
            for data_obj in filtered_data_objects {
                let key = format!("{}/{}", &manifest.prefix, data_obj.relative_href);

                let file_name = Path::new(&key).file_name().unwrap();
                let output = output_dir.join(&id).join(file_name);

                let task = DownloadTask {bucket: manifest.bucket.clone(), key, output };
                tasks.push(task);
            }
        }

        Ok(DownloadPlan {
            kind: CollectionKind::CopernicusSentinel2Level2A,
            tasks,
        })
    }

    pub fn image_selection_template() -> ImageSelection {
        let kind: String = CollectionKind::CopernicusSentinel2Level2A.into();
        ImageSelection::from_template(&toml::toml! {
            collection_kind = kind

            name = "Sentinel-2 Level 2A Surface Reflectance"

            description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
            derived from the associated Level-1C products. The atmospheric correction of\n\
            Sentinel-2 images includes the correction of the scattering of air molecules\n\
            (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
            in particular ozone, oxygen and water vapour and the correction of absorption and\n\
            scattering due to aerosol particles. Level 2A product are considered an ARD product."

            web_app = "https://browser.dataspace.copernicus.eu/?zoom=10&lat=-77.83027&lng=166.79993"

            ids_to_download = [
                "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
                "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
                "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
                "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
            ]

            [[products]]
            id = "B02_10m"
            name = "Red"
            download = false

            [[products]]
            id = "B03_10m"
            name = "Green"
            download = false

            [[products]]
            id = "B04_10m"
            name = "Blue"
            download = false

            [[products]]
            id = "B08_10m"
            name = "NIR"
            download = false

            [[products]]
            id = "TCI_10m"
            name = "True Color"
            download = true
        }).expect("Compiler will catch any syntax errors")
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_image_selection_template_deserializes() {
            let _template = crate::element84::sentinel2level2::image_selection_template();
        }
    }
}

mod manifest {
    use std::collections::HashMap;
    use roxmltree::Node;
    use stac::Item;
    use crate::image_selection::Product;
    use crate::{Error, Result};

    pub struct Manifest {
        pub bucket: String,
        pub prefix: String,
        pub content: String,
    }

    impl Manifest {
        pub fn parse(&self) -> Result<Vec<DataObject>> {
            let mut data_objects: Vec<DataObject> = vec![];
            let doc = roxmltree::Document::parse(&self.content)?;

            let data_object_section = doc
                .descendants().find(|n| n.has_tag_name("dataObjectSection"))
                .ok_or(Error::ManifestError)?;

            for data_object in data_object_section.children() {
                if let Some(d) = DataObject::new(data_object) {
                    data_objects.push(d);
                }
            }
            Ok(data_objects)
        }
    }

    pub fn extract_bucket_and_prefix(item: &Item) -> Option<(String, String)> {
        let s3_dir = item
            .assets
            .get("PRODUCT")?
            .additional_fields
            .get("alternate")?
            .get("s3")?
            .get("href")?
            .as_str()?;

        let parts: Vec<&str> = s3_dir.split('/').collect();
        let bucket = parts[1].to_owned();
        let prefix = parts[2..].join("/");

        Some((bucket, prefix))
    }

    pub fn filter_data_objects(
        products_to_download: &[Product],
        data_objects: &[DataObject],
    ) -> Result<Vec<DataObject>> {
        // Create a HashMap for faster lookup
        let data_object_map: HashMap<_, _> = data_objects.iter().map(|obj| (&obj.id, obj)).collect();

        products_to_download
            .iter()
            .map(|product| {
                data_object_map
                    .iter()
                    // The Product.id is a substring of the corresponding DataObject.id
                    .find(|(&id, _)| id.contains(&product.id))
                    .map(|(_, &obj)| obj.clone())
                    .ok_or(Error::ManifestError)
            })
            .collect::<Result<Vec<_>>>() // Collect into Result<Vec<DataObject>>
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
                .children().find(|n| n.has_tag_name("byteStream"))?;
            let filesize: u64 = byte_stream.attribute("size")?.parse().ok()?;
            Some(filesize)
        }

        fn extract_relative_href(data_object: Node) -> Option<String> {
            let file_location = data_object
                .descendants().find(|n| n.has_tag_name("fileLocation"))?;
            let relative_href = file_location
                .attribute("href")?
                .strip_prefix("./")?
                .to_string();
            Some(relative_href)
        }

        fn extract_checksum_algorithm(data_object: Node) -> Option<String> {
            let checksum = data_object
                .descendants().find(|n| n.has_tag_name("checksum"))?;
            let checksum_algorithm = checksum.attribute("checksumName")?.to_string();
            Some(checksum_algorithm)
        }

        fn extract_checksum(data_object: Node) -> Option<String> {
            let checksum = data_object
                .descendants().find(|n| n.has_tag_name("checksum"))?;
            let checksum = checksum.text()?.to_string();
            Some(checksum)
        }
    }
}
