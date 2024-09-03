mod provider;
pub use provider::Provider;

pub mod sentinel2level2a {
    use super::manifest::{DataObject, Manifest};
    use crate::download_plan::{DownloadPlan, DownloadTask};
    use crate::image_selection::{ImageSelection, Product};
    use crate::s3::S3ObjOps;
    use anyhow::{anyhow, Result};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use toml;

    fn image_selection_toml() -> toml::Table {
        toml::toml! {
            id = "copernicus.sentinel2level2a"

            provider = "Copernicus"

            name = "Sentinel-2 Level 2A Surface Reflectance"

            description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
            derived from the associated Level-1C products. The atmospheric correction of\n\
            Sentinel-2 images includes the correction of the scattering of air molecules\n\
            (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
            in particular ozone, oxygen and water vapour and the correction of absorption and\n\
            scattering due to aerosol particles. Level 2A product are considered an ARD product."

            // Select 'Further details about the data collection' to view a descrition of the bands
            docs = "https://documentation.dataspace.copernicus.eu/Data/SentinelMissions/Sentinel2.html#sentinel-2-level-2a-surface-reflectance"

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
        }
    }

    fn filter_data_objects(
        products_to_download: &[Product],
        data_objects: &[DataObject],
    ) -> Result<Vec<DataObject>> {
        // Create a HashMap for faster lookup
        let data_object_map: HashMap<_, _> =
            data_objects.iter().map(|obj| (&obj.id, obj)).collect();

        products_to_download
            .iter()
            .map(|product| {
                data_object_map
                    .iter()
                    // The Product.id is a substring of the corresponding DataObject.id
                    .find(|(&id, _)| id.contains(&product.id))
                    .map(|(_, &obj)| obj.clone())
                    .ok_or_else(|| {
                        anyhow!(
                            "No corresponding DataObject found in Manifest for Product with id: {}",
                            product.id
                        )
                    })
            })
            .collect::<Result<Vec<_>>>() // Collect into Result<Vec<DataObject>>
    }

    pub async fn generate_download_plan(
        provider: &impl S3ObjOps,
        selection: &ImageSelection,
        output_dir: PathBuf,
    ) -> Result<DownloadPlan> {
        let ids_to_download = selection
            .ids_to_download()
            .ok_or(anyhow!("No ids to download"))?;
        let products_to_download = selection
            .products_to_download()
            .ok_or(anyhow!("No products selected for download"))?;

        let mut tasks: Vec<DownloadTask> = vec![];

        for id in ids_to_download {
            let manifest = Manifest::fetch(provider, &id).await?;
            let data_objects = manifest.parse()?;
            let filtered_data_objects = filter_data_objects(&products_to_download, &data_objects)?;

            // Create a DownloadTask for each filtered_data_object
            for data_obj in filtered_data_objects {
                let bucket = manifest.bucket.as_str();

                let prefix = manifest.prefix.as_str();
                let key = format!("{}/{}", prefix, data_obj.relative_href);

                let file_name = Path::new(&key).file_name().unwrap();
                let output = output_dir.join(&id).join(file_name);

                let task = DownloadTask::new(bucket, &key, output.to_str().unwrap());
                tasks.push(task)
            }
        }
        Ok(DownloadPlan::new(tasks))
    }

    #[cfg(test)]
    mod tests {
        use crate::copernicus::Provider;
        use super::*;
        use crate::s3;

        const TEST_OUTPUT_DIR: &str = "/tmp/slow-stac-test";
        #[tokio::test]
        async fn test_generate_download_plan() {
            let client = s3::client_from_profile("copernicus").await;
            let provider = Provider::new(client);
            let selection = ImageSelection::from_template(&image_selection_toml());
            let output_dir = PathBuf::from(TEST_OUTPUT_DIR);
            let download_plan = generate_download_plan(&provider, &selection, output_dir)
                .await
                .unwrap();
            let path = PathBuf::from(TEST_OUTPUT_DIR).join("download_plan.json");
            download_plan.write(&path).unwrap();
            assert_eq!(path.exists(), true);
        }
    }
}

mod manifest {
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
        pub async fn fetch(provider: &impl S3ObjOps, id: &str) -> Result<Self> {
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
}
