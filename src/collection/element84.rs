pub mod sentinel2level2 {
    use crate::util;
    use crate::ImageSelection;
    use crate::Result;
    use crate::{Client, CollectionKind};
    use crate::{DownloadPlan, DownloadTask, Error};
    use std::path::Path;

    pub async fn get_stac_item(client: &Client, id: &str) -> Result<stac::Item> {
        let url = format!(
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/{id}"
        );
        let item = client.web_get(&url).await?.json::<stac::Item>().await?;
        Ok(item)
    }
    pub async fn create_download_plan(
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
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

            for product in &products_to_download {
                let asset = item.assets.get(&product.id).ok_or(Error::NoMatchingAsset)?;
                let bucket = util::extract_s3_bucket(&asset.href)
                    .ok_or(Error::S3UrlParseError(asset.href.clone()))?;
                let key = util::extract_s3_key(&asset.href)
                    .ok_or(Error::S3UrlParseError(asset.href.clone()))?;
                let name = Path::new(&key).file_name().unwrap();
                let output = output_dir.join(&id).join(name);

                let task = DownloadTask {
                    bucket,
                    key,
                    output,
                };
                tasks.push(task)
            }
        }
        Ok(DownloadPlan {
            kind: CollectionKind::CopernicusSentinel2Level2A,
            tasks,
        })
    }

    pub fn image_selection_template() -> ImageSelection {
        let kind: String = CollectionKind::Element84Sentinel2Level2A.into();
        ImageSelection::from_template(&toml::toml! {
            collection_kind = kind

            name = "Sentinel-2 Collection 1 Level 2A Surface Reflectance"

            description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
            derived from the associated Level-1C products. The atmospheric correction of\n\
            Sentinel-2 images includes the correction of the scattering of air molecules\n\
            (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
            in particular ozone, oxygen and water vapour and the correction of absorption and\n\
            scattering due to aerosol particles. Level 2A product are considered an ARD product."

            web_app = "https://console.earth-search.aws.element84.com/"

            ids_to_download = [
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
            ]

            [[products]]
            id = "red"
            name = "Red"
            download = false

            [[products]]
            id = "green"
            name = "Green"
            download = false

            [[products]]
            id = "blue"
            name = "Blue"
            download = false

            [[products]]
            id = "nir"
            name = "NIR"
            download = false

            [[products]]
            id = "visual"
            name = "True Color"
            download = true
        }).expect("Toml syntax error")
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_image_selection_template_deserializes() {
            let _template = image_selection_template();
        }
    }
}
