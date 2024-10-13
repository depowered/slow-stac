use crate::adapter::{Adapter, AdapterKind};
use crate::{util, Client, DownloadPlan, DownloadTask, Error, ImageSelection};
use std::path::Path;

pub struct Sentinel2Level2A;

impl Adapter for Sentinel2Level2A {
    fn adapter_name(_kind: AdapterKind) -> String {
        String::from("Element84Sentinel2Level2A")
    }

    fn image_selection_template(kind: AdapterKind) -> ImageSelection {
        let adapter_kind: String = kind.into();
        ImageSelection::from_template(&toml::toml! {
            adapter_kind = adapter_kind

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

    async fn create_client(_kind: AdapterKind, profile: Option<String>) -> crate::Result<Client> {
        let builder = match profile {
            Some(p) => Client::builder().with_aws_profile(&p),
            None => Client::builder().without_credentials(),
        };
        let client = builder.set_region("us-west-2").build().await?;
        Ok(client)
    }

    async fn get_stac_item(
        _kind: AdapterKind,
        client: &Client,
        id: &str,
    ) -> crate::Result<stac::Item> {
        let url = format!(
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/{id}"
        );
        let item = client.web_get(&url).await?.json::<stac::Item>().await?;
        Ok(item)
    }

    async fn create_download_plan(
        kind: AdapterKind,
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
    ) -> crate::Result<DownloadPlan> {
        let ids_to_download = image_selection
            .ids_to_download()
            .ok_or(Error::NoIdsToDownload)?;
        let products_to_download = image_selection
            .products_to_download()
            .ok_or(Error::NoProductsSelected)?;

        let mut tasks: Vec<DownloadTask> = vec![];

        for id in ids_to_download {
            let item = Sentinel2Level2A::get_stac_item(kind, client, &id).await?;

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
            kind: AdapterKind::Element84Sentinel2Level2A,
            tasks,
        })
    }
}
