use crate::download_plan::{DownloadPlan, DownloadTask};
use crate::image_selection::{ImageSelection, Product};
use anyhow::{anyhow, Result};
use regex::Regex;
use stac::{Asset, Item};
use std::path::{Path, PathBuf};
use toml;

const COLLECTION_ID: &str = "sentinel-2-c1-l2a";

#[allow(dead_code)]
pub fn image_selection_toml() -> toml::Table {
    toml::toml! {
        id = "element84.sentinel2collection1level2a"

        provider = "Element84"

        name = "Sentinel-2 Collection 1 Level 2A Surface Reflectance"

        description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
        derived from the associated Level-1C products. The atmospheric correction of\n\
        Sentinel-2 images includes the correction of the scattering of air molecules\n\
        (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
        in particular ozone, oxygen and water vapour and the correction of absorption and\n\
        scattering due to aerosol particles. Level 2A product are considered an ARD product."

        docs = "https://sentinels.copernicus.eu/web/sentinel/sentinel-data-access/sentinel-products/sentinel-2-data-products/collection-1-level-2a"

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
    }
}

pub async fn generate_download_plan(
    selection: &ImageSelection,
    output_dir: PathBuf,
) -> anyhow::Result<DownloadPlan> {
    let ids_to_download = selection
        .ids_to_download()
        .ok_or(anyhow!("No ids to download"))?;
    let products_to_download = selection
        .products_to_download()
        .ok_or(anyhow!("No products selected for download"))?;

    let mut tasks: Vec<DownloadTask> = vec![];

    for id in ids_to_download {
        let item = fetch_single_item(COLLECTION_ID, &id).await?;
        let assets = map_products_to_assets(&item, &products_to_download).ok_or(anyhow!(
            "Did not find matching assets for specified products"
        ))?;
        for asset in assets {
            let S3UrlParts { bucket, key, .. } = get_s3_url_parts(&asset.href)?;

            let file_name = Path::new(&key).file_name().unwrap();
            let output = output_dir.join(&id).join(file_name);

            let task = DownloadTask::new(&bucket, &key, output.to_str().unwrap());
            tasks.push(task)
        }
    }
    Ok(DownloadPlan::new(&selection.id, tasks))
}

async fn fetch_single_item(collection: &str, id: &str) -> Result<Item> {
    let url =
        format!("https://earth-search.aws.element84.com/v1/collections/{collection}/items/{id}");
    println!("{url}");
    let item = reqwest::get(url).await?.json::<Item>().await?;
    Ok(item)
}

fn map_products_to_assets(item: &Item, products: &[Product]) -> Option<Vec<Asset>> {
    let mut assets = vec![];
    for product in products {
        let asset = item.assets.get(&product.id)?.clone();
        assets.push(asset);
    }
    Some(assets)
}

struct S3UrlParts {
    bucket: String,
    region: String,
    key: String,
}

fn get_s3_url_parts(url: &str) -> Result<S3UrlParts> {
    let pattern = r"https://(?<bucket>[^.]+)\.s3\.(?<region>[^.]+)\.amazonaws\.com/(?<key>.+)";
    let re = Regex::new(pattern).expect("Regex pattern should always compile");

    let captures = re
        .captures(url)
        .ok_or(anyhow!("No regex matches found for: {}", url))?;

    let (_, [bucket, region, key]) = captures.extract();
    Ok(S3UrlParts {
        bucket: bucket.to_string(),
        region: region.to_string(),
        key: key.to_string(),
    })
}
