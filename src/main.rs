use std::path::Path;

use anyhow::Result;
use s3_operations::download_s3_object;
use stac_operations::{read_item_from_file, AssetInfo};

pub mod s3_operations;
pub mod stac_operations;

#[tokio::main]
async fn main() -> Result<()> {
    let input = Path::new("./inputs/S2A_T08VPH_20240504T195929_L2A.geojson");
    let item = read_item_from_file(input)?;
    let key = "visual";

    let asset_info = AssetInfo::from_item(&item, key)?;
    let output_path = format!("./outputs/{}_TCI.tif", asset_info.item_id);
    println!("Attempting to download remote file to: {}", &output_path);
    download_s3_object(&asset_info.href, &output_path).await?;

    Ok(())
}
