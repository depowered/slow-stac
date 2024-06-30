use anyhow::{anyhow, Result};
use s3_operations::download_s3_object;

pub mod provider;
pub mod s3_operations;
pub mod stac_operations;

use crate::provider::element84::{get_remote_file_info, Element84Collection};

#[tokio::main]
async fn main() -> Result<()> {
    let collection = Element84Collection::Sentinel2C1L2A;
    let id = "S2A_T08VPH_20240504T195929_L2A";
    let kind = provider::element84::sentinel_2_c1_l2a::AssetKind::Visual;

    let remote_file = get_remote_file_info(collection, id, kind)
        .await
        .ok_or(anyhow!("Error reading remote file info"))?;

    let output_path = format!("./outputs/{}_TCI.tif", remote_file.id);
    println!(
        "Attempting to download remote file from: {}",
        &remote_file.url.as_str()
    );
    println!("Attempting to download remote file to: {}", &output_path);
    download_s3_object(&remote_file.url.as_str(), &output_path).await?;

    Ok(())
}
