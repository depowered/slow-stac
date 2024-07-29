use std::path::PathBuf;
use anyhow::Result;
use crate::provider::copernicus::{get_s3_client, Sentinel2Level2A, try_download};

pub mod provider;
pub mod s3_operations;
pub mod stac_operations;


#[tokio::main]
async fn main() -> Result<()> {
    let client = get_s3_client().await;
    let id= "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE";
    let product = Sentinel2Level2A::TCI;
    let output_dir = PathBuf::from("./outputs");
    
    try_download(client, id, product, output_dir).await?;
    
    Ok(())
}
