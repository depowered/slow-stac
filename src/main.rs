use std::path::PathBuf;

use anyhow::Result;

use crate::s3_operations::{get_object, S3Object};

pub mod s3_operations;

#[tokio::main]
async fn main() -> Result<()> {
    let url = "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/7/V/DG/2024/5/S2A_T07VDG_20240529T205023_L2A/TCI.tif";
    let src = S3Object::from_url(&url).unwrap();
    let dst = PathBuf::from("./outputs/S2A_T07VDG_20240529T205023_L2A_TCI.tif");
    let _ = get_object(src, dst).await?;
    Ok(())
}
