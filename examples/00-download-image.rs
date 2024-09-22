use slow_stac_reorg::{Client, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<()> {
    let bucket = "eodata";
    let key = "Sentinel-2/MSI/L2A/2024/05/04/S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE/GRANULE/L2A_T08VPH_A046314_20240504T195929/IMG_DATA/R10m/T08VPH_20240504T195901_TCI_10m.jp2";
    let client = Client::builder()
        .with_aws_profile("copernicus")
        .set_region("us-west-2")
        .build()
        .await?;

    let head = client.s3_head_object(bucket, key).await?;
    println!("Object size: {} bytes", head.content_length.unwrap());

    let dst_dir = Path::new("examples/.output");
    if !dst_dir.exists() {
        std::fs::create_dir(dst_dir)?;
    }
    let dst = PathBuf::from(dst_dir).join("T08VPH_20240504T195901_TCI_10m.jp2");
    let mut buffer = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true) // Overwrite if exists
        .open(dst)?;

    println!("Downloading image");
    let mut response = client.s3_get_object(bucket, key, None).await?;
    while let Some(bytes) = response.body.try_next().await? {
        buffer.write_all(&bytes)?;
    }
    println!("Download complete!");
    Ok(())
}
