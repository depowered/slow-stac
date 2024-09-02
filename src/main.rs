use anyhow::Result;
use copernicus::sentinel2level2a;
use image_selection::ImageSelection;
use std::path::PathBuf;

mod copernicus;
mod download_plan;
mod image_selection;
pub mod provider;
mod s3;
pub mod s3_operations;
pub mod stac_operations;

#[tokio::main]
async fn main() -> Result<()> {
    let input_dir = PathBuf::from("./inputs");
    let output_dir = PathBuf::from("./outputs");

    let image_selection_toml = input_dir.join("image_selection.toml");
    let selection = ImageSelection::read(image_selection_toml)?;

    let client = s3::client_from_profile("copernicus").await;
    let plan =
        sentinel2level2a::generate_download_plan(&client, &selection, output_dir.clone()).await?;
    let _ = plan.write(output_dir.join("download_plan.json"))?;
    
    let _ = plan.execute(&client).await?;

    Ok(())
}
