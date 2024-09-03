use anyhow::Result;
use copernicus::sentinel2level2a;
use image_selection::ImageSelection;
use std::path::PathBuf;
use copernicus::Provider;

mod collection;
mod copernicus;
mod download_plan;
mod error;
mod image_selection;
mod s3;
pub mod s3_operations;
pub mod stac_operations;

#[tokio::main]
async fn main() -> Result<()> {
    let input_dir = PathBuf::from("./inputs");
    let output_dir = PathBuf::from("./outputs");

    let image_selection_toml = input_dir.join("image_selection.toml");
    let selection = ImageSelection::read(image_selection_toml)?;

    let provider = Provider::from_profile("copernicus").await;

    let plan =
        sentinel2level2a::generate_download_plan(&provider, &selection, output_dir.clone()).await?;
    let _ = plan.write(output_dir.join("download_plan.json"))?;

    let _ = plan.execute(&provider).await?;

    Ok(())
}
