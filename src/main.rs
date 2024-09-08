use anyhow::Result;
use std::path::PathBuf;

extern crate slow_stac;
use slow_stac::copernicus::sentinel2level2a;
use slow_stac::copernicus::Provider;
use slow_stac::image_selection::ImageSelection;

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
