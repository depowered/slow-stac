use anyhow::Result;
use std::path::PathBuf;

extern crate slow_stac;
use slow_stac::element84::sentinel2collection1level2a;
use slow_stac::element84::Provider;
use slow_stac::image_selection::ImageSelection;

#[tokio::main]
async fn main() -> Result<()> {
    let output_dir = PathBuf::from("./outputs/element84");

    let selection =
        ImageSelection::from_template(&sentinel2collection1level2a::image_selection_toml());

    let plan =
        sentinel2collection1level2a::generate_download_plan(&selection, output_dir.clone()).await?;
    let _ = plan.write(output_dir.join("download_plan.json"))?;

    let provider = Provider::as_anon().await;
    let _ = plan.execute(&provider).await?;

    Ok(())
}
