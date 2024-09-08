use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// A tool for downloading satellite imagery from S3 on slow or unstable connections
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Select the images to download
    Select {
        /// Collection to retrieve images from
        collection: Collection,

        /// Directory to save image selection toml
        output_dir: PathBuf,
    },
    /// Prepare the download plan
    Prepare {
        /// Toml file defining image ids and product types to download
        image_selection: PathBuf,

        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
    /// Execute the download plan
    Download {
        /// Json file defining images to download
        download_plan: PathBuf,
    },
}

#[derive(Copy, Clone, ValueEnum, Debug)]
enum Collection {
    /// Sentinel 2 Level 2A via Copernicus Browser
    CopSentinel2,
    /// Sentinel 2 Level 2A via Element84 Earth Search
    E84Sentinel2,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Select {
            collection,
            output_dir,
        } => {
            handle_select(collection, output_dir)?;
        }
        Commands::Prepare {
            image_selection,
            output_dir,
        } => {
            handle_prepare(image_selection, output_dir).await?;
        }
        Commands::Download { download_plan } => {
            handle_download(download_plan).await?;
        }
    }
    Ok(())
}

fn handle_select(collection: &Collection, output_dir: &PathBuf) -> Result<()> {
    let (template, filename) = match collection {
        Collection::CopSentinel2 => {
            let template = slow_stac::copernicus::sentinel2level2a::image_selection_toml();
            let filename = "cop_sentinel2_selection.toml";
            (template, filename)
        }
        Collection::E84Sentinel2 => {
            let template =
                slow_stac::element84::sentinel2collection1level2a::image_selection_toml();
            let filename = "cop_sentinel2_selection.toml";
            (template, filename)
        }
    };
    let selection = slow_stac::image_selection::ImageSelection::from_template(&template);
    let path = output_dir.join(filename);
    if path.exists() {
        return Err(anyhow!("File already exists {:?}", path));
    }
    selection.write(&path)?;
    println!("Wrote template image selection file to {:?}", &path);
    Ok(())
}

async fn handle_prepare(image_selection: &PathBuf, output_dir: &PathBuf) -> Result<()> {
    if !output_dir.exists() {
        return Err(anyhow!("Directory does not exist {:?}", output_dir));
    }
    let selection = slow_stac::image_selection::ImageSelection::read(image_selection)
        .with_context(|| anyhow!("Could not parse the provided file"))?;
    let (plan, filename) = match selection.id.as_str() {
        "copernicus.sentinel2level2a" => {
            let provider = slow_stac::copernicus::Provider::from_profile("copernicus").await;
            let plan = slow_stac::copernicus::sentinel2level2a::generate_download_plan(
                &provider,
                &selection,
                output_dir.clone(),
            )
            .await?;
            let filename = "cop_sentinel2_download_plan.json";
            (plan, filename)
        }
        "element84.sentinel2collection1level2a" => {
            let plan = slow_stac::element84::sentinel2collection1level2a::generate_download_plan(
                &selection,
                output_dir.clone(),
            )
            .await?;
            let filename = "e84_sentinel2_download_plan.json";
            (plan, filename)
        }
        _ => return Err(anyhow!("Unknown id: {}", selection.id)),
    };
    let path = output_dir.join(filename);
    if path.exists() {
        return Err(anyhow!("File already exists {:?}", path));
    }
    plan.write(&path)?;
    println!("Wrote download plan file to {:?}", &path);
    Ok(())
}

async fn handle_download(download_plan: &PathBuf) -> Result<()> {
    let plan = slow_stac::download_plan::DownloadPlan::read(download_plan)?;
    match plan.selection_id.as_str() {
        "copernicus.sentinel2level2a" => {
            let provider = slow_stac::copernicus::Provider::from_profile("copernicus").await;
            plan.execute(&provider).await?;
        }
        "element84.sentinel2collection1level2a" => {
            let provider = slow_stac::element84::Provider::as_anon().await;
            plan.execute(&provider).await?;
        }
        _ => return Err(anyhow!("Unknown id: {}", plan.selection_id)),
    };
    Ok(())
}
