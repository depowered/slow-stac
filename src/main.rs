use clap::{Parser, Subcommand, ValueEnum};
use slow_stac_reorg::ImageSelection;
use slow_stac_reorg::Result;
use slow_stac_reorg::{CollectionKind, DownloadPlan};
use std::path::PathBuf;
use resolve_path::PathResolveExt;

/// A tool for downloading satellite imagery from S3 on slow or unstable connections
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// AWS profile name to get credentials from
    #[arg(long)]
    aws_profile: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, ValueEnum, Debug)]
enum Collection {
    /// Sentinel 2 Level 2A via Copernicus Browser
    CopSentinel2,
    /// Sentinel 2 Level 2A via Element84 Earth Search
    E84Sentinel2,
}

#[derive(Subcommand)]
enum Command {
    /// Select the images to download
    Select {
        /// Collection to retrieve images from
        collection: Collection,

        /// Path to write the .toml for image selection
        selection_toml: PathBuf,
    },
    /// Prepare the download plan
    Plan {
        /// Path to read the .toml for image selection
        selection_toml: PathBuf,

        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
    /// Execute the download plan
    Download {
        /// Json file defining images to download
        plan_json: PathBuf,
    },
}

fn map_cli_collection(c: &Collection) -> CollectionKind {
    match c {
        Collection::E84Sentinel2 => CollectionKind::Element84Sentinel2Level2A,
        Collection::CopSentinel2 => CollectionKind::CopernicusSentinel2Level2A,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Command::Select {
            collection,
            selection_toml,
        } => {
            let kind = map_cli_collection(collection);
            let image_selection = kind.image_selection_template();
            print!("Writing image selection .toml to {:?}", selection_toml);
            image_selection.write(selection_toml)?;
        }
        Command::Plan {
            selection_toml,
            output_dir,
        } => {
            println!("Reading selection toml from {:?}", selection_toml);

            let image_selection = ImageSelection::read(selection_toml)?;
            let collection = image_selection.collection_kind()?;
            let client = collection.create_client(cli.aws_profile).await?;

            println!("Building download plan");
            let output_dir= output_dir.resolve();
            let plan = collection
                .create_download_plan(&client, &image_selection, &output_dir)
                .await?;

            let output = output_dir
                .join(selection_toml.file_stem().unwrap_or_default())
                .with_extension("json");
            println!("Writing plan json to {:?}", output);
            let _ = plan.write(output);
        }
        Command::Download { plan_json: plan } => {
            println!("Reading plan json from {:?}", plan);
            let plan = DownloadPlan::read(plan)?;
            let collection = plan.kind;
            let _client = collection.create_client(cli.aws_profile).await?;

            // TODO: Implement download loop
            print!("Downloading images");
        }
    }
    Ok(())
}
