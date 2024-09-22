use std::path::PathBuf;
use clap::{Parser, Subcommand, ValueEnum};
use slow_stac_reorg::Result;
use slow_stac_reorg::ImageSelection;
use slow_stac_reorg::CollectionKind;

/// A tool for downloading satellite imagery from S3 on slow or unstable connections
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// AWS profile name to get credentials from
    #[arg(long)]
    aws_profile: Option<String>,

    /// Collection to retrieve images from
    collection: Collection,

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
        /// Path to write the .toml for image selection
        selection: PathBuf,
    },
    /// Prepare the download plan
    Plan {
        /// Path to read the .toml for image selection
        selection: PathBuf,

        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
    /// Execute the download plan
    Download {
        /// Json file defining images to download
        plan: PathBuf,
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
        Command::Select { selection } => {
            let collection = map_cli_collection(&cli.collection);
            let image_selection = collection.image_selection_template();
            print!("Writing image selection .toml to {:?}", selection);
            image_selection.write(selection)?;
        }
        Command::Plan { selection, output_dir, } => {
            println!("Reading selection toml from {:?}", selection);
            let _image_selection = ImageSelection::read(selection)?;

            let _collection = map_cli_collection(&cli.collection);

            // TODO: Transform image selection into download plan
            println!("Writing plan json to {:?}", output_dir);
        }
        Command::Download { plan } => {
            println!("Reading plan json from {:?}", plan);

            // TODO: Implement download loop
            print!("Downloading images");
        }
    }
    Ok(())
}
