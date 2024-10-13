use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use resolve_path::PathResolveExt;
use slow_stac_reorg::adapter::AdapterKind;
use slow_stac_reorg::DownloadPlan;
use slow_stac_reorg::Result;
use slow_stac_reorg::{ImageSelection, Range};
use std::io::Write;
use std::path::PathBuf;

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

        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
    /// Prepare the download plan
    Plan {
        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
    /// Execute the download plan
    Download {
        /// Directory to save downloaded images
        output_dir: PathBuf,
    },
}

fn get_adapter_kind(c: &Collection) -> AdapterKind {
    match c {
        Collection::CopSentinel2 => AdapterKind::CopernicusSentinel2Level2A,
        Collection::E84Sentinel2 => AdapterKind::Element84Sentinel2Level2A,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Command::Select {
            collection,
            output_dir,
        } => {
            let adapter = get_adapter_kind(collection);
            let image_selection = adapter.image_selection_template();

            if !output_dir.exists() {
                panic!("Output directory does not exist: {:?}", output_dir);
            }

            let output = output_dir.join("image_selection.toml");
            if output.exists() {
                panic!("image_selection.toml already exists at {:?}", output_dir)
            }

            println!("Writing image selection toml to {:?}", output);
            image_selection.write(output)?;
        }
        Command::Plan {
            output_dir,
        } => {
            let selection_toml = output_dir.join("image_selection.toml");
            println!("Reading selection toml from {:?}", selection_toml);

            let image_selection = ImageSelection::read(&selection_toml)?;
            let adapter = image_selection.adapter_kind()?;
            let client = adapter.create_client(cli.aws_profile).await?;

            println!("Building download plan");
            let output_dir = output_dir.resolve();
            let plan = adapter
                .create_download_plan(&client, &image_selection, &output_dir)
                .await?;

            let output = output_dir.join("download_plan.json");
            println!("Writing plan json to {:?}", output);
            let _ = plan.write(output);
        }
        Command::Download { output_dir } => {
            let download_plan = output_dir.join("download_plan.json");
            println!("Reading plan json from {:?}", download_plan);
            let plan = DownloadPlan::read(&download_plan)?;
            let adapter = plan.kind;
            let client = adapter.create_client(cli.aws_profile).await?;

            let task_count = plan.tasks.len();
            println!("Found {task_count} task(s) in plan");

            for (index, task) in plan.tasks.iter().enumerate() {
                let count = index + 1;
                let n_of_m = format!("[{count}/{task_count}]");
                println!("{n_of_m} {task:?}");

                // Check if the output file already exists; moving onto the next task if so
                if task.output.exists() {
                    println!("{n_of_m} Already downloaded");
                    continue;
                }
                // Make parent directories as necessary
                let parent_dir = task.output.parent().unwrap();
                if !parent_dir.exists() {
                    std::fs::create_dir_all(parent_dir)?;
                }
                // Check if partial file exists and get its size
                let mut partial = task.output.file_name().unwrap_or_default().to_os_string();
                partial.push(".partial");
                let mut partial_file = std::fs::OpenOptions::new()
                    .read(true)
                    .create(true)
                    .append(true)
                    .open(&partial)?;
                let mut partial_size = partial_file.metadata()?.len();

                // Get object details from S3
                let head_object = client.s3_head_object(&task.bucket, &task.key).await?;
                let total_size = head_object
                    .content_length()
                    .expect("HeadObjects contains content_length")
                    as u64;

                if partial_size < total_size {
                    let mut object = client
                        .s3_get_object(
                            &task.bucket,
                            &task.key,
                            Some(Range {
                                start_byte: partial_size,
                                end_byte: total_size - 1,
                            }),
                        )
                        .await?;

                    let pb = init_progress_bar(&n_of_m, partial_size, total_size);
                    while let Some(bytes) = object.body.try_next().await? {
                        let bytes_len = bytes.len() as u64;
                        partial_file.write_all(&bytes)?;
                        partial_size += bytes_len;
                        pb.set_position(partial_size);
                    }
                    pb.finish();
                }
                // Rename the file to remove .partial suffix
                std::fs::rename(partial, &task.output)?;
                println!("{n_of_m} Download complete");
            }
        }
    }
    Ok(())
}

fn init_progress_bar(prefix: &str, partial_size: u64, total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::with_template("{prefix} [{elapsed_precise}] [{bar:50.cyan/blue}] [{percent}%] {decimal_bytes}/{decimal_total_bytes} ({decimal_bytes_per_sec}) (Remaining: {eta})")
        .unwrap()
        .progress_chars("#>-"));
    pb.set_prefix(prefix.to_owned());
    pb.set_position(partial_size);
    pb
}
