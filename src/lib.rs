mod client;
mod download_plan;
mod error;
mod image_selection;
pub mod util;
pub mod adapter;

pub use client::{Client, ClientBuilder, Range};
pub use download_plan::{DownloadPlan, DownloadTask};
pub use error::{Error, Result};
pub use image_selection::ImageSelection;
