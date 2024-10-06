mod client;
mod collection;
mod download_plan;
mod error;
mod image_selection;
pub mod util;

pub use client::{Client, ClientBuilder, Range};
pub use collection::copernicus;
pub use collection::element84;
pub use collection::CollectionKind;
pub use download_plan::{DownloadPlan, DownloadTask};
pub use error::{Error, Result};
pub use image_selection::ImageSelection;
