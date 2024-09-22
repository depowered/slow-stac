mod client;
mod collection;
mod image_selection;
mod error;

pub use collection::element84;
pub use collection::copernicus;
pub use collection::CollectionKind;
pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use image_selection::ImageSelection;
