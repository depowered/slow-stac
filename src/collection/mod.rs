use crate::download_plan::DownloadPlan;
use crate::image_selection::ImageSelection;
use crate::{Client, Error, Result};
use derive_more::FromStr;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub mod copernicus;
pub mod element84;

#[derive(Deserialize, Serialize, Debug, Copy, Clone, FromStr)]
pub enum CollectionKind {
    Element84Sentinel2Level2A,
    CopernicusSentinel2Level2A,
}

// Constants
impl CollectionKind {
    pub fn bucket_name(&self) -> String {
        match self {
            CollectionKind::Element84Sentinel2Level2A => {
                String::from("e84-earth-search-sentinel-data")
            }
            CollectionKind::CopernicusSentinel2Level2A => String::from("eodata"),
        }
    }

    pub fn region_name(&self) -> String {
        String::from("us-west-2")
    }

    pub fn image_selection_template(&self) -> ImageSelection {
        match self {
            CollectionKind::Element84Sentinel2Level2A => {
                element84::sentinel2level2::image_selection_template()
            }
            CollectionKind::CopernicusSentinel2Level2A => {
                copernicus::sentinel2level2::image_selection_template()
            }
        }
    }
}

// Client construction
impl CollectionKind {
    pub async fn create_client(&self, profile: Option<String>) -> Result<Client> {
        let client = match profile {
            None => match self {
                CollectionKind::CopernicusSentinel2Level2A => return Err(Error::AWSProfileNotSet),
                CollectionKind::Element84Sentinel2Level2A => {
                    Client::builder()
                        .without_credentials()
                        .set_region(&self.region_name())
                        .build()
                        .await?
                }
            },
            Some(p) => {
                Client::builder()
                    .with_aws_profile(&p)
                    .set_region(&self.region_name())
                    .build()
                    .await?
            }
        };
        Ok(client)
    }
}

// Fetching functions
impl CollectionKind {
    pub async fn get_stac_item(&self, client: &Client, id: &str) -> Result<stac::Item> {
        let item = match self {
            CollectionKind::Element84Sentinel2Level2A => {
                element84::sentinel2level2::get_stac_item(client, id).await?
            }
            CollectionKind::CopernicusSentinel2Level2A => {
                copernicus::sentinel2level2::get_stac_item(client, id).await?
            }
        };
        Ok(item)
    }
}

// Download plan
impl CollectionKind {
    pub async fn create_download_plan(
        &self,
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
    ) -> Result<DownloadPlan> {
        let plan = match self {
            CollectionKind::Element84Sentinel2Level2A => {
                element84::sentinel2level2::create_download_plan(
                    client,
                    image_selection,
                    output_dir,
                )
                .await?
            }
            CollectionKind::CopernicusSentinel2Level2A => {
                copernicus::sentinel2level2::create_download_plan(
                    client,
                    image_selection,
                    output_dir,
                )
                .await?
            }
        };
        Ok(plan)
    }
}

#[allow(clippy::from_over_into)]
impl Into<String> for CollectionKind {
    fn into(self) -> String {
        match self {
            CollectionKind::Element84Sentinel2Level2A => String::from("Element84Sentinel2Level2A"),
            CollectionKind::CopernicusSentinel2Level2A => {
                String::from("CopernicusSentinel2Level2A")
            }
        }
    }
}
