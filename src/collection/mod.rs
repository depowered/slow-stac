use crate::image_selection::ImageSelection;
use crate::{Client, Result, Error};

pub mod element84;
pub mod copernicus;

pub enum CollectionKind {
    Element84Sentinel2Level2,
    CopernicusSentinel2Level2,
}

// Constants
impl CollectionKind {
    pub fn bucket_name(&self) -> String {
        match self {
            CollectionKind::Element84Sentinel2Level2 => String::from("earth-search"),
            CollectionKind::CopernicusSentinel2Level2 => String::from("eodata"),
        }
    }

    pub fn region_name(&self) -> String {
        String::from("us-west-2")
    }

    pub fn image_selection_template(&self) -> ImageSelection {
        match self {
            CollectionKind::Element84Sentinel2Level2 => element84::sentinel2level2::image_selection_template(),
            CollectionKind::CopernicusSentinel2Level2 => copernicus::sentinel2level2::image_selection_template(),
        }
    }
}

// Client construction
impl CollectionKind {
    pub async fn create_client(&self, profile: Option<String>) -> Result<Client> {
        let client = match (self, profile) {
            // A profile is required for Copernicus collections
            (CollectionKind::CopernicusSentinel2Level2, None) => return Err(Error::AWSProfileNotSet),
            // Otherwise, if a profile is provided, use it
            (_, Some(p)) => {
                Client::builder().with_aws_profile(&p).set_region(&self.region_name()).build().await?
            }
            // Use an anonymous client in all other cases
            _ => {
                Client::builder().without_credentials().set_region(&self.region_name()).build().await?
            }
        };
        Ok(client)
    }
}

// Fetching functions
impl CollectionKind {
    pub async fn get_stac_item(&self, id: &str) -> Result<stac::Item> {
        let item = match self {
            CollectionKind::Element84Sentinel2Level2 => element84::sentinel2level2::get_stac_item(id).await?,
            CollectionKind::CopernicusSentinel2Level2 => copernicus::sentinel2level2::get_stac_item(id).await?,
        };
        Ok(item)
    }
}
