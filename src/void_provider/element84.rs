use anyhow::Result;
use stac::{href_to_url, Item, ItemCollection};

use super::{AssetKey, RemoteFileInfo, STACCollection};

const SEARCH_API: &str = "https://earth-search.aws.element84.com/v1/search";

pub enum Element84Collection {
    Sentinel2C1L2A,
}

impl STACCollection for Element84Collection {
    fn key(&self) -> &str {
        match self {
            Self::Sentinel2C1L2A => sentinel_2_c1_l2a::COLLECTION,
        }
    }
}

async fn get_item_by_id(collection: impl STACCollection, id: &str) -> Result<Item> {
    let item_collection: ItemCollection = reqwest::Client::new()
        .post(SEARCH_API)
        .json(&serde_json::json!({"collections": vec![collection.key()], "ids": vec![id]}))
        .send()
        .await?
        .json()
        .await?;

    let item = item_collection.items[0].clone();

    Ok(item)
}

async fn extract_remote_file_info(item: Item, asset: impl AssetKey) -> Option<RemoteFileInfo> {
    let asset = item.assets.get(asset.key())?;

    let id = item.id;
    let url = href_to_url(&asset.href)?;
    let filesize = asset.additional_fields.get("file:size")?.as_u64();
    let checksum = Some(
        asset
            .additional_fields
            .get("file:checksum")?
            .as_str()?
            .to_owned(),
    );

    Some(RemoteFileInfo { id, url, filesize, checksum })
}

pub async fn get_remote_file_info(
    collection: impl STACCollection,
    id: &str,
    kind: impl AssetKey,
) -> Option<RemoteFileInfo> {
    let item = get_item_by_id(collection, id).await.ok()?;
    extract_remote_file_info(item, kind).await
}

pub mod sentinel_2_c1_l2a {
    use crate::provider::{AssetDescription, AssetKey};

    pub const COLLECTION: &str = "sentinel-2-c1-l2a";

    #[derive(Debug)]
    pub enum AssetKind {
        Red,
        Green,
        Blue,
        Visual,
        NIR,
        SWIR22,
        RedEdge2,
        RedEdge3,
        RedEdge1,
        SWIR16,
        WVP,
        NIR08,
        SCL,
        AOT,
        Coastal,
        NIR09,
        Cloud,
        Snow,
        Preview,
        GranuleMetadata,
        TileInfoMetadata,
        ProductMetadata,
        Thumbnail,
    }

    impl AssetKey for AssetKind {
        fn key(&self) -> &str {
            match self {
                Self::Red => "red",
                Self::Green => "green",
                Self::Blue => "blue",
                Self::Visual => "visual",
                Self::NIR => "nir",
                _ => panic!("The key function is not implement for the given variant"),
            }
        }
    }

    impl AssetDescription for AssetKind {
        fn description(&self) -> &str {
            match self {
                Self::Red => "Red - 10m",
                Self::Green => "Green - 10m",
                Self::Blue => "Blue - 10m",
                Self::Visual => "True color image",
                Self::NIR => "NIR 1 - 10m",
                _ => panic!("The description function is not implement for the given variant"),
            }
        }
    }
}
