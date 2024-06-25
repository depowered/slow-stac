use std::{fs::read_to_string, path::Path};

use anyhow::{anyhow, Result};
use stac::{Asset, Item, ItemCollection};

const SEARCH_API: &str = "https://earth-search.aws.element84.com/v1/search";
const SENTINEL_2_C1_L2A: &str = "sentinel-2-c1-l2a";

#[derive(Debug)]
pub struct AssetInfo {
    pub item_id: String,
    pub href: String,
    pub checksum: Option<String>,
    pub size: Option<i64>,
}

impl AssetInfo {
    pub fn from_item(item: &Item, key: &str) -> Result<Self> {
        let asset = item
            .assets
            .get(key)
            .ok_or(anyhow!("Key not found: {}", key))?;
        Ok(Self {
            item_id: item.id.to_owned(),
            href: asset.href.to_owned(),
            checksum: Self::extract_checksum(asset),
            size: Self::extract_file_size(asset),
        })
    }

    fn extract_checksum(asset: &Asset) -> Option<String> {
        let checksum = asset
            .additional_fields
            .get("file:checksum")?
            .as_str()?
            .to_owned();
        Some(checksum)
    }

    fn extract_file_size(asset: &Asset) -> Option<i64> {
        let size = asset.additional_fields.get("file:size")?.as_i64()?;
        Some(size)
    }
}

pub async fn search_by_ids(ids: Vec<String>) -> Result<ItemCollection> {
    let item_collection: ItemCollection = reqwest::Client::new()
        .post(SEARCH_API)
        .json(&serde_json::json!({"collections": vec![SENTINEL_2_C1_L2A], "ids": ids}))
        .send()
        .await?
        .json()
        .await?;

    Ok(item_collection)
}

pub fn read_item_from_file(path: &Path) -> Result<Item> {
    let content = read_to_string(path)?;
    let item: Item = serde_json::from_str(&content)?;
    Ok(item)
}
