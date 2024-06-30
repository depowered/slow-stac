use url::Url;
pub mod element84;

pub struct RemoteFileInfo {
    pub id: String,
    pub url: Url,
    pub filesize: Option<u64>,
    pub checksum: Option<String>,
}

pub trait STACCollection {
    fn key(&self) -> &str;
}

pub trait AssetKey {
    fn key(&self) -> &str;
}

pub trait AssetDescription {
    fn description(&self) -> &str;
}
