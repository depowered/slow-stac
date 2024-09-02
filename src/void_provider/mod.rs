use url::Url;
pub mod element84;
pub mod copernicus;

pub struct S3Asset {
    pub item_id: String,
    pub s3_bucket: String,
    pub s3_key: String,
    pub filesize: Option<u64>,
    pub checksum: Option<String>,
}

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
