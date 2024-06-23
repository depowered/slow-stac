use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region};
use regex::Regex;
use std::{fs::File, io::Write, path::PathBuf};

use aws_sdk_s3::Client;

#[derive(Debug, PartialEq)]
pub struct S3Object {
    pub region: String,
    pub bucket: String,
    pub key: String,
}

impl S3Object {
    pub fn from_url(url: &str) -> Result<Self> {
        let re = Regex::new(
            r"https:\/\/(?<bucket>[\d\w-]+)\.s3\.(?<region>[\d\w-]+)\.amazonaws.com\/(?<key>.+)",
        )
        .expect("Regex pattern should always compile");

        let captures = re
            .captures(url)
            .ok_or(anyhow!("No regex matches found for: {}", url))?;

        let (_, [bucket, region, key]) = captures.extract();

        Ok(Self {
            region: region.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }
}

pub async fn get_anon_client(region: String) -> Client {
    let region = Region::new(region);
    let config = aws_config::defaults(BehaviorVersion::latest())
        .no_credentials()
        .region(region)
        .load()
        .await;
    Client::new(&config)
}

pub async fn get_object(src: S3Object, dst: PathBuf) -> Result<usize, anyhow::Error> {
    let mut file = File::create(dst.clone())?;

    let client = get_anon_client(src.region).await;

    let mut object = client
        .get_object()
        .bucket(src.bucket)
        .key(src.key)
        .send()
        .await?;

    let mut byte_count = 0_usize;
    while let Some(bytes) = object.body.try_next().await? {
        let bytes_len = bytes.len();
        file.write_all(&bytes)?;
        byte_count += bytes_len;
    }

    Ok(byte_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_object_from_url() {
        let url = "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/7/V/DG/2024/5/S2A_T07VDG_20240529T205023_L2A/B08.tif";
        let object = S3Object::from_url(url).unwrap();
        assert_eq!(
            object,
            S3Object {
                bucket: "e84-earth-search-sentinel-data".to_string(),
                region: "us-west-2".to_string(),
                key: "sentinel-2-c1-l2a/7/V/DG/2024/5/S2A_T07VDG_20240529T205023_L2A/B08.tif"
                    .to_string()
            }
        );
    }
}
