use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region};
use regex::Regex;
use std::fs::OpenOptions;
use std::path::Path;
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

pub async fn get_object(src: S3Object, dst: PathBuf) -> Result<usize> {
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

pub async fn download_s3_object(url: &str, output_path: &str) -> Result<()> {
    let object = S3Object::from_url(url)?;
    let client = get_anon_client(object.region).await;

    if Path::new(output_path).exists() {
        println!("Output file already exists");
        return Ok(());
    }

    let partial_path = format!("{}.partial", output_path);

    // Check if partial file exists and get its size
    let mut partial_file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(&partial_path)?;
    let mut byte_count = partial_file.metadata()?.len();

    // Get object details from S3
    let head_object = client
        .head_object()
        .bucket(&object.bucket)
        .key(&object.key)
        .send()
        .await?;

    let total_size = head_object
        .content_length()
        .ok_or(anyhow!("Error reading size of remote object"))? as u64;

    let progress = (byte_count as f64 / total_size as f64) * 100.;
    if progress > 0.0 {
        println!("Resuming download from {:.2}% completion", progress);
    }

    if byte_count < total_size {
        println!("Downloading...");
        let range = format!("bytes={}-{}", byte_count, total_size - 1);

        let mut response = client
            .get_object()
            .bucket(&object.bucket)
            .key(&object.key)
            .range(range)
            .send()
            .await?;

        while let Some(bytes) = response.body.try_next().await? {
            let bytes_len = bytes.len() as u64;
            partial_file.write_all(&bytes)?;
            byte_count += bytes_len;
        }
    }

    println!("Download complete");
    // Rename the file to remove .partial suffix
    std::fs::rename(partial_path, output_path)?;

    Ok(())
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
