use crate::s3::S3ObjOps;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Deserialize, Serialize, Debug)]
pub struct DownloadTask {
    bucket: String,
    key: String,
    output: String,
}
impl DownloadTask {
    pub fn new(bucket: &str, key: &str, output: &str) -> Self {
        DownloadTask {
            bucket: bucket.to_string(),
            key: key.to_string(),
            output: output.to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DownloadPlan {
    tasks: Vec<DownloadTask>,
}

impl DownloadPlan {
    pub fn new(tasks: Vec<DownloadTask>) -> Self {
        Self { tasks }
    }
    
    #[allow(dead_code)]
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let plan: Self = serde_json::from_str(&content)?;
        Ok(plan)
    }

    pub fn write<P: AsRef<Path>>(self: &Self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub async fn execute(self: &Self, provider: &impl S3ObjOps) -> Result<()> {
        for task in self.tasks.iter() {
            println!("Current task: {:?}", task);
            try_download(provider, &task.bucket, &task.key, &task.output).await?;
        }
        Ok(())
    }
}

pub async fn try_download(
    provider: &impl S3ObjOps,
    bucket: &str,
    key: &str,
    output: &str,
) -> Result<()> {
    // Check if the output file already exists; return early if so
    let dst = Path::new(output);
    if dst.exists() {
        println!("Output file already exists");
        return Ok(());
    }

    // Make parent directories as necessary
    let parent_dir = dst.parent().unwrap();
    if !parent_dir.exists() {
        fs::create_dir_all(parent_dir)?;
    }

    // Check if partial file exists and get its size
    let partial = format!("{}.partial", output);
    let mut partial_file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(&partial)?;
    let mut byte_count = partial_file.metadata()?.len();

    // Get object details from S3
    let head_object = provider.head_object(bucket, key).await?;

    let total_size = head_object
        .content_length()
        .ok_or(anyhow!("Error reading size of remote object"))? as u64;

    let progress = (byte_count as f64 / total_size as f64) * 100.;
    if progress > 0.0 {
        println!("Resuming download from {:.2}% completion", progress);
    }

    if byte_count < total_size {
        println!("Downloading...");

        let mut response = provider
            .get_object_range(bucket, key, byte_count, total_size - 1)
            .await?;

        while let Some(bytes) = response.body.try_next().await? {
            let bytes_len = bytes.len() as u64;
            partial_file.write_all(&bytes)?;
            byte_count += bytes_len;
        }
    }

    println!("Download complete");
    // Rename the file to remove .partial suffix
    fs::rename(partial, dst)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_OUTPUT_PATH: &str = "/tmp/download_plan.json";

    fn mock_download_plan() -> DownloadPlan {
        DownloadPlan {
            tasks: vec![
                DownloadTask {
                    bucket: "mybucket".to_string(),
                    key: "path/to/file1.txt".to_string(),
                    output: "path/to/write/file1.txt".to_string(),
                },
                DownloadTask {
                    bucket: "mybucket".to_string(),
                    key: "path/to/file2.txt".to_string(),
                    output: "path/to/write/file2.txt".to_string(),
                },
                DownloadTask {
                    bucket: "mybucket".to_string(),
                    key: "path/to/file3.txt".to_string(),
                    output: "path/to/write/file3.txt".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_write_json() {
        let path = Path::new(TEST_OUTPUT_PATH);
        let plan = mock_download_plan();
        plan.write(path).unwrap();
        assert_eq!(path.exists(), true);
    }

    #[test]
    fn test_read_json() {
        let path = Path::new(TEST_OUTPUT_PATH);
        let plan = mock_download_plan();
        plan.write(path).unwrap();

        let plan = DownloadPlan::read(path).unwrap();
        assert_eq!(plan.tasks.len(), 3);
    }
}
