use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::adapter::AdapterKind;
use crate::Result;

#[derive(Deserialize, Serialize, Debug)]
pub struct DownloadPlan {
    pub kind: AdapterKind,
    pub tasks: Vec<DownloadTask>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DownloadTask {
    pub bucket: String,
    pub key: String,
    pub output: PathBuf,
}

impl DownloadPlan {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let plan: Self = serde_json::from_str(&content)?;
        Ok(plan)
    }

    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
