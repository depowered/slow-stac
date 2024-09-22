use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

use crate::Result;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ImageSelection {
    pub id: String,
    provider: String,
    name: String,
    description: String,
    docs: String,
    ids_to_download: Vec<String>,
    products: Vec<Product>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Product {
    pub id: String,
    name: String,
    download: bool,
}

impl ImageSelection {
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let selection: Self = toml::from_str(&content)?;
        Ok(selection)
    }

    #[allow(dead_code)]
    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn from_template(table: &toml::Table) -> Result<Self> {
        let selection: Self =
            toml::from_str(&table.to_string())?;
        Ok(selection)
    }

    pub fn products_to_download(&self) -> Option<Vec<Product>> {
        let products = self.products.clone();
        let to_download = products
            .into_iter()
            .filter(|p| p.download )
            .collect::<Vec<_>>();
        if to_download.is_empty() {
            return None;
        }
        Some(to_download)
    }

    pub fn ids_to_download(&self) -> Option<Vec<String>> {
        if self.ids_to_download.is_empty() {
            return None;
        }
        // Remove duplicates
        let ids = self
            .ids_to_download
            .clone()
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Some(ids)
    }
}
