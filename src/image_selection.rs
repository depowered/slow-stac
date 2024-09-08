use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use toml;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ImageSelection {
    id: String,
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
    pub fn write<P: AsRef<Path>>(self: &Self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn from_template(table: &toml::Table) -> Self {
        let selection: Self =
            toml::from_str(&table.to_string()).expect("Error serializing template");
        selection
    }

    pub fn products_to_download(self: &Self) -> Option<Vec<Product>> {
        let products = self.products.clone();
        let to_download = products
            .into_iter()
            .filter(|p| p.download == true)
            .collect::<Vec<_>>();
        if to_download.is_empty() {
            return None;
        }
        Some(to_download)
    }

    pub fn ids_to_download(self: &Self) -> Option<Vec<String>> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::copernicus::sentinel2level2a;

    const TEMPLATE_PATH: &str = "/tmp/image_selection_template.toml";

    #[test]
    fn test_template() {
        let selection = ImageSelection::from_template(&sentinel2level2a::image_selection_toml());
        assert_eq!(selection.id, "copernicus.sentinel2level2a");
        assert_eq!(selection.products.len(), 5);
    }

    #[test]
    fn test_write_toml() {
        let path = Path::new(TEMPLATE_PATH);
        let selection = ImageSelection::from_template(&sentinel2level2a::image_selection_toml());
        assert_eq!(selection.write(path).is_ok(), true)
    }

    #[test]
    fn test_read_toml() {
        let path = Path::new(TEMPLATE_PATH);
        let selection = ImageSelection::from_template(&sentinel2level2a::image_selection_toml());
        selection.write(path).unwrap();

        let selection = ImageSelection::read(path).unwrap();
        assert_eq!(selection.id, "copernicus.sentinel2level2a");
        assert_eq!(selection.products.len(), 5);
    }
}
