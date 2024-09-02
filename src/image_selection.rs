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

    pub fn write<P: AsRef<Path>>(self: &Self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

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

fn sentinel2level2a_template() -> toml::Table {
    toml::toml! {
        id = "copernicus.sentinel2level2a"

        provider = "Copernicus"

        name = "Sentinel-2 Level 2A Surface Reflectance"

        description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
        derived from the associated Level-1C products. The atmospheric correction of\n\
        Sentinel-2 images includes the correction of the scattering of air molecules\n\
        (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
        in particular ozone, oxygen and water vapour and the correction of absorption and\n\
        scattering due to aerosol particles. Level 2A product are considered an ARD product."

        // Select 'Further details about the data collection' to view a descrition of the bands
        docs = "https://documentation.dataspace.copernicus.eu/Data/SentinelMissions/Sentinel2.html#sentinel-2-level-2a-surface-reflectance"

        ids_to_download = [
            "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
            "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
            "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
            "S2A_MSIL2A_20240504T195901_N0510_R128_T08VPH_20240505T015750.SAFE",
        ]

        [[products]]
        id = "B02_10m"
        name = "Red"
        download = false

        [[products]]
        id = "B03_10m"
        name = "Green"
        download = false

        [[products]]
        id = "B04_10m"
        name = "Blue"
        download = false

        [[products]]
        id = "B08_10m"
        name = "NIR"
        download = false

        [[products]]
        id = "TCI_10m"
        name = "True Color"
        download = true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE_PATH: &str = "/tmp/image_selection_template.toml";

    #[test]
    fn test_template() {
        let selection = ImageSelection::from_template(&sentinel2level2a_template());
        assert_eq!(selection.id, "copernicus.sentinel2level2a");
        assert_eq!(selection.products.len(), 5);
    }

    #[test]
    fn test_write_toml() {
        let path = Path::new(TEMPLATE_PATH);
        let selection = ImageSelection::from_template(&sentinel2level2a_template());
        assert_eq!(selection.write(path).is_ok(), true)
    }

    #[test]
    fn test_read_toml() {
        let path = Path::new(TEMPLATE_PATH);
        let selection = ImageSelection::from_template(&sentinel2level2a_template());
        selection.write(path).unwrap();

        let selection = ImageSelection::read(path).unwrap();
        assert_eq!(selection.id, "copernicus.sentinel2level2a");
        assert_eq!(selection.products.len(), 5);
    }
}
