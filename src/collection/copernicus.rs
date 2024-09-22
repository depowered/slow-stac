use crate::ImageSelection;
use crate::Result;


pub mod sentinel2level2 {
    use super::*;

    pub async fn get_stac_item(id: &str) -> Result<stac::Item> {
        let url = format!("https://catalogue.dataspace.copernicus.eu/stac/collections/SENTINEL-2/items/{id}");
        let item = reqwest::get(url).await?.json::<stac::Item>().await?;
        Ok(item)
    }

    pub fn image_selection_template() -> ImageSelection {
        ImageSelection::from_template(&toml::toml! {
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
        }).expect("Toml syntax error")
    }
}