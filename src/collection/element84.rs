use crate::ImageSelection;
use crate::Result;

pub mod sentinel2level2 {
    use super::*;

    pub async fn get_stac_item(id: &str) -> Result<stac::Item> {
        let url =
            format!("https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/{id}");
        let item = reqwest::get(url).await?.json::<stac::Item>().await?;
        Ok(item)
    }

    pub fn image_selection_template() -> ImageSelection {
        ImageSelection::from_template(&toml::toml! {
            id = "element84.sentinel2collection1level2a"

            provider = "Element84"

            name = "Sentinel-2 Collection 1 Level 2A Surface Reflectance"

            description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
            derived from the associated Level-1C products. The atmospheric correction of\n\
            Sentinel-2 images includes the correction of the scattering of air molecules\n\
            (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
            in particular ozone, oxygen and water vapour and the correction of absorption and\n\
            scattering due to aerosol particles. Level 2A product are considered an ARD product."

            docs = "https://sentinels.copernicus.eu/web/sentinel/sentinel-data-access/sentinel-products/sentinel-2-data-products/collection-1-level-2a"

            ids_to_download = [
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
                "S2A_T08VPH_20240504T195929_L2A",
            ]

            [[products]]
            id = "red"
            name = "Red"
            download = false

            [[products]]
            id = "green"
            name = "Green"
            download = false

            [[products]]
            id = "blue"
            name = "Blue"
            download = false

            [[products]]
            id = "nir"
            name = "NIR"
            download = false

            [[products]]
            id = "visual"
            name = "True Color"
            download = true
        }).expect("Toml syntax error")
    }
}