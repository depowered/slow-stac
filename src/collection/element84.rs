pub mod sentinel2level2 {
    use crate::DownloadPlan;
    use crate::ImageSelection;
    use crate::Result;
    use crate::{Client, CollectionKind};
    use std::path::PathBuf;

    pub async fn get_stac_item(client: &Client, id: &str) -> Result<stac::Item> {
        let url = format!(
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/{id}"
        );
        let item = client.web_get(&url).await?.json::<stac::Item>().await?;
        Ok(item)
    }

    pub fn image_selection_template() -> ImageSelection {
        let kind: String = CollectionKind::CopernicusSentinel2Level2A.into();
        ImageSelection::from_template(&toml::toml! {
            collection_kind = kind

            name = "Sentinel-2 Collection 1 Level 2A Surface Reflectance"

            description = "Level 2A product provides atmospherically corrected Surface Reflectance (SR) images,\n\
            derived from the associated Level-1C products. The atmospheric correction of\n\
            Sentinel-2 images includes the correction of the scattering of air molecules\n\
            (Rayleigh scattering), of the absorbing and scattering effects of atmospheric gases,\n\
            in particular ozone, oxygen and water vapour and the correction of absorption and\n\
            scattering due to aerosol particles. Level 2A product are considered an ARD product."

            web_app = "https://console.earth-search.aws.element84.com/"

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

    pub async fn create_download_plan(
        _client: &Client,
        _image_selection: &ImageSelection,
        _output_dir: PathBuf,
    ) -> Result<DownloadPlan> {
        todo!()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_image_selection_template_deserializes() {
            let template = image_selection_template();
        }
    }
}
