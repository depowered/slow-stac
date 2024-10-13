use crate::adapter::{copernicus, element84};
use crate::adapter::{Adapter, AdapterKind};
use crate::{Client, DownloadPlan, ImageSelection, Result};
use std::path::Path;

pub struct AdapterDispatcher;

impl Adapter for AdapterDispatcher {
    fn adapter_name(kind: AdapterKind) -> String {
        match kind {
            AdapterKind::CopernicusSentinel2Level2A => {
                copernicus::Sentinel2Level2A::adapter_name(kind)
            }
            AdapterKind::Element84Sentinel2Level2A => {
                element84::Sentinel2Level2A::adapter_name(kind)
            }
        }
    }

    fn image_selection_template(kind: AdapterKind) -> ImageSelection {
        match kind {
            AdapterKind::CopernicusSentinel2Level2A => {
                copernicus::Sentinel2Level2A::image_selection_template(kind)
            }
            AdapterKind::Element84Sentinel2Level2A => {
                element84::Sentinel2Level2A::image_selection_template(kind)
            }
        }
    }

    async fn create_client(kind: AdapterKind, profile: Option<String>) -> Result<Client> {
        match kind {
            AdapterKind::CopernicusSentinel2Level2A => {
                copernicus::Sentinel2Level2A::create_client(kind, profile).await
            }
            AdapterKind::Element84Sentinel2Level2A => {
                element84::Sentinel2Level2A::create_client(kind, profile).await
            }
        }
    }

    async fn get_stac_item(kind: AdapterKind, client: &Client, id: &str) -> Result<stac::Item> {
        match kind {
            AdapterKind::CopernicusSentinel2Level2A => {
                copernicus::Sentinel2Level2A::get_stac_item(kind, client, id).await
            }
            AdapterKind::Element84Sentinel2Level2A => {
                element84::Sentinel2Level2A::get_stac_item(kind, client, id).await
            }
        }
    }

    async fn create_download_plan(
        kind: AdapterKind,
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
    ) -> Result<DownloadPlan> {
        match kind {
            AdapterKind::CopernicusSentinel2Level2A => {
                copernicus::Sentinel2Level2A::create_download_plan(
                    kind,
                    client,
                    image_selection,
                    output_dir,
                )
                .await
            }
            AdapterKind::Element84Sentinel2Level2A => {
                element84::Sentinel2Level2A::create_download_plan(
                    kind,
                    client,
                    image_selection,
                    output_dir,
                )
                .await
            }
        }
    }
}
