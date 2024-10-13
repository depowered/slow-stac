use crate::adapter::{Adapter, AdapterDispatcher};
use crate::{Client, DownloadPlan, ImageSelection};
use derive_more::FromStr;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Deserialize, Serialize, Debug, Copy, Clone, FromStr)]
pub enum AdapterKind {
    Element84Sentinel2Level2A,
    CopernicusSentinel2Level2A,
}

impl AdapterKind {
    fn adapter_name(self) -> String {
        AdapterDispatcher::adapter_name(self)
    }

    pub fn image_selection_template(self) -> ImageSelection {
        AdapterDispatcher::image_selection_template(self)
    }

    pub async fn create_client(self, profile: Option<String>) -> crate::Result<Client> {
        AdapterDispatcher::create_client(self, profile).await
    }

    #[allow(dead_code)]
    async fn get_stac_item(self, client: &Client, id: &str) -> crate::Result<stac::Item> {
        AdapterDispatcher::get_stac_item(self, client, id).await
    }

    pub async fn create_download_plan(
        self,
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
    ) -> crate::Result<DownloadPlan> {
        AdapterDispatcher::create_download_plan(self, client, image_selection, output_dir).await
    }
}

#[allow(clippy::from_over_into)]
impl Into<String> for AdapterKind {
    fn into(self) -> String {
        self.adapter_name()
    }
}
