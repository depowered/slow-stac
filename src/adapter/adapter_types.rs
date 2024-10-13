use crate::adapter::AdapterKind;
use crate::{Client, DownloadPlan, ImageSelection, Result};
use std::path::Path;

pub trait Adapter {
    fn adapter_name(kind: AdapterKind) -> String;

    fn image_selection_template(kind: AdapterKind) -> ImageSelection;

    async fn create_client(kind: AdapterKind, profile: Option<String>) -> Result<Client>;

    async fn get_stac_item(kind: AdapterKind, client: &Client, id: &str) -> Result<stac::Item>;

    async fn create_download_plan(
        kind: AdapterKind,
        client: &Client,
        image_selection: &ImageSelection,
        output_dir: &Path,
    ) -> Result<DownloadPlan>;
}
