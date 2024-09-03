use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use crate::error::MapError;
use crate::s3;

pub struct Provider {
    client: Client,
}

impl Provider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn from_profile(profile_name: &str) -> Self {
        let client = s3::client_from_profile(profile_name).await;
        Self { client }
    }
}
impl s3::S3ObjOps for Provider {
    async fn head_object(self: &Self, bucket: &str, key: &str) -> anyhow::Result<HeadObjectOutput> {
        let head = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(head)
    }

    async fn get_object(self: &Self, bucket: &str, key: &str) -> anyhow::Result<GetObjectOutput> {
        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .customize()
            .map_request(strip_x_id_get_object_param_from_uri)
            .send()
            .await?;
        Ok(object)
    }

    async fn get_object_range(
        self: &Self,
        bucket: &str,
        key: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> anyhow::Result<GetObjectOutput> {
        let range = format!("bytes={}-{}", start_byte, end_byte);
        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .customize()
            .map_request(strip_x_id_get_object_param_from_uri)
            .send()
            .await?;
        Ok(object)
    }
}

/// The copernicus S3 API throws a fit if the param 'x-id=GetObject' is present in the request. This
/// function can be passed to the `GetObjectFluentBuilder::map_request()` method to strip the offending
/// param from the generated uri.
fn strip_x_id_get_object_param_from_uri(
    req: HttpRequest,
) -> Result<HttpRequest, MapError> {
    let mut r = req.try_clone().ok_or(MapError::Clone)?;
    let _ = r.set_uri(r.uri().replace("x-id=GetObject", ""));
    Ok(r)
}
