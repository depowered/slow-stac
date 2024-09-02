//! Utility functions for creating s3 clients and modifying s3 requests
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use thiserror::Error;

const DEFAULT_REGION: &str = "us-east-1";

pub async fn client_from_profile(profile_name: &str) -> Client {
    let base_config = aws_config::from_env()
        .profile_name(profile_name)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .region(Region::new(DEFAULT_REGION))
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

pub async fn anon_client() -> Client {
    let region = Region::new(DEFAULT_REGION);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .no_credentials()
        .region(region)
        .load()
        .await;
    Client::new(&config)
}

/// The copernicus S3 API throws a fit if the param 'x-id=GetObject' is present in the request. This
/// function can be passed to the `GetObjectFluentBuilder::map_request()` method to strip the offending
/// param from the generated uri.
pub fn strip_x_id_get_object_param_from_uri(
    req: HttpRequest,
) -> std::result::Result<HttpRequest, crate::provider::copernicus::MapError> {
    let mut r = req
        .try_clone()
        .ok_or(crate::provider::copernicus::MapError::Clone)?;
    let _ = r.set_uri(r.uri().replace("x-id=GetObject", ""));
    Ok(r)
}

#[derive(Error, Debug)]
enum MapError {
    #[error("Unable to clone request")]
    Clone,
}
