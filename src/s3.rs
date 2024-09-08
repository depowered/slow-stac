//! Utility functions for creating s3 clients and modifying s3 requests
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::Client;

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

#[allow(dead_code)]
pub async fn anon_client() -> Client {
    let region = Region::new(DEFAULT_REGION);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .no_credentials()
        .region(region)
        .load()
        .await;
    Client::new(&config)
}

pub trait S3ObjOps {
    async fn head_object(self: &Self, bucket: &str, key: &str) -> anyhow::Result<HeadObjectOutput>;

    async fn get_object(self: &Self, bucket: &str, key: &str) -> anyhow::Result<GetObjectOutput>;

    async fn get_object_range(
        self: &Self,
        bucket: &str,
        key: &str,
        start_byte: u64,
        end_byte: u64,
    ) -> anyhow::Result<GetObjectOutput>;
}
