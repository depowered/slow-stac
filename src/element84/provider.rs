use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use crate::s3;

pub struct Provider {
    client: Client,
}

impl Provider {
    #[allow(dead_code)]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn from_profile(profile_name: &str) -> Self {
        let client = s3::client_from_profile(profile_name).await;
        Self { client }
    }
    
    pub async fn as_anon() -> Self {
        let region = "us-west-2";
        let client = s3::anon_client(region).await;
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
            .send()
            .await?;
        Ok(object)
    }
}
