use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;

use crate::{Error, Result};

pub struct Range {
    start_byte: u64,
    end_byte: u64,
}

pub struct Client {
    s3_client: aws_sdk_s3::Client,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn s3_head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput> {
        let head = self
            .s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(head)
    }

    pub async fn s3_get_object(&self, bucket: &str, key: &str, range: Option<Range>) -> Result<GetObjectOutput> {
        let builder = self
            .s3_client
            .get_object()
            .bucket(bucket)
            .key(key);

        let builder = match range {
            Some(r) => builder.range(format!("bytes={}-{}", r.start_byte, r.end_byte)),
            None => builder,
        };

        let object = match bucket {
            "eodata" => {
                builder
                    .customize()
                    .mutate_request(|req| {
                        let _ = req.set_uri(req.uri().replace("x-id=GetObject", ""));
                    }).send().await?
            },
            _ => builder.send().await?
        };

        Ok(object)
    }
}

#[derive(Debug, Default)]
pub struct ClientBuilder {
    config_loader: Option<aws_config::ConfigLoader>,
    region: Option<String>,
}

impl ClientBuilder {
    pub fn without_credentials(mut self) -> Self {
        let base_config = aws_config::from_env().no_credentials();
        self.config_loader = Some(base_config);
        self
    }

    pub fn with_aws_profile(mut self, name: &str) -> Self {
        let base_config = aws_config::from_env().profile_name(name);
        self.config_loader = Some(base_config);
        self
    }

    pub fn set_region(mut self, name: &str) -> Self {
        self.region = Some(name.to_owned());
        self
    }


    pub async fn build(self) -> Result<Client> {
        let base_config = self.config_loader.ok_or_else(|| Error::CredentialsNotSet)?.load().await;
        let region = self.region.ok_or_else(|| Error::RegionNotSet)?;
        let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
            .region(aws_sdk_s3::config::Region::new(region))
            .force_path_style(true)
            .build();

        Ok(Client {
            s3_client: aws_sdk_s3::Client::from_conf(s3_config)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_credentials_not_set_error() {
        let result = Client::builder().set_region("us-west-2").build().await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::CredentialsNotSet)));
    }

    #[tokio::test]
    async fn test_region_not_set_error() {
        let result = Client::builder().without_credentials().build().await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::RegionNotSet)));
    }
}