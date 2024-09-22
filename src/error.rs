use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use derive_more::{From, FromStrError};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    // -- ClientBuilder
    AWSRegionNotSet,
    AWSProfileNotSet,

    // -- Client
    #[from]
    S3HeadObject(aws_sdk_s3::error::SdkError<HeadObjectError>),
    #[from]
    S3GetObject(aws_sdk_s3::error::SdkError<GetObjectError>),
    #[from]
    S3ByteStream(aws_smithy_types::byte_stream::error::Error),

    // -- ImageSelection
    #[from]
    DeserializeToml(toml::de::Error),
    #[from]
    SerializeToml(toml::ser::Error),

    // -- Download Plan
    #[from]
    DeserializeJson(serde_json::Error),
    SerializeJson(serde_json::Error),

    // -- Collection
    #[from]
    FetchItem(reqwest::Error),
    #[from]
    NoMatchingVariant(FromStrError),

    #[from]
    IO(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}