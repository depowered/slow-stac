use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use derive_more::From;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    // -- ClientBuilder
    RegionNotSet,
    CredentialsNotSet,

    // -- Client
    #[from]
    S3HeadObjectError(aws_sdk_s3::error::SdkError<HeadObjectError>),
    #[from]
    S3GetObjectError(aws_sdk_s3::error::SdkError<GetObjectError>),
    #[from]
    S3ByteStream(aws_smithy_types::byte_stream::error::Error),

    #[from]
    IO(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}