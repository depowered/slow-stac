use thiserror::Error;

#[derive(Error, Debug)]
pub enum MapError {
    #[error("Unable to clone request")]
    Clone,
}
