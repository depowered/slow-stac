pub fn extract_s3_key(href: &str) -> Option<String> {
    let url = reqwest::Url::parse(href).ok()?;
    let key = match url.scheme() {
        "s3" => url
            .path()
            .split_once('/')
            .map(|(_bucket, key)| String::from(key)),
        "https" | "http" => url.path().strip_prefix('/').map(String::from),
        _ => return None,
    };
    key
}

pub fn extract_s3_bucket(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    let bucket = match url.scheme() {
        "s3" => url.host_str().map(String::from),
        "https" | "http" => url
            .host_str()
            .and_then(|s| s.split_once('.'))
            .map(|(bucket, _rest)| String::from(bucket)),
        _ => return None,
    };
    bucket
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_extract_key_from_s3() {
        let href = "s3://sentinel-s2-l2a/tiles/8/V/PH/2024/5/4/0/metadata.xml";

        assert_eq!(
            extract_s3_key(href).unwrap(),
            String::from("tiles/8/V/PH/2024/5/4/0/metadata.xml")
        );
    }

    #[tokio::test]
    async fn test_extract_key_from_https() {
        let href = "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/8/V/PH/2024/5/S2A_T08VPH_20240504T195929_L2A/B04.tif";

        assert_eq!(
            extract_s3_key(href).unwrap(),
            String::from("sentinel-2-c1-l2a/8/V/PH/2024/5/S2A_T08VPH_20240504T195929_L2A/B04.tif")
        );
    }

    #[tokio::test]
    async fn test_extract_bucket_from_s3() {
        let href = "s3://sentinel-s2-l2a/tiles/8/V/PH/2024/5/4/0/metadata.xml";

        assert_eq!(
            extract_s3_bucket(href).unwrap(),
            String::from("sentinel-s2-l2a")
        );
    }

    #[tokio::test]
    async fn test_extract_bucket_from_https() {
        let href = "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/8/V/PH/2024/5/S2A_T08VPH_20240504T195929_L2A/B04.tif";

        assert_eq!(
            extract_s3_bucket(href).unwrap(),
            String::from("e84-earth-search-sentinel-data")
        );
    }
}
