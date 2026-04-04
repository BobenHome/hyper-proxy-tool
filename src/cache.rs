use bytes::Bytes;
use hyper::StatusCode;
use std::time::{Duration, Instant};

/// Cached HTTP response
#[derive(Clone)]
pub struct CachedResponse {
    pub status: StatusCode,
    pub headers: hyper::HeaderMap,
    pub body: Bytes,
    pub expires_at: Instant,
}

/// Parse Cache-Control header to get max-age duration
pub fn parse_cache_max_age(headers: &hyper::HeaderMap) -> Option<Duration> {
    let cache_control = headers
        .get(hyper::header::CACHE_CONTROL)?
        .to_str()
        .ok()?
        .to_lowercase();

    // If contains no-store or private, don't cache
    if cache_control.contains("no-store") || cache_control.contains("private") {
        return None;
    }

    // Extract max-age=xxx
    if let Some(idx) = cache_control.find("max-age=") {
        let num_str: String = cache_control[idx + 8..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();

        if let Ok(seconds) = num_str.parse::<u64>()
            && seconds > 0
        {
            return Some(Duration::from_secs(seconds));
        }
    }
    None
}
