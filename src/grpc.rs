use bytes::Bytes;
use http_body_util::{BodyExt, Empty, combinators::BoxBody};
use hyper::{HeaderMap, Response, header::HeaderValue};
use prost::{Enumeration, Message};
use std::time::Duration;

use crate::error::ProxyError;
use crate::pipeline::RejectReason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcPathParts {
    pub service: String,
    pub method: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum GrpcCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::Internal => "INTERNAL",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Unauthenticated => "UNAUTHENTICATED",
        }
    }
}

pub fn grpc_status_label(code: i32) -> &'static str {
    match code {
        -1 => "MISSING",
        0 => GrpcCode::Ok.as_str(),
        1 => GrpcCode::Cancelled.as_str(),
        2 => GrpcCode::Unknown.as_str(),
        3 => GrpcCode::InvalidArgument.as_str(),
        4 => GrpcCode::DeadlineExceeded.as_str(),
        5 => GrpcCode::NotFound.as_str(),
        6 => GrpcCode::AlreadyExists.as_str(),
        7 => GrpcCode::PermissionDenied.as_str(),
        8 => GrpcCode::ResourceExhausted.as_str(),
        9 => GrpcCode::FailedPrecondition.as_str(),
        10 => GrpcCode::Aborted.as_str(),
        11 => GrpcCode::OutOfRange.as_str(),
        12 => GrpcCode::Unimplemented.as_str(),
        13 => GrpcCode::Internal.as_str(),
        14 => GrpcCode::Unavailable.as_str(),
        15 => GrpcCode::DataLoss.as_str(),
        16 => GrpcCode::Unauthenticated.as_str(),
        _ => "UNKNOWN_CODE",
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    pub service: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthCheckResponse {
    #[prost(enumeration = "HealthServingStatus", tag = "1")]
    pub status: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum HealthServingStatus {
    Unknown = 0,
    Serving = 1,
    NotServing = 2,
    ServiceUnknown = 3,
}

pub fn parse_grpc_path(path: &str) -> Option<GrpcPathParts> {
    let trimmed = path.strip_prefix('/')?;
    let (service, method) = trimmed.split_once('/')?;
    if service.is_empty() || method.is_empty() || method.contains('/') {
        return None;
    }

    Some(GrpcPathParts {
        service: service.to_string(),
        method: method.to_string(),
    })
}

pub fn parse_grpc_timeout(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?.trim();
    if value.len() < 2 || value.len() > 9 {
        return None;
    }

    let (digits, unit) = value.split_at(value.len() - 1);
    if digits.is_empty() || digits.len() > 8 || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let amount = digits.parse::<u64>().ok()?;
    let unit_char = unit.chars().next()?;
    match unit_char {
        'H' => amount.checked_mul(60 * 60).map(Duration::from_secs),
        'M' => amount.checked_mul(60).map(Duration::from_secs),
        'S' => Some(Duration::from_secs(amount)),
        'm' => Some(Duration::from_millis(amount)),
        'u' => Some(Duration::from_micros(amount)),
        'n' => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}

pub fn effective_grpc_timeout(
    route_timeout: Option<Duration>,
    grpc_timeout: Option<Duration>,
    respect_grpc_timeout: bool,
) -> Option<Duration> {
    if !respect_grpc_timeout {
        return route_timeout;
    }

    match (route_timeout, grpc_timeout) {
        (Some(route), Some(grpc)) => Some(route.min(grpc)),
        (Some(route), None) => Some(route),
        (None, Some(grpc)) => Some(grpc),
        (None, None) => None,
    }
}

pub fn grpc_code_for_reject(reason: RejectReason) -> GrpcCode {
    match reason {
        RejectReason::RouteNotFound => GrpcCode::NotFound,
        RejectReason::IpRateLimited | RejectReason::RouteRateLimited => GrpcCode::ResourceExhausted,
        RejectReason::AuthMissing
        | RejectReason::AuthInvalidScheme
        | RejectReason::AuthInvalidToken => GrpcCode::Unauthenticated,
        RejectReason::AuthConfigError
        | RejectReason::PluginDenied
        | RejectReason::GrpcInvalidProtocol
        | RejectReason::GrpcInvalidContentType
        | RejectReason::GrpcInvalidPath => GrpcCode::Internal,
        RejectReason::UpstreamNotFound | RejectReason::NoHealthyUpstream => GrpcCode::Unavailable,
    }
}

pub fn parse_grpc_status(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")?
        .to_str()
        .ok()?
        .parse::<i32>()
        .ok()
}

pub fn parse_grpc_message(headers: &HeaderMap) -> Option<String> {
    headers
        .get("grpc-message")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

pub fn grpc_trailers(code: GrpcCode, message: impl Into<String>) -> HeaderMap {
    let message = message.into();
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_str(&code.as_i32().to_string()).unwrap(),
    );
    if !message.is_empty()
        && let Ok(value) = HeaderValue::from_str(&message)
    {
        trailers.insert("grpc-message", value);
    }
    trailers
}

pub fn grpc_trailers_only_response(
    code: GrpcCode,
    message: impl Into<String>,
) -> Response<BoxBody<Bytes, ProxyError>> {
    let trailers = grpc_trailers(code, message);

    let body = Empty::<Bytes>::new()
        .with_trailers(async move { Some(Ok(trailers)) })
        .map_err(|never| match never {})
        .boxed();

    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap()
}

pub fn grpc_frame(payload: &[u8]) -> Bytes {
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Bytes::from(frame)
}

pub fn decode_grpc_frame(bytes: &[u8]) -> Option<Bytes> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }

    let len = u32::from_be_bytes(bytes[1..5].try_into().ok()?) as usize;
    let end = 5usize.checked_add(len)?;
    if bytes.len() < end {
        return None;
    }

    Some(Bytes::copy_from_slice(&bytes[5..end]))
}

pub fn encode_health_check_request(service: Option<&str>) -> Bytes {
    let request = HealthCheckRequest {
        service: service.unwrap_or_default().to_string(),
    };
    let payload = request.encode_to_vec();
    grpc_frame(&payload)
}

pub fn decode_health_check_response(bytes: &[u8]) -> Option<HealthServingStatus> {
    let payload = decode_grpc_frame(bytes)?;
    let response = HealthCheckResponse::decode(payload).ok()?;
    HealthServingStatus::try_from(response.status).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{CONTENT_TYPE, HeaderValue};

    #[test]
    fn parses_grpc_service_method_path() {
        let parts = parse_grpc_path("/helloworld.Greeter/SayHello").unwrap();
        assert_eq!(parts.service, "helloworld.Greeter");
        assert_eq!(parts.method, "SayHello");
    }

    #[test]
    fn rejects_invalid_grpc_path_without_method() {
        assert!(parse_grpc_path("/helloworld.Greeter").is_none());
    }

    #[test]
    fn parses_grpc_timeout_header() {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-timeout", HeaderValue::from_static("1500m"));

        assert_eq!(
            parse_grpc_timeout(&headers),
            Some(Duration::from_millis(1500))
        );
    }

    #[test]
    fn rejects_invalid_grpc_timeout_header() {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-timeout", HeaderValue::from_static("100000000m"));

        assert_eq!(parse_grpc_timeout(&headers), None);
    }

    #[test]
    fn effective_timeout_uses_shorter_deadline() {
        assert_eq!(
            effective_grpc_timeout(
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(3)),
                true
            ),
            Some(Duration::from_secs(3))
        );
    }

    #[test]
    fn grpc_code_mapping_uses_expected_values() {
        assert_eq!(
            grpc_code_for_reject(RejectReason::RouteRateLimited),
            GrpcCode::ResourceExhausted
        );
        assert_eq!(
            grpc_code_for_reject(RejectReason::AuthInvalidToken),
            GrpcCode::Unauthenticated
        );
    }

    #[test]
    fn content_type_header_coexists_with_grpc_timeout_tests() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
        headers.insert("grpc-timeout", HeaderValue::from_static("5S"));

        assert_eq!(parse_grpc_timeout(&headers), Some(Duration::from_secs(5)));
    }

    #[test]
    fn encodes_and_decodes_grpc_frame() {
        let frame = grpc_frame(b"hello");
        let payload = decode_grpc_frame(&frame).unwrap();
        assert_eq!(payload.as_ref(), b"hello");
    }

    #[test]
    fn decodes_health_check_response_frame() {
        let payload = HealthCheckResponse {
            status: HealthServingStatus::Serving as i32,
        }
        .encode_to_vec();

        let status = decode_health_check_response(&grpc_frame(&payload)).unwrap();
        assert_eq!(status, HealthServingStatus::Serving);
    }
}
