use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::RouteConfig;
use crate::error::ProxyError;

/// JWT claims
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (typically UserID)
    pub sub: String,
    /// Expiration time
    pub exp: Option<usize>,
}

/// Check authentication for a request
///
/// Returns:
/// - Ok(Some(claims)) -> Auth success, returns user info
/// - Ok(None) -> Route doesn't require auth
/// - Err(response) -> Auth failed, return 401 response
pub fn check_auth(
    req: &Request<Incoming>,
    route: &RouteConfig,
    jwt_key_opt: &Option<Arc<DecodingKey>>,
) -> Result<Option<Claims>, Response<http_body_util::combinators::BoxBody<Bytes, ProxyError>>> {
    // 1. If route doesn't require auth, allow through
    if !route.auth {
        return Ok(None);
    }

    // 2. If route requires auth but no secret configured, it's a config error
    let key = match jwt_key_opt {
        Some(k) => k,
        None => {
            tracing::error!("Route requires auth but no jwt_secret configured");
            return Err(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(
                    Full::new(Bytes::from("Auth Configuration Error"))
                        .map_err(|e| match e {})
                        .boxed(),
                )
                .unwrap());
        }
    };

    // 3. Extract Authorization: Bearer <token>
    let auth_header = match req.headers().get("Authorization") {
        Some(h) => h.to_str().unwrap_or(""),
        None => return Err(make_401("Missing Authorization Header")),
    };

    if !auth_header.starts_with("Bearer ") {
        return Err(make_401("Invalid Auth Scheme"));
    }

    let token = &auth_header[7..]; // Remove "Bearer "

    // 4. Validate token
    let validation = Validation::new(Algorithm::HS256);

    match jsonwebtoken::decode::<Claims>(token, key, &validation) {
        Ok(token_data) => Ok(Some(token_data.claims)),
        Err(e) => {
            tracing::warn!("JWT Validation Failed: {:?}", e);
            Err(make_401("Invalid Token"))
        }
    }
}

/// Create a 401 Unauthorized response
pub fn make_401(
    msg: &'static str,
) -> Response<http_body_util::combinators::BoxBody<Bytes, ProxyError>> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Full::new(Bytes::from(msg)).map_err(|e| match e {}).boxed())
        .unwrap()
}
