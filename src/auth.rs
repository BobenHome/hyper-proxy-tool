use hyper::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::RouteConfig;

/// JWT claims
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// Subject (typically UserID)
    pub sub: String,
    /// Expiration time
    pub exp: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    MissingHeader,
    InvalidScheme,
    InvalidToken,
    ConfigError,
}

/// Validate Authorization headers in a protocol-agnostic way.
pub fn validate_auth_headers(
    headers: &HeaderMap,
    route: &RouteConfig,
    jwt_key_opt: &Option<Arc<DecodingKey>>,
) -> Result<Option<Claims>, AuthError> {
    if !route.auth {
        return Ok(None);
    }

    let key = match jwt_key_opt {
        Some(k) => k,
        None => {
            tracing::error!("Route requires auth but no jwt_secret configured");
            return Err(AuthError::ConfigError);
        }
    };

    let auth_header = match headers.get("Authorization") {
        Some(h) => h.to_str().unwrap_or(""),
        None => return Err(AuthError::MissingHeader),
    };

    if !auth_header.starts_with("Bearer ") {
        return Err(AuthError::InvalidScheme);
    }

    let token = &auth_header[7..];
    let validation = Validation::new(Algorithm::HS256);

    match jsonwebtoken::decode::<Claims>(token, key, &validation) {
        Ok(token_data) => Ok(Some(token_data.claims)),
        Err(e) => {
            tracing::warn!("JWT Validation Failed: {:?}", e);
            Err(AuthError::InvalidToken)
        }
    }
}
