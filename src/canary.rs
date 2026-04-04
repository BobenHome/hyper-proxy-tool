use hyper::body::Incoming;
use hyper::Request;
use rand::RngExt;

use crate::config::RouteConfig;

/// Select target upstream based on canary rules
///
/// Priority:
/// 1. Header match (highest priority)
/// 2. Weight-based routing
/// 3. Default to main upstream
pub fn select_target_upstream<'a>(req: &Request<Incoming>, route: &'a RouteConfig) -> &'a str {
    // 1. Check if canary config exists
    if let Some(canary) = &route.canary {
        // 2. Header match rule (highest priority)
        if let Some(key) = &canary.header_key
            && let Some(val) = req.headers().get(key)
        {
            // If specific value is configured, must match exactly
            if let Some(expected_val) = &canary.header_value {
                if val.as_bytes() == expected_val.as_bytes() {
                    return &canary.upstream;
                }
            } else {
                // If no value configured, key presence is enough
                return &canary.upstream;
            }
        }

        // 3. Weight-based routing (lower priority)
        if canary.weight > 0 {
            let random_val: u8 = rand::rng().random_range(1..=100);
            if random_val <= canary.weight {
                return &canary.upstream;
            }
        }
    }

    // 4. Default to main upstream
    &route.upstream
}
