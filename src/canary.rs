use hyper::HeaderMap;
use rand::RngExt;

use crate::config::RouteConfig;

/// Select target upstream based on canary rules
///
/// Priority:
/// 1. Header match (highest priority)
/// 2. Weight-based routing
/// 3. Default to main upstream
pub fn select_target_upstream<'a>(headers: &HeaderMap, route: &'a RouteConfig) -> (&'a str, bool) {
    if let Some(canary) = &route.canary {
        if let Some(key) = &canary.header_key
            && let Some(val) = headers.get(key)
        {
            if let Some(expected_val) = &canary.header_value {
                if val.as_bytes() == expected_val.as_bytes() {
                    return (&canary.upstream, true);
                }
            } else {
                return (&canary.upstream, true);
            }
        }

        if canary.weight > 0 {
            let random_val: u8 = rand::rng().random_range(1..=100);
            if random_val <= canary.weight {
                return (&canary.upstream, true);
            }
        }
    }

    (&route.upstream, false)
}
