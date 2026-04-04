use hyper::body::Incoming;
use hyper::Request;
use std::future;
use tower::BoxError;
use tower::retry::Policy;

/// Proxy retry policy for upstream requests
#[derive(Clone)]
pub struct ProxyRetryPolicy {
    max_attempts: usize,
}

impl ProxyRetryPolicy {
    /// Create a new retry policy
    pub fn new(max_attempts: usize) -> Self {
        Self { max_attempts }
    }
}

impl<B, E> Policy<Request<B>, hyper::Response<Incoming>, E> for ProxyRetryPolicy
where
    B: Clone,
    E: Into<BoxError>,
{
    type Future = future::Ready<()>;

    fn retry(
        &mut self,
        _req: &mut Request<B>,
        result: &mut Result<hyper::Response<Incoming>, E>,
    ) -> Option<Self::Future> {
        if self.max_attempts == 0 {
            return None;
        }
        let should_retry = match result {
            Ok(res) => res.status().is_server_error(),
            Err(_) => true,
        };
        if should_retry {
            self.max_attempts -= 1;
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Request<B>) -> Option<Request<B>> {
        let mut builder = Request::builder()
            .method(req.method())
            .uri(req.uri())
            .version(req.version());
        for (k, v) in req.headers() {
            builder = builder.header(k, v);
        }
        builder.body(req.body().clone()).ok()
    }
}
