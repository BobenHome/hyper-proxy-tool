use std::error::Error;

/// Generic proxy error type
pub type ProxyError = Box<dyn Error + Send + Sync + 'static>;

/// Create a proxy error from a message
pub fn error<T: Into<String>>(msg: T) -> ProxyError {
    Box::new(std::io::Error::other(msg.into()))
}
