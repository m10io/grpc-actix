//! Traits for appending HTTP headers to requests, responses, and trailers.

use http::{request, response};

use http::header::{HeaderName, HeaderValue};
use hyper::HeaderMap;

/// Interface for adding headers to a request, response, or trailer.
pub trait Headers {
    /// Appends a header.
    fn append(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self;
}

impl Headers for request::Builder {
    #[inline]
    fn append(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        self.header(name, value)
    }
}

impl Headers for response::Builder {
    #[inline]
    fn append(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        self.header(name, value)
    }
}

impl Headers for HeaderMap {
    #[inline]
    fn append(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        self.append(name, value);
        self
    }
}
