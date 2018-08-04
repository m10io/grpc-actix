//! Traits for working with [`http::request::Builder`] and [`http::response::Builder`] instances.
//!
//! [`http::request::Builder`]: https://docs.rs/http/0.1/http/request/struct.Builder.html
//! [`http::response::Builder`]: https://docs.rs/http/0.1/http/response/struct.Builder.html

use http::{request, response};

use http::header::{HeaderName, HeaderValue};
use http::HttpTryFrom;

/// Interface for adding headers to a [`http::request::Builder`] or [`http::response::Builder`].
///
/// [`http::request::Builder`]: https://docs.rs/http/0.1/http/request/struct.Builder.html
/// [`http::response::Builder`]: https://docs.rs/http/0.1/http/response/struct.Builder.html
pub trait HeaderBuilder {
    /// Appends a header to the request/response.
    fn header<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        HeaderName: HttpTryFrom<K>,
        HeaderValue: HttpTryFrom<V>;
}

impl HeaderBuilder for request::Builder {
    #[inline]
    fn header<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        HeaderName: HttpTryFrom<K>,
        HeaderValue: HttpTryFrom<V>,
    {
        self.header(key, value)
    }
}

impl HeaderBuilder for response::Builder {
    #[inline]
    fn header<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        HeaderName: HttpTryFrom<K>,
        HeaderValue: HttpTryFrom<V>,
    {
        self.header(key, value)
    }
}
