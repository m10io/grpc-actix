//! gRPC metadata support.

use base64;
use hyper;

use std::collections::hash_map;

use super::headers::Headers;
use super::status::*;
use bytes::Bytes;
use hyper::header::{HeaderName, HeaderValue};
use std::collections::HashMap;

/// [`Metadata`] errors.
///
/// [`Metadata`]: struct.Metadata.html
#[derive(Debug, Eq, Fail, PartialEq)]
pub enum MetadataError {
    /// Key contains an invalid character.
    #[fail(display = "invalid key character at index {}", index)]
    InvalidKeyCharacter { index: usize },

    /// Key is empty.
    #[fail(display = "empty key specified")]
    EmptyKey,

    /// Key is a reserved HTTP header name (either a standard HTTP header used by gRPC or a
    /// gRPC-specific header that starts with "grpc-").
    #[fail(display = "key is a reserved HTTP header name")]
    ReservedKey,

    /// ASCII value (key doesn't end with "-bin") contains an invalid ASCII character.
    #[fail(display = "invalid ASCII data character at index {}", index)]
    InvalidValueCharacter { index: usize },

    /// ASCII value (key doesn't end with "-bin") has whitespace padding.
    #[fail(display = "ASCII data has whitespace padding")]
    PaddedValue,
}

/// gRPC metadata.
///
/// Metadata allows clients and servers to pass extra information with gRPC requests and responses.
/// Aside from validity tests, gRPC ignores the contents of metadata.
///
/// Metadata is provided as a set of arbitrary key-value pairs. Keys are simple strings consisting
/// only of numbers (`0-9`), lower-case letters (`a-z`), underscores (`_`), dashes (`-`), and
/// periods (`.`). Values are either printable ASCII-text only (0x20-0x7E) without any leading or
/// trailing spaces or, if the key ends with "-bin", unrestricted binary data (note that binary data
/// is base64-encoded when transmitted over the network, so ASCII data is preferable if possible).
///
/// Multiple values can be stored with each key. Order of the keys is not guaranteed to be retained
/// during transmission, but the ordering of values within a given key will be retained.
#[derive(Default)]
pub struct Metadata {
    /// Key-value store.
    pairs: HashMap<String, Vec<Box<[u8]>>>,
}

impl Metadata {
    /// Adds a value for a given key.
    ///
    /// If the key already exists, the given value will be appended to its list of values.
    pub fn add_value<K, V>(&mut self, key: K, value: V) -> Result<(), MetadataError>
    where
        K: Into<String>,
        V: Into<Box<[u8]>> + AsRef<[u8]>,
    {
        let key = key.into();
        Self::validate_key(&key)?;

        let value = value.into();
        Self::validate_value(value.as_ref(), Self::is_binary_key(&key))?;

        match self.pairs.entry(key) {
            hash_map::Entry::Occupied(entry) => {
                entry.into_mut().push(value);
            }

            hash_map::Entry::Vacant(entry) => {
                let mut value_vec = Vec::new();
                value_vec.push(value);
                entry.insert(value_vec);
            }
        }

        Ok(())
    }

    /// Returns an iterator over all keys and the values associated with them.
    ///
    /// Values are provided as another iterator that returns a byte slice for each individual value.
    pub fn iter(&self) -> impl Iterator<Item = (&str, impl Iterator<Item = &[u8]>)> {
        self.pairs
            .iter()
            .map(|(key, values)| (key.as_str(), values.iter().map(|value| value.as_ref())))
    }

    /// Returns an iterator over all values for the given key.
    pub fn values<K>(&self, key: K) -> impl Iterator<Item = &[u8]>
    where
        K: AsRef<str>,
    {
        self.pairs
            .get(key.as_ref())
            .map_or_else(|| [].iter(), |values| values.iter())
            .map(|value| value.as_ref())
    }

    /// Returns the first value associated with a given key, or `None` if the key does not exist.
    pub fn get<K>(&self, key: K) -> Option<&[u8]>
    where
        K: AsRef<str>,
    {
        self.pairs
            .get(key.as_ref())
            .map(|values| values[0].as_ref())
    }

    /// Appends the metadata in this object to the headers of an HTTP request or response being
    /// built, consuming this object in the process.
    pub(crate) fn append_to_headers<H>(self, headers: &mut H) -> Result<(), Status>
    where
        H: Headers,
    {
        for (key, values) in self.pairs {
            let mut merged_values = Vec::new();
            if Self::is_binary_key(&key) {
                let mut value_iter = values.into_iter();
                let first = value_iter.next().unwrap();
                let mut value_string = base64::encode_config(&first, base64::STANDARD);
                merged_values.extend_from_slice(value_string.as_bytes());

                for value in value_iter {
                    // Re-use the string to minimize memory reallocations.
                    value_string.clear();
                    base64::encode_config_buf(&value, base64::STANDARD, &mut value_string);
                    merged_values.push(b',');
                    merged_values.extend_from_slice(value_string.as_bytes());
                }
            } else {
                let mut value_iter = values.into_iter();
                let first = value_iter.next().unwrap();
                merged_values.append(&mut Vec::from(first));

                for value in value_iter {
                    merged_values.push(b',');
                    merged_values.append(&mut Vec::from(value));
                }
            }

            let key = HeaderName::from_bytes(key.as_str().as_bytes()).map_err(|_| {
                Status::new(
                    StatusCode::Internal,
                    Some(format!(
                        "failed to convert metadata key '{}' to an HTTP header name",
                        key
                    )),
                )
            })?;
            let merged_values =
                HeaderValue::from_shared(Bytes::from(merged_values)).map_err(|_| {
                    Status::new(
                        StatusCode::Internal,
                        Some(format!(
                            "failed to convert metadata value for key '{}' to an HTTP header value",
                            key.as_str(),
                        )),
                    )
                })?;

            headers.append(key, merged_values);
        }

        Ok(())
    }

    /// Creates metadata from a set of HTTP headers.
    pub(crate) fn from_header_map(headers: &hyper::HeaderMap) -> Self {
        let mut metadata = Self::default();
        for (header_name, header_value) in headers.iter() {
            let header_name_str = header_name.as_str();

            // Ignore headers that aren't valid metadata key-value pairs.
            if Self::validate_key(header_name_str).is_err() {
                continue;
            }

            let is_binary = Self::is_binary_key(header_name_str);
            for value in header_value.as_bytes().split(|&byte| byte == b',') {
                // Values shouldn't be padded with whitespace, but we can trim off any found just in
                // case the remote gRPC implementation doesn't generate entirely valid output.
                let value_len = value.len();
                let mut start = 0;
                while start < value_len && Self::is_ascii_whitespace(value[start]) {
                    start += 1;
                }

                let end = if start == value_len {
                    value_len
                } else {
                    // Since we already checked for a zero-length value after stripping leading
                    // whitespace, we don't need to worry about reading past `start` while stripping
                    // trailing whitespace since we're guaranteed to encounter at least one
                    // non-whitespace character, and `value_len` is guaranteed to be non-zero.
                    let mut last = value_len - 1;
                    while Self::is_ascii_whitespace(value[last]) {
                        last -= 1;
                    }

                    debug_assert!(start <= last);

                    last + 1
                };

                // clippy false positive; see issue #2799.
                #[allow(unknown_lints, redundant_field_names)]
                let value = &value[start..end];
                let add_result = if is_binary {
                    match base64::decode_config(value, base64::STANDARD) {
                        Err(e) => {
                            warn!(
                                "Invalid base64-encoded data found in binary metadata value: {}.",
                                e
                            );
                            Ok(())
                        }

                        Ok(decoded) => metadata.add_value(header_name_str, decoded),
                    }
                } else {
                    metadata.add_value(header_name_str, value)
                };

                if let Err(e) = add_result {
                    warn!("Failed to add metadata: {}.", e);
                }
            }
        }

        metadata
    }

    /// Returns whether a metadata key is a binary key.
    #[inline]
    fn is_binary_key(key: &str) -> bool {
        key.ends_with("-bin")
    }

    /// Checks whether a string contains a valid metadata key.
    fn validate_key(key: &str) -> Result<(), MetadataError> {
        if key.is_empty() {
            Err(MetadataError::EmptyKey)
        } else if Self::is_key_standard_header_name(key) || Self::is_key_grpc_header_name(key) {
            Err(MetadataError::ReservedKey)
        } else {
            for (index, &byte) in key.as_bytes().iter().enumerate() {
                if !Self::is_valid_key_byte(byte) {
                    return Err(MetadataError::InvalidKeyCharacter { index });
                }
            }

            Ok(())
        }
    }

    /// Returns whether a byte is part of a valid metadata key.
    #[inline]
    fn is_valid_key_byte(byte: u8) -> bool {
        byte >= b'0' && byte <= b'9'
            || byte >= b'a' && byte <= b'z'
            || byte == b'_'
            || byte == b'-'
            || byte == b'.'
    }

    /// Returns whether a key contains a standard HTTP header name used by gRPC.
    #[inline]
    fn is_key_standard_header_name(key: &str) -> bool {
        key == "te" || key == "content-type" || key == "user-agent"
    }

    /// Returns whether a key is a gRPC-specific header.
    #[inline]
    fn is_key_grpc_header_name(key: &str) -> bool {
        key.starts_with("grpc-")
    }

    /// Checks whether a slice contains valid a metadata value.
    fn validate_value(value: &[u8], is_binary: bool) -> Result<(), MetadataError> {
        if is_binary || value.is_empty() {
            Ok(())
        } else if value[0] == b' ' || value[value.len() - 1] == b' ' {
            // We only need to check for regular space characters when checking for padding, as it
            // is the only valid whitespace character for ASCII values (other characters will fail
            // the subsequent test for valid characters).
            Err(MetadataError::PaddedValue)
        } else {
            for (index, &byte) in value.iter().enumerate() {
                if !Self::is_valid_ascii_value_byte(byte) {
                    return Err(MetadataError::InvalidValueCharacter { index });
                }
            }

            Ok(())
        }
    }

    /// Returns whether a byte is part of a valid ASCII metadata value.
    #[inline]
    fn is_valid_ascii_value_byte(byte: u8) -> bool {
        byte >= 0x20 && byte <= 0x7e
    }

    /// Returns whether a byte is any ASCII whitespace value (not just the space character).
    #[inline]
    fn is_ascii_whitespace(byte: u8) -> bool {
        // Include vertical tab (0x0b), form feed (0x0c), and Unicode next line (0x85), which don't
        // have corresponding escape sequences in Rust.
        byte == b'\t'
            || byte == b'\n'
            || byte == b'\r'
            || byte == b' '
            || byte == 0x0b
            || byte == 0x0c
            || byte == 0x85
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies `Metadata::add_value()` returns `MetadataError::EmptyKey` if the key is empty.
    #[test]
    fn metadata_add_value_empty_key() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("", &b"EFGH"[..]).unwrap_err(),
            MetadataError::EmptyKey
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` returns `Metadata::InvalidKeyCharacter` if the key contains
    /// an invalid character.
    #[test]
    fn metadata_add_value_invalid_key() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("abcD", &b"EFGH"[..]).unwrap_err(),
            MetadataError::InvalidKeyCharacter { index: 3 }
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` returns `Metadata::ReservedKey` if the key is a standard
    /// HTTP header used in gRPC calls.
    #[test]
    fn metadata_add_value_reserved_key_standard() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata
                .add_value("content-type", &b"application/grpc"[..])
                .unwrap_err(),
            MetadataError::ReservedKey
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` returns `Metadata::ReservedKey` if the key begins with
    /// "grpc-".
    #[test]
    fn metadata_add_value_reserved_key_grpc() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("grpc-abcd", &b"EFGH"[..]).unwrap_err(),
            MetadataError::ReservedKey
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` returns `Metadata::PaddedValue` if the value contains any
    /// whitespace padding.
    #[test]
    fn metadata_add_value_padded_value() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("abcd", &b" EFGH "[..]).unwrap_err(),
            MetadataError::PaddedValue
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` returns `Metadata::InvalidValueCharacter` if the value
    /// contains an invalid character.
    #[test]
    fn metadata_add_value_invalid_value() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("abcd", &b"EF\tGH"[..]).unwrap_err(),
            MetadataError::InvalidValueCharacter { index: 2 }
        );
        assert!(metadata.iter().next().is_none());
    }

    /// Verifies `Metadata::add_value()` works with valid ASCII values.
    #[test]
    fn metadata_add_value_ascii() {
        let mut metadata = Metadata::default();
        assert!(metadata.add_value("abcd", &b"EFGH"[..]).is_ok());
        assert!(metadata.add_value("abcd", &b"IJKL"[..]).is_ok());

        let mut values = metadata.values("abcd");
        assert_eq!(values.next(), Some(&b"EFGH"[..]));
        assert_eq!(values.next(), Some(&b"IJKL"[..]));
        assert_eq!(values.next(), None);
    }

    /// Verifies `Metadata::add_value()` works with binary values.
    #[test]
    fn metadata_add_value_binary() {
        let mut metadata = Metadata::default();
        assert!(metadata.add_value("abcd-bin", &b" EFGH "[..]).is_ok());
        assert!(metadata.add_value("abcd-bin", &b"IJ\tKL"[..]).is_ok());

        let mut values = metadata.values("abcd-bin");
        assert_eq!(values.next(), Some(&b" EFGH "[..]));
        assert_eq!(values.next(), Some(&b"IJ\tKL"[..]));
        assert_eq!(values.next(), None);
    }
}
