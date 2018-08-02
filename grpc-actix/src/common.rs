//! Shared types used by both client and server code.

use std::collections::hash_map;

use std::collections::HashMap;

/// [`Metadata`] errors.
///
/// [`Metadata`]: struct.Metadata.html
#[derive(Debug, Eq, Fail, PartialEq)]
pub enum MetadataError {
    /// Key contains an invalid character.
    #[fail(display = "invalid key character at index {}", index)]
    InvalidKey { index: usize },

    /// Key is empty.
    #[fail(display = "empty key specified")]
    EmptyKey,

    /// ASCII value (key doesn't end with "-bin") contains an invalid ASCII character.
    #[fail(display = "invalid ASCII data character at index {}", index)]
    InvalidValue { index: usize },

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

    /// Returns whether a metadata key is a binary key.
    #[inline]
    fn is_binary_key(key: &str) -> bool {
        key.ends_with("-bin")
    }

    /// Checks whether a string contains a valid metadata key.
    fn validate_key(key: &str) -> Result<(), MetadataError> {
        if key.is_empty() {
            Err(MetadataError::EmptyKey)
        } else {
            for (index, &byte) in key.as_bytes().iter().enumerate() {
                if !Self::is_valid_key_byte(byte) {
                    return Err(MetadataError::InvalidKey { index });
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
                    return Err(MetadataError::InvalidValue { index });
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

    /// Verifies `Metadata::add_value()` returns `Metadata::InvalidKey` if the key contains an
    /// invalid character.
    #[test]
    fn metadata_add_value_invalid_key() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("abcD", &b"EFGH"[..]).unwrap_err(),
            MetadataError::InvalidKey { index: 3 }
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

    /// Verifies `Metadata::add_value()` returns `Metadata::InvalidValue` if the value contains an
    /// invalid character.
    #[test]
    fn metadata_add_value_invalid_value() {
        let mut metadata = Metadata::default();
        assert_eq!(
            metadata.add_value("abcd", &b"EF\tGH"[..]).unwrap_err(),
            MetadataError::InvalidValue { index: 2 }
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
