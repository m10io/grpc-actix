//! gRPC status types.

use hyper;

use actix::prelude::*;

use std::{error, fmt};

use bytes::{Buf, BufMut};
use std::borrow::Cow;

/// Known gRPC status codes (copied from `include/grpc/impl/codegen/status.h` in the gRPC repo).
///
/// Note that implementations may use codes other than those specified here, so code should not
/// attempt to cast a status code received in a request/response to a `Status` enum.
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(u32)]
pub enum StatusCode {
    /// Not an error; returned on success.
    Ok = 0,

    /// The operation was cancelled (typically by the caller).
    Cancelled = 1,

    /// Unknown error.  An example of where this error may be returned is
    /// if a Status value received from another address space belongs to
    /// an error-space that is not known in this address space.  Also
    /// errors raised by APIs that do not return enough error information
    /// may be converted to this error.
    Unknown = 2,

    /// Client specified an invalid argument.  Note that this differs
    /// from FAILED_PRECONDITION.  INVALID_ARGUMENT indicates arguments
    /// that are problematic regardless of the state of the system
    /// (e.g., a malformed file name).
    InvalidArgument = 3,

    /// Deadline expired before operation could complete.  For operations
    /// that change the state of the system, this error may be returned
    /// even if the operation has completed successfully.  For example, a
    /// successful response from a server could have been delayed long
    /// enough for the deadline to expire.
    DeadlineExceeded = 4,

    /// Some requested entity (e.g., file or directory) was not found.
    NotFound = 5,

    /// Some entity that we attempted to create (e.g., file or directory)
    /// already exists.
    AlreadyExists = 6,

    /// The caller does not have permission to execute the specified
    /// operation.  PERMISSION_DENIED must not be used for rejections
    /// caused by exhausting some resource (use RESOURCE_EXHAUSTED
    /// instead for those errors).  PERMISSION_DENIED must not be
    /// used if the caller can not be identified (use UNAUTHENTICATED
    /// instead for those errors).
    PermissionDenied = 7,

    /// The request does not have valid authentication credentials for the
    /// operation.
    Unauthenticated = 16,

    /// Some resource has been exhausted, perhaps a per-user quota, or
    /// perhaps the entire file system is out of space.
    ResourceExhausted = 8,

    /// Operation was rejected because the system is not in a state
    /// required for the operation's execution.  For example, directory
    /// to be deleted may be non-empty, an rmdir operation is applied to
    /// a non-directory, etc.
    /// A litmus test that may help a service implementor in deciding
    /// between FAILED_PRECONDITION, ABORTED, and UNAVAILABLE:
    ///  (a) Use UNAVAILABLE if the client can retry just the failing call.
    ///  (b) Use ABORTED if the client should retry at a higher-level
    ///      (e.g., restarting a read-modify-write sequence).
    ///  (c) Use FAILED_PRECONDITION if the client should not retry until
    ///      the system state has been explicitly fixed.  E.g., if an "rmdir"
    ///      fails because the directory is non-empty, FAILED_PRECONDITION
    ///      should be returned since the client should not retry unless
    ///      they have first fixed up the directory by deleting files from it.
    ///  (d) Use FAILED_PRECONDITION if the client performs conditional
    ///      REST Get/Update/Delete on a resource and the resource on the
    ///      server does not match the condition. E.g., conflicting
    ///      read-modify-write on the same resource.
    FailedPrecondition = 9,

    /// The operation was aborted, typically due to a concurrency issue
    /// like sequencer check failures, transaction aborts, etc.
    /// See litmus test above for deciding between FAILED_PRECONDITION,
    /// ABORTED, and UNAVAILABLE.
    Aborted = 10,

    /// Operation was attempted past the valid range.  E.g., seeking or
    /// reading past end of file.
    /// Unlike INVALID_ARGUMENT, this error indicates a problem that may
    /// be fixed if the system state changes. For example, a 32-bit file
    /// system will generate INVALID_ARGUMENT if asked to read at an
    /// offset that is not in the range [0,2^32-1], but it will generate
    /// OUT_OF_RANGE if asked to read from an offset past the current
    /// file size.
    /// There is a fair bit of overlap between FAILED_PRECONDITION and
    /// OUT_OF_RANGE.  We recommend using OUT_OF_RANGE (the more specific
    /// error) when it applies so that callers who are iterating through
    /// a space can easily look for an OUT_OF_RANGE error to detect when
    /// they are done.
    OutOfRange = 11,

    /// Operation is not implemented or not supported/enabled in this service.
    Unimplemented = 12,

    /// Internal errors.  Means some invariants expected by underlying
    /// system has been broken.  If you see one of these errors,
    /// something is very broken.
    Internal = 13,

    /// The service is currently unavailable.  This is a most likely a
    /// transient condition and may be corrected by retrying with
    /// a backoff.
    /// WARNING: Although data MIGHT not have been transmitted when this
    /// status occurs, there is NOT A GUARANTEE that the server has not seen
    /// anything. So in general it is unsafe to retry on this status code
    /// if the call is non-idempotent.
    /// See litmus test above for deciding between FAILED_PRECONDITION,
    /// ABORTED, and UNAVAILABLE.
    Unavailable = 14,

    /// Unrecoverable data loss or corruption.
    DataLoss = 15,
}

impl Into<u32> for StatusCode {
    fn into(self) -> u32 {
        self as u32
    }
}

/// gRPC status code and message.
#[derive(Debug, Default)]
pub struct Status {
    /// Status code.
    pub code: u32,
    /// Status message.
    pub message: Option<String>,
}

impl Status {
    /// Creates a `Status` with the given status code and optional message string.
    #[inline]
    pub fn new<C, S>(code: C, message: Option<S>) -> Self
    where
        C: Into<u32>,
        S: Into<String>,
    {
        Self {
            code: code.into(),
            message: message.map(S::into),
        }
    }

    /// Creates a `Status` from a status code and any type that implements [`Display`].
    ///
    /// [`Display`]: https://doc.rust-lang.org/std/fmt/trait.Display.html
    #[inline]
    pub fn from_display<C, D>(code: C, display: D) -> Self
    where
        C: Into<u32>,
        D: fmt::Display,
    {
        Self {
            code: code.into(),
            message: Some(format!("{}", display)),
        }
    }

    /// Creates a `Status` from an [`actix::MailboxError`] with a [`StatusCode::Internal`] code and
    /// the given context information included in its message string.
    ///
    /// [`actix::MailboxError`]: https://docs.rs/actix/0.7/actix/enum.MailboxError.html
    /// [`StatusCode::Internal`]: enum.StatusCode.html#variant.Internal
    pub fn from_mailbox_error(error: &MailboxError, message_name: &str, target_name: &str) -> Self {
        Self {
            code: StatusCode::Internal.into(),
            message: Some(format!(
                "failed to send '{}' message to '{}': {}",
                message_name, target_name, error
            )),
        }
    }

    // Converts Status to a HeaderValue
    pub fn to_header_value(&self) -> Result<hyper::header::HeaderValue, Status> {
        hyper::header::HeaderValue::from_bytes(format!("{}", self.code).as_str().as_bytes())
            .map_err(|_| {
                Status::new(
                    StatusCode::Internal,
                    Some("failed to parse status code as an HTTP header value"),
                )
            })
    }
}

impl From<hyper::Error> for Status {
    fn from(error: hyper::Error) -> Self {
        let code = if error.is_parse() || error.is_user() {
            StatusCode::Internal
        } else if error.is_canceled() || error.is_closed() {
            StatusCode::Cancelled
        } else {
            StatusCode::Unknown
        };

        Status::from_display(code, error)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.message {
            None => write!(f, "gRPC status {}", self.code),
            Some(message) => write!(f, "gRPC status {} - {}", self.code, message),
        }
    }
}

// Implement `Error` for `Status` to allow it to be used as the error type for futures and streams
// passed to `hyper` functions.
impl error::Error for Status {}

/// Percent-encodes a gRPC status message string as per the gRPC specification.
///
/// Bytes containing printable, non-space ASCII characters other than the percent sign (0x20-0x7E,
/// excluding 0x25) are passed as-is. All remaining bytes are written out as their two-digit hex
/// equivalent, prefixed with a percent sign (e.g. a percent sign character itself is written out as
/// `%25`).
pub(crate) fn percent_encode<B>(message: &str, mut output: B)
where
    B: BufMut,
{
    for &byte in message.as_bytes() {
        if byte >= 0x20 && byte <= 0x7e && byte != 0x25 {
            output.put_u8(byte);
        } else {
            output.put_u8(b'%');
            output.put_u8(nibble_to_hex(byte >> 4));
            output.put_u8(nibble_to_hex(byte & 0xf));
        }
    }
}

/// Percent-decodes a gRPC status message string as per the gRPC specification.
///
/// When decoding, any byte containing a percent sign is expected to be followed by a two-digit hex
/// value specifying the value of the encoded byte. While [`percent_encode()`] only encodes specific
/// values, this function will decode any parsed hex value.
///
/// The decoded bytes are converted to a UTF-8 string. The gRPC specification states that invalid
/// values must not be discarded, at worst simply returning the original percent-encoded string.
/// This function will insert `U+FFFD REPLACEMENT CHARACTER` (`�`) for any invalid percent-encoded
/// values as well as any invalid decoded UTF-8 sequences.
///
/// [`percent_encode()`]: fn.percent_encode.html
pub(crate) fn percent_decode<B>(input: B) -> String
where
    B: Buf,
{
    let replacement = "�";

    // Only perform percent decoding first.
    let mut decoded = Vec::with_capacity(input.remaining());
    let mut input_iter = input.iter().fuse();
    while let Some(byte) = input_iter.next() {
        if byte != b'%' {
            decoded.push(byte);
            continue;
        }

        // Extract both characters for the percent-encoded byte value before attempting to decode
        // them into nibble values so that we don't accidentally treat the low-nibble character in
        // the pair as its own character in the next loop iteration if the high-nibble character is
        // invalid.
        if let Some(hex_hi) = input_iter.next() {
            if let Some(hex_lo) = input_iter.next() {
                if let Some(hi) = hex_to_nibble(hex_hi) {
                    if let Some(lo) = hex_to_nibble(hex_lo) {
                        decoded.push((hi << 4) | lo);
                        continue;
                    }
                }
            }
        }

        decoded.extend_from_slice(replacement.as_bytes());
    }

    // Generate a valid UTF-8 string with invalid sequences converted to the replacement character.
    match String::from_utf8_lossy(decoded.as_slice()) {
        Cow::Borrowed(_) => {
            // Since we've been returned a reference back to the original bytes in `decoded`, we
            // know that it contains entirely valid UTF-8 data, so we can safely call
            // `String::from_utf8_unchecked()` at this point to directly convert `decoded` to a
            // `String` in order to save unnecessary memory reallocations (i.e.
            // `Cow::into_owned()`) or redundant UTF-8 checks (i.e. `String::from_utf8()`).
            unsafe { String::from_utf8_unchecked(decoded) }
        }

        Cow::Owned(valid_string) => valid_string,
    }
}

/// Returns the upper-case hex ASCII character for a nibble value.
#[inline]
fn nibble_to_hex(value: u8) -> u8 {
    debug_assert!(value < 16);

    let offset = if value < 10 { b'0' } else { b'A' - 10 };

    value + offset
}

/// Returns the nibble value for a hex ASCII character (upper- or lower-case).
///
/// If `hex` is not a valid hex character, `None` will be returned.
#[inline]
fn hex_to_nibble(hex: u8) -> Option<u8> {
    if hex >= b'0' && hex <= b'9' {
        Some(hex - b'0')
    } else if hex >= b'a' && hex <= b'f' {
        Some(hex - (b'a' - 10))
    } else if hex >= b'A' && hex <= b'F' {
        Some(hex - (b'A' - 10))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Verifies `percent_encode()` works as expected.
    #[test]
    fn percent_encode_works() {
        let test = "ab%cd¢☺⊙";
        let expected = "ab%25cd%C2%A2%E2%98%BA%E2%8A%99";

        let mut encoded = Vec::new();
        percent_encode(test, &mut encoded);
        assert_eq!(encoded.as_slice(), expected.as_bytes());
    }

    /// Verifies `percent_decode()` works as expected.
    #[test]
    fn percent_decode_works() {
        // `percent_decode()` should work with lower-case hex and characters that did not need to be
        // percent-encoded.
        let test = "%61b%25cd%C2%a2%E2%98%Ba%e2%8A%99";
        let expected = "ab%cd¢☺⊙";

        let decoded = percent_decode(Cursor::new(test.as_bytes()));
        assert_eq!(decoded, expected);
    }

    /// Verifies that running `percent_decode()` on the result of `percent_encode()` yields the
    /// original string.
    #[test]
    fn percent_encode_decode_works() {
        let test = "ab%cd¢☺⊙";

        let mut encoded = Vec::new();
        percent_encode(test, &mut encoded);

        let decoded = percent_decode(Cursor::new(encoded));
        assert_eq!(decoded, test);
    }

    /// Verifies `percent_decode()` replaces invalid percent-encoded sequences with the Unicode
    /// replacement character.
    #[test]
    fn percent_decode_invalid_percent_encoding() {
        let test = "%61b%R5cd%GQ%E2%98%Ba%e2%8A%99";
        let expected = "ab�cd�☺⊙";

        let decoded = percent_decode(Cursor::new(test.as_bytes()));
        assert_eq!(decoded, expected);
    }

    /// Verifies `percent_decode()` replaces invalid UTF-8 sequences with the Unicode replacement
    /// character.
    #[test]
    fn percent_decode_invalid_utf8() {
        let test = "%61b%25cd%C2%a2%22%98%Ba%e2%8A%99";
        let expected = "ab%cd¢\"��⊙";

        let decoded = percent_decode(Cursor::new(test.as_bytes()));
        assert_eq!(decoded, expected);
    }
}
