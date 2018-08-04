//! gRPC status types.

use hyper;

use std::{error, fmt};

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
