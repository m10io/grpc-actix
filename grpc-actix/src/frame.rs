//! Encoding and decoding of raw message data in HTTP/2 frames.

use prost;

use super::status::*;

use bytes::{Buf, BufMut};

/// Decodes raw gRPC message bytes from the contents of an HTTP/2 data frame.
///
/// The gRPC specification allows for a completely empty HTTP/2 data frame to be sent at the end of
/// a streaming request in order to ensure that the last data frame has the `END_STREAM` flag
/// correctly set. This function assumes the frame given contains data, so handling of empty frames
/// should be performed prior to calling this function.
pub fn decode<M, D>(mut data: D) -> Result<M, Status>
where
    M: prost::Message + Default,
    D: Buf,
{
    // Make sure enough data is available for the message header (1-byte compression flag and 4-byte
    // message length).
    if data.remaining() < 5 {
        return Err(Status::new(
            StatusCode::Internal,
            Some("insufficient HTTP frame data for gRPC message header"),
        ));
    }

    let compression = data.get_u8();
    if compression > 1 {
        return Err(Status::new(
            StatusCode::Internal,
            Some(format!(
                "unexpected gRPC message compression flag '{}'",
                compression
            )),
        ));
    }

    // TODO: Support compression.
    if compression != 0 {
        return Err(Status::new(
            StatusCode::Unimplemented,
            Some("gRPC message compression not currently supported"),
        ));
    }

    let message_len = data.get_u32_be() as usize;
    if message_len > data.remaining() {
        return Err(Status::new(
            StatusCode::Internal,
            Some(format!(
                "gRPC message expected length {}, but only {} bytes were available",
                message_len,
                data.remaining()
            )),
        ));
    }

    M::decode(data.take(message_len)).map_err(|e| Status::from_display(StatusCode::Internal, e))
}

/// Encode raw gRPC message bytes into an HTTP/2 data frame.
pub fn encode<M, O>(message: &M, mut output: O) -> Result<(), Status>
where
    M: prost::Message,
    O: BufMut,
{
    // Need enough space for the message and its header.
    let message_len = message.encoded_len();
    if output.remaining_mut() < message_len + 5 {
        return Err(Status::new(
            StatusCode::Internal,
            Some(format!(
                concat!(
                    "insufficient space in output buffer for gRPC message (requires {} bytes, but ",
                    "only {} bytes available)",
                ),
                message_len + 5,
                output.remaining_mut(),
            )),
        ));
    }

    output.put_u8(0);
    output.put_u32_be(message_len as u32);
    message
        .encode(&mut output)
        .map_err(|e| Status::from_display(StatusCode::Internal, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::io::Cursor;

    /// Test protobuf message type.
    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        pub name: String,
    }

    impl TestMessage {
        pub fn new_test() -> Self {
            Self {
                name: String::from("test_message"),
            }
        }
    }

    /// Verifies `encode()` works as expected.
    #[test]
    fn encode_works() {
        let message = TestMessage::new_test();

        // We don't need to test `prost` message encoding, just our length-prefixed message
        // encoding, so we'll append the message bytes to the expected header.
        let mut protobuf_encoded = Vec::new();
        message.encode(&mut protobuf_encoded).unwrap();

        let message_len = protobuf_encoded.len() as u32;
        let mut expected: Vec<u8> = vec![
            0,
            (message_len >> 24) as u8,
            (message_len >> 16) as u8,
            (message_len >> 8) as u8,
            message_len as u8,
        ];
        expected.extend_from_slice(&protobuf_encoded);

        let mut encoded = Vec::new();
        encode(&message, &mut encoded).unwrap();
        assert_eq!(encoded, expected);
    }

    /// Verifies `decode()` works as expected.
    #[test]
    fn decode_works() {
        let message = TestMessage::new_test();

        // We don't need to test `prost` message encoding, just our length-prefixed message
        // encoding, so we'll append the message bytes to the expected header.
        let mut protobuf_encoded = Vec::new();
        message.encode(&mut protobuf_encoded).unwrap();

        let message_len = protobuf_encoded.len() as u32;
        let mut encoded: Vec<u8> = vec![
            0,
            (message_len >> 24) as u8,
            (message_len >> 16) as u8,
            (message_len >> 8) as u8,
            message_len as u8,
        ];
        encoded.extend_from_slice(&protobuf_encoded);

        let decoded: TestMessage = decode(Cursor::new(&encoded)).unwrap();
        assert_eq!(decoded, message);
    }

    /// Verifies that running `decode()` on the result of `encode()` yields the original message.
    #[test]
    fn encode_decode_works() {
        let message = TestMessage::new_test();

        let mut encoded = Vec::new();
        encode(&message, &mut encoded).unwrap();

        let decoded: TestMessage = decode(Cursor::new(&encoded)).unwrap();
        assert_eq!(decoded, message);
    }

    /// Verifies `decode()` fails on bad input.
    #[test]
    fn decode_fails() {
        let message = TestMessage::new_test();

        let mut encoded = Vec::new();
        encode(&message, &mut encoded).unwrap();
        encoded[1] += 1;

        decode::<TestMessage, _>(Cursor::new(&encoded))
            .expect_err("frame::decode() did not fail as expected");
    }
}
