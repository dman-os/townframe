//! The framed Unix descriptor transport for ADR 042 §4.
//! A request is `[u32 header_len][version][u16 operation_len][u32 params_len][u16 fd_count]` followed by UTF-8 operation bytes and JSON parameters. The entire frame is at most 64 KiB, and every descriptor is attached with the first frame bytes via `SCM_RIGHTS`.
//! Responses use `[u32 frame_len][version][status][u32 result_count][u32 payload_len]` followed by JSON.

#![cfg(unix)]

use rustix::fd::AsFd;
use rustix::fd::BorrowedFd;
use rustix::fd::OwnedFd;
use rustix::io::IoSlice;
use rustix::io::IoSliceMut;
use rustix::net::RecvAncillaryBuffer;
use rustix::net::RecvAncillaryMessage;
use rustix::net::RecvFlags;
use rustix::net::ReturnFlags;
use rustix::net::SendAncillaryBuffer;
use rustix::net::SendAncillaryMessage;
use serde_json::Value;

use std::io;
use std::mem::MaybeUninit;

/// Maximum encoded request or response frame size.
pub const MAX_HEADER_OCTETS: usize = 64 * 1024;
/// Maximum descriptors carried by one request.
pub const MAX_DESCRIPTORS: usize = 64;
const VERSION: u8 = 1;
const PREFIX_OCTETS: usize = 4;
const FIXED_HEADER_OCTETS: usize = 1 + 2 + 4 + 2;

/// A decoded descriptor request and its owned descriptor body.
pub struct DescriptorFrame {
    pub operation: String,
    pub parameters: Value,
    pub body: DescriptorBody,
}

impl std::fmt::Debug for DescriptorFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescriptorFrame")
            .field("operation", &self.operation)
            .field("parameters", &self.parameters)
            .field("body", &self.body)
            .finish()
    }
}

/// The bytes and ancillary data returned by one `recvmsg` call.
pub struct ReceivedPart {
    pub bytes: Vec<u8>,
    pub descriptors: Vec<OwnedFd>,
    pub ancillary_truncated: bool,
    pub unexpected_ancillary: bool,
}

/// Encodes one descriptor request frame.
pub fn encode_request(
    operation: &str,
    parameters: &Value,
    descriptor_count: usize,
) -> io::Result<Vec<u8>> {
    let operation = operation.as_bytes();
    let parameters = serde_json::to_vec(parameters)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let descriptor_count = u16::try_from(descriptor_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many descriptors"))?;
    let operation_len = u16::try_from(operation.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "operation name is too long"))?;
    let parameters_len = u32::try_from(parameters.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "parameters are too long"))?;
    let header_len = FIXED_HEADER_OCTETS
        .checked_add(operation.len())
        .and_then(|length| length.checked_add(parameters.len()))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "frame length overflow"))?;
    if header_len > MAX_HEADER_OCTETS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor frame header is too long",
        ));
    }
    let mut frame = Vec::with_capacity(PREFIX_OCTETS + header_len);
    frame.extend_from_slice(&(header_len as u32).to_be_bytes());
    frame.push(VERSION);
    frame.extend_from_slice(&operation_len.to_be_bytes());
    frame.extend_from_slice(&parameters_len.to_be_bytes());
    frame.extend_from_slice(&descriptor_count.to_be_bytes());
    frame.extend_from_slice(operation);
    frame.extend_from_slice(&parameters);
    Ok(frame)
}

/// Decodes a request frame and verifies its descriptor count.
pub fn decode_request(bytes: &[u8], descriptors: Vec<OwnedFd>) -> io::Result<DescriptorFrame> {
    if bytes.len() < PREFIX_OCTETS + FIXED_HEADER_OCTETS {
        return Err(protocol_error(
            "descriptor frame is shorter than its header",
        ));
    }
    let header_len = u32::from_be_bytes(bytes[..PREFIX_OCTETS].try_into().unwrap()) as usize;
    if header_len > MAX_HEADER_OCTETS || bytes.len() != PREFIX_OCTETS + header_len {
        return Err(protocol_error("descriptor frame length is invalid"));
    }
    let header = &bytes[PREFIX_OCTETS..];
    if header[0] != VERSION {
        return Err(protocol_error("unsupported descriptor frame version"));
    }
    let operation_len = u16::from_be_bytes(header[1..3].try_into().unwrap()) as usize;
    let parameters_len = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;
    let descriptor_count = u16::from_be_bytes(header[7..9].try_into().unwrap()) as usize;
    if descriptor_count > MAX_DESCRIPTORS || descriptor_count != descriptors.len() {
        return Err(protocol_error("descriptor count does not match the frame"));
    }
    let data_start = FIXED_HEADER_OCTETS;
    let operation_end = data_start
        .checked_add(operation_len)
        .ok_or_else(|| protocol_error("operation length overflows the frame"))?;
    let parameters_end = operation_end
        .checked_add(parameters_len)
        .ok_or_else(|| protocol_error("parameter length overflows the frame"))?;
    if parameters_end != header.len() {
        return Err(protocol_error(
            "descriptor frame fields do not fill the frame",
        ));
    }
    let operation = std::str::from_utf8(&header[data_start..operation_end])
        .map_err(|_| protocol_error("operation is not UTF-8"))?
        .to_owned();
    let parameters = serde_json::from_slice(&header[operation_end..parameters_end])
        .map_err(|error| protocol_error(&format!("parameters are not JSON: {error}")))?;
    Ok(DescriptorFrame {
        operation,
        parameters,
        body: DescriptorBody::from_fds(descriptors),
    })
}

/// Receives bytes and any `SCM_RIGHTS` descriptors from one socket read.
pub fn recv_part<Fd: AsFd>(fd: Fd, buffer: &mut [u8]) -> io::Result<ReceivedPart> {
    let mut control = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_DESCRIPTORS))];
    let mut ancillary = RecvAncillaryBuffer::new(&mut control);
    let mut iov = [IoSliceMut::new(buffer)];
    let message = rustix::net::recvmsg(fd, &mut iov, &mut ancillary, RecvFlags::CMSG_CLOEXEC)?;
    let mut descriptors = Vec::new();
    let mut unexpected_ancillary = false;
    for message in ancillary.drain() {
        match message {
            RecvAncillaryMessage::ScmRights(fds) => descriptors.extend(fds),
            _ => unexpected_ancillary = true,
        }
    }
    Ok(ReceivedPart {
        bytes: buffer[..message.bytes].to_vec(),
        descriptors,
        ancillary_truncated: message.flags.contains(ReturnFlags::CTRUNC),
        unexpected_ancillary,
    })
}

/// Sends bytes and, when present, attaches descriptors with `SCM_RIGHTS`.
pub fn send_part<Fd: AsFd>(fd: Fd, bytes: &[u8], descriptors: &[OwnedFd]) -> io::Result<usize> {
    let iov = [IoSlice::new(bytes)];
    if descriptors.is_empty() {
        return Ok(rustix::net::sendmsg(
            fd,
            &iov,
            &mut SendAncillaryBuffer::default(),
            rustix::net::SendFlags::empty(),
        )?);
    }
    let borrowed: Vec<BorrowedFd<'_>> = descriptors.iter().map(AsFd::as_fd).collect();
    let mut control = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_DESCRIPTORS))];
    let mut ancillary = SendAncillaryBuffer::new(&mut control);
    if !ancillary.push(SendAncillaryMessage::ScmRights(&borrowed)) {
        return Err(protocol_error("descriptor ancillary data does not fit"));
    }
    Ok(rustix::net::sendmsg(
        fd,
        &iov,
        &mut ancillary,
        rustix::net::SendFlags::empty(),
    )?)
}

/// Encodes one descriptor response frame.
pub fn encode_response(status: u8, result_count: usize, payload: &Value) -> io::Result<Vec<u8>> {
    if status > 1 || (status != 0 && result_count != 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid descriptor response status or result count",
        ));
    }
    let payload = serde_json::to_vec(payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let result_count = u32::try_from(result_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many results"))?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "response is too long"))?;
    let frame_len = 1 + 1 + 4 + 4 + payload.len();
    if frame_len > MAX_HEADER_OCTETS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor response is too long",
        ));
    }
    let mut frame = Vec::with_capacity(PREFIX_OCTETS + frame_len);
    frame.extend_from_slice(&(frame_len as u32).to_be_bytes());
    frame.extend_from_slice(&[VERSION, status]);
    frame.extend_from_slice(&result_count.to_be_bytes());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decodes and validates one descriptor response frame.
pub fn decode_response(bytes: &[u8]) -> io::Result<(u8, usize, Value)> {
    if bytes.len() < PREFIX_OCTETS + 10 {
        return Err(protocol_error(
            "descriptor response is shorter than its header",
        ));
    }
    let frame_len = u32::from_be_bytes(bytes[..PREFIX_OCTETS].try_into().unwrap()) as usize;
    if frame_len > MAX_HEADER_OCTETS || bytes.len() != PREFIX_OCTETS + frame_len {
        return Err(protocol_error("descriptor response length is invalid"));
    }
    let frame = &bytes[PREFIX_OCTETS..];
    if frame[0] != VERSION {
        return Err(protocol_error("unsupported descriptor response version"));
    }
    let status = frame[1];
    if status > 1 {
        return Err(protocol_error("unsupported descriptor response status"));
    }
    let result_count = u32::from_be_bytes(frame[2..6].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(frame[6..10].try_into().unwrap()) as usize;
    if status != 0 && result_count != 0 {
        return Err(protocol_error("refusal response has results"));
    }
    if payload_len != frame.len() - 10 {
        return Err(protocol_error(
            "descriptor response payload length is invalid",
        ));
    }
    let payload = serde_json::from_slice(&frame[10..])
        .map_err(|error| protocol_error(&format!("descriptor response is not JSON: {error}")))?;
    Ok((status, result_count, payload))
}

fn protocol_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Owned file descriptors carried by the descriptor transport. They remain owned until a provider
/// consumes them, so every malformed-frame path closes them by dropping this value. Construct this
/// body only at the descriptor edge or in an embedded in-process client, never from serialized data.
#[derive(Debug)]
pub struct DescriptorBody {
    descriptors: Vec<OwnedFd>,
}

impl DescriptorBody {
    /// Constructs a body at the descriptor edge or embedded in-process client.
    /// It must never be constructed from serialized transport data.
    pub fn from_fds(descriptors: Vec<OwnedFd>) -> Self {
        Self { descriptors }
    }

    /// Returns the number of owned descriptors.
    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    /// Returns whether no descriptors are owned.
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }

    /// Transfers ownership of the descriptors to the caller.
    pub fn into_fds(self) -> Vec<OwnedFd> {
        self.descriptors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    #[test]
    fn pipe_batch_preserves_all_descriptors() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        let first: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
        let second: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
        let frame = encode_request("org.example.importFile", &serde_json::json!({}), 2).unwrap();
        send_part(&sender, &frame, &[first, second]).unwrap();

        let mut buffer = vec![0; MAX_HEADER_OCTETS + PREFIX_OCTETS];
        let received = recv_part(&receiver, &mut buffer).unwrap();
        assert_eq!(received.descriptors.len(), 2);
        let decoded = decode_request(&received.bytes, received.descriptors).unwrap();
        assert_eq!(decoded.operation, "org.example.importFile");
        assert_eq!(decoded.body.len(), 2);
    }

    #[test]
    fn late_descriptors_are_visible_to_the_connection_driver() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        let frame = encode_request("org.example.importFile", &serde_json::json!({}), 1).unwrap();
        send_part(&sender, &frame, &[]).unwrap();
        let mut buffer = vec![0; MAX_HEADER_OCTETS + PREFIX_OCTETS];
        let first = recv_part(&receiver, &mut buffer).unwrap();
        assert!(first.descriptors.is_empty());
        let descriptor: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
        send_part(&sender, b"late", &[descriptor]).unwrap();

        let second = recv_part(&receiver, &mut buffer).unwrap();
        assert_eq!(second.bytes, b"late");
        assert_eq!(second.descriptors.len(), 1);
    }

    #[test]
    fn ancillary_truncation_is_reported() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        let descriptors: Vec<OwnedFd> = (0..MAX_DESCRIPTORS * 2)
            .map(|_| std::fs::File::open("/etc/hosts").unwrap().into())
            .collect();
        let borrowed: Vec<BorrowedFd<'_>> = descriptors.iter().map(AsFd::as_fd).collect();
        let mut control =
            [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_DESCRIPTORS * 2))];
        let mut ancillary = SendAncillaryBuffer::new(&mut control);
        assert!(ancillary.push(SendAncillaryMessage::ScmRights(&borrowed)));
        rustix::net::sendmsg(
            &sender,
            &[IoSlice::new(b"x")],
            &mut ancillary,
            rustix::net::SendFlags::empty(),
        )
        .unwrap();

        let mut buffer = [0; 1];
        let received = recv_part(&receiver, &mut buffer).unwrap();
        assert!(
            received.ancillary_truncated,
            "got {} descriptors without CTRUNC",
            received.descriptors.len()
        );
    }
}
