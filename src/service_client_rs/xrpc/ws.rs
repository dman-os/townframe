use base64::{Engine, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

pub fn accept_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(GUID.as_bytes());
    STANDARD.encode(hasher.finalize())
}

pub enum WsMessage {
    Text(String),
    Close,
}

pub struct WsIo<S> {
    stream: S,
    client: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsIo<S> {
    pub fn new(stream: S, client: bool) -> Self {
        Self { stream, client }
    }
    pub async fn write_text(&mut self, text: &str) -> io::Result<()> {
        self.write_frame(0x1, text.as_bytes()).await
    }
    pub async fn write_close(&mut self) -> io::Result<()> {
        self.write_frame(0x8, &[]).await
    }

    async fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut header = vec![0x80 | opcode];
        let mask_bit = if self.client { 0x80 } else { 0 };
        match payload.len() {
            len if len < 126 => header.push(mask_bit | len as u8),
            len if len <= u16::MAX as usize => {
                header.push(mask_bit | 126);
                header.extend_from_slice(&(len as u16).to_be_bytes());
            }
            len => {
                header.push(mask_bit | 127);
                header.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        if self.client {
            let mask: [u8; 4] = uuid::Uuid::now_v7().as_bytes()[..4]
                .try_into()
                .expect("uuid prefix is a mask key");
            header.extend_from_slice(&mask);
            self.stream.write_all(&header).await?;
            let masked: Vec<u8> = payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4])
                .collect();
            self.stream.write_all(&masked).await?;
        } else {
            self.stream.write_all(&header).await?;
            self.stream.write_all(payload).await?;
        }
        self.stream.flush().await
    }

    pub async fn read_message(&mut self) -> io::Result<Option<WsMessage>> {
        let mut assembled: Vec<u8> = Vec::new();
        loop {
            let mut head = [0u8; 2];
            if self.stream.read_exact(&mut head).await.is_err() {
                return Ok(None);
            }
            let fin = head[0] & 0x80 != 0;
            let opcode = head[0] & 0x0f;
            let masked = head[1] & 0x80 != 0;
            let mut len = u64::from(head[1] & 0x7f);
            if len == 126 {
                let mut ext = [0u8; 2];
                self.stream.read_exact(&mut ext).await?;
                len = u64::from(u16::from_be_bytes(ext));
            } else if len == 127 {
                let mut ext = [0u8; 8];
                self.stream.read_exact(&mut ext).await?;
                len = u64::from_be_bytes(ext);
            }
            if len as usize > MAX_MESSAGE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "websocket message exceeds the size limit",
                ));
            }
            let mask = if masked {
                let mut mask = [0u8; 4];
                self.stream.read_exact(&mut mask).await?;
                Some(mask)
            } else {
                None
            };
            let mut payload = vec![0u8; len as usize];
            self.stream.read_exact(&mut payload).await?;
            if let Some(mask) = mask {
                for (index, byte) in payload.iter_mut().enumerate() {
                    *byte ^= mask[index % 4];
                }
            }
            match opcode {
                0x8 => return Ok(Some(WsMessage::Close)),
                0x9 => self.write_frame(0xA, &payload).await?,
                0xA => {}
                0x0..=0x2 => {
                    assembled.extend_from_slice(&payload);
                    if fin {
                        return Ok(Some(WsMessage::Text(
                            String::from_utf8(assembled).map_err(|error| {
                                io::Error::new(io::ErrorKind::InvalidData, error)
                            })?,
                        )));
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported websocket opcode",
                    ));
                }
            }
        }
    }
}
