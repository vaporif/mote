use std::mem::size_of;
use std::path::Path;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use eyre::WrapErr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub oldest_block: u64,
    pub tip_block: u64,
}

const SUBSCRIBE_MSG: u8 = 0x01;
const SUBSCRIBE_SIZE: usize = 1 + size_of::<u64>();
const HANDSHAKE_SIZE: usize = 1 + 2 * size_of::<u64>();

pub async fn connect_and_subscribe(
    socket_path: &Path,
    resume_block: u64,
) -> eyre::Result<(Handshake, std::os::unix::net::UnixStream)> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .wrap_err_with(|| format!("connecting to ExEx socket at {}", socket_path.display()))?;

    let mut msg = [0u8; SUBSCRIBE_SIZE];
    msg[0] = SUBSCRIBE_MSG;
    msg[1..].copy_from_slice(&resume_block.to_le_bytes());
    stream.write_all(&msg).await?;

    let mut resp = [0u8; HANDSHAKE_SIZE];
    stream.read_exact(&mut resp).await?;

    let oldest_block = u64::from_le_bytes(resp[1..9].try_into().expect("8 bytes"));
    let tip_block = u64::from_le_bytes(resp[9..].try_into().expect("8 bytes"));

    let handshake = Handshake {
        oldest_block,
        tip_block,
    };

    let std_stream = stream.into_std()?;
    std_stream.set_nonblocking(false)?;
    Ok((handshake, std_stream))
}

pub fn read_batches(
    stream: std::os::unix::net::UnixStream,
) -> eyre::Result<impl Iterator<Item = eyre::Result<RecordBatch>>> {
    let reader =
        StreamReader::try_new(stream, None).wrap_err("creating Arrow IPC stream reader")?;
    Ok(reader.map(|r| r.map_err(|e| eyre::eyre!(e))))
}
