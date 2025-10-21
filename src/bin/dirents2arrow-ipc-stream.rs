use rs_dirents2arrow_ipc_stream::{IpcStreamWriter, dirname2batch_full};
use std::io::{self, BufWriter};

fn main() -> io::Result<()> {
    // Get directory from command line arguments or use current directory
    let dir_path = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    // Generate the RecordBatch from the directory entries
    let record_batch = dirname2batch_full(&dir_path)?;

    // Get a buffered writer for stdout
    let stdout = io::stdout();
    let stdout_handle = stdout.lock();
    let buf_writer = BufWriter::new(stdout_handle);

    // Create a new IPC stream writer
    let mut stream_writer = IpcStreamWriter(
        arrow_ipc::writer::StreamWriter::try_new(buf_writer, &record_batch.schema())
            .map_err(io::Error::other)?,
    );

    // Write the record batch and finish the stream
    stream_writer.write(&record_batch)?;
    stream_writer.finish()?;

    Ok(())
}
