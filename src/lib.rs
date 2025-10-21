use std::fs::DirEntry;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use std::path::Path;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::builder::BooleanBuilder;
use arrow_array::builder::StringBuilder;
use arrow_array::builder::TimestampMicrosecondBuilder;
use arrow_array::builder::UInt32Builder;
use arrow_array::builder::UInt64Builder;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use io::BufWriter;
use io::Write;

pub struct IpcStreamWriter<W: Write>(pub StreamWriter<BufWriter<W>>);

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn write(&mut self, r: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(r).map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)
    }

    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }
}

pub fn dirent_schema_full() -> Schema {
    Schema::new(vec![
        Field::new("basename", DataType::Utf8, false),
        Field::new("filetype", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("len", DataType::UInt64, false),
        Field::new("readonly", DataType::Boolean, false),
        Field::new("permissions", DataType::UInt32, false),
        Field::new(
            "created",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "accessed",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "modified",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ])
}

pub fn dirents2batch_full<I>(dirents: I) -> Result<RecordBatch, io::Error>
where
    I: Iterator<Item = Result<DirEntry, io::Error>>,
{
    let schema = dirent_schema_full();
    let mut basenames = StringBuilder::new();
    let mut filetypes = StringBuilder::new();
    let mut paths = StringBuilder::new();
    let mut lens = UInt64Builder::new();
    let mut readonlies = BooleanBuilder::new();
    let mut permissions = UInt32Builder::new();
    let mut createds = TimestampMicrosecondBuilder::new();
    let mut accesseds = TimestampMicrosecondBuilder::new();
    let mut modifieds = TimestampMicrosecondBuilder::new();

    for dirent in dirents {
        let dirent = dirent?;
        let path = dirent.path();
        let metadata = dirent.metadata()?;
        let created = metadata.created();
        let accessed = metadata.accessed();
        let modified = metadata.modified();
        basenames.append_value(dirent.file_name().to_string_lossy());
        let file_type = metadata.file_type();
        filetypes.append_value(if file_type.is_dir() {
            "dir"
        } else if file_type.is_file() {
            "file"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "unknown"
        });
        paths.append_value(path.to_string_lossy());
        lens.append_value(metadata.len());
        let perms = metadata.permissions();
        readonlies.append_value(perms.readonly());
        permissions.append_value(perms.mode());
        createds.append_option(
            created
                .ok()
                .and_then(|x| x.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as i64),
        );
        accesseds.append_option(
            accessed
                .ok()
                .and_then(|x| x.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as i64),
        );
        modifieds.append_option(
            modified
                .ok()
                .and_then(|x| x.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as i64),
        );
    }

    let basename_array: ArrayRef = Arc::new(basenames.finish());
    let filetype_array: ArrayRef = Arc::new(filetypes.finish());
    let path_array: ArrayRef = Arc::new(paths.finish());
    let len_array: ArrayRef = Arc::new(lens.finish());
    let readonly_array: ArrayRef = Arc::new(readonlies.finish());
    let permission_array: ArrayRef = Arc::new(permissions.finish());
    let created_array: ArrayRef = Arc::new(createds.finish());
    let accessed_array: ArrayRef = Arc::new(accesseds.finish());
    let modified_array: ArrayRef = Arc::new(modifieds.finish());

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            basename_array,
            filetype_array,
            path_array,
            len_array,
            readonly_array,
            permission_array,
            created_array,
            accessed_array,
            modified_array,
        ],
    )
    .map_err(io::Error::other)
}

pub fn dirname2batch_full<P>(dirname: P) -> Result<RecordBatch, io::Error>
where
    P: AsRef<Path>,
{
    let dirents = std::fs::read_dir(dirname)?;
    dirents2batch_full(dirents)
}
