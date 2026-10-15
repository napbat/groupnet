//! Why an [`IpcAddress`](super::IpcAddress) cannot carry local-only IPC.

use std::{error, fmt, io};

/// Longest accepted Windows pipe name, in bytes, including the `\\.\pipe\`
/// prefix (the native limit).
#[cfg(windows)]
pub(super) const MAX_PIPE_NAME: usize = 256;

/// An [`IpcAddress`](super::IpcAddress) that does not meet the platform's
/// local-only rules, or whose filesystem state violates them.
///
/// Validation runs before a listener opens and whenever a peer is registered;
/// the Unix privacy rules are checked again when the listener binds and when a
/// session dials, because the filesystem can change in between.
#[derive(Debug)]
pub enum IpcAddressError {
    /// The pipe name is longer than the native 256-byte limit.
    #[cfg(windows)]
    PipeNameTooLong {
        /// The name's length in bytes.
        len: usize,
    },
    /// The name does not start with the local `\\.\pipe\` prefix (a remote or
    /// non-pipe path).
    #[cfg(windows)]
    NotLocalPipe,
    /// Nothing follows the `\\.\pipe\` prefix.
    #[cfg(windows)]
    EmptyPipeName,
    /// The pipe name contains a NUL, `\` or `/` after its prefix.
    #[cfg(windows)]
    PipeNameCharacter,
    /// The socket path is relative.
    #[cfg(unix)]
    RelativePath,
    /// The socket path has no final file-name component.
    #[cfg(unix)]
    NoFileName,
    /// The socket's parent directory could not be inspected or resolved.
    #[cfg(unix)]
    ParentInaccessible(io::Error),
    /// The socket's parent is not a directory (a symlink is refused too).
    #[cfg(unix)]
    ParentNotDirectory,
    /// The socket's parent directory grants group or other permissions.
    #[cfg(unix)]
    ParentNotPrivate {
        /// The directory's permission bits.
        mode: u32,
    },
    /// The socket's parent directory belongs to another user than the
    /// listener's creator.
    #[cfg(unix)]
    ParentForeignOwner,
    /// The bind path stopped being this listener's socket while it was
    /// created.
    #[cfg(unix)]
    SocketReplaced,
    /// A peer's socket is not an owner-only socket owned by its directory's
    /// owner.
    #[cfg(unix)]
    PeerSocketNotPrivate,
}

impl fmt::Display for IpcAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(windows)]
            Self::PipeNameTooLong { len } => write!(
                f,
                "IPC pipe name is {len} bytes, above the {MAX_PIPE_NAME}-byte limit"
            ),
            #[cfg(windows)]
            Self::NotLocalPipe => f.write_str(r"IPC requires a local \\.\pipe\name address"),
            #[cfg(windows)]
            Self::EmptyPipeName => f.write_str(r"IPC pipe name is empty after \\.\pipe\"),
            #[cfg(windows)]
            Self::PipeNameCharacter => {
                f.write_str(r"IPC pipe name contains a NUL, '\' or '/' after its prefix")
            }
            #[cfg(unix)]
            Self::RelativePath => f.write_str("IPC socket path must be absolute"),
            #[cfg(unix)]
            Self::NoFileName => f.write_str("IPC socket path needs a file name"),
            #[cfg(unix)]
            Self::ParentInaccessible(_) => f.write_str("IPC socket directory cannot be inspected"),
            #[cfg(unix)]
            Self::ParentNotDirectory => {
                f.write_str("IPC socket parent is not a directory (symlinks are refused)")
            }
            #[cfg(unix)]
            Self::ParentNotPrivate { mode } => write!(
                f,
                "IPC socket directory grants group or other access (mode {mode:o})"
            ),
            #[cfg(unix)]
            Self::ParentForeignOwner => {
                f.write_str("IPC socket directory must belong to the socket creator")
            }
            #[cfg(unix)]
            Self::SocketReplaced => f.write_str("IPC bind path changed during creation"),
            #[cfg(unix)]
            Self::PeerSocketNotPrivate => {
                f.write_str("IPC peer socket is not private and owner-controlled")
            }
        }
    }
}

impl error::Error for IpcAddressError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        #[cfg(unix)]
        if let Self::ParentInaccessible(error) = self {
            return Some(error);
        }
        None
    }
}

/// Malformed addresses are `InvalidInput`; Unix privacy violations are
/// `PermissionDenied`; a parent directory that cannot be inspected keeps its
/// filesystem error's kind. The typed error stays the inner error.
impl From<IpcAddressError> for io::Error {
    fn from(error: IpcAddressError) -> Self {
        let kind = match &error {
            #[cfg(unix)]
            IpcAddressError::ParentInaccessible(cause) => cause.kind(),
            #[cfg(unix)]
            IpcAddressError::NoFileName
            | IpcAddressError::ParentNotDirectory
            | IpcAddressError::ParentNotPrivate { .. }
            | IpcAddressError::ParentForeignOwner
            | IpcAddressError::SocketReplaced
            | IpcAddressError::PeerSocketNotPrivate => io::ErrorKind::PermissionDenied,
            #[cfg(unix)]
            IpcAddressError::RelativePath => io::ErrorKind::InvalidInput,
            #[cfg(windows)]
            IpcAddressError::PipeNameTooLong { .. }
            | IpcAddressError::NotLocalPipe
            | IpcAddressError::EmptyPipeName
            | IpcAddressError::PipeNameCharacter => io::ErrorKind::InvalidInput,
        };
        Self::new(kind, error)
    }
}
