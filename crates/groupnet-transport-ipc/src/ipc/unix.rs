//! Unix socket lifecycle: bind-only creation, owner-only access, identity cleanup.

use std::fs::{self, Metadata};
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

use super::super::IpcAddress;
use super::super::address::IpcAddressError;

pub(in crate::ipc) type Stream = UnixStream;

#[derive(Debug)]
struct OwnedPath {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for OwnedPath {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            // Never unlink a stale socket, symlink, regular file, or a new
            // listener that replaced our own path while we were running.
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub(in crate::ipc) struct Listener {
    socket: UnixListener,
    _path: OwnedPath,
}

impl Listener {
    pub(in crate::ipc) fn bind(address: &IpcAddress, _max_sessions: usize) -> io::Result<Self> {
        let IpcAddress::Unix(path) = address;
        let (path, parent) = private_path(path)?;
        // UnixListener::bind refuses every existing path; deliberately never
        // "recover" stale sockets, which may belong to another live process.
        let socket = UnixListener::bind(&path)?;
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket() {
            return Err(IpcAddressError::SocketReplaced.into());
        }
        let owned = OwnedPath {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        if metadata.uid() != parent.uid() {
            return Err(IpcAddressError::ParentForeignOwner.into());
        }
        fs::set_permissions(&owned.path, fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            socket,
            _path: owned,
        })
    }

    pub(in crate::ipc) async fn accept(&mut self) -> io::Result<Stream> {
        self.socket.accept().await.map(|(stream, _)| stream)
    }
}

pub(in crate::ipc) fn validate_address(address: &IpcAddress) -> Result<(), IpcAddressError> {
    let IpcAddress::Unix(path) = address;
    private_path(path).map(|_| ())
}

pub(in crate::ipc) async fn connect(address: &IpcAddress) -> io::Result<Stream> {
    let IpcAddress::Unix(path) = address;
    let (path, parent) = private_path(path)?;
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_socket()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != parent.uid()
    {
        return Err(IpcAddressError::PeerSocketNotPrivate.into());
    }
    UnixStream::connect(path).await
}

fn private_path(path: &Path) -> Result<(PathBuf, Metadata), IpcAddressError> {
    if !path.is_absolute() {
        return Err(IpcAddressError::RelativePath);
    }
    let (Some(name), Some(parent)) = (path.file_name(), path.parent()) else {
        return Err(IpcAddressError::NoFileName);
    };
    let metadata = fs::symlink_metadata(parent).map_err(IpcAddressError::ParentInaccessible)?;
    if !metadata.file_type().is_dir() {
        return Err(IpcAddressError::ParentNotDirectory);
    }
    if metadata.mode() & 0o077 != 0 {
        return Err(IpcAddressError::ParentNotPrivate {
            mode: metadata.mode() & 0o7777,
        });
    }
    let parent = fs::canonicalize(parent).map_err(IpcAddressError::ParentInaccessible)?;
    // Use a canonical parent so subsequent operations do not follow a mutable
    // ancestor symlink to a different directory.
    Ok((parent.join(name), metadata))
}
