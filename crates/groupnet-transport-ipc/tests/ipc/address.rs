//! Unique native addresses for one test: a random pipe name on Windows, a
//! socket in a fresh owner-only directory on Unix.

use groupnet_transport_ipc::IpcAddress;
use ring::rand::{SecureRandom, SystemRandom};

/// One unique listener address, released with the test.
#[derive(Debug)]
pub(crate) struct Address {
    pub(crate) ipc: IpcAddress,
    #[cfg(unix)]
    pub(crate) directory: std::path::PathBuf,
}

impl Address {
    pub(crate) fn new() -> Self {
        use std::fmt::Write;
        let mut random = [0_u8; 16];
        SystemRandom::new().fill(&mut random).unwrap();
        let mut suffix = String::with_capacity(32);
        for byte in random {
            write!(suffix, "{byte:02x}").unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let directory = std::env::temp_dir().join(format!("groupnet-ipc-{suffix}"));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&directory)
                .unwrap();
            Self {
                ipc: IpcAddress::Unix(directory.join("socket")),
                directory,
            }
        }
        #[cfg(windows)]
        {
            Self {
                ipc: IpcAddress::NamedPipe(format!(r"\\.\pipe\groupnet-ipc-{suffix}")),
            }
        }
    }
}

#[cfg(unix)]
impl Drop for Address {
    fn drop(&mut self) {
        // Only the empty, uniquely created directory is removed. A leftover
        // socket or unrelated replacement is deliberately not recursively erased.
        let _ = std::fs::remove_dir(&self.directory);
    }
}
