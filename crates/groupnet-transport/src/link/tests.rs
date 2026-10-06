use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct Endpoint {
    dropped: Arc<AtomicBool>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl Transport for Endpoint {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }
}

#[test]
fn bound_link_controls_do_not_retain_closed_endpoint() {
    let dropped = Arc::new(AtomicBool::new(false));
    let link = BoundLink::new(
        Endpoint {
            dropped: dropped.clone(),
        },
        LinkConfig::new(Vec::new()),
    );
    let control = link.driver.control();
    let provider: Box<dyn LinkProvider> = Box::new(link);
    let bound = futures::executor::block_on(provider.bind(NodeId::new("local"))).unwrap();
    let retained_control = bound.driver.control();
    assert!(!dropped.load(Ordering::SeqCst));
    futures::executor::block_on(bound.driver.close());
    assert!(dropped.load(Ordering::SeqCst));
    drop((control, retained_control));
}
