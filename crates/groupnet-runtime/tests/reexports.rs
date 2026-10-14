//! Every sibling-crate type the runtime's public signatures require callers to
//! name is reachable from `groupnet_runtime` itself.

use groupnet_runtime::messaging::Messaging;
use groupnet_runtime::{ManagedProtocol, OrderedProtocol, UnorderedProtocol};

/// Compiles only while `T`'s path resolves.
fn named<T: ?Sized>() {}

/// Compiles only while descriptor `I` binds protocol `P`.
fn binds<I: groupnet_runtime::PeerImplementation<Protocol = P>, P>() {}

#[test]
fn routing_and_tunnel_types_are_reachable_from_the_runtime() {
    named::<groupnet_runtime::Router>();
    named::<groupnet_runtime::RouterConfig>();
    named::<groupnet_runtime::TunnelConfig>();
    named::<groupnet_runtime::TunnelTransport>();
    named::<groupnet_runtime::TlsIdentity>();
    named::<groupnet_runtime::TlsIdentityError>();
    named::<groupnet_runtime::PeerIdentity>();
    named::<dyn groupnet_runtime::LinkProvider>();
}

#[test]
fn tls_identity_error_causes_are_reachable_from_the_runtime() {
    named::<groupnet_runtime::rustls::Error>();
    named::<groupnet_runtime::rustls::server::VerifierBuilderError>();
    let _: fn(groupnet_runtime::rustls::Error) -> groupnet_runtime::TlsIdentityError =
        groupnet_runtime::TlsIdentityError::Credentials;
    let _: fn(
        groupnet_runtime::rustls::server::VerifierBuilderError,
    ) -> groupnet_runtime::TlsIdentityError = groupnet_runtime::TlsIdentityError::Verifier;
}

#[test]
fn session_and_message_types_are_reachable_from_the_runtime() {
    named::<groupnet_runtime::TunneledStream>();
    named::<groupnet_runtime::UnorderedSession>();
    named::<groupnet_runtime::UnorderedConfig>();
    named::<groupnet_runtime::UnorderedConfigError>();
    named::<groupnet_runtime::UnorderedOptions>();
    named::<groupnet_runtime::UnorderedDelivery>();
    named::<groupnet_runtime::messaging::MessagingConfig>();
    named::<groupnet_runtime::messaging::MessagingConfigError>();
    named::<groupnet_runtime::messaging::ProtocolId>();
    named::<groupnet_runtime::messaging::Bytes>();
    named::<groupnet_runtime::messaging::ReceiveHandle>();
}

#[test]
fn built_in_descriptors_bind_the_reexported_managed_views() {
    binds::<groupnet_runtime::Messages, ManagedProtocol<Messaging>>();
    binds::<groupnet_runtime::Ordered, ManagedProtocol<OrderedProtocol>>();
    binds::<groupnet_runtime::Unordered, ManagedProtocol<UnorderedProtocol>>();
}
