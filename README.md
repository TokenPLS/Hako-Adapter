# Hako Adapter

English · [简体中文](README.zh-CN.md)

Swift components for connecting an Apple Packet Tunnel provider to a proxy kernel. This repository contains the packet-flow bridge and provider lifecycle code used by the Hako client.

## Official website and client

- [Official website](https://clash.md/)
- [Download Clash on the App Store](https://apps.apple.com/app/id6794257189)

For the complete application, see [Hako-Client](https://github.com/TokenPLS/Hako-Client). The proxy kernel and SDK build tools are in [Hako](https://github.com/TokenPLS/Hako).

## Components

| Source | Responsibility |
| --- | --- |
| [`PacketFlowBridge.swift`](Sources/HakoAdapter/PacketFlowBridge.swift) | Transfers packets between `NEPacketTunnelFlow` and a file descriptor, with framing, bounded queues and flow statistics |
| [`ProviderLifecycle.swift`](Sources/HakoAdapter/ProviderLifecycle.swift) | Coordinates provider state, session ownership, reloads, teardown and physical network path monitoring |

These components use Apple system frameworks. They do not include a proxy engine, an application interface or a complete `NEPacketTunnelProvider` implementation.

## Requirements

- macOS with Xcode and the SDK for your target platform.
- Swift 5.9 or later.
- Deployment targets: iOS 15+, macOS 13+ or tvOS 17+.
- A Packet Tunnel extension with its required capabilities and signing configured by your application.

## Integrate the sources

The current types have Swift `internal` access. Compile the source files in your consuming target; importing the package from another module does not expose these types as a public API.

1. Check out a specific revision of this repository.
2. Add the files under `Sources/HakoAdapter` to your Packet Tunnel extension target, keeping the accompanying license.
3. Supply your kernel connection and coordinate startup, network settings, packet handling and shutdown in your provider.

The [Hako-Client bootstrap script](https://github.com/TokenPLS/Hako-Client/blob/main/scripts/bootstrap.py) demonstrates fetching a pinned revision and placing the sources in the consuming project.

`PacketFlowBridge.start()` returns the core-facing descriptor of a socket pair. The descriptor carries packets with a four-byte address-family prefix; it is not Apple's private utun descriptor. Keep the bridge alive for the tunnel session and call `stop()` during teardown. See the source for descriptor ownership and error handling.

## Status and feedback

This is pre-release integration code. Pin a revision and validate the lifecycle and packet flow in your own application. The repository's package declaration describes its build structure; it does not promise a stable external Swift API.

Report component problems in [Issues](https://github.com/TokenPLS/Hako-Adapter/issues), including the revision, target platform and a minimal reproduction. App issues belong in [Hako-Client](https://github.com/TokenPLS/Hako-Client/issues); kernel issues belong in [Hako](https://github.com/TokenPLS/Hako/issues). Remove credentials and subscription links from public reports. Follow [SECURITY.md](SECURITY.md) for security reports.

## License

[GPL-3.0](LICENSE). See the source and license file for attribution.
