<h1 align="center">Hako Adapter</h1>

<p align="center">Apple Network Extension building blocks for the <a href="https://github.com/TokenPLS/Hako"><strong>Hako</strong> kernel</a>.</p>

---

`HakoAdapter` is a small, dependency-free Swift library extracted from a
production Packet Tunnel provider. It contains the two pieces of an Apple
NE data plane that are genuinely hard to get right, with no product or UI
code attached:

| Component | What it does |
|---|---|
| **`PacketFlowBridge`** | Bridges `NEPacketTunnelFlow` to a kernel-owned tun file descriptor. Apple packets are framed with the Darwin utun-style 4-byte address-family prefix and passed over a bounded `AF_UNIX`/`SOCK_DGRAM` socketpair — the kernel consumes a duplicate of the core-facing descriptor, and neither endpoint is Apple's private utun fd. Bounded queues with drop accounting, batched drains, dispatch-source lifecycle, and a single-failure model. `snapshot()` exposes counters (packets/drops/pending/latency) for diagnostics. |
| **`ProviderLifecycle`** | A strict provider state machine (`idle → starting → running → stopping`, plus `failed`) that serializes start/stop/sleep/wake transitions, rejects invalid overlaps, and defines teardown policy so a half-started tunnel can never leak network settings. |

Both files import only system frameworks (`Foundation`, `Network`,
`NetworkExtension`, `Darwin`, `os`). There is no dependency on the Hako
kernel — the bridge speaks plain file descriptors — so the library also
works with any core that can read/write a utun-framed descriptor.

## Requirements

- iOS 15+ / macOS 13+, Swift 5.9+
- A Packet Tunnel Provider target (`com.apple.networkextension.packet-tunnel`)
- For a full tunnel: the Hako kernel (`Hako.xcframework`, built with
  `make lib_apple` from [TokenPLS/Hako](https://github.com/TokenPLS/Hako))

## Install

Swift Package Manager (this repository is a package), or copy the two files
from `Sources/HakoAdapter/` into your extension target — they are
self-contained by design.

## Wiring sketch

Inside your `NEPacketTunnelProvider`, after applying
`NEPacketTunnelNetworkSettings`:

```swift
let bridge = PacketFlowBridge(packetFlow: packetFlow) { error in
    // Single-failure model: tear the tunnel down on any bridge error.
}
let coreFD = try bridge.start() // core-facing end of the socketpair

// Hand `coreFD` to the kernel. With Hako, return it from the platform
// callback that the kernel invokes to open the tun (the kernel dup()s it,
// so your side keeps ownership of the original).

// On teardown:
bridge.stop()
```

Use `ProviderLifecycle` to gate `startTunnel` / `stopTunnel` / `sleep` /
`wake` so overlapping system callbacks cannot race your provider state:

```swift
let lifecycle = ProviderLifecycle()
try lifecycle.beginStart()
// … apply settings, start the kernel, start the bridge …
try lifecycle.didStart()
```

## Status

Pre-release. Extracted from an actively developed codebase; the API may
still change with its production consumer. Issues and PRs are welcome —
kernel-side behavior belongs in
[TokenPLS/Hako](https://github.com/TokenPLS/Hako).

## License

GPL-3.0 (see `LICENSE`). © 2026 The Hako Authors.
