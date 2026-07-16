# Security Policy

Hako Adapter provides Network Extension data-plane components that move user
network traffic. We take security seriously and appreciate coordinated
disclosure.

## Reporting a vulnerability

**Do not open a public issue, discussion, or pull request for a security
problem.**

Report privately via **GitHub Private Vulnerability Reporting**: the
repository's **Security** tab → **Report a vulnerability**.

Please include: affected commit, platform (iOS/macOS + OS version), a minimal
reproduction, and the impact you believe it has. **Redact real secrets** —
never include real subscription URLs, proxy passwords, private keys, tokens,
or personal device identifiers.

We aim to acknowledge within **3 business days** and to coordinate public
disclosure within about **90 days** of the report. We will credit you in the
advisory unless you prefer to remain anonymous.

## Scope

**In scope** — issues in this repository:

- Packet leakage or mis-framing in `PacketFlowBridge`: packets escaping the
  socketpair path, address-family confusion, or descriptor lifetime bugs
  (use-after-close, double-close, fd leaks).
- Memory-safety, crash, or DoS defects in the bridge's queueing/backpressure
  path reachable from network input.
- Lifecycle bypasses in `ProviderLifecycle` that allow a torn-down provider
  to keep network settings applied.

**Out of scope** — report elsewhere:

- The Hako kernel and its gomobile SDK —
  [TokenPLS/Hako](https://github.com/TokenPLS/Hako).
- Upstream mihomo behavior —
  [MetaCubeX/mihomo](https://github.com/MetaCubeX/mihomo).
- The consuming application: credential storage, UI, provisioning, signing.

## Supported versions

Pre-1.0; only the most recent snapshot and `main` receive security fixes.
