import Darwin
import Foundation
import NetworkExtension
import os

protocol PacketFlowIO: AnyObject {
    func readPackets(completionHandler: @escaping @Sendable ([Data], [NSNumber]) -> Void)
    func writePackets(_ packets: [Data], withProtocols protocols: [NSNumber]) -> Bool
}

extension NEPacketTunnelFlow: PacketFlowIO {}

enum PacketFlowBridgeError: LocalizedError {
    case systemCall(String, Int32)
    case malformedBatch
    case malformedFrame
    case packetWriteRejected

    var errorDescription: String? {
        switch self {
        case let .systemCall(name, code):
            return "PacketFlow bridge \(name) failed (errno \(code))"
        case .malformedBatch:
            return "PacketFlow returned mismatched packet/protocol arrays"
        case .malformedFrame:
            return "PacketFlow bridge received a malformed framed packet"
        case .packetWriteRejected:
            return "PacketFlow rejected packets emitted by the core"
        }
    }
}

struct PacketFlowBridgeSnapshot: Equatable {
    let packetsToCore: UInt64
    let packetsToSystem: UInt64
    let droppedToCore: UInt64
    let droppedToSystem: UInt64
    let pendingPackets: Int
    let pendingBytes: Int
    let peakPendingPackets: Int
    let peakPendingBytes: Int
    let queueLatencySamples: UInt64
    let averageQueueLatencyNanoseconds: UInt64
    let maxQueueLatencyNanoseconds: UInt64
    let systemWriteCalls: UInt64
    let maxSystemWriteBatch: Int
    let configuredMaxDrainBatch: Int
    let coreToSystemFlushNanoseconds: UInt64
}

struct PacketFlowBridgeConfiguration: Equatable {
    static let `default` = PacketFlowBridgeConfiguration()
    static let maxAllowedDrainBatch = 256
    static let maxAllowedFlushNanoseconds: UInt64 = 5_000_000

    let maxDrainBatch: Int
    let coreToSystemFlushNanoseconds: UInt64

    init() {
        maxDrainBatch = 64
        coreToSystemFlushNanoseconds = 0
    }

    init(validatingMaxDrainBatch maxDrainBatch: Int, coreToSystemFlushNanoseconds: UInt64) throws {
        guard (1...Self.maxAllowedDrainBatch).contains(maxDrainBatch) else {
            throw PacketFlowBridgeConfigurationError.invalidMaxDrainBatch(maxDrainBatch)
        }
        guard coreToSystemFlushNanoseconds <= Self.maxAllowedFlushNanoseconds else {
            throw PacketFlowBridgeConfigurationError.invalidFlushNanoseconds(
                coreToSystemFlushNanoseconds
            )
        }
        self.maxDrainBatch = maxDrainBatch
        self.coreToSystemFlushNanoseconds = coreToSystemFlushNanoseconds
    }
}

enum PacketFlowBridgeConfigurationError: LocalizedError {
    case invalidMaxDrainBatch(Int)
    case invalidFlushNanoseconds(UInt64)

    var errorDescription: String? {
        switch self {
        case let .invalidMaxDrainBatch(value):
            return "PacketFlow max drain batch must be 1...256, got \(value)"
        case let .invalidFlushNanoseconds(value):
            return "PacketFlow flush window must be 0...5000000 ns, got \(value)"
        }
    }
}

private struct PendingPacket {
    let frame: Data
    let enqueuedAt: UInt64
}

/// Both read and write dispatch sources monitor the same flow-side fd. Darwin
/// requires that descriptor to remain open until every source's cancel handler
/// has run; closing it immediately after cancel is a libdispatch client crash.
private final class PacketFlowSourceCancellation: @unchecked Sendable {
    private let lock = NSLock()
    private var remaining = 2
    private var descriptor: Int32

    init(descriptor: Int32) {
        self.descriptor = descriptor
    }

    func sourceDidCancel() {
        lock.lock()
        remaining -= 1
        let shouldClose = remaining == 0
        let fd = descriptor
        if shouldClose { descriptor = -1 }
        lock.unlock()
        if shouldClose, fd >= 0 { Darwin.close(fd) }
    }
}

/// Public-API packet adapter for NEPacketTunnelFlow.
///
/// Apple packets are framed with the Darwin utun-style 4-byte address-family
/// prefix and passed over a bounded AF_UNIX/SOCK_DGRAM socketpair. The Go side
/// consumes a duplicate of `coreFileDescriptor`; neither endpoint is Apple's
/// private utun descriptor.
final class PacketFlowBridge: @unchecked Sendable {
    private static let headerSize = 4
    private static let maxIPPacketSize = 65_535

    private let packetFlow: PacketFlowIO
    private let queue = DispatchQueue(label: "app.hako.adapter.packetflow")
    private let queueKey = DispatchSpecificKey<UInt8>()
    private let maxPendingPackets: Int
    private let maxPendingBytes: Int
    private let monotonicNanoseconds: @Sendable () -> UInt64
    private let configuration: PacketFlowBridgeConfiguration
    private let onFailure: (Error) -> Void
    private let log = Logger(subsystem: "app.hako.adapter", category: "packetflow")

    private var flowFileDescriptor: Int32 = -1
    private(set) var coreFileDescriptor: Int32 = -1
    private var readSource: DispatchSourceRead?
    private var writeSource: DispatchSourceWrite?
    private var sourceCancellation: PacketFlowSourceCancellation?
    private var writeSourceIsResumed = false
    private var flowReadOutstanding = false
    private var running = false
    private var didFail = false
    private var coreDrainScheduled = false
    private var coreDrainGeneration: UInt64 = 0

    private var pendingToCore: [PendingPacket] = []
    private var pendingHead = 0
    private var pendingBytes = 0
    private var peakPendingPackets = 0
    private var peakPendingBytes = 0

    private var packetsToCore: UInt64 = 0
    private var packetsToSystem: UInt64 = 0
    private var droppedToCore: UInt64 = 0
    private var droppedToSystem: UInt64 = 0
    private var queueLatencySamples: UInt64 = 0
    private var totalQueueLatencyNanoseconds: UInt64 = 0
    private var maxQueueLatencyNanoseconds: UInt64 = 0
    private var systemWriteCalls: UInt64 = 0
    private var maxSystemWriteBatch = 0

    init(
        packetFlow: PacketFlowIO,
        maxPendingPackets: Int = 1_024,
        maxPendingBytes: Int = 4 * 1_024 * 1_024,
        configuration: PacketFlowBridgeConfiguration = .default,
        monotonicNanoseconds: @escaping @Sendable () -> UInt64 = { DispatchTime.now().uptimeNanoseconds },
        onFailure: @escaping (Error) -> Void
    ) {
        self.packetFlow = packetFlow
        self.maxPendingPackets = max(1, maxPendingPackets)
        self.maxPendingBytes = max(4_096, maxPendingBytes)
        self.configuration = configuration
        self.monotonicNanoseconds = monotonicNanoseconds
        self.onFailure = onFailure
        queue.setSpecific(key: queueKey, value: 1)
    }

    deinit {
        stop()
    }

    func start() throws -> Int32 {
        try syncOnQueue {
            guard !running else { return coreFileDescriptor }

            var descriptors = [Int32](repeating: -1, count: 2)
            let socketResult = descriptors.withUnsafeMutableBufferPointer { buffer in
                socketpair(AF_UNIX, SOCK_DGRAM, 0, buffer.baseAddress!)
            }
            guard socketResult == 0 else {
                throw PacketFlowBridgeError.systemCall("socketpair", errno)
            }

            flowFileDescriptor = descriptors[0]
            coreFileDescriptor = descriptors[1]
            do {
                try configureDescriptor(flowFileDescriptor)
                try configureDescriptor(coreFileDescriptor)
            } catch {
                closeDescriptor(&flowFileDescriptor)
                closeDescriptor(&coreFileDescriptor)
                throw error
            }

            running = true
            didFail = false
            coreDrainGeneration &+= 1
            installSources()
            scheduleFlowRead()
            log.info("public PacketFlow bridge started")
            return coreFileDescriptor
        }
    }

    func stop() {
        syncOnQueue {
            teardown()
        }
    }

    func snapshot() -> PacketFlowBridgeSnapshot {
        syncOnQueue {
            PacketFlowBridgeSnapshot(
                packetsToCore: packetsToCore,
                packetsToSystem: packetsToSystem,
                droppedToCore: droppedToCore,
                droppedToSystem: droppedToSystem,
                pendingPackets: pendingToCore.count - pendingHead,
                pendingBytes: pendingBytes,
                peakPendingPackets: peakPendingPackets,
                peakPendingBytes: peakPendingBytes,
                queueLatencySamples: queueLatencySamples,
                averageQueueLatencyNanoseconds: queueLatencySamples == 0
                    ? 0
                    : totalQueueLatencyNanoseconds / queueLatencySamples,
                maxQueueLatencyNanoseconds: maxQueueLatencyNanoseconds,
                systemWriteCalls: systemWriteCalls,
                maxSystemWriteBatch: maxSystemWriteBatch,
                configuredMaxDrainBatch: configuration.maxDrainBatch,
                coreToSystemFlushNanoseconds: configuration.coreToSystemFlushNanoseconds
            )
        }
    }

    private func configureDescriptor(_ descriptor: Int32) throws {
        let flags = fcntl(descriptor, F_GETFL)
        guard flags >= 0, fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
            throw PacketFlowBridgeError.systemCall("fcntl(O_NONBLOCK)", errno)
        }
        let descriptorFlags = fcntl(descriptor, F_GETFD)
        guard descriptorFlags >= 0, fcntl(descriptor, F_SETFD, descriptorFlags | FD_CLOEXEC) == 0 else {
            throw PacketFlowBridgeError.systemCall("fcntl(FD_CLOEXEC)", errno)
        }
        var socketBuffer = Int32(1 * 1_024 * 1_024)
        _ = setsockopt(
            descriptor,
            SOL_SOCKET,
            SO_RCVBUF,
            &socketBuffer,
            socklen_t(MemoryLayout<Int32>.size)
        )
        _ = setsockopt(
            descriptor,
            SOL_SOCKET,
            SO_SNDBUF,
            &socketBuffer,
            socklen_t(MemoryLayout<Int32>.size)
        )
    }

    private func installSources() {
        let cancellation = PacketFlowSourceCancellation(descriptor: flowFileDescriptor)
        sourceCancellation = cancellation
        let readSource = DispatchSource.makeReadSource(fileDescriptor: flowFileDescriptor, queue: queue)
        readSource.setEventHandler { [weak self] in
            self?.scheduleCoreDrain()
        }
        readSource.setCancelHandler {
            cancellation.sourceDidCancel()
        }
        self.readSource = readSource
        readSource.resume()

        let writeSource = DispatchSource.makeWriteSource(fileDescriptor: flowFileDescriptor, queue: queue)
        writeSource.setEventHandler { [weak self] in
            self?.flushPendingToCore()
        }
        writeSource.setCancelHandler {
            cancellation.sourceDidCancel()
        }
        self.writeSource = writeSource
    }

    private func scheduleFlowRead() {
        guard running, !flowReadOutstanding else { return }
        flowReadOutstanding = true
        packetFlow.readPackets { [weak self] packets, protocols in
            guard let self else { return }
            self.queue.async {
                self.flowReadOutstanding = false
                guard self.running else { return }
                guard packets.count == protocols.count else {
                    self.fail(PacketFlowBridgeError.malformedBatch)
                    return
                }
                for (packet, proto) in zip(packets, protocols) {
                    self.enqueueToCore(Self.frame(packet: packet, protocolFamily: proto.int32Value))
                }
                self.flushPendingToCore()
                self.scheduleFlowRead()
            }
        }
    }

    private func enqueueToCore(_ frame: Data) {
        let count = pendingToCore.count - pendingHead
        guard count < maxPendingPackets, pendingBytes + frame.count <= maxPendingBytes else {
            droppedToCore += 1
            return
        }
        pendingToCore.append(PendingPacket(frame: frame, enqueuedAt: monotonicNanoseconds()))
        pendingBytes += frame.count
        peakPendingPackets = max(peakPendingPackets, count + 1)
        peakPendingBytes = max(peakPendingBytes, pendingBytes)
    }

    private func flushPendingToCore() {
        guard running else { return }
        while pendingHead < pendingToCore.count {
            let pending = pendingToCore[pendingHead]
            let sent = pending.frame.withUnsafeBytes { bytes in
                send(flowFileDescriptor, bytes.baseAddress, bytes.count, MSG_DONTWAIT)
            }
            if sent == pending.frame.count {
                pendingBytes -= pending.frame.count
                pendingHead += 1
                packetsToCore += 1
                recordQueueLatency(since: pending.enqueuedAt)
                continue
            }
            if sent < 0, errno == EINTR {
                continue
            }
            if sent < 0, errno == EAGAIN || errno == EWOULDBLOCK {
                compactPendingQueueIfNeeded()
                resumeWriteSourceIfNeeded()
                return
            }
            fail(PacketFlowBridgeError.systemCall("send", errno))
            return
        }
        pendingToCore.removeAll(keepingCapacity: true)
        pendingHead = 0
        pendingBytes = 0
        suspendWriteSourceIfNeeded()
    }

    private func compactPendingQueueIfNeeded() {
        guard pendingHead > 0,
              pendingHead >= 256 || pendingHead * 2 >= pendingToCore.count
        else { return }
        pendingToCore.removeFirst(pendingHead)
        pendingHead = 0
    }

    private func recordQueueLatency(since enqueuedAt: UInt64) {
        let now = monotonicNanoseconds()
        let latency = now >= enqueuedAt ? now - enqueuedAt : 0
        queueLatencySamples &+= 1
        let (total, overflow) = totalQueueLatencyNanoseconds.addingReportingOverflow(latency)
        totalQueueLatencyNanoseconds = overflow ? UInt64.max : total
        maxQueueLatencyNanoseconds = max(maxQueueLatencyNanoseconds, latency)
    }

    private func scheduleCoreDrain() {
        guard running, !coreDrainScheduled else { return }
        coreDrainScheduled = true
        let generation = coreDrainGeneration
        let work: @Sendable () -> Void = { [weak self] in
            guard let self else { return }
            self.queue.async {
                guard self.running, self.coreDrainGeneration == generation else { return }
                self.coreDrainScheduled = false
                if self.drainCorePackets() {
                    self.scheduleCoreDrain()
                }
            }
        }
        if configuration.coreToSystemFlushNanoseconds == 0 {
            work()
        } else {
            queue.asyncAfter(
                deadline: .now() + .nanoseconds(Int(configuration.coreToSystemFlushNanoseconds)),
                execute: work
            )
        }
    }

    /// Drains one configured write batch. Returns true when the batch ceiling
    /// was reached, so the caller schedules another bounded turn instead of
    /// relying on undocumented DispatchSource re-delivery behavior.
    private func drainCorePackets() -> Bool {
        guard running else { return false }
        var packets: [Data] = []
        var protocols: [NSNumber] = []
        var storage = [UInt8](repeating: 0, count: Self.maxIPPacketSize + Self.headerSize)

        for _ in 0..<configuration.maxDrainBatch {
            let received = storage.withUnsafeMutableBytes { bytes in
                recv(flowFileDescriptor, bytes.baseAddress, bytes.count, MSG_DONTWAIT)
            }
            if received < 0, errno == EINTR {
                continue
            }
            if received < 0, errno == EAGAIN || errno == EWOULDBLOCK {
                break
            }
            if received < 0 {
                fail(PacketFlowBridgeError.systemCall("recv", errno))
                return false
            }
            guard received >= Self.headerSize,
                  let decoded = Self.decode(frame: Data(storage.prefix(received)))
            else {
                droppedToSystem += 1
                continue
            }
            packets.append(decoded.packet)
            protocols.append(NSNumber(value: decoded.protocolFamily))
        }

        guard !packets.isEmpty else { return false }
        guard packetFlow.writePackets(packets, withProtocols: protocols) else {
            droppedToSystem += UInt64(packets.count)
            fail(PacketFlowBridgeError.packetWriteRejected)
            return false
        }
        packetsToSystem += UInt64(packets.count)
        systemWriteCalls &+= 1
        maxSystemWriteBatch = max(maxSystemWriteBatch, packets.count)
        return packets.count == configuration.maxDrainBatch
    }

    private func resumeWriteSourceIfNeeded() {
        guard let writeSource, !writeSourceIsResumed else { return }
        writeSourceIsResumed = true
        writeSource.resume()
    }

    private func suspendWriteSourceIfNeeded() {
        guard let writeSource, writeSourceIsResumed else { return }
        writeSource.suspend()
        writeSourceIsResumed = false
    }

    private func fail(_ error: Error) {
        guard !didFail else { return }
        didFail = true
        log.error("PacketFlow bridge failed: \(error.localizedDescription, privacy: .private)")
        teardown()
        onFailure(error)
    }

    private func teardown() {
        running = false
        coreDrainGeneration &+= 1
        coreDrainScheduled = false
        flowReadOutstanding = false
        pendingToCore.removeAll(keepingCapacity: false)
        pendingHead = 0
        pendingBytes = 0

        if let writeSource {
            if !writeSourceIsResumed {
                writeSource.resume()
            }
            writeSource.cancel()
            self.writeSource = nil
            writeSourceIsResumed = false
        }
        readSource?.cancel()
        readSource = nil

        // DispatchSource cancel is asynchronous. The shared cancellation
        // token closes this fd only after both source cancel handlers run.
        flowFileDescriptor = -1
        sourceCancellation = nil
        closeDescriptor(&coreFileDescriptor)
    }

    private func closeDescriptor(_ descriptor: inout Int32) {
        if descriptor >= 0 {
            Darwin.close(descriptor)
            descriptor = -1
        }
    }

    private func syncOnQueue<T>(_ body: () throws -> T) rethrows -> T {
        if DispatchQueue.getSpecific(key: queueKey) != nil {
            return try body()
        }
        return try queue.sync(execute: body)
    }

    static func frame(packet: Data, protocolFamily: Int32) -> Data {
        var family = UInt32(bitPattern: protocolFamily).bigEndian
        var framed = Data(bytes: &family, count: headerSize)
        framed.append(packet)
        return framed
    }

    static func decode(frame: Data) -> (packet: Data, protocolFamily: Int32)? {
        guard frame.count >= headerSize else { return nil }
        var rawFamily: UInt32 = 0
        _ = withUnsafeMutableBytes(of: &rawFamily) { destination in
            frame.copyBytes(to: destination, from: 0..<headerSize)
        }
        let family = Int32(bitPattern: UInt32(bigEndian: rawFamily))
        guard family == AF_INET || family == AF_INET6 else { return nil }
        return (Data(frame.dropFirst(headerSize)), family)
    }
}
