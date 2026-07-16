import Darwin
import Foundation
import Network

enum ProviderLifecycleState: String, Equatable {
    case idle
    case starting
    case running
    case stopping
    case failed
}

enum ProviderLifecycleError: LocalizedError {
    case invalidTransition(from: ProviderLifecycleState, operation: String)

    var errorDescription: String? {
        switch self {
        case let .invalidTransition(state, operation):
            return "cannot \(operation) packet tunnel while lifecycle is \(state.rawValue)"
        }
    }
}

/// Network settings belong to the active NetworkExtension session. While a
/// failed start is still being unwound by the provider, removing settings is
/// part of rollback. Once Apple calls `stopTunnel`, however, the system is
/// already tearing the session down; issuing a second settings mutation races
/// NEAgent and can fail after the utun has disappeared.
enum ProviderTeardownPolicy {
    case failedStart
    case inProcessRestart
    case systemStop

    var clearsNetworkSettings: Bool {
        self == .failedStart
    }
}

/// Small lock-backed state machine because NetworkExtension may deliver a
/// stop while startup is still unwinding. Resource cleanup remains owned by
/// the provider; this type only makes transition decisions deterministic.
final class ProviderLifecycle: @unchecked Sendable {
    private let lock = NSLock()
    private var value: ProviderLifecycleState = .idle

    var state: ProviderLifecycleState {
        lock.lock()
        defer { lock.unlock() }
        return value
    }

    func beginStart() throws {
        lock.lock()
        defer { lock.unlock() }
        guard value == .idle || value == .failed else {
            throw ProviderLifecycleError.invalidTransition(from: value, operation: "start")
        }
        value = .starting
    }

    func didStart() throws {
        lock.lock()
        defer { lock.unlock() }
        guard value == .starting else {
            throw ProviderLifecycleError.invalidTransition(from: value, operation: "complete start")
        }
        value = .running
    }

    func didFailStart() {
        lock.lock()
        defer { lock.unlock() }
        if value == .starting {
            value = .failed
        }
    }

    /// Returns false when a previous stop already completed or is in flight.
    func beginStop() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard value != .idle, value != .stopping else { return false }
        value = .stopping
        return true
    }

    func didStop() {
        lock.lock()
        value = .idle
        lock.unlock()
    }
}

/// FIFO async mutex for serializing Apple's start/stop callbacks without
/// blocking a cooperative executor thread. Platform callbacks invoked by Go
/// do not enter this gate, so OpenTun can complete while start owns it.
actor ProviderOperationGate {
    private var isEntered = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func enter() async {
        guard isEntered else {
            isEntered = true
            return
        }
        await withCheckedContinuation { waiters.append($0) }
    }

    func leave() {
        guard !waiters.isEmpty else {
            isEntered = false
            return
        }
        waiters.removeFirst().resume()
    }
}

struct PhysicalPathSnapshot: Equatable, Sendable {
    let interfaceName: String
    let interfaceType: String
    let interfaceIndex: UInt32
    let satisfied: Bool
    let expensive: Bool
    let constrained: Bool
    let supportsIPv4: Bool
    let supportsIPv6: Bool

    var isReady: Bool {
        satisfied && interfaceIndex != 0 && !interfaceName.isEmpty
    }
}

protocol PhysicalPathMonitoring: AnyObject {
    var updateHandler: ((PhysicalPathSnapshot) -> Void)? { get set }
    func start(queue: DispatchQueue)
    func cancel()
}

final class ApplePhysicalPathMonitor: PhysicalPathMonitoring {
    private let monitor: NWPathMonitor
    private let handlerLock = NSLock()
    private var handler: ((PhysicalPathSnapshot) -> Void)?

    var updateHandler: ((PhysicalPathSnapshot) -> Void)? {
        get {
            handlerLock.lock(); defer { handlerLock.unlock() }
            return handler
        }
        set {
            handlerLock.lock()
            handler = newValue
            handlerLock.unlock()
        }
    }

    init(requiredInterfaceType: NWInterface.InterfaceType? = nil) {
        if let requiredInterfaceType {
            monitor = NWPathMonitor(requiredInterfaceType: requiredInterfaceType)
        } else {
            monitor = NWPathMonitor()
        }
    }

    func start(queue: DispatchQueue) {
        monitor.pathUpdateHandler = { [weak self] path in
            guard let self else { return }
            let isSatisfied = path.status == .satisfied
            // Apple documents availableInterfaces as preference ordered. The
            // utun and unsupported interfaces must never become physical
            // egress candidates.
            let interface = isSatisfied ? path.availableInterfaces.first {
                $0.type == .wifi || $0.type == .cellular || $0.type == .wiredEthernet
            } : nil
            let index = interface.map { if_nametoindex($0.name) } ?? 0
            let snapshot = PhysicalPathSnapshot(
                interfaceName: interface?.name ?? "",
                interfaceType: Self.interfaceTypeName(interface?.type),
                interfaceIndex: index,
                satisfied: isSatisfied && index != 0,
                expensive: path.isExpensive,
                constrained: path.isConstrained,
                supportsIPv4: path.supportsIPv4,
                supportsIPv6: path.supportsIPv6
            )
            updateHandler?(snapshot)
        }
        monitor.start(queue: queue)
    }

    func cancel() {
        updateHandler = nil
        monitor.pathUpdateHandler = nil
        monitor.cancel()
    }

    private static func interfaceTypeName(_ type: NWInterface.InterfaceType?) -> String {
        switch type {
        case .wifi: return "wifi"
        case .cellular: return "cellular"
        case .wiredEthernet: return "wiredEthernet"
        case .loopback: return "loopback"
        case .other: return "other"
        case nil: return "none"
        @unknown default: return "unknown"
        }
    }
}

enum PhysicalPathStartupGateError: Error, Equatable {
    case timedOut
    case superseded
}

final class PhysicalPathStartupGate: @unchecked Sendable {
    struct Generation: Equatable, Sendable {
        fileprivate let value: UInt64
    }

    private enum State {
        case waiting
        case ready
        case superseded
    }

    private let lock = NSLock()
    private var currentGeneration: UInt64 = 0
    private var ready = false

    func begin() -> Generation {
        lock.lock()
        currentGeneration &+= 1
        ready = false
        let generation = Generation(value: currentGeneration)
        lock.unlock()
        return generation
    }

    @discardableResult
    func update(_ generation: Generation, isReady: Bool) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard generation.value == currentGeneration else { return false }
        ready = isReady
        return true
    }

    @discardableResult
    func invalidate(_ generation: Generation) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard generation.value == currentGeneration else { return false }
        currentGeneration &+= 1
        ready = false
        return true
    }

    func wait(
        for generation: Generation,
        timeoutNanoseconds: UInt64,
        pollNanoseconds: UInt64 = 50_000_000
    ) async throws {
        let start = DispatchTime.now().uptimeNanoseconds
        while DispatchTime.now().uptimeNanoseconds - start < timeoutNanoseconds {
            try Task.checkCancellation()
            switch state(for: generation) {
            case .ready:
                return
            case .superseded:
                throw PhysicalPathStartupGateError.superseded
            case .waiting:
                break
            }
            try await Task.sleep(nanoseconds: max(1, pollNanoseconds))
        }
        try Task.checkCancellation()
        switch state(for: generation) {
        case .ready:
            return
        case .superseded:
            throw PhysicalPathStartupGateError.superseded
        case .waiting:
            throw PhysicalPathStartupGateError.timedOut
        }
    }

    private func state(for generation: Generation) -> State {
        lock.lock(); defer { lock.unlock() }
        guard generation.value == currentGeneration else { return .superseded }
        return ready ? .ready : .waiting
    }
}

final class PhysicalPathMonitorSession: @unchecked Sendable {
    private let lock = NSLock()
    private let gate = PhysicalPathStartupGate()
    private var monitor: (any PhysicalPathMonitoring)?
    private var generation: PhysicalPathStartupGate.Generation?

    func start(
        monitor newMonitor: any PhysicalPathMonitoring,
        queue: DispatchQueue,
        observer: @escaping (PhysicalPathSnapshot) -> Void
    ) -> PhysicalPathStartupGate.Generation {
        let previous = detachCurrent()
        previous?.updateHandler = nil
        previous?.cancel()

        let newGeneration = gate.begin()
        let monitorIdentifier = ObjectIdentifier(newMonitor)
        lock.lock()
        monitor = newMonitor
        generation = newGeneration
        lock.unlock()

        newMonitor.updateHandler = { [weak self] snapshot in
            guard let self,
                  self.isCurrent(monitorIdentifier, generation: newGeneration)
            else { return }
            // Publish readiness only after the consumer has installed the
            // snapshot used by the dialer socket hook and runtime diagnostics.
            observer(snapshot)
            _ = self.gate.update(newGeneration, isReady: snapshot.isReady)
        }
        newMonitor.start(queue: queue)
        return newGeneration
    }

    func wait(
        for generation: PhysicalPathStartupGate.Generation,
        timeoutNanoseconds: UInt64
    ) async throws {
        try await gate.wait(for: generation, timeoutNanoseconds: timeoutNanoseconds)
    }

    func stop() {
        lock.lock()
        let currentMonitor = monitor
        let currentGeneration = generation
        monitor = nil
        generation = nil
        lock.unlock()
        currentMonitor?.updateHandler = nil
        currentMonitor?.cancel()
        if let currentGeneration {
            _ = gate.invalidate(currentGeneration)
        }
    }

    private func detachCurrent() -> (any PhysicalPathMonitoring)? {
        lock.lock()
        let currentMonitor = monitor
        let currentGeneration = generation
        monitor = nil
        generation = nil
        lock.unlock()
        if let currentGeneration {
            _ = gate.invalidate(currentGeneration)
        }
        return currentMonitor
    }

    private func isCurrent(
        _ identifier: ObjectIdentifier,
        generation candidate: PhysicalPathStartupGate.Generation
    ) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard let monitor, let generation else { return false }
        return ObjectIdentifier(monitor) == identifier && generation == candidate
    }
}
