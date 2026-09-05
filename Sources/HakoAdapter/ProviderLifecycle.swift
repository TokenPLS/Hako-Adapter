import Darwin
import Foundation
import Network
import NetworkExtension

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

/// A named meaning for an `NEProviderStopReason`, with the raw code kept
/// alongside for a bug report. The exported log used to read `reason=5`, which
/// tells a reader nothing they can act on and makes "I stopped it myself" look
/// like a crash; this leads with the meaning.
///
/// Takes the enum, not an Int, so a name can only ever be attached to a case
/// Apple actually declares. That much the compiler does enforce, and it is the
/// half that matters most: an earlier hand-written Int table named raw 2
/// "plugin-failed", which is Apple's word for a different enum entirely
/// (`NEVPNConnectionErrorPluginFailed`), and no amount of proofreading caught it.
///
/// What the compiler does NOT do is fail the build when Apple adds a case.
/// `@unknown default` downgrades the missing-case diagnostic to `warning: switch
/// must be exhaustive` and the build still exits 0 (measured; no
/// warnings-as-errors is set for these targets). So bumping the SDK means
/// re-reading NEProvider.h by hand -- that is the actual backstop, and the
/// reason raw 17 went unnamed until a review found it.
///
/// Every name is Apple's own case name, kebab-cased, so a reader holding an
/// exported log can search Apple's documentation for the word and find the case
/// that stopped their tunnel. Where Apple's spelling is surprising the spelling
/// still wins: `superceded` and `canceled` are what the SDK and the documentation
/// say, and a "corrected" spelling would return zero hits.
///
/// `.internalError` is newer (iOS 18.1) than this file's deployment floor and is
/// only legal here because Swift does not apply availability checking to enum
/// patterns; the same case in expression position is a hard error at iOS 15.0,
/// which is why the test has to reach it through `#available`.
func hakoStopReasonSummary(_ reason: NEProviderStopReason) -> String {
    let meaning: String
    switch reason {
    case .none: meaning = "none"
    case .userInitiated: meaning = "user-initiated"
    case .providerFailed: meaning = "provider-failed"
    case .noNetworkAvailable: meaning = "no-network-available"
    case .unrecoverableNetworkChange: meaning = "unrecoverable-network-change"
    case .providerDisabled: meaning = "provider-disabled"
    case .authenticationCanceled: meaning = "authentication-canceled"
    case .configurationFailed: meaning = "configuration-failed"
    case .idleTimeout: meaning = "idle-timeout"
    case .configurationDisabled: meaning = "configuration-disabled"
    case .configurationRemoved: meaning = "configuration-removed"
    case .superceded: meaning = "superceded"
    case .userLogout: meaning = "user-logout"
    case .userSwitch: meaning = "user-switch"
    case .connectionFailed: meaning = "connection-failed"
    case .sleep: meaning = "sleep"
    case .appUpdate: meaning = "app-update"
    case .internalError: meaning = "internal-error"
    @unknown default: meaning = "unrecognized"
    }
    return "\(meaning) (reason=\(reason.rawValue))"
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

/// A lease for one tunnel-start attempt. Late work may publish state only
/// while its session remains current; abandoned work can compensate once
/// while no successor owns the tunnel settings.
final class ProviderTunnelSessionLease: @unchecked Sendable {
    /// A single attempt's identity. Opaque on purpose: holders may only hand it
    /// back, never reason about ordering.
    struct Session: Equatable {
        fileprivate let generation: UInt64
    }

    private let lock = NSLock()
    private var issued: UInt64 = 0
    private var current: UInt64?
    /// Set when a live session was dropped: Apple cannot cancel
    /// `setTunnelNetworkSettings`, so the abandoned attempt may still owe the
    /// system a cleanup even though nothing of it was ever published.
    private var owesCompensation = false

    /// Opens a session and makes it the current one, superseding any predecessor.
    func begin() -> Session {
        lock.lock()
        defer { lock.unlock() }
        issued &+= 1
        current = issued
        return Session(generation: issued)
    }

    /// Drops the current session. A timeout or a teardown calls this; every
    /// later `commitIfCurrent` for that session is refused.
    func invalidate() {
        lock.lock()
        if current != nil { owesCompensation = true }
        current = nil
        lock.unlock()
    }

    /// Runs `commit` only if `session` is still the current one, under the lock
    /// that `invalidate` takes -- so the two cannot interleave.
    ///
    /// - Returns: whether `commit` ran.
    @discardableResult
    func commitIfCurrent(_ session: Session, _ commit: () -> Void) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard current == session.generation else { return false }
        commit()
        return true
    }

    /// Pays an abandoned attempt's cleanup debt, but only while the lease is
    /// idle and only once.
    ///
    /// A successor that has already begun owns whatever is in the system now,
    /// so clearing there would tear down a tunnel that is coming up correctly.
    /// Timeout, stop and a late completion may arrive in any order; whichever
    /// finds the lease idle pays, and the rest are refused.
    ///
    /// - Returns: whether `compensate` ran.
    @discardableResult
    func compensateIfIdle(_ compensate: () -> Void) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard current == nil, owesCompensation else { return false }
        owesCompensation = false
        compensate()
        return true
    }
}

/// Tracks synchronous and asynchronous configuration reloads. Async callers
/// receive a process-scoped ticket and poll retained outcomes without waiting
/// for the configuration apply operation. Synchronous replies remain inline.
final class ProviderReloadEnvelope: @unchecked Sendable {
    /// What `reloadStatus` says. `ticket` is 0 and `state` is `idle` until the
    /// first asynchronous reload of the session; `error` is present only for
    /// `failed`, and is Core's sentence verbatim (the App branches on its
    /// tail); `unknown` answers a ticket this ledger has nothing true to say
    /// about — never issued (the App is asking about a reload it sent to an
    /// extension process that has since been replaced) or older than the last
    /// few kept — so a stranger's outcome is never read as its own.
    struct Status: Equatable {
        enum State: String, Equatable {
            case idle
            case applying
            case ok
            case failed
            case unknown
        }

        let session: String
        let ticket: Int
        let state: State
        let error: String?

        var jsonObject: [String: Any] {
            var object: [String: Any] = ["session": session, "ticket": ticket, "state": state.rawValue]
            if let error { object["error"] = error }
            return object
        }
    }

    /// The answer to a `reload` request: the JSON object to send back, and —
    /// for an accepted asynchronous reload — the `start` that enters Core on
    /// a task of its own. The caller invokes `start` once, after the reply has
    /// been handed to the framework, so the acknowledgement has left before
    /// Core begins to build anything; for the synchronous form and for
    /// refusals there is nothing to start. Calling `start` again does nothing
    /// (two applies of one text would race on Core's mutex and the later
    /// would overwrite the ticket's outcome), and dropping it without ever
    /// calling it abandons the ticket as failed rather than leaving it
    /// applying for the life of the process with every later asynchronous
    /// reload refused behind it.
    struct Reply {
        let object: [String: Any]
        let start: (() -> Void)?
    }

    /// The gate behind `Reply.start`: enters Core once, and abandons the
    /// ticket if it is released unentered.
    private final class Start {
        private let lock = NSLock()
        private var started = false
        private let envelope: ProviderReloadEnvelope
        private let ticket: Int
        private let apply: () throws -> Void

        init(envelope: ProviderReloadEnvelope, ticket: Int, apply: @escaping () throws -> Void) {
            self.envelope = envelope
            self.ticket = ticket
            self.apply = apply
        }

        func run() {
            lock.lock()
            guard !started else {
                lock.unlock()
                return
            }
            started = true
            lock.unlock()
            let envelope = self.envelope
            let ticket = self.ticket
            let apply = self.apply
            Task.detached(priority: .userInitiated) {
                envelope.finish(ticket, envelope.run(apply))
            }
        }

        deinit {
            lock.lock()
            let unstarted = !started
            lock.unlock()
            if unstarted {
                envelope.finish(ticket, Outcome(error: "hako: reload was accepted but never started"))
            }
        }
    }

    static let capability = "reload-async-v1"

    private let lock = NSLock()
    /// Names this ledger for the App: one value per instance, hence per
    /// extension process (the provider holds one for its lifetime).
    private let session = UUID().uuidString
    private var nextTicket = 0
    private var inFlight: Set<Int> = []
    /// Synchronous reloads holding Core right now. Not ticketed — answered
    /// in their own reply — but Core is busy for as long as they run, which
    /// is what an asynchronous request has to be told.
    private var synchronousRunning = 0
    /// Outcomes by ticket, kept for the last few reloads, so a reload
    /// finishing later cannot overwrite an earlier ticket's answer. Bounded,
    /// because the App only ever asks about the reload it just sent.
    private var outcomes: [Int: Status] = [:]
    private static let outcomesKept = 16

    /// Answers a `reload` request. `apply` is the call into Core, handed in so
    /// the envelope's promises can be tested without one; it is invoked at
    /// most once — inline for the synchronous form, and for the asynchronous
    /// form only when the returned `start` is invoked, on a detached task.
    func reply(
        to request: [String: Any],
        apply: @escaping () throws -> Void
    ) -> Reply {
        let asynchronous = Self.isJSONTrue(request["async"])
        lock.lock()
        guard asynchronous else {
            synchronousRunning += 1
            lock.unlock()
            let outcome = run(apply)
            lock.lock()
            synchronousRunning -= 1
            lock.unlock()
            if let error = outcome.error {
                return Reply(object: ["error": error], start: nil)
            }
            return Reply(object: ["ok": true], start: nil)
        }
        if !inFlight.isEmpty || synchronousRunning > 0 {
            var refusal: [String: Any] = ["error": "reload in progress"]
            if let running = inFlight.max() {
                refusal["ticket"] = running
            }
            lock.unlock()
            return Reply(object: refusal, start: nil)
        }
        nextTicket += 1
        let ticket = nextTicket
        inFlight.insert(ticket)
        lock.unlock()
        let start = Start(envelope: self, ticket: ticket, apply: apply)
        return Reply(
            object: ["accepted": true, "ticket": ticket, "session": session],
            start: { start.run() }
        )
    }

    /// Answers a `reloadStatus` request at the wire: `ticket` absent means the
    /// newest reload; a whole number means that reload; anything else is a
    /// malformed question and gets an error, not a plausible answer to a
    /// different question. Reads this object only; never enters Core.
    func statusReply(to request: [String: Any]) -> [String: Any] {
        guard let field = request["ticket"] else {
            return status().jsonObject
        }
        guard let number = field as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID(),
              let ticket = Int(exactly: number)
        else {
            return ["error": "bad ticket"]
        }
        return status(ticket: ticket).jsonObject
    }

    /// The opt-in is the JSON boolean `true` and nothing else. Foundation
    /// bridges the number 1 to `Bool` true, and a caller that wrote
    /// `"async": 1` by mistake would read an acknowledgement as a final
    /// result; the boolean is told apart from a number by its CF type, which
    /// is what JSONSerialization produces for `true`.
    private static func isJSONTrue(_ value: Any?) -> Bool {
        guard let number = value as? NSNumber, CFGetTypeID(number) == CFBooleanGetTypeID() else {
            return false
        }
        return number.boolValue
    }

    /// Answers `reloadStatus`. Reads this object only; never enters Core.
    ///
    /// Without a ticket the answer is about the newest asynchronous reload of
    /// the session; with one, about that reload — which is what an App that
    /// holds a ticket should ask, so that another reload finishing in between
    /// cannot be mistaken for its own.
    func status(ticket requested: Int? = nil) -> Status {
        lock.lock()
        defer { lock.unlock() }
        guard let ticket = requested else {
            if nextTicket == 0 {
                return Status(session: session, ticket: 0, state: .idle, error: nil)
            }
            return statusLocked(of: nextTicket)
        }
        return statusLocked(of: ticket)
    }

    /// Tickets are issued from 1, so 0 and below were never issued.
    private func statusLocked(of ticket: Int) -> Status {
        if ticket >= 1, inFlight.contains(ticket) {
            return Status(session: session, ticket: ticket, state: .applying, error: nil)
        }
        return outcomes[ticket] ?? Status(session: session, ticket: ticket, state: .unknown, error: nil)
    }

    private struct Outcome {
        let error: String?
    }

    private func run(_ apply: () throws -> Void) -> Outcome {
        do {
            try apply()
            return Outcome(error: nil)
        } catch {
            return Outcome(error: error.localizedDescription)
        }
    }

    private func finish(_ ticket: Int, _ outcome: Outcome) {
        lock.lock()
        inFlight.remove(ticket)
        outcomes[ticket] = Status(
            session: session,
            ticket: ticket,
            state: outcome.error == nil ? .ok : .failed,
            error: outcome.error
        )
        while outcomes.count > Self.outcomesKept, let oldest = outcomes.keys.min() {
            outcomes.removeValue(forKey: oldest)
        }
        lock.unlock()
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
