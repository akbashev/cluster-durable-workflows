import Foundation

extension TimeInterval {
  /// Mirrors swift-foundation's internal `TimeInterval.init(_: Duration)`
  /// (FoundationInternationalization/TimeInterval+Utils.swift) — no public
  /// Date↔Duration API exists to delegate to. If one ships, delete this.
  init(_ duration: Duration) {
    self = Double(duration.components.seconds) + 1e-18 * Double(duration.components.attoseconds)
  }
}
