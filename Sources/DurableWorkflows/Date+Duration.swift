import Foundation

extension Date {
  /// Wall-clock timestamp `duration` from now — the storage-side deadline
  /// for durable timers.
  init(after duration: Duration) {
    self.init(timeIntervalSinceNow: TimeInterval(duration))
  }
}
