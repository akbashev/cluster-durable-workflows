// swift-tools-version: 6.4
import CompilerPluginSupport
import PackageDescription

/// The language as Swift 7 will have it, for every target.
let swiftSettings: [SwiftSetting] = [
  .enableUpcomingFeature("ExistentialAny"),
  .enableUpcomingFeature("ImmutableWeakCaptures"),
  .enableUpcomingFeature("InferIsolatedConformances"),
  .enableUpcomingFeature("InternalImportsByDefault"),
  .enableUpcomingFeature("MemberImportVisibility"),
  .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
]

let package = Package(
  name: "durable-workflows",
  platforms: [
    .macOS("26.4")
  ],
  products: [
    .library(name: "DurableWorkflows", targets: ["DurableWorkflows"])
  ],
  dependencies: [
    .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.11.0"),
    .package(url: "https://github.com/akbashev/cluster-event-sourcing.git", branch: "turgai-integration"),
    .package(url: "https://github.com/akbashev/cluster-virtual-actors.git", branch: "stable-references"),
    .package(url: "https://github.com/turgai-labs/swift-distributed-actors.git", branch: "turgai-integration"),
    .package(url: "https://github.com/swiftlang/swift-syntax.git", "509.0.0"..<"605.0.0"),
  ],
  targets: [
    .macro(
      name: "DurableWorkflowsMacros",
      dependencies: [
        .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
        .product(name: "SwiftSyntax", package: "swift-syntax"),
        .product(name: "SwiftSyntaxBuilder", package: "swift-syntax"),
        .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
        .product(name: "SwiftDiagnostics", package: "swift-syntax"),
      ],
      swiftSettings: swiftSettings
    ),
    .target(
      name: "DurableWorkflows",
      dependencies: [
        "DurableWorkflowsMacros",
        .product(name: "EventSourcing", package: "cluster-event-sourcing"),
        .product(name: "VirtualActors", package: "cluster-virtual-actors"),
        .product(name: "DistributedCluster", package: "swift-distributed-actors"),
        .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
      ],
      swiftSettings: swiftSettings
    ),
    .testTarget(
      name: "DurableWorkflowsTests",
      dependencies: [
        "DurableWorkflows",
        .product(name: "EventSourcing", package: "cluster-event-sourcing"),
        .product(name: "VirtualActors", package: "cluster-virtual-actors"),
        .product(name: "DistributedCluster", package: "swift-distributed-actors"),
      ],
      swiftSettings: swiftSettings
    ),
  ]
)
