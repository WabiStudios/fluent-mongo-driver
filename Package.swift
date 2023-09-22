// swift-tools-version: 5.9
import PackageDescription

let package = Package(
  name: "fluent-mongo-driver",
  platforms: [
    .macOS(.v13),
    .iOS(.v16),
    .visionOS(.v1),
    .watchOS(.v6),
    .tvOS(.v13),
  ],
  products: [
    .library(name: "FluentMongoDriver", targets: ["FluentMongoDriver"]),
  ],
  dependencies: [
    .package(url: "https://github.com/vapor/fluent-kit.git", from: "1.45.0"),
    .package(url: "https://github.com/WabiStudios/MongoKitten.git", from: "6.7.16"),
    .package(url: "https://github.com/WabiStudios/DNSClient.git", from: "2.4.2"),
  ],
  targets: [
    .target(
      name: "FluentMongoDriver",
      dependencies: [
        .product(name: "FluentKit", package: "fluent-kit"),
        .product(name: "MongoKitten", package: "MongoKitten"),
        .product(name: "DNSClient", package: "DNSClient"),
      ]
    ),
    .testTarget(
      name: "FluentMongoDriverTests",
      dependencies: [
        .target(name: "FluentMongoDriver"),
        .product(name: "FluentBenchmark", package: "fluent-kit"),
      ]
    ),
  ]
)
