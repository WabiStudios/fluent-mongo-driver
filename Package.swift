// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "fluent-mongo-driver",
    platforms: [
      .macOS(.v13),
      .iOS(.v16),
      .tvOS(.v12),
      .visionOS(.v1),
      .watchOS(.v4),
    ],
    products: [
        .library(name: "FluentMongoDriver", targets: ["FluentMongoDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/fluent-kit.git", from: "1.37.0"),
        .package(url: "https://github.com/WabiStudios/MongoKitten.git", from: "9.0.0"),
        .package(url: "https://github.com/orlandos-nl/DNSClient.git", exact: "2.3.0"),
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
