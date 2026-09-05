// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "HakoAdapter",
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
        .tvOS(.v17),
    ],
    products: [
        .library(name: "HakoAdapter", targets: ["HakoAdapter"]),
    ],
    targets: [
        .target(name: "HakoAdapter", path: "Sources/HakoAdapter"),
    ]
)
