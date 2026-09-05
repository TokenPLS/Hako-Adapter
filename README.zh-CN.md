# Hako Adapter

[English](README.md) · 简体中文

用于连接 Apple Packet Tunnel 扩展与代理内核的 Swift 组件。本仓库包含 Hako 客户端使用的数据包桥接与扩展生命周期代码。

## 官网与客户端下载

- [官方网站](https://clash.md/)
- [在 App Store 下载 Clash](https://apps.apple.com/app/id6794257189)

完整应用见 [Hako-Client](https://github.com/TokenPLS/Hako-Client)，代理内核与 SDK 构建工具见 [Hako](https://github.com/TokenPLS/Hako)。

## 组件

| 源文件 | 职责 |
| --- | --- |
| [`PacketFlowBridge.swift`](Sources/HakoAdapter/PacketFlowBridge.swift) | 在 `NEPacketTunnelFlow` 与文件描述符之间传递数据包，处理帧格式、有界队列和流量统计 |
| [`ProviderLifecycle.swift`](Sources/HakoAdapter/ProviderLifecycle.swift) | 协调扩展状态、会话所有权、重载、清理与物理网络路径监测 |

这些组件使用 Apple 系统框架，不包含代理引擎、应用界面或完整的 `NEPacketTunnelProvider` 实现。

## 环境要求

- macOS、Xcode 及目标平台 SDK。
- Swift 5.9 或更新版本。
- 部署目标为 iOS 15+、macOS 13+ 或 tvOS 17+。
- 由应用自行配置所需能力与签名的 Packet Tunnel 扩展。

## 集成源码

当前类型使用 Swift 的 `internal` 访问级别，需要与调用方一起编译进同一个构建目标。仅从另一个模块导入该包，无法将这些类型作为公开 API 使用。

1. 检出本仓库的固定提交。
2. 将 `Sources/HakoAdapter` 下的文件加入 Packet Tunnel 扩展构建目标，并保留随附许可证。
3. 在自己的扩展中接入内核，协调启动、网络设置、数据包处理和停止流程。

[Hako-Client 的依赖准备脚本](https://github.com/TokenPLS/Hako-Client/blob/main/scripts/bootstrap.py) 展示了如何获取固定版本，并将源码放入调用方工程。

`PacketFlowBridge.start()` 返回套接字对中面向内核的文件描述符，数据包带四字节地址族前缀；它不是 Apple 私有的 utun 描述符。隧道会话期间应持有桥接对象，并在清理时调用 `stop()`。描述符所有权与错误处理以源码为准。

## 状态与反馈

当前为预发布集成代码。请固定具体提交，并在自己的应用中验证生命周期和数据包收发。仓库的包声明用于描述构建结构，不代表已经提供稳定的外部 Swift API。

组件问题请提交到本仓库的 [Issues](https://github.com/TokenPLS/Hako-Adapter/issues)，附上提交、目标平台和最小复现。应用问题请提交到 [Hako-Client](https://github.com/TokenPLS/Hako-Client/issues)，内核问题请提交到 [Hako](https://github.com/TokenPLS/Hako/issues)。公开反馈中请移除凭据和订阅链接。安全问题请遵循 [SECURITY.md](SECURITY.md)。

## 许可证

[GPL-3.0](LICENSE)。署名信息见源码与许可证文件。
