//! # rumqttc — 原生 MQTT 客户端库
//!
//! 这是一个基于 Rust 编写的 MQTT 客户端库，编译为 Android 动态库（.so 文件），
//! 通过 JNI（Java Native Interface）供 Kotlin/Java 端调用。
//!
//! ## 为什么用 Rust 而不是纯 Java/Kotlin？
//!
//! 1. **性能更好**：Rust 是编译型语言，没有 GC 停顿
//! 2. **内存安全**：Rust 的所有权机制在编译期就能防止内存泄漏和空指针
//! 3. **不会卡主线程**：所有网络操作都在后台异步执行
//!
//! ## 模块划分
//!
//! 本项目分为 4 个模块，各司其职：
//!
//! | 模块 | 文件 | 职责 |
//! |------|------|------|
//! | `config` | config.rs | 存放 MQTT 连接配置（服务器地址、密码、QoS 等） |
//! | `callbacks` | callbacks.rs | 管理 JNI 回调，把 Rust 端的事件通知回传给 Kotlin |
//! | `core` | core.rs | 核心 MQTT 客户端逻辑（连接、重连、订阅、发布） |
//! | `jni` | jni.rs | JNI 桥接层，定义 Kotlin 调用的 native 方法 |
//!
//! ## 调用流程
//!
//! ```text
//! Kotlin 端                    Rust 端
//! ────────                    ────────
//! RumqttcClient()    →     nativeCreate()      创建核心实例，返回指针
//! .connect(...)         →     nativeConnect()     启动连接 + 事件循环
//! .publish(...)         →     nativePublish()     发送消息
//! .isConnected()        →     nativeIsConnected() 查询连接状态
//! .destroy()            →     nativeDestroy()     释放所有资源
//!
//! Rust → Kotlin 回调：
//!   connectComplete()   连接/重连成功时调用
//!   connectionLost()    连接断开时调用
//!   onMsg()             收到 MQTT 消息时调用
//!   onError()           发生错误时调用
//! ```

/// MQTT 连接配置模块
///
/// 定义 [`MqttConfig`] 结构体，存放连接 MQTT 服务器所需的全部参数。
pub mod config;

/// JNI 回调管理模块
///
/// 定义 [`CallbackManager`] 结构体，负责把 Rust 端的 MQTT 事件
/// （连接成功、收到消息等）通过 JNI 反向调用回传给 Kotlin 端。
pub mod callbacks;

/// 核心 MQTT 客户端模块
///
/// 定义 [`NativeMqttCore`] 结构体，包含：
/// - Tokio 异步运行时
/// - 事件循环（负责连接、收发消息、心跳）
/// - 无限重连逻辑
/// - 发布消息功能
pub mod core;

/// JNI 导出函数模块
///
/// 定义所有 `extern "C"` 函数，函数名必须严格匹配 Kotlin 端的声明。
/// 命名规则：`Java_包名_类名_方法名`，例如：
/// `Java_com_rust_rumqttc_RumqttcClient_nativeCreate`
pub mod jni;
