//! # 核心 MQTT 客户端模块
//!
//! 这是整个 MQTT 库的"大脑"，负责：
//!
//! 1. **创建 Tokio 异步运行时** — 所有网络操作都在后台线程执行，不卡主线程
//! 2. **建立 MQTT 连接** — 连接到指定的服务器地址和端口
//! 3. **事件循环** — 不断接收服务器事件（连接成功、收到消息、连接断开等）
//! 4. **无限重连** — 连接断开后自动等待一段时间再重新连接，永不放弃
//! 5. **发布消息** — 向指定主题发送数据
//! 6. **优雅关闭** — 收到销毁指令后，等待后台任务完成再释放资源
//!
//! ## 关键设计决策
//!
//! ### 为什么用 `new_multi_thread()` 而不是 `new_current_thread()`？
//!
//! Tokio 有两种运行时：
//!
//! | 类型 | 特点 | 适合场景 |
//! |------|------|---------|
//! | `current_thread` | 只在当前线程运行任务，必须 `block_on()` 才能驱动 | 简单单线程应用 |
//! | `multi_thread` | 自动创建线程池，`spawn()` 后任务自动运行 | **本项目** |
//!
//! 本项目使用 `spawn()` 来启动后台事件循环，但**不会**对返回的 `JoinHandle` 做 `block_on()`。
//! 如果用 `current_thread`，`spawn()` 出去的任务永远不会被执行（因为没有人在驱动 runtime）。
//! 用 `multi_thread` 就不需要 `block_on()`，后台任务会自动在线程池里运行。
//!
//! ### 为什么用 `Arc<Mutex<Option<AsyncClient>>>` 而不是直接存 `AsyncClient`？
//!
//! - `AsyncClient` 是在连接成功后才创建的，所以初始值是"空的"→ 用 `Option`
//! - 事件循环（后台线程）和 `publish()`（前台线程）都可能访问它 → 用 `Arc<Mutex<>>`
//! - `Arc` 让多个线程可以同时持有同一个值的引用
//! - `Mutex` 保证同一时刻只有一个线程在读写，防止数据竞争
//!
//! ## 核心数据结构
//!
//! [`NativeMqttCore`] — 持有运行时、客户端、回调管理器等所有状态

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, NetworkOptions, Packet, QoS};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::callbacks::{CallbackManager, ERR_PUBLISH_FAILED};
use crate::config::MqttConfig;
use crate::MutexExt;

/// MQTT 消息（事件循环 → 回调分发器之间传递的数据）
///
/// 当 MQTT 事件循环收到服务器推送的消息时，
/// 不会直接调用 JNI 回调（会阻塞事件循环），
/// 而是把消息封装成这个结构体，发送到消息队列。
/// 由独立的回调分发器 task 从队列中取出并调用 JNI。
struct MqttMessage {
    /// 消息主题，例如 "/device/001/data"
    topic: String,
    /// 消息内容（UTF-8 字符串），通常是 JSON
    payload: String,
}

/// 待发布的消息请求（publish() → publish_sender task）
///
/// 为什么需要这个结构体？
///
/// 优化前的 publish() 每次调用都 spawn 一个新的 Tokio task，
/// 如果上层在连接成功后立即发 1000 条消息，就会产生 1000 个 task，
/// 每个 task 都要抢锁、clone client、await 网络发送，开销巨大。
///
/// 优化后：publish() 只是把数据塞进 channel（零开销），
/// 由一个常驻的 publish_sender task 统一从 channel 取出并发送。
/// 1000 次 publish 只产生 1000 次 channel 写入，没有额外 task 创建。
struct PublishRequest {
    /// 目标主题，例如 "/device/001/data"
    topic: String,
    /// 消息内容（字节数组），可以是任何二进制数据
    payload: Vec<u8>,
    /// 消息服务质量等级（0/1/2）
    qos: QoS,
}

/// 核心 MQTT 客户端
///
/// 这是整个库的核心结构体，包含了运行一个 MQTT 客户端所需的全部组件。
/// 它的生命周期由 JNI 层管理：
///
/// 1. `nativeCreate()` → `NativeMqttCore::new()` → `Box::into_raw()` 分配在堆上
/// 2. Kotlin 端持有一个 `Long` 类型的指针值
/// 3. `nativeDestroy()` → `Box::from_raw()` 回收内存 → 自动触发 `Drop`
///
/// ## 字段详解
///
/// | 字段 | 类型 | 说明 |
/// |------|------|------|
/// | `client` | `Arc<Mutex<Option<AsyncClient>>>` | MQTT 异步客户端句柄 |
/// | `runtime` | `tokio::runtime::Runtime` | Tokio 多线程运行时 |
/// | `cancel_token` | `CancellationToken` | 用于通知后台任务停止 |
/// | `is_connected` | `Arc<AtomicBool>` | 连接状态标志（原子读写，无锁） |
/// | `callbacks` | `Mutex<Option<Arc<CallbackManager>>>` | 回调管理器 |
/// | `msg_tx` | `Option<mpsc::Sender<MqttMessage>>` | 消息队列发送端（connect 时创建） |
pub struct NativeMqttCore {
    /// MQTT 异步客户端句柄
    ///
    /// `AsyncClient` 是 rumqttc 提供的客户端，它的特点是**轻量可克隆**：
    /// 内部只是一个 channel sender（消息通道的发送端），clone 的成本极低。
    ///
    /// 为什么用 `Arc<Mutex<Option<...>>>` 这层包装？
    ///
    /// - `Option`：客户端在创建时还没有连接，所以初始值是 `None`
    /// - `Mutex`：前台线程（publish）和后台线程（事件循环）都可能需要读写它
    /// - `Arc`：让 `connect()` 中 spawn 出去的异步任务也能持有它的引用
    ///
    /// **重要**：在异步代码中使用 `std::sync::Mutex` 时，必须确保
    /// `MutexGuard`（锁的保护范围）不会跨越 `.await` 点。
    /// 因为 `MutexGuard` 不是 `Send` 的，跨越 await 会导致编译错误。
    /// 解决方案：在锁内 clone 出 `AsyncClient`，然后立刻释放锁，再 `.await`。
    client: Arc<Mutex<Option<AsyncClient>>>,

    /// Tokio 多线程运行时
    ///
    /// Tokio 是 Rust 最流行的异步运行时，负责调度和执行所有的 async 任务。
    ///
    /// 我们使用 `new_multi_thread()` 创建多线程运行时，原因：
    /// - `connect()` 中用 `runtime.spawn()` 启动后台事件循环
    /// - `publish()` 中也用 `runtime.spawn()` 启动异步发布任务
    /// - 多线程运行时让 `spawn()` 出去的任务自动在线程池中运行，不需要手动 `block_on()`
    ///
    /// 如果用 `new_current_thread()`，必须有人在当前线程调用 `block_on()` 来驱动 runtime，
    /// 否则 spawn 出去的任务永远不会被执行——这是我们早期遇到的最严重的 bug。
    runtime: tokio::runtime::Runtime,

    /// 取消令牌（CancellationToken）
    ///
    /// `tokio_util` 提供的取消机制，用于优雅地停止后台任务。
    ///
    /// 工作原理：
    /// 1. 后台事件循环在每次循环开头检查 `cancel_token.is_cancelled()`
    /// 2. 在 `tokio::select!` 中也监听 `cancel_token.cancelled()` 这个 future
    /// 3. 当 `shutdown()` 被调用时，调用 `cancel_token.cancel()`
    /// 4. 后台任务检测到取消信号，退出循环
    ///
    /// 为什么不用 `tokio::task::JoinHandle::abort()`？
    /// 因为 `abort()` 是强制终止，可能导致资源泄漏（比如未释放的 GlobalRef）。
    /// `CancellationToken` 是协作式取消，让后台任务有机会做清理工作。
    cancel_token: CancellationToken,

    /// 连接状态标志
    ///
    /// 使用 `AtomicBool`（原子布尔值）而不是 `Mutex<bool>`，原因：
    ///
    /// - **无锁**：原子操作不需要加锁，性能极高（纳秒级）
    /// - **线程安全**：可以在任何线程安全地读写
    /// - `Ordering::Relaxed`：最宽松的内存序，不保证与其他变量的顺序关系。
    ///   对于我们这个场景足够了——我们只关心"是否连接"这个单一状态，
    ///   不需要它和其他变量保持一致。
    ///
    /// 用 `Arc` 包装是因为事件循环（后台线程）和 `is_connected()` 方法（前台线程）
    /// 都需要访问它。
    is_connected: Arc<AtomicBool>,

    /// 是否已经执行过 shutdown
    ///
    /// 防止 shutdown() 被重复执行：
    /// - nativeDestroy() 先显式调用 shutdown()
    /// - 然后 drop() 触发 Drop trait → 再次调用 shutdown()
    /// - 如果没有这个标记，第二次 shutdown 会多等约 3 秒
    is_shutdown: AtomicBool,

    /// 回调管理器
    ///
    /// 在 `connect()` 时由 JNI 层创建并存入，之后 `publish()` 等操作可以复用。
    ///
    /// 为什么用 `Mutex<Option<Arc<CallbackManager>>>` 而不是直接存 `CallbackManager`？
    ///
    /// - `Option`：`new()` 时还没有回调，在 `connect()` 时才设置
    /// - `Mutex`：JNI 线程（设置回调）和 Rust 线程（使用回调）可能并发访问
    /// - `Arc`：事件循环中的异步任务也需要持有回调管理器的引用，
    ///   用 `Arc` 可以 clone 一份给异步任务，不影响主结构体中的那份
    callbacks: Mutex<Option<Arc<CallbackManager>>>,

    /// 消息队列发送端
    ///
    /// 事件循环收到消息后，通过这个 sender 把消息发送到队列。
    /// 队列的另一端是回调分发器，负责取出消息并调用 JNI 回调。
    ///
    /// 为什么用 `Option<mpsc::Sender>`？
    /// - `new()` 时还没有 channel，在 `connect()` 时才创建
    /// - `mpsc` = multiple producer, single consumer（多生产者，单消费者）
    /// - 支持异步 `send().await`，队列满时会背压（等待而不是丢弃）
    msg_tx: Mutex<Option<mpsc::Sender<MqttMessage>>>,

    /// 发布消息 channel 发送端
    ///
    /// `publish()` 把待发送的消息塞进这个 channel，
    /// 常驻的 `publish_sender` task 从 channel 取出并调用 rumqttc 发送。
    ///
    /// ## 为什么用 channel 而不是直接 spawn？
    ///
    /// | 方式 | 1000 次 publish 的开销 |
    /// |------|----------------------|
    /// | spawn 模式（优化前） | 1000 个 task 创建 + 1000 次锁竞争 + 1000 次 clone |
    /// | channel 模式（优化后） | 1000 次 channel 写入（纳秒级） |
    ///
    /// 常驻 task 只有一个，它持有 client 的引用，
    /// 串行地从 channel 取出消息并发送，没有锁竞争。
    publish_tx: Mutex<Option<mpsc::Sender<PublishRequest>>>,
}

impl NativeMqttCore {
    /// 创建一个新的核心客户端实例
    ///
    /// 这个方法会：
    /// 1. 创建一个 Tokio 多线程运行时（`new_multi_thread()`）
    /// 2. 初始化所有字段为默认值
    ///
    /// ## 返回值
    ///
    /// - `Ok(NativeMqttCore)` — 创建成功
    /// - `Err(String)` — 创建 Tokio 运行时失败（极少发生，除非系统资源不足）
    ///
    /// ## 为什么返回 `Result` 而不是 panic？
    ///
    /// 因为这是 FFI（跨语言调用）的入口，如果 Rust 代码 panic，
    /// 会导致整个 JVM 进程崩溃（ART abort），用户体验极差。
    /// 所以所有可能失败的地方都用 `Result` + 错误回调来处理。
    pub fn new() -> Result<Self, String> {
        // 创建 Tokio 多线程运行时
        //
        // 配置说明：
        // - worker_threads(2): 固定 2 个工作线程，避免默认按 CPU 核心数创建导致资源浪费
        //   MQTT 客户端 I/O 密集但并发不高，2 个线程足够处理收发消息
        // - thread_name("mqtt-io"): 线程名前缀，方便在 Logcat 中过滤排查
        //   实际线程名为 "mqtt-io-0" 和 "mqtt-io-1"
        // - enable_all(): 启用所有 Tokio 功能（I/O 驱动、时间驱动等）
        //   如果不加，调用 tokio::time::sleep() 会 panic
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mqtt-io")
            .enable_all()
            .build()
            .map_err(|e| format!("创建 Tokio runtime 失败: {}", e))?;

        Ok(Self {
            // 客户端初始为空，connect() 时才会创建
            client: Arc::new(Mutex::new(None)),
            runtime,
            // 创建一个新的取消令牌，初始状态是"未取消"
            cancel_token: CancellationToken::new(),
            // 初始状态为"未连接"
            is_connected: Arc::new(AtomicBool::new(false)),
            // 初始状态为"未关闭"
            is_shutdown: AtomicBool::new(false),
            // 回调管理器初始为空，connect() 时由 JNI 层设置
            callbacks: Mutex::new(None),
            // 消息队列发送端初始为空，connect() 时创建 channel
            msg_tx: Mutex::new(None),
            // 发布消息 channel 发送端初始为空，connect() 时创建 channel
            publish_tx: Mutex::new(None),
        })
    }

    /// 查询当前是否已连接
    ///
    /// 这是一个**无锁**操作，直接读取原子变量的值，开销在纳秒级别。
    /// 可以在任何线程（包括主线程）安全调用，不会阻塞。
    ///
    /// ## 返回值
    ///
    /// - `true` — 当前已与 MQTT 服务器建立连接
    /// - `false` — 未连接（可能是还没连接，也可能是断开了正在重连）
    pub fn is_connected(&self) -> bool {
        // `Ordering::Relaxed` 表示最宽松的内存序
        // 对于"查询状态"这种场景足够了，不需要严格的同步保证
        self.is_connected.load(Ordering::Relaxed)
    }

    /// 设置回调管理器
    ///
    /// 在 JNI 层的 `nativeConnect()` 中调用，把 Kotlin 端传入的回调对象
    /// 包装成 `CallbackManager` 后存入核心结构体。
    ///
    /// ## 为什么要在 connect 之前设置？
    ///
    /// 因为 connect 启动的后台事件循环需要回调来通知 Kotlin 端，
    /// 如果回调还没设置就启动事件循环，收到消息也无法通知上层。
    pub fn set_callbacks(&self, callbacks: Arc<CallbackManager>) {
        let mut guard = self.callbacks.lock_unpoisoned();
        *guard = Some(callbacks);
        // guard 在这里被自动释放（离开作用域时 drop）
    }

    /// 获取回调管理器的引用（内部方法）
    ///
    /// 返回 `Arc<CallbackManager>` 的克隆，这样调用者可以独立持有引用，
    /// 不需要一直持有 Mutex 的锁。
    ///
    /// ## 返回值
    ///
    /// - `Some(Arc<CallbackManager>)` — 回调管理器已设置
    /// - `None` — 还没调用 `set_callbacks()`
    fn get_callbacks(&self) -> Option<Arc<CallbackManager>> {
        let guard = self.callbacks.lock_unpoisoned();
        // `.clone()` 这里克隆的是 Arc（智能指针），不是 CallbackManager 本身
        // Arc::clone() 只是增加引用计数，成本是 O(1) 的
        guard.clone()
    }

    /// 启动连接并进入事件循环
    ///
    /// 这个方法会：
    /// 1. 从 `self.callbacks` 中取出回调管理器
    /// 2. 把连接所需的所有数据打包，用 `runtime.spawn()` 启动一个异步任务
    /// 3. 异步任务中运行 `event_loop()`，负责连接、收发消息、重连
    ///
    /// ## 注意
    ///
    /// - 这个方法**立即返回**，不会等待连接成功
    /// - 连接成功后会通过 `callbacks.connect_complete()` 回调通知
    /// - 如果回调管理器还没设置，会记录错误日志并直接返回
    pub fn connect(&self, config: MqttConfig) {
        // clone Arc，让异步任务也能持有这些共享数据的引用
        // 这些 clone 只是增加引用计数，不会复制底层数据
        let client_arc = self.client.clone();
        let cancel_token = self.cancel_token.clone();
        let is_connected = self.is_connected.clone();
        let callbacks = self.get_callbacks();

        // 检查回调管理器是否已设置
        // `let Some(x) = y else { ... }` 是 Rust 的模式匹配语法：
        // 如果 callbacks 是 Some，就解包赋值给 callbacks；
        // 如果是 None，就执行 else 分支
        let Some(callbacks) = callbacks else {
            log::error!("[MQTT] 回调管理器未设置，无法连接");
            return;
        };

        // 创建消息队列
        //
        // mpsc::channel() 返回 (sender, receiver)：
        // - sender: 事件循环用它发送消息到队列
        // - receiver: 回调分发器用它从队列取消息
        //
        // 容量从 config.message_buffer_size 读取，默认 10000
        let (msg_tx, msg_rx) = mpsc::channel::<MqttMessage>(config.message_buffer_size);

        // 把 sender 存入结构体，供 publish() 等方法使用
        {
            let mut guard = self.msg_tx.lock_unpoisoned();
            *guard = Some(msg_tx.clone());
        }

        // 启动回调分发器
        //
        // 这是一个独立的异步任务，负责：
        // 1. 从消息队列中取出消息
        // 2. 调用 JNI 回调通知 Kotlin 端
        // 3. 即使回调很慢，也不会阻塞事件循环
        let callbacks_clone = callbacks.clone();
        let cancel_token_clone = cancel_token.clone();
        self.runtime.spawn(async move {
            Self::callback_dispatcher(msg_rx, callbacks_clone, cancel_token_clone).await;
        });

        // 创建发布消息 channel
        //
        // publish() 把待发送的消息塞进这个 channel，
        // 常驻的 publish_sender task 从 channel 取出并调用 rumqttc 发送。
        // 容量 200，足够缓冲测试时的突发消息，正常运行通常不到 10。
        let (publish_tx, publish_rx) = mpsc::channel::<PublishRequest>(200);

        // 把 sender 存入结构体，供 publish() 使用
        {
            let mut guard = self.publish_tx.lock_unpoisoned();
            *guard = Some(publish_tx.clone());
        }

        // 启动发布消息转发器
        //
        // 这是一个常驻的异步任务，串行地从 channel 取出消息并发送，
        // 避免了每次 publish 都 spawn 新 task 的开销。
        let publish_client_arc = client_arc.clone();
        let publish_callbacks = callbacks.clone();
        let publish_is_connected = is_connected.clone();
        self.runtime.spawn(async move {
            Self::publish_sender(publish_rx, publish_client_arc, publish_callbacks, publish_is_connected).await;
        });

        // 启动事件循环
        //
        // `runtime.spawn()` 和 `tokio::spawn()` 的区别：
        // - `runtime.spawn()` 可以在任何线程调用（包括非 async 上下文）
        // - `tokio::spawn()` 只能在 async 上下文（runtime 内部）调用
        //
        // 因为我们当前在 JNI 线程（不是 async 上下文），所以必须用 `runtime.spawn()`
        self.runtime.spawn(async move {
            // `async move` 表示这个异步闭包会"拿走"（move）所有捕获的变量
            // 这样即使 `connect()` 函数返回了，异步任务仍然持有这些变量
            Self::event_loop(config, callbacks, client_arc, cancel_token, is_connected, msg_tx).await;
        });
    }

    /// 发布消息转发器（常驻任务）
    ///
    /// 串行地从 channel 取出消息并发送给 MQTT broker，
    /// 避免了每次 publish 都 spawn 新 task 的开销。
    ///
    /// ## 工作流程
    ///
    /// 1. 从 `publish_rx` channel 中取出 `PublishRequest`
    /// 2. 从 `client_arc` 中获取 `AsyncClient`（加锁 → clone → 释放锁）
    /// 3. 调用 rumqttc 的 `client.publish()` 发送消息
    /// 4. 循环直到 channel 关闭
    ///
    /// ## 性能特点
    ///
    /// - 只有一个 task 在运行，没有额外开销
    /// - 串行发送，避免锁竞争
    /// - channel 容量 1000，足够缓冲突发消息
    async fn publish_sender(
        mut publish_rx: mpsc::Receiver<PublishRequest>,
        client_arc: Arc<Mutex<Option<AsyncClient>>>,
        callbacks: Arc<CallbackManager>,
        is_connected: Arc<AtomicBool>,
    ) {
        log::info!("[MQTT] publish_sender 任务启动");

        while let Some(req) = publish_rx.recv().await {
            // 快速检查连接状态（原子读，纳秒级开销）
            // 断线时 rumqttc 的 AsyncClient::publish() 仍会返回 Ok（消息只是入内部队列），
            // 但 QoS 0 的消息在断线后会被直接丢弃，不会在重连后重发。
            // 这里提前拦截并通知上层，避免消息"假成功"。
            if !is_connected.load(Ordering::Relaxed) {
                let payload_str = String::from_utf8_lossy(&req.payload);
                let msg = format!("未连接，消息丢弃: {}", payload_str);
                log::warn!("[MQTT] {}", msg);
                callbacks.on_error(ERR_PUBLISH_FAILED, &msg);
                continue;
            }

            // 从 Mutex 中取出 client（加锁 → clone → 释放锁）
            let client = {
                let guard = client_arc.lock_unpoisoned();
                guard.clone()
            };

            // 如果 client 存在，发送消息
            if let Some(c) = client {
                match c.publish(req.topic.clone(), req.qos, false, req.payload).await {
                    Ok(_) => {
                        log::debug!("[MQTT] 发布成功: {}", req.topic);
                    }
                    Err(e) => {
                        let msg = format!("发布失败: {} - {}", req.topic, e);
                        log::error!("[MQTT] {}", msg);
                        callbacks.on_error(ERR_PUBLISH_FAILED, &msg);
                    }
                }
            } else {
                let payload_str = String::from_utf8_lossy(&req.payload);
                let msg = format!("client 不存在，丢弃消息: {}", payload_str);
                log::warn!("[MQTT] {}", msg);
                callbacks.on_error(ERR_PUBLISH_FAILED, &msg);
            }
        }

        log::info!("[MQTT] publish_sender 任务结束");
    }

    /// 核心事件循环：持续处理 MQTT 事件，利用 rumqttc 内置重连机制
    ///
    /// 这是整个 MQTT 客户端的心脏，运行在 Tokio 线程池的某个后台线程上。
    ///
    /// ## 工作流程
    ///
    /// ```text
    /// ┌──────────────────────────────────────────────┐
    /// │  单循环（事件处理循环）                        │
    /// │                                              │
    /// │  1. 检查是否被取消（cancel_token）             │
    /// │  2. 调用 eventloop.poll() 获取下一个事件      │
    /// │     ├─ 收到 ConnAck → 标记连接成功 → 订阅主题  │
    /// │     ├─ 收到 Publish → 发送到消息队列           │
    /// │     ├─ 收到错误 → 回调 connectionLost()       │
    /// │     │              → sleep 后继续（rumqttc    │
    /// │     │                内部自动重连）            │
    /// │     └─ 收到取消信号 → 直接退出                 │
    /// │  3. 回到第 1 步                               │
    /// └──────────────────────────────────────────────┘
    /// ```
    ///
    /// ## rumqttc 内置重连机制
    ///
    /// rumqttc 的 `EventLoop::poll()` 有内置重连逻辑：
    /// - 当连接断开时，`poll()` 内部会调用 `clean()` 保存未确认的 QoS 1/2 消息
    /// - 下一次 `poll()` 自动触发重连，返回新的 `ConnAck`
    /// - 我们只需要持续调用 `poll()`，不需要手动重建 `AsyncClient`
    ///
    /// ## 参数说明
    ///
    /// - `config` — MQTT 连接配置（拥有所有权，因为整个生命周期都在这个函数里）
    /// - `callbacks` — 回调管理器（Arc 包装，可以和外部共享）
    /// - `client_arc` — 客户端句柄的共享引用
    /// - `cancel_token` — 取消令牌
    /// - `is_connected` — 连接状态标志
    /// - `msg_tx` — 消息队列发送端，收到消息时发送到队列
    async fn event_loop(
        config: MqttConfig,
        callbacks: Arc<CallbackManager>,
        client_arc: Arc<Mutex<Option<AsyncClient>>>,
        cancel_token: CancellationToken,
        is_connected: Arc<AtomicBool>,
        msg_tx: mpsc::Sender<MqttMessage>,
    ) {
        // 记录是否曾经成功连接过
        // 用于区分"首次连接"和"重连"：
        // - 首次连接成功 → connect_complete(false, server_uri)
        // - 重连成功 → connect_complete(true, server_uri)
        let mut was_ever_connected = false;

        // 构造服务器地址字符串，用于日志和回调
        // 例如 "192.168.1.100:1883"
        let server_uri = format!("{}:{}", config.host, config.port);

        // ── 只创建一次 MqttOptions、AsyncClient 和 EventLoop ──
        //
        // rumqttc 0.25.1 的 EventLoop 有内置自动重连机制：
        // 当连接断开时，poll() 内部会调用 clean() 保存状态，
        // 下一次 poll() 自动触发 TCP 连接 + MQTT 握手。
        // 所以我们只需要持续调用 poll()，不需要每次重连都重建客户端。
        let mut mqttoptions = MqttOptions::new(&config.client_id, &config.host, config.port);

        // 设置心跳间隔：每隔这么多秒发送一次 PINGREQ
        // 服务器如果 1.5 倍心跳时间内没收到任何数据，会主动断开连接
        mqttoptions.set_keep_alive(Duration::from_secs(config.keep_alive_secs));

        // 设置 Clean Session 标志
        // true = 每次连接都用全新会话，服务器不保留离线消息
        // false = 使用持久会话，服务器保留离线消息，重连后推送
        mqttoptions.set_clean_session(config.clean_session);

        // 如果配置了用户名，设置鉴权信息
        // 注意：只有 username 非空时才设置，空字符串表示不需要鉴权
        if !config.username.is_empty() {
            mqttoptions.set_credentials(&config.username, &config.password);
        }

        // AsyncClient::new() 返回两个值：
        // - client: 客户端句柄，用于发布消息、订阅主题
        // - eventloop: 事件循环，用于接收服务器事件
        //
        // 第二个参数是内部消息通道的容量
        // 设置为 1000，足够缓冲突发的大量消息
        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 1000);

        // 配置网络连接选项
        // connection_timeout_secs: TCP 连接建立的超时时间
        let mut network_options = NetworkOptions::new();
        network_options.set_connection_timeout(config.connection_timeout_secs);
        eventloop.set_network_options(network_options);

        // 把 client 存入共享的 Arc<Mutex<>> 中
        // 这样 publish() 方法和 publish_sender 任务都能拿到 client 来发送消息
        {
            // 花括号 {} 限定了 MutexGuard 的作用域
            // guard 离开花括号后自动释放锁
            let mut guard = client_arc.lock_unpoisoned();
            *guard = Some(client.clone());
            // 锁在这里被释放（guard 被 drop）
        }

        log::info!("[MQTT] 正在连接 {}...", server_uri);

        // ════════════════════════════════════════════════════
        // 单循环：持续调用 poll()，利用 rumqttc 内置重连
        //
        // rumqttc 的重连流程：
        // 1. 连接断开 → select() 返回 Err（如 ConnectionAborted）
        // 2. poll() 内部自动调用 clean()：
        //    - 设置 self.network = None
        //    - 保存未确认的 QoS 1/2 消息到 pending 队列
        // 3. poll() 返回 Err 给我们
        // 4. 我们 sleep 等待重连间隔
        // 5. 下次 poll() 发现 network == None，自动触发 TCP 连接 + MQTT 握手
        // 6. 重连成功 → 返回 Ok(Event::Incoming(Packet::ConnAck(...)))
        //
        // 关键点：不要 break、不要清理 client_arc、不要重建 AsyncClient
        // ════════════════════════════════════════════════════
        loop {
            // `tokio::select!` 是 Tokio 的多路复用宏
            // 它会同时等待多个异步操作，哪个先完成就执行哪个分支
            //
            // 这里同时监听两个事件：
            // 1. cancel_token 被取消（destroy 被调用）
            // 2. eventloop 产生新事件（收到消息、连接断开等）
            tokio::select! {
                // 分支 1：检查是否收到取消信号
                _ = cancel_token.cancelled() => {
                    log::info!("[MQTT] 收到取消信号");
                    // 清理客户端句柄
                    let mut guard = client_arc.lock_unpoisoned();
                    *guard = None;
                    return; // 直接退出整个函数
                }

                // 分支 2：从事件循环中获取下一个事件
                result = eventloop.poll() => {
                    match result {
                        // ── 收到 ConnAck：连接确认（首次连接或 rumqttc 内置重连成功）──
                        Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                            log::info!(
                                "[MQTT] 连接成功: {} (session_present={})",
                                server_uri, ack.session_present
                            );

                            // 更新连接状态为"已连接"
                            is_connected.store(true, Ordering::Relaxed);

                            // 回调通知 Kotlin 端：连接成功
                            // was_ever_connected 区分首次连接和重连：
                            // - false = 首次连接
                            // - true = 断线重连（rumqttc 内置重连触发）
                            callbacks.connect_complete(was_ever_connected, &server_uri);
                            was_ever_connected = true;

                            // 连接成功后，自动订阅所有配置的主题
                            // 这样每次重连后都能自动恢复订阅，不需要上层重新操作
                            //
                            // 注意：即使 session_present=true（服务器保留了会话），
                            // 重新订阅也是安全的（幂等操作）
                            let qos = config.qos;
                            for topic in &config.topics {
                                match client.subscribe(topic, qos).await {
                                    Ok(_) => log::info!("[MQTT] 订阅成功: {}", topic),
                                    Err(e) => log::error!("[MQTT] 订阅失败: {} - {}", topic, e),
                                }
                            }
                        }

                        // ── 收到 Publish：服务器推送的消息 ──
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            // 把消息内容（字节数组）转换为字符串
                            // 先尝试 from_utf8（零拷贝验证），失败才 fallback 到 lossy 转换
                            // JSON 消息通常都是合法 UTF-8，可以省掉不必要的堆分配
                            let topic = publish.topic;
                            let payload = match std::str::from_utf8(&publish.payload) {
                                Ok(s) => s.to_string(),
                                Err(_) => String::from_utf8_lossy(&publish.payload).to_string(),
                            };

                            log::info!("[MQTT] 收到消息: topic={}, size={}", topic, payload.len());

                            // 发送到消息队列，而不是直接调用回调
                            //
                            // 为什么用 send().await 而不是 try_send()?
                            // - send().await: 队列满时会等待（背压），不会丢消息
                            // - try_send(): 队列满时立即返回错误，会丢消息
                            //
                            // 对于工业场景，不丢消息比低延迟更重要，
                            // 所以宁可短暂阻塞事件循环，也要保证消息送达。
                            if let Err(e) = msg_tx.send(MqttMessage { topic, payload }).await {
                                log::error!("[MQTT] 发送消息到队列失败: {}", e);
                                // channel 已关闭，退出事件循环
                                break;
                            }
                        }

                        // ── 其他事件（心跳响应等）：忽略 ──
                        Ok(_) => {}

                        // ── 错误处理（连接断开、连接失败等）──
                        //
                        // rumqttc 内部已经执行了 clean()，保存了 pending 消息。
                        // 下次 poll() 会自动触发重连。
                        // 我们只需要记录日志、回调通知，然后等待重连间隔。
                        Err(e) => {
                            let err_msg = format!("{}", e);

                            // 用 is_connected 判断是否是"刚刚断开"，避免重连期间重复回调
                            // - true  = 之前是连接状态，这次是真正的断线 → 回调 connectionLost()
                            // - false = 已经在重连中或从未连接过 → 不回调
                            let was_just_connected = is_connected.load(Ordering::Relaxed);

                            if was_just_connected {
                                // 刚刚从连接状态断开 → 回调一次 connectionLost()
                                log::info!("[MQTT] 连接已断开: {}，等待重连...", err_msg);
                                is_connected.store(false, Ordering::Relaxed);
                                callbacks.connection_lost(&err_msg);
                            } else if !was_ever_connected {
                                // 从未连接成功过 → 首次连接失败
                                log::info!("[MQTT] 连接失败: {}", err_msg);
                            } else {
                                // 在重连中，这次重连失败 → 不回调，但打印日志便于调试
                                log::info!("[MQTT] 重连失败: {}，{}秒后重试", err_msg, config.reconnect_interval_secs);
                            }

                            // ═══ 关键：不清理 client_arc，不 break ═══
                            //
                            // - client_arc 保持有效：publish_sender 任务可以继续发送
                            //   （rumqttc 会在重连后自动处理缓冲区中的消息）
                            // - 不 break：继续循环，下次 poll() 触发 rumqttc 内置重连

                            // 错误后等待重连间隔，避免疯狂重试
                            //
                            // rumqttc 的 poll() 在 network == None 时会立即尝试连接，
                            // 如果服务器不可达，会很快返回 Err，
                            // 我们需要 sleep 避免忙等（busy loop）。
                            //
                            // 用 select! 而不是直接 sleep，目的是在等待期间也能响应取消信号
                            tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    log::info!("[MQTT] 重连等待期间被取消");
                                    return;
                                }
                                _ = tokio::time::sleep(Duration::from_secs(config.reconnect_interval_secs)) => {
                                    // 等待完成，继续下一次循环（rumqttc 内部自动重连）
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// 回调分发器：从消息队列取消息并调用 JNI 回调
    ///
    /// 这是一个独立的异步任务，与事件循环并行运行。
    /// 它的职责是：
    /// 1. 从消息队列（mpsc channel）中取出消息
    /// 2. 调用 JNI 回调通知 Kotlin 端
    /// 3. 即使回调很慢，也不会阻塞事件循环
    ///
    /// 为什么需要单独的分发器？
    /// - 事件循环必须快速处理网络事件（心跳、连接状态等）
    /// - JNI 回调可能很慢（数据库写入、UI 更新等）
    /// - 如果在事件循环中直接调用回调，会阻塞整个 MQTT 连接
    /// - 用队列解耦后，事件循环只管收消息，分发器只管调回调
    async fn callback_dispatcher(
        mut msg_rx: mpsc::Receiver<MqttMessage>,
        callbacks: Arc<CallbackManager>,
        cancel_token: CancellationToken,
    ) {
        log::info!("[MQTT] 回调分发器启动");

        loop {
            // 使用 select! 同时监听两个事件：
            // 1. 收到取消信号（shutdown 被调用）
            // 2. 从队列中取出消息
            tokio::select! {
                // 分支 1：收到取消信号
                _ = cancel_token.cancelled() => {
                    log::info!("[MQTT] 回调分发器收到取消信号，退出");
                    break;
                }

                // 分支 2：从队列中取消息
                msg = msg_rx.recv() => {
                    match msg {
                        Some(MqttMessage { topic, payload }) => {
                            log::debug!("[MQTT] 分发器处理消息: topic={}, size={}", topic, payload.len());

                            // 调用 JNI 回调，通知 Kotlin 端
                            // 即使这个调用很慢，也不会阻塞事件循环
                            callbacks.on_msg(&topic, &payload);

                            log::debug!("[MQTT] 回调完成: topic={}", topic);
                        }
                        None => {
                            // channel 已关闭（所有 sender 都被 drop 了）
                            log::info!("[MQTT] 消息队列已关闭，分发器退出");
                            break;
                        }
                    }
                }
            }
        }

        log::info!("[MQTT] 回调分发器已退出");
    }

    /// 发布消息到指定主题
    ///
    /// 这个方法会被 JNI 层的 `nativePublish()` 调用。
    /// 它不会阻塞调用线程，只是把消息塞进 channel，由常驻的 publish_sender task 转发。
    ///
    /// ## 参数说明
    ///
    /// - `topic` — 目标主题，例如 `"/device/001/data"`
    /// - `payload` — 消息内容（字节数组），可以是任何二进制数据
    /// - `qos` — 消息服务质量等级（0/1/2）
    ///
    /// ## 性能优化
    ///
    /// 优化前：每次 publish 都 spawn 一个新的 Tokio task
    /// - 1000 次 publish = 1000 个 task + 1000 次锁竞争
    ///
    /// 优化后：publish 只是往 channel 里塞数据
    /// - 1000 次 publish = 1000 次 channel 写入（纳秒级）
    /// - 只有一个常驻 task 串行发送，没有锁竞争
    ///
    /// ## 错误处理
    ///
    /// 发布失败时不会 panic，而是通过 `callbacks.on_error()` 回调通知 Kotlin 端。
    pub fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS) {
        // 从 publish_tx 中取出 sender，发送消息到 channel
        //
        // 注意：tx.send() 是 async 方法，不能在非 async 上下文调用。
        // 这里用 tx.try_send()（同步方法），队列满时立即返回错误而不是等待。
        // 对于 publish 这种高频操作，try_send 足够快（channel 容量 1000，正常不会满）。
        let guard = self.publish_tx.lock_unpoisoned();
        if let Some(ref tx) = *guard {
            if let Err(e) = tx.try_send(PublishRequest { topic, payload, qos }) {
                log::error!("[MQTT] 发送发布请求到队列失败: {}", e);
            }
        } else {
            log::warn!("[MQTT] publish_tx 未初始化，无法发布消息");
        }
    }

    /// 优雅关闭客户端
    ///
    /// 这个方法会：
    /// 1. 发送取消信号，通知后台事件循环停止
    /// 2. 等待最多 3 秒让后台任务完成清理
    /// 3. 清理客户端句柄
    /// 4. 重置连接状态
    ///
    /// ## 为什么要等待？
    ///
    /// 如果直接退出而不等待，后台任务可能还在运行，
    /// 此时如果 GlobalRef 已经被释放了，后台任务还在尝试调用回调方法，
    /// 就会导致 use-after-free（使用已释放的内存），导致程序崩溃。
    ///
    /// ## 为什么只等 3 秒？
    ///
    /// 工业设备对关闭速度有要求，不能无限等待。
    /// 3 秒是一个折中值：足够让大部分清理操作完成，
    /// 又不会因为等待太久影响用户体验。
    pub fn shutdown(&mut self) {
        // 防止重复执行
        //
        // 为什么需要这个检查？
        // - nativeDestroy() 会先显式调用 shutdown()
        // - 然后 drop(boxed) 触发 Drop trait → 再次调用 shutdown()
        // - 如果没有这个标记，第二次 shutdown 会多等约 3 秒
        //
        // swap(true, SeqCst) 原子地把值设为 true，返回旧值：
        // - 旧值是 false → 第一次执行，继续
        // - 旧值是 true → 已经执行过，直接返回
        if self.is_shutdown.swap(true, Ordering::SeqCst) {
            log::debug!("[MQTT] shutdown 已执行过，跳过");
            return;
        }

        log::info!("[MQTT] 开始关闭...");

        // 第一步：发送取消信号
        // 事件循环中的 `cancel_token.cancelled()` 和 `cancel_token.is_cancelled()`
        // 会立即感知到这个信号
        self.cancel_token.cancel();

        // 第二步：等待后台任务退出
        //
        // 这里用 `block_on()` 在 shutdown 线程上等待
        // `timeout(3s, sleep(500ms))` 的意思是：
        // - 先 sleep 500ms，给后台任务一些时间做清理
        // - 整个等待最多 3 秒（虽然 sleep 只要 500ms，但保留扩展空间）
        let _ = self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                // 等待 500ms 让后台任务处理取消信号并退出
                tokio::time::sleep(Duration::from_millis(500)).await;
            }).await
        });

        // 第三步：清理客户端句柄
        {
            let mut guard = self.client.lock_unpoisoned();
            *guard = None;
        }

        // 第四步：重置连接状态
        self.is_connected.store(false, Ordering::Relaxed);

        log::info!("[MQTT] 关闭完成");
    }
}

/// 实现 `Drop` trait，确保即使忘记调用 `shutdown()` 也能正确清理
///
/// `Drop` 是 Rust 的析构函数，当对象被销毁时自动调用。
/// 这保证了即使 JNI 层的代码有 bug（比如忘记调用 destroy），
/// 后台任务也会被正确停止，不会泄漏资源。
///
/// 在什么情况下会触发 Drop？
/// - `nativeDestroy()` 中调用 `Box::from_raw()` 后 `drop(boxed)`
/// - 进程退出时操作系统回收内存（此时 Drop 可能不会被调用，
///   但进程都退出了，操作系统会清理所有资源）
impl Drop for NativeMqttCore {
    fn drop(&mut self) {
        self.shutdown();
    }
}
