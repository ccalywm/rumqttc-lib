# rumqttc — 基于 Rust 的 Android MQTT 客户端库

## 一、项目概述

这是一个用 Rust 编写的 MQTT 客户端库，编译为 Android 动态库（`.so` 文件），通过 JNI（Java Native Interface）供 Kotlin/Java 端调用。

### 为什么用 Rust 而不是纯 Java/Kotlin？

| 对比维度 | 纯 Java/Kotlin | Rust（本方案） |
|---------|---------------|--------------|
| **性能** | JVM 解释执行 + JIT，有 GC 停顿 | 编译为机器码，零 GC 开销 |
| **内存安全** | 依赖 GC，可能有内存泄漏 | 编译期所有权检查，无泄漏 |
| **线程安全** | 运行时检查，易出死锁/竞态 | 编译期 `Send`/`Sync` 保证 |
| **包体积** | — | 约 2-3 MB（strip 后） |
| **崩溃风险** | Java 异常可 catch | `unsafe` 操作不当会直接 SIGSEGV |

---

## 二、整体架构

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Android App (Kotlin)                     │
│                                                                  │
│  ┌─────────────────────────┐    ┌──────────────────────────┐   │
│  │  RumqttcClient.kt      │    │   MqttCallback.kt         │   │
│  │  (高层 API 封装)         │    │   (回调接口定义)           │   │
│  └───────────┬─────────────┘    └──────────────▲────────────┘   │
│              │ JNI 调用                         │ JNI 回调       │
├──────────────┼─────────────────────────────────┼────────────────┤
│              ▼                                 │                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     jni.rs (JNI 桥接层)                    │   │
│  │  - 参数解析（String → &str，byte[] → Vec<u8>）            │   │
│  │  - 指针生命周期管理（Box::into_raw / Box::from_raw）       │   │
│  │  - 错误码转换                                              │   │
│  └───────────┬──────────────────────────────────▲────────────┘   │
│              │                                  │                │
│              ▼                                  │                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    core.rs (核心逻辑层)                    │   │
│  │                                                           │   │
│  │  ┌─────────────────┐  ┌──────────────┐  ┌────────────┐  │   │
│  │  │  事件循环        │  │ 回调分发器    │  │ 发布转发器  │  │   │
│  │  │  (event_loop)    │→ │ (dispatcher) │  │ (pub_send) │  │   │
│  │  │                  │  │              │  │            │  │   │
│  │  │  连接/重连/收消息 │  │ 队列→JNI回调 │  │ 队列→rumqttc│  │   │
│  │  └─────────────────┘  └──────────────┘  └────────────┘  │   │
│  │                                                           │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │              Tokio Runtime (2 线程)                   │ │   │
│  │  │              线程名: mqtt-io-0, mqtt-io-1             │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│              │                                  │                │
│              ▼                                  │                │
│  ┌──────────────────────┐          ┌───────────────────────┐   │
│  │   config.rs           │          │   callbacks.rs         │   │
│  │   (MQTT 配置结构体)    │          │   (JNI 回调管理器)     │   │
│  └──────────────────────┘          └───────────────────────┘   │
│                                                                  │
│                     Rust 动态库 (.so)                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 三、模块详解

### 3.1 `config.rs` — MQTT 配置结构体

定义 `MqttConfig` 结构体，存储连接所需的所有参数。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `host` | `String` | `""` | MQTT 服务器地址（IP 或域名） |
| `port` | `u16` | `1883` | 端口号 |
| `client_id` | `String` | `""` | 客户端唯一标识（建议用设备 MAC） |
| `username` | `String` | `""` | 用户名（空字符串表示无鉴权） |
| `password` | `String` | `""` | 密码 |
| `topics` | `Vec<String>` | `[]` | 需要订阅的主题列表 |
| `qos` | `QoS` | `AtMostOnce` | 消息服务质量等级（0/1/2） |
| `keep_alive_secs` | `u64` | `10` | 心跳间隔（秒） |
| `reconnect_interval_secs` | `u64` | `10` | 重连间隔（秒） |
| `clean_session` | `bool` | `true` | 是否清除会话 |
| `message_buffer_size` | `usize` | `10000` | 消息队列容量 |

### 3.2 `callbacks.rs` — JNI 回调管理器

`CallbackManager` 负责从 Rust 后台线程调用 Kotlin 的回调方法。

**核心机制：**

```text
Rust 后台线程                    JVM (Kotlin)
─────────────                    ──────────
1. java_vm.attach_current_thread(|env| ...)  →    注册线程到 JVM（闭包模式）
2. 获取 Global<JObject<'static>>              →    防止 Kotlin 对象被 GC
3. env.new_string(topic)        →    创建 Java String 对象
4. env.call_method(cb, jni_str!("onMsg"), ...) →    调用 Kotlin 方法（宏签名）
5. ? 错误传播 + resolve 统一处理               →    自动处理 JNI 异常
```

**回调方法：**

| Rust 方法 | Kotlin 接口方法 | 触发时机 |
|-----------|----------------|---------|
| `connect_complete()` | `connectComplete(reconnect, serverURI)` | 连接/重连成功 |
| `connection_lost()` | `connectionLost(cause)` | 连接断开 |
| `on_msg()` | `onMsg(topic, payload)` | 收到 MQTT 消息 |
| `on_error()` | `onError(code, message)` | 发生错误 |

**错误码定义：**

| 常量名 | 值 | 含义 |
|--------|---|------|
| `ERR_INVALID_PARAMS` | 1 | 参数无效（host/clientId 为空） |
| `ERR_INIT_FAILED` | 2 | 初始化失败 |
| `ERR_PUBLISH_FAILED` | 3 | 发布消息失败 |

### 3.3 `core.rs` — 核心 MQTT 客户端

这是整个库的核心，包含以下关键组件：

#### 3.3.1 `NativeMqttCore` 结构体

```rust
pub struct NativeMqttCore {
    client: Arc<Mutex<Option<AsyncClient>>>,  // MQTT 客户端句柄
    runtime: tokio::runtime::Runtime,          // Tokio 异步运行时（2 线程）
    cancel_token: CancellationToken,           // 取消令牌（用于优雅关闭）
    is_connected: Arc<AtomicBool>,             // 连接状态（原子布尔值）
    is_shutdown: AtomicBool,                   // 是否已关闭（防止重复 shutdown）
    callbacks: Mutex<Option<Arc<CallbackManager>>>,  // 回调管理器
    msg_tx: Mutex<Option<mpsc::Sender<MqttMessage>>>,      // 消息队列发送端
    publish_tx: Mutex<Option<mpsc::Sender<PublishRequest>>>, // 发布队列发送端
}
```

**字段详解：**

| 字段 | 类型包装 | 作用 |
|------|---------|------|
| `client` | `Arc<Mutex<Option<T>>>` | MQTT 客户端句柄，重连时会替换 |
| `runtime` | 直接持有 | Tokio 多线程运行时，2 个工作线程，名称 `mqtt-io-0/1` |
| `cancel_token` | 直接持有 | 用于通知后台任务停止 |
| `is_connected` | `Arc<AtomicBool>` | 无锁连接状态，支持高并发读取 |
| `is_shutdown` | `AtomicBool` | 防止 shutdown() 被重复执行（Drop + nativeDestroy） |
| `callbacks` | `Mutex<Option<Arc<T>>>` | 回调管理器，connect 时设置 |
| `msg_tx` | `Mutex<Option<T>>` | 消息队列发送端，event_loop 用它把消息入队 |
| `publish_tx` | `Mutex<Option<T>>` | 发布队列发送端，publish() 用它把请求入队 |

#### 3.3.2 三个常驻异步任务

`connect()` 方法会启动三个异步任务：

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Tokio Runtime (2 线程)                   │
│                                                                  │
│  ┌──────────────────┐   ┌──────────────────┐   ┌────────────┐ │
│  │  event_loop       │   │ callback_dispatcher│   │publish_send│ │
│  │                   │   │                    │   │            │ │
│  │  连接服务器        │   │  从 msg_rx 取消息  │   │从 pub_rx   │ │
│  │  接收 MQTT 事件    │──→│  调用 JNI 回调    │   │取请求并发送│ │
│  │  断线重连          │   │                    │   │            │ │
│  │                   │   │                    │   │            │ │
│  │  生产者            │   │  消费者            │   │  消费者    │ │
│  └────────┬──────────┘   └─────────▲──────────┘   └─────▲──────┘ │
│           │                        │                    │        │
│           │ msg_tx.send()          │ msg_rx.recv()      │        │
│           │                        │                    │        │
│           └──────────── mpsc channel ──────────────────┘        │
│                     (容量: message_buffer_size)                  │
│                                                                  │
│  publish() ──→ publish_tx.try_send() ──→ publish_rx ──→ rumqttc │
│                     (容量: 200)                                  │
└─────────────────────────────────────────────────────────────────┘
```

**为什么用消息队列解耦？**

| 场景 | 不用队列 | 用队列 |
|------|---------|--------|
| 收到 1000 条消息 | 逐条调用 JNI，阻塞事件循环 | 消息入队，分发器异步处理 |
| JNI 回调慢（数据库写入） | 事件循环被阻塞，心跳超时 | 事件循环不受影响 |
| 连接断开 | 正在回调的消息丢失 | 队列中的消息继续处理 |

#### 3.3.3 `event_loop()` — 事件循环

这是整个 MQTT 客户端的心脏，采用**单循环**架构，利用 rumqttc 内置重连机制：

```text
单循环（事件处理循环）
│
├─ 1. 检查 cancel_token，如果被取消则退出
├─ 2. 调用 eventloop.poll() 获取下一个事件
│     ├─ 收到 ConnAck → 标记连接成功 → 订阅所有主题
│     ├─ 收到 Publish → 发送到消息队列（msg_tx.send().await）
│     ├─ 收到错误 → 判断是否刚刚断开：
│     │     ├─ 是（is_connected=true）→ 回调 connectionLost() → 设为 false
│     │     └─ 否（已在重连中）→ 静默，不回调
│     └─ 收到取消信号 → 直接退出
├─ 3. 错误后等待 reconnect_interval_secs 秒
└─ 4. 回到第 1 步（rumqttc 自动重连）
```

**关键设计：**

- 使用 `tokio::select!` 同时监听取消信号和 MQTT 事件
- 利用 rumqttc 0.25.1 的**内置重连机制**：`poll()` 返回 `Err` 后，下次调用自动触发 TCP 连接 + MQTT 握手
- `AsyncClient` 和 `EventLoop` 只创建一次，整个生命周期复用
- 断线时不清理 `client_arc`，`publish_sender` 可以继续发送（rumqttc 会缓冲）
- 错误后 sleep `reconnect_interval_secs` 秒，避免疯狂重试
- 只在真正断开的瞬间回调一次 `connectionLost()`，重连期间的失败静默处理
- 消息通过 `msg_tx.send().await` 发送到队列（背压机制，队列满时等待）
- 连接成功后自动订阅所有配置的主题

#### 3.3.4 `callback_dispatcher()` — 回调分发器

```text
循环：
├─ 收到取消信号 → 退出
└─ 从 msg_rx 取出消息 → 调用 callbacks.on_msg()
```

**职责：** 从消息队列中取出消息，调用 JNI 回调通知 Kotlin 端。即使回调很慢（数据库写入、UI 更新），也不会阻塞事件循环。

#### 3.3.5 `publish_sender()` — 发布转发器

```text
循环：
└─ 从 publish_rx 取出请求 → 从 Mutex 中 clone client → 调用 rumqttc 发送
```

**为什么需要这个转发器？**

| 优化前 | 优化后 |
|--------|--------|
| 每次 publish 都 spawn 新 task | 只有一个常驻 task |
| 1000 次 publish = 1000 个 task | 1000 次 publish = 1000 次 channel 写入 |
| 每个 task 都要抢锁 clone client | 只有转发器一个 task 抢锁 |
| CPU 占用 35%（实测） | CPU 占用 < 1% |

#### 3.3.6 `publish()` — 发布消息

```rust
pub fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS) {
    let guard = self.publish_tx.lock().unwrap();
    if let Some(ref tx) = *guard {
        // 使用 try_send()（同步方法），队列满时立即返回错误
        if let Err(e) = tx.try_send(PublishRequest { topic, payload, qos }) {
            log::error!("[MQTT] 发送发布请求到队列失败: {}", e);
        }
    }
}
```

**关键点：**
- 使用 `try_send()` 而非 `send()`，因为 `publish()` 是同步方法
- `try_send()` 队列满时立即返回错误，不会阻塞调用线程
- channel 容量 1000，正常情况不会满

### 3.4 `jni.rs` — JNI 桥接层

定义了 5 个 JNI 导出函数：

| JNI 函数名 | Kotlin 调用 | 作用 |
|-----------|-------------|------|
| `Java_..._nativeCreate` | `nativeCreate(debug)` | 创建实例，返回指针 |
| `Java_..._nativeConnect` | `nativeConnect(ptr, ...)` | 启动连接 |
| `Java_..._nativePublish` | `nativePublish(ptr, topic, payload, qos)` | 发布消息 |
| `Java_..._nativeIsConnected` | `nativeIsConnected(ptr)` | 查询连接状态 |
| `Java_..._nativeDestroy` | `nativeDestroy(ptr)` | 销毁实例 |

**指针生命周期：**

```text
Kotlin 端                    Rust 端
────────                    ────────
nativeCreate()        →     Box::into_raw(Box::new(core))  分配堆内存
ptr: Long = 0x7f...         0x7f... → NativeMqttCore

nativeDestroy(ptr)    →     Box::from_raw(ptr)             回收堆内存
ptr = 0                     内存被释放，触发 Drop
```

---

## 四、线程模型

整个系统涉及以下线程：

| 线程名 | 来源 | 职责 |
|--------|------|------|
| `mqtt-io-0` | Tokio runtime | 运行异步任务（event_loop、dispatcher、publish_sender） |
| `mqtt-io-1` | Tokio runtime | 同上（Tokio 调度器分配任务） |
| `RumqttcClient-JNI` | Kotlin executor | 执行 JNI 调用（connect、publish、destroy） |
| 主线程 | Android | 调用 `isConnected()`（纳秒级，安全） |

**线程安全保证：**

| 数据 | 保护机制 |
|------|---------|
| `client` | `Arc<Mutex<Option<T>>>`，读写都加锁 |
| `is_connected` | `Arc<AtomicBool>`，无锁原子操作 |
| `callbacks` | `Mutex<Option<Arc<T>>>`，设置时加锁 |
| `msg_tx` / `publish_tx` | `Mutex<Option<T>>`，设置时加锁 |

---

## 五、数据流

### 5.1 接收消息流

```text
MQTT Broker
    │
    ▼
rumqttc eventloop.poll()
    │
    ▼ (Event::Incoming(Packet::Publish))
event_loop: 解析 topic + payload
    │
    ▼ msg_tx.send(MqttMessage { topic, payload }).await
mpsc channel (容量: message_buffer_size)
    │
    ▼ msg_rx.recv().await
callback_dispatcher
    │
    ▼ callbacks.on_msg(topic, payload)
CallbackManager:
    │
    ├─ java_vm.attach_current_thread(|env| ...)  // 闭包模式注册线程到 JVM
    ├─ env.new_string(topic)         // 创建 Java String
    ├─ env.new_string(payload)       // 创建 Java String
    └─ env.call_method(cb, jni_str!("onMsg"), jni_sig!(...), ...) // 宏签名调用
    │
    ▼
Kotlin: callback.onMsg(topic, payload)
```

### 5.2 发布消息流

```text
Kotlin: client.publish(topic, payload, qos)
    │
    ▼ executor.execute { nativePublish(...) }
JNI: nativePublish
    │
    ▼ core.publish(topic, payload, qos)
publish_tx.try_send(PublishRequest { ... })
    │
    ▼ (mpsc channel, 容量 1000)
publish_rx.recv().await
    │
    ▼ publish_sender task
client_arc.lock().unwrap().clone()  // 获取 AsyncClient
    │
    ▼ client.publish(topic, qos, false, payload).await
rumqttc → MQTT Broker
```

---

## 六、构建指南

### 6.1 环境要求

| 工具 | 版本要求 | 用途 |
|------|---------|------|
| Rust | 1.70+ | 编译器 |
| Android NDK | r25+ (推荐 r29) | 交叉编译链接器 |
| cargo | 随 Rust 安装 | 构建工具 |

### 6.2 添加 Android 目标

```bash
rustup target add aarch64-linux-android    # 64 位 ARM
rustup target add armv7-linux-androideabi  # 32 位 ARM（可选）
```

### 6.3 配置交叉编译

创建 `.cargo/config.toml`：

```toml
[target.aarch64-linux-android]
linker = "/path/to/ndk/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"

[target.armv7-linux-androideabi]
linker = "/path/to/ndk/toolchains/llvm/prebuilt/linux-x86_64/bin/armv7a-linux-androideabi21-clang"
```

**注意：** 路径需要根据你的 NDK 安装位置调整。

### 6.4 编译命令

```bash
# 清理
cargo clean

# 编译 Android 64 位
cargo build --target aarch64-linux-android --release

# 编译 Android 32 位（可选）
cargo build --target armv7-linux-androideabi --release

# 同时编译 32 位和 64 位
cargo clean && \
  cargo build --target aarch64-linux-android --release && \
  cargo build --target armv7-linux-androideabi --release
```

### 6.5 输出文件

编译成功后，`.so` 文件位于：

```
target/aarch64-linux-android/release/librumqttc.so
target/armv7-linux-androideabi/release/librumqttc.so
```

将这些文件复制到 Android 项目的 `app/src/main/jniLibs/` 目录：

```
app/src/main/jniLibs/
├── arm64-v8a/librumqttc.so
└── armeabi-v7a/librumqttc.so
```

---

## 七、Kotlin 端使用

### 7.1 文件清单

| 文件 | 作用 |
|------|------|
| `MqttCallback.kt` | 回调接口定义 + 错误码常量 |
| `RumqttcClient.kt` | 高层 API 封装，管理 JNI 调用 |

### 7.2 最简用法

```kotlin
val client = RumqttcClient()

client.connect(
    host = "192.168.1.100",
    port = 1883,
    clientId = "device_mac_address",
    username = "user",
    password = "pass",
    qos = 0,
    topics = arrayOf("topic1", "topic2"),
    callback = object : MqttCallback {
        override fun connectComplete(reconnect: Boolean, serverURI: String) {
            Log.i("MQTT", "连接成功: $serverURI")
        }
        override fun connectionLost(cause: String) {
            Log.w("MQTT", "连接断开: $cause")
        }
        override fun onMsg(topic: String, payload: String) {
            Log.d("MQTT", "收到消息: $topic -> $payload")
        }
        override fun onError(code: Int, message: String) {
            Log.e("MQTT", "错误[$code]: $message")
        }
    }
)

// 发布消息
client.publish("topic1", "hello".toByteArray(), qos = 0)

// 查询连接状态
if (client.isConnected()) {
    Log.d("MQTT", "当前已连接")
}

// 销毁（必须在 Activity/Service 销毁时调用）
client.destroy()
```

### 7.3 高级用法

```kotlin
// 开启调试日志 + 自定义线程池
val client = RumqttcClient(
    debug = true,                        // 输出详细日志
    executor = SingleThreads.get()       // 使用项目现有的线程池
)

client.connect(
    host = "192.168.1.100",
    port = 1883,
    clientId = "device_mac",
    username = "user",
    password = "pass",
    qos = 1,                             // QoS 1: 至少送达一次
    topics = arrayOf("topic1", "topic2"),
    callback = myCallback,
    keepAliveSecs = 30,                  // 心跳间隔 30 秒
    reconnectIntervalSecs = 5,           // 重连间隔 5 秒
    messageBufferSize = 50000            // 消息队列容量 50000
)
```

### 7.4 API 参考

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `RumqttcClient(debug, executor)` | `debug: Boolean`, `executor: Executor?` | — | 构造函数 |
| `connect(...)` | 见下方 | `Unit` | 异步连接 |
| `publish(topic, payload, qos)` | `String`, `ByteArray`, `Int` | `Unit` | 异步发布 |
| `isConnected()` | 无 | `Boolean` | 同步查询 |
| `destroy()` | 无 | `Unit` | 销毁实例 |

**`connect()` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `host` | `String` | — | 服务器地址 |
| `port` | `Int` | — | 端口号 |
| `clientId` | `String` | — | 客户端 ID |
| `username` | `String` | — | 用户名 |
| `password` | `String` | — | 密码 |
| `qos` | `Int` | — | QoS 等级 (0/1/2) |
| `topics` | `Array<String>` | — | 订阅主题 |
| `callback` | `MqttCallback` | — | 回调接口 |
| `keepAliveSecs` | `Int` | `10` | 心跳间隔（秒） |
| `reconnectIntervalSecs` | `Int` | `10` | 重连间隔（秒） |
| `messageBufferSize` | `Int` | `10000` | 消息队列容量 |
| `cleanSession` | `Boolean` | `true` | 是否清除会话 |

---

## 八、错误处理

### 8.1 错误码

| 错误码 | 常量 | 含义 | 常见原因 |
|--------|------|------|---------|
| 1 | `INVALID_PARAMS` | 参数无效 | host 或 clientId 为空 |
| 2 | `INIT_FAILED` | 初始化失败 | Tokio runtime 创建失败 |
| 3 | `PUBLISH_FAILED` | 发布失败 | 网络错误、未连接 |

### 8.2 错误回调时机

| 错误 | 回调方法 | 说明 |
|------|---------|------|
| 参数错误 | `onError(1, msg)` | connect 时校验失败 |
| 连接断开 | `connectionLost(cause)` | 网络故障、服务器拒绝 |
| 发布失败 | `onError(4, msg)` | 未连接、网络错误 |

---

## 九、性能特性

### 9.1 消息队列

- **容量**：可配置（默认 10000）
- **背压机制**：队列满时 `event_loop` 会等待（`send().await`），不会丢消息
- **解耦**：事件循环和 JNI 回调完全解耦，互不阻塞

### 9.2 发布优化

- **单 task 转发**：所有 publish 请求通过一个常驻 task 串行发送
- **无锁竞争**：1000 次 publish 只产生 1000 次 channel 写入
- **rumqttc 内部队列**：容量 1000，足够缓冲突发消息

### 9.3 线程配置

- **Tokio 工作线程**：2 个，名称 `mqtt-io-0` 和 `mqtt-io-1`
- **资源占用**：MQTT 客户端 I/O 密集但并发不高，2 线程足够
- **Logcat 过滤**：可按线程名 `mqtt-io` 过滤日志

---

## 十、注意事项

### 10.1 必须调用 `destroy()`

`nativeCreate()` 使用了 `Box::into_raw()` 将 Rust 对象放入堆中，**不会自动释放**。必须在 Activity/Service 的 `onDestroy()` 中调用 `destroy()`，否则会导致内存泄漏。

```kotlin
override fun onDestroy() {
    super.onDestroy()
    mqttClient.destroy()
}
```

### 10.2 不要在回调中做耗时操作

`onMsg()` 回调发生在 Tokio 工作线程中，如果在回调中执行耗时操作（数据库写入、网络请求），会阻塞其他消息的处理。

```kotlin
// ❌ 错误：在回调中做数据库写入
override fun onMsg(topic: String, payload: String) {
    database.insert(payload)  // 会阻塞消息队列
}

// ✅ 正确：异步处理
override fun onMsg(topic: String, payload: String) {
    executor.execute { database.insert(payload) }
}
```

### 10.3 `tcp://` 前缀会自动剥离

`connect()` 的 `host` 参数支持 `tcp://` 或 `ssl://` 前缀，会自动剥离：

```kotlin
// 以下两种写法等价
client.connect(host = "192.168.1.100", ...)
client.connect(host = "tcp://192.168.1.100", ...)
```

### 10.4 `isConnected()` 可安全在主线程调用

这个方法只是读取一个原子布尔值，开销在纳秒级别，不会阻塞 UI。

---

## 十一、调试技巧

### 11.1 开启日志

```kotlin
val client = RumqttcClient(debug = true)
```

### 11.2 Logcat 过滤

```bash
# 只看 MQTT 相关日志
adb logcat | grep -E "MQTT|JNI|Callback"

# 按线程过滤
adb logcat | grep "mqtt-io"
```

### 11.3 常见问题排查

| 问题 | 可能原因 | 排查方法 |
|------|---------|---------|
| 连接不上 | 网络不通、服务器地址错误 | 检查 Logcat 中的连接日志 |
| 收不到消息 | 主题不匹配、QoS 问题 | 检查订阅主题和服务器发布的主题 |
| 发布失败 | 未连接、网络故障 | 检查 `onError` 回调的错误码 |
| 内存泄漏 | 忘记调用 `destroy()` | 检查 Activity 生命周期 |

---

## 十二、项目文件清单

```
rumqttc/
├── .cargo/
│   └── config.toml              # 交叉编译配置
├── src/
│   ├── lib.rs                   # 库入口，声明 4 个模块
│   ├── config.rs                # MqttConfig 结构体
│   ├── callbacks.rs             # JNI 回调管理器
│   ├── core.rs                  # 核心 MQTT 客户端逻辑
│   └── jni.rs                   # JNI 导出函数
├── kotlin/
│   └── com/rust/rumqttc/
│       ├── MqttCallback.kt      # 回调接口定义
│       └── RumqttcClient.kt    # Kotlin 高层 API
├── Cargo.toml                   # Rust 依赖配置
├── Cargo.lock                   # 依赖版本锁定
└── README.md                    # 本文件
```

---

## 十三、依赖清单

| 依赖 | 版本 | 用途 |
|------|------|------|
| `rumqttc` | 0.25.1 | MQTT 客户端核心 |
| `tokio` | 1.37 | 异步运行时（多线程） |
| `tokio-util` | 0.7 | CancellationToken（优雅取消） |
| `jni` | 0.21 | JNI 类型和工具 |
| `log` | 0.4 | 日志门面 |
| `android_logger` | 0.15 | Android Logcat 日志桥接 |

---

## 十四、License

本项目仅供内部使用。
