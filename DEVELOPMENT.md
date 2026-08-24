# rumqttc 开发指南

> 本文档面向开发者和 AI 助手，提供项目架构、关键设计决策、已知问题和开发规范的全面参考。

---

## 1. 项目概览

**rumqttc** 是一个基于 Rust 的 Android MQTT 客户端库，编译为动态库（`.so`），通过 JNI 供 Kotlin/Java 调用。

### 1.1 核心价值

- **性能**：Rust 编译为机器码，无 GC 停顿
- **安全**：所有权系统在编译期防止内存泄漏和空指针
- **稳定性**：Tokio 异步运行时 + 自动重连机制

### 1.2 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Rust | 2024 edition | 语言版本 |
| rumqttc | 0.25.1 | MQTT 客户端核心 |
| tokio | 1.x | 异步运行时（2 worker 线程） |
| jni | 0.22 | JNI 绑定（0.22 适配完成，使用 EnvUnowned 闭包模式） |
| rustls | 0.23 + ring | TLS 加密后端（当前未启用 TLS 连接） |
| android_logger | 0.15 | Android Logcat 日志桥接 |
| tokio-util | 0.7 | CancellationToken（优雅取消） |

---

## 2. 架构设计

### 2.1 模块划分

```
src/
├── lib.rs         # 库入口，声明子模块
├── config.rs      # MqttConfig 配置结构
├── callbacks.rs   # JNI 回调管理（Rust → Kotlin）
├── core.rs        # 核心 MQTT 逻辑（连接/重连/发布）
└── jni.rs         # JNI 桥接层（Kotlin → Rust）
```

### 2.2 数据流架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Kotlin 层                               │
│  RumqttcClient.kt  ──────────────────────┐              │
│  MqttCallback.kt       ◄────────────────────┤              │
└────────────────────────┼─────────────────────┼──────────────┘
                         │ JNI 调用            │ JNI 回调
                         ▼                     │
┌────────────────────────┼─────────────────────┼──────────────┐
│  jni.rs                │                     │              │
│  ├─ nativeCreate()     │                     │              │
│  ├─ nativeConnect()    │                     │              │
│  ├─ nativePublish()    │                     │              │
│  ├─ nativeIsConnected()│                     │              │
│  └─ nativeDestroy()    │                     │              │
└────────────────────────┼─────────────────────┼──────────────┘
                         │                     │
                         ▼                     │
┌────────────────────────┼─────────────────────┼──────────────┐
│  core.rs               │                     │              │
│                        │                     │              │
│  ┌─────────────────────┼─────────────────────┼─────────┐   │
│  │ Tokio Runtime (2 threads: mqtt-io-0/1)              │   │
│  │                                                      │   │
│  │  ┌──────────────┐  mpsc channel  ┌──────────────┐  │   │
│  │  │ event_loop   │ ─────────────► │ callback_    │  │   │
│  │  │ (连接/重连/   │  (msg_tx)      │ dispatcher   │──┼───┼──► JNI 回调
│  │  │  收消息)      │                │ (取消息/调    │  │   │
│  │  └──────────────┘                │  回调)        │  │   │
│  │                                  └──────────────┘  │   │
│  │                                                      │   │
│  │  ┌──────────────┐  mpsc channel  ┌──────────────┐  │   │
│  │  │ publish()    │ ─────────────► │ publish_     │  │   │
│  │  │ (JNI 调用)    │  (publish_tx)  │ sender       │──┼───┼──► rumqttc
│  │  └──────────────┘                │ (取请求/发    │  │   │
│  │                                  │  消息)        │  │   │
│  │                                  └──────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### 2.3 为什么用消息队列解耦？

**问题场景**：如果事件循环直接调用 JNI 回调，当回调耗时（数据库写入、UI 更新）时，会阻塞事件循环，导致：
- 心跳超时 → 服务器断开连接
- 消息积压 → 内存溢出

**解决方案**：
- **event_loop** 只管收消息，快速 `msg_tx.send().await` 后继续处理下一个事件
- **callback_dispatcher** 独立运行，即使回调慢也不影响事件循环
- **背压机制**：队列满时 `send().await` 会阻塞，迫使事件循环减速，但**不丢消息**

---

## 3. 核心组件详解

### 3.1 NativeMqttCore 结构体

```rust
pub struct NativeMqttCore {
    client:        Arc<Mutex<Option<AsyncClient>>>,  // MQTT 客户端句柄
    runtime:       tokio::runtime::Runtime,          // Tokio 运行时（2 线程）
    cancel_token:  CancellationToken,                // 协作式取消令牌
    is_connected:  Arc<AtomicBool>,                  // 无锁连接状态
    callbacks:     Mutex<Option<Arc<CallbackManager>>>, // 回调管理器
    msg_tx:        Mutex<Option<mpsc::Sender<MqttMessage>>>, // 消息队列发送端
    publish_tx:    Mutex<Option<mpsc::Sender<PublishRequest>>>, // 发布队列发送端
}
```

**字段包装层解释**：

| 包装 | 原因 |
|------|------|
| `Arc<T>` | 允许多线程共享（spawn 出去的异步任务也能持有引用） |
| `Mutex<T>` | 保护内部数据的可变访问（前台/后台线程都可能读写） |
| `Option<T>` | 表示"可能未初始化"（如连接前 client 为空） |

### 3.2 event_loop() — 双层循环架构

```rust
async fn event_loop(
    config: MqttConfig,
    callbacks: Arc<CallbackManager>,
    client_arc: Arc<Mutex<Option<AsyncClient>>>,
    cancel_token: CancellationToken,
    is_connected: Arc<AtomicBool>,
    msg_tx: mpsc::Sender<MqttMessage>,
) {
    let mut was_ever_connected = false;

    loop {
        // 外层循环：重连循环
        if cancel_token.is_cancelled() { break; }

        // 创建新的 MqttOptions 和 AsyncClient
        let mut mqttoptions = MqttOptions::new(&config.client_id, &config.host, config.port);
        // ... 配置 keep_alive、clean_session、credentials 等

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        
        // 将新 client 存入共享 Arc<Mutex<>>
        {
            let mut guard = client_arc.lock().unwrap();
            *guard = Some(client);
        }

        // 内层循环：事件处理
        let mut connected = false;
        loop {
            tokio::select! {
                // 分支 1：取消信号
                _ = cancel_token.cancelled() => {
                    // 清理 client，直接 return
                    return;
                }

                // 分支 2：MQTT 事件
                result = eventloop.poll() => {
                    match result {
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            // 连接成功
                            connected = true;
                            is_connected.store(true, Ordering::Relaxed);
                            callbacks.connect_complete(was_ever_connected, &server_uri);
                            was_ever_connected = true;

                            // 自动订阅所有 topics
                            for topic in &config.topics {
                                let _ = client.subscribe(topic, config.qos).await;
                            }
                        }

                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            // 收到消息
                            let topic = publish.topic;
                            let payload = String::from_utf8_lossy(&publish.payload).to_string();
                            
                            // 发送到消息队列（背压模式，不丢消息）
                            if let Err(_) = msg_tx.send(MqttMessage { topic, payload }).await {
                                break; // channel 关闭，跳出内层循环
                            }
                        }

                        Ok(_) => { /* 其他事件（心跳等）忽略 */ }

                        Err(e) => {
                            // 连接错误
                            if connected {
                                is_connected.store(false, Ordering::Relaxed);
                                callbacks.connection_lost(&e.to_string());
                            }
                            break; // 跳出内层循环，进入重连
                        }
                    }
                }
            }
        }

        // 清理 client
        {
            let mut guard = client_arc.lock().unwrap();
            *guard = None;
        }

        // 等待重连间隔（可取消）
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(config.reconnect_interval_secs)) => {}
        }
    }
}
```

**关键设计点**：

1. **每次重连都重新创建 AsyncClient**：rumqttc 的 Eventloop 在出错后无法复用，必须重建
2. **`was_ever_connected` 区分首次连接和重连**：首次传 `false`，之后传 `true`
3. **重连等待可取消**：使用 `tokio::select!` 在 sleep 期间也能响应取消信号

### 3.3 CallbackManager — JNI 回调管理

```rust
pub struct CallbackManager {
    java_vm:    Arc<jni::JavaVM>,                       // JVM 引用（跨线程使用）
    global_ref: Option<Global<JObject<'static>>>,       // Kotlin callback 对象的全局引用
}
```

**线程附加策略（JNI 0.22 闭包模式）**：

```rust
fn attach<F>(&self, f: F)
where
    F: FnOnce(&mut Env, &Global<JObject<'static>>) -> jni::errors::Result<()>,
{
    let Some(global_ref) = self.global_ref.as_ref() else { return };
    let result: Result<(), jni::errors::Error> =
        self.java_vm.attach_current_thread(|env| f(env, global_ref));
    if let Err(e) = result {
        error!("[Callback] 无法附加线程到 JVM: {}", e);
    }
}
```

**为什么用 `attach_current_thread` + 闭包模式？**

- JNI 0.22 统一使用 `attach_current_thread()` 配合闭包，内部自动管理线程生命周期
- 闭包内同时提供 `env` 和 `global_ref`，无需额外查找
- 闭包返回 `Result`，支持 `?` 错误传播，简化异常处理

**异常清除（自动处理）**：

JNI 0.22 的闭包模式配合 `?` 传播，错误由 `with_env().resolve::<LogErrorAndDefault>()` 统一处理，无需手动调用 `exception_clear()`。

### 3.4 publish_sender() — 发布消息转发器

```rust
async fn publish_sender(
    mut publish_rx: mpsc::Receiver<PublishRequest>,
    client_arc: Arc<Mutex<Option<AsyncClient>>>,
    callbacks: Arc<CallbackManager>,
) {
    while let Some(req) = publish_rx.recv().await {
        // 加锁 → clone client → 释放锁
        let client = {
            let guard = client_arc.lock().unwrap();
            guard.clone()
        };

        if let Some(c) = client {
            if let Err(e) = c.publish(req.topic, req.qos, false, req.payload).await {
                let msg = format!("发布失败: {} - {}", req.topic, e);
                log::error!("[MQTT] {}", msg);
                callbacks.on_error(ERR_PUBLISH_FAILED, &msg);
            }
        } else {
            let msg = format!("客户端未连接，无法发布: {}", req.topic);
            log::error!("[MQTT] {}", msg);
            callbacks.on_error(ERR_PUBLISH_FAILED, &msg);
        }
    }
}
```

**为什么需要这个转发器？**

**优化前**：每次 `publish()` 都 spawn 一个新的异步任务
- 1000 次 publish → 1000 个 task
- 每个 task 都要竞争 `client_arc` 的锁
- 大量锁竞争 + 任务调度开销 → CPU 占用 35%

**优化后**：单一常驻 task 串行处理
- 1000 次 publish → 1000 次 channel 写入（微秒级）
- 只有 `publish_sender` 一个 task 竞争锁
- CPU 占用降至 1-2%

---

## 4. JNI 桥接层

### 4.1 指针管理

```rust
// 创建实例（JNI 0.22 使用 EnvUnowned 作为入口函数参数）
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_..._nativeCreate(
    _env: EnvUnowned, _class: JClass, debug: jboolean,
) -> jlong {
    let core = match NativeMqttCore::new() {
        Ok(c) => c,
        Err(e) => { log::error!("[JNI] 创建失败: {}", e); return 0; }
    };
    Box::into_raw(Box::new(core)) as jlong  // 分配堆内存，返回指针
}

// 销毁实例（使用 with_env + resolve 闭包模式）
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_..._nativeDestroy(
    mut env: EnvUnowned, _class: JClass, ptr: jlong,
) {
    env.with_env(|_env| {
        let Some(core) = (unsafe { try_get_core(ptr) }) else {
            return Ok::<(), jni::errors::Error>(());
        };
        core.shutdown();
        let boxed = unsafe { Box::from_raw(ptr as *mut NativeMqttCore) };
        drop(boxed);
        Ok(())
    }).resolve::<jni::errors::LogErrorAndDefault>()
}
```

**JNI 0.22 关键变化**：
- 入口函数参数类型从 `JNIEnv` 改为 `EnvUnowned`
- 需要调用 `env.with_env(|env| { ... })` 获取可用的 `Env` 引用
- 错误通过 `.resolve::<LogErrorAndDefault>()` 统一处理
- `jboolean` 从 `u8` 变为原生 `bool`，无需 `!= 0` 转换

**生命周期**：

```
Kotlin 端                    Rust 端
nativeCreate()        →     Box::into_raw(Box::new(core)) as jlong
ptr: Long                    0x7f... → NativeMqttCore

nativeDestroy(ptr)    →     Box::from_raw(ptr as *mut) + drop()
ptr = 0                      内存释放，触发 Drop
```

### 4.2 参数转换

```rust
// 安全提取 JNI 字符串（JNI 0.22 使用 mutf8_chars 方法）
fn safe_get_string(env: &mut jni::Env, jstr: &JString) -> String {
    if jstr.is_null() { return String::new(); }
    match jstr.mutf8_chars(env) {
        Ok(s) => s.to_string(),
        Err(e) => {
            log::error!("[JNI] get_string 失败: {}", e);
            String::new()
        }
    }
}

// 安全提取 JNI 字节数组
fn safe_get_byte_array(env: &mut jni::Env, arr: &JByteArray) -> Vec<u8> {
    if arr.is_null() { return Vec::new(); }
    match env.convert_byte_array(arr) {
        Ok(v) => v,
        Err(e) => {
            log::error!("[JNI] convert_byte_array 失败: {}", e);
            Vec::new()
        }
    }
}

// 安全提取 JNI 字符串数组（JNI 0.22 使用泛型和方法式访问）
fn safe_get_string_array(env: &mut jni::Env, arr: &JObjectArray<JString>) -> Vec<String> {
    if arr.is_null() { return Vec::new(); }
    let mut result = Vec::new();
    if let Ok(len) = arr.len(env) {
        for i in 0..len {
            if let Ok(elem) = arr.get_element(env, i as usize) {
                let s = safe_get_string(env, &elem);
                if !s.is_empty() { result.push(s); }
            }
        }
    }
    result
}
```

**JNI 0.22 关键变化**：
- `env.get_string(jstr)` → `jstr.mutf8_chars(env)`
- `env.get_array_length(arr)` → `arr.len(env)`
- `env.get_object_array_element(arr, i)` → `arr.get_element(env, i)`
- `JObjectArray` → `JObjectArray<JString>`（带泛型参数）

### 4.3 nativeConnect 完整流程

```rust
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_..._nativeConnect(
    mut env: EnvUnowned, _class: JClass, core_ptr: jlong,
    host: JString, port: jni::sys::jint, client_id: JString,
    username: JString, password: JString,
    qos: jni::sys::jint, keep_alive_secs: jni::sys::jint,
    reconnect_interval_secs: jni::sys::jint,
    message_buffer_size: jni::sys::jint,
    clean_session: jboolean,  // JNI 0.22 中原生 bool
    connection_timeout_secs: jni::sys::jint,
    topics_array: JObjectArray<JString>,  // JNI 0.22 带泛型参数
    callback_obj: JObject,
) {
    env.with_env(|env| {
        // 1. 验证指针
        let Some(core) = (unsafe { try_get_core(core_ptr) }) else {
            return Ok::<(), jni::errors::Error>(());
        };

        // 2. 验证 callback 非 null
        if callback_obj.is_null() {
            log::error!("[JNI] callback_obj 为空");
            return Ok(());
        }

        // 3. 创建 CallbackManager（LocalRef → GlobalRef）
        let callbacks = match CallbackManager::new(env, callback_obj) {
            Ok(cb) => Arc::new(cb),
            Err(e) => {
                log::error!("[JNI] 创建 CallbackManager 失败: {}", e);
                return Ok(());
            }
        };

        // 4. 解析配置
        let mut config = MqttConfig::default();
        let host_str = safe_get_string(env, &host);
        config.host = if host_str.starts_with("tcp://") || host_str.starts_with("ssl://") {
            host_str[6..].to_string()
        } else {
            host_str
        };
        config.port = port as u16;
        config.client_id = safe_get_string(env, &client_id);
        config.username = safe_get_string(env, &username);
        config.password = safe_get_string(env, &password);
        config.qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        };
        config.keep_alive_secs = if keep_alive_secs > 0 { keep_alive_secs as u64 } else { 10 };
        config.reconnect_interval_secs = if reconnect_interval_secs > 0 { reconnect_interval_secs as u64 } else { 10 };
        config.message_buffer_size = if message_buffer_size > 0 { message_buffer_size as usize } else { 10000 };
        config.clean_session = clean_session;  // jboolean 现在是原生 bool
        config.connection_timeout_secs = if connection_timeout_secs > 0 { connection_timeout_secs as u64 } else { 5 };
        config.topics = safe_get_string_array(env, &topics_array);

        // 5. 参数校验
        if config.host.is_empty() || config.client_id.is_empty() {
            let msg = format!("host 或 clientId 为空 (host='{}', clientId='{}')", config.host, config.client_id);
            log::error!("[JNI] {}", msg);
            callbacks.on_error(ERR_INVALID_PARAMS, &msg);
            return Ok(());
        }

        // 6. 设置回调并启动连接
        core.set_callbacks(callbacks);
        core.connect(config);
        Ok(())
    }).resolve::<jni::errors::LogErrorAndDefault>()
}
```

**JNI 0.22 关键变化**：
- 函数签名使用 `EnvUnowned` 而非 `JNIEnv`
- 整个函数体包裹在 `env.with_env(|env| { ... }).resolve::<LogErrorAndDefault>()` 中
- `clean_session` 直接使用（jboolean 现在是原生 bool，无需 `!= 0` 转换）
- 新增 `connection_timeout_secs` 参数
- `JObjectArray` 改为 `JObjectArray<JString>`（带泛型参数）

---

## 5. 线程模型

### 5.1 线程清单

| 线程名 | 来源 | 职责 |
|--------|------|------|
| `mqtt-io-0` | Tokio runtime | 运行异步任务（event_loop、dispatcher、publish_sender） |
| `mqtt-io-1` | Tokio runtime | 同上（Tokio 调度器分配任务） |
| `RumqttcClient-JNI` | Kotlin executor | 执行 JNI 调用（connect、publish、destroy） |
| 主线程 | Android | 调用 `isConnected()`（纳秒级，安全） |

### 5.2 线程安全保证

| 数据 | 保护机制 |
|------|---------|
| `client` | `Arc<Mutex<Option<T>>>`，读写都加锁 |
| `is_connected` | `Arc<AtomicBool>`，无锁原子操作 |
| `callbacks` | `Mutex<Option<Arc<T>>>`，设置时加锁 |
| `msg_tx` / `publish_tx` | `Mutex<Option<T>>`，设置时加锁 |

**关键原则**：锁的作用域尽量小，绝不跨 `.await` 点持锁。

```rust
// ✅ 正确：在锁内 clone，释放锁后再 await
let client = {
    let guard = client_arc.lock().unwrap();
    guard.clone()
}; // guard 在这里被 drop
client.unwrap().publish(...).await; // 没有锁跨越 await

// ❌ 错误：MutexGuard 跨越了 .await 点
let guard = client_arc.lock().unwrap();
guard.as_ref().unwrap().publish(...).await; // guard 跨越了 await！
```

---

## 6. 已知问题与限制

### 6.1 shutdown() 的双重调用

**问题**：`nativeDestroy` 先显式调用 `core.shutdown()`，然后 `drop(boxed)` 又触发 `Drop::drop()` → 再次 `shutdown()`。

**影响**：不会崩溃，但会多等待约 3 秒（`block_on` sleep 500ms 再执行一次）。

**建议修复**：在 `shutdown()` 中用 `AtomicBool` 标记是否已关闭：

```rust
pub struct NativeMqttCore {
    // ... existing fields ...
    is_shutdown: AtomicBool,  // 新增
}

pub fn shutdown(&mut self) {
    // 防止重复执行
    if self.is_shutdown.swap(true, Ordering::SeqCst) {
        return;
    }
    // ... 原有逻辑 ...
}
```

### 6.2 publish_tx channel 容量不一致

**问题**：
- `msg_tx` 容量从 `config.message_buffer_size`（默认 10000）读取
- `publish_tx` 容量硬编码为 50（代码注释说 1000，实际是 50）

**影响**：上层突发大量 publish 时，50 很容易打满。`publish()` 使用 `try_send()`（非阻塞），队列满时消息直接丢弃。

**建议修复**：
1. 将 `publish_tx` 容量改为从 config 读取（或硬编码为 1000）
2. 或者改用 `runtime.block_on(tx.send(...))` 实现背压（但会阻塞 JNI 调用线程）

### 6.3 不支持 TLS/SSL 连接

**问题**：虽然 `rumqttc` 依赖配置了 `rustls + ring`，但代码中没有配置 TLS 选项，实际连接仍然是明文。

**影响**：无法连接到需要 TLS 的 MQTT 服务器（如 `mqtts://` 或 `wss://`）。

**建议修复**：
1. 在 `MqttConfig` 中添加 `use_tls: bool` 字段
2. 在 `event_loop` 中根据 `use_tls` 配置 `MqttOptions::set_transport(Transport::tls_with_config(...))`
3. 可能需要处理证书验证（自签名证书 vs 系统 CA）

### 6.4 重连无退避策略

**问题**：重连间隔固定为 `reconnect_interval_secs` 秒，没有指数退避（exponential backoff）。

**影响**：在网络长时间不可用时，会以固定频率不断尝试连接，浪费电量和网络资源。

**建议修复**：实现指数退避：

```rust
let mut backoff = config.reconnect_interval_secs;
let max_backoff = 300; // 最多 5 分钟

loop {
    // ... 连接尝试 ...
    
    // 等待重连（指数退避）
    tokio::time::sleep(Duration::from_secs(backoff)).await;
    backoff = (backoff * 2).min(max_backoff);
}
```

### 6.5 日志级别控制粒度不足

**问题**：`nativeCreate` 的 `debug` 参数只区分 Info 和 Off 两级。

**影响**：生产环境中可能需要 Warn/Error 级别的日志，当前无法实现。

**建议修复**：将 `debug: jboolean` 改为 `log_level: jint`，支持 0=Off, 1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace。

---

## 7. 开发规范

### 7.1 注释规范

所有代码必须附带**极其详细的中文注释**，目标是让初学者也能看懂每一行。

**注释模板**：

```rust
/// 中文概述（一句话）
///
/// 详细说明：在系统中的角色、被谁调用、何时调用。
///
/// ## 参数
/// - `param1` — 含义（类型、约束、默认值）
/// - `param2` — 含义
///
/// ## 返回值
/// - 成功时 — 返回什么
/// - 失败时 — 返回什么 / 抛出什么
///
/// ## 注意事项
/// 调用者需要知道的约束或陷阱。
```

**注释技巧**：

1. **术语即用即解释**：遇到专业术语，用一句话就地解释
2. **类型选择要解释"为什么用它"**：不能只说用了什么，必须说为什么选它
3. **用"错误 vs 正确"对比展示易错点**
4. **用表格整理枚举/配置项**
5. **用 ASCII 流程图展示复杂逻辑**
6. **用时序图展示跨模块/跨语言交互**

**注释密度参考**：

- 每个**类/结构体**：≥ 10 行注释
- 每个**字段**：≥ 3 行注释
- 每个**公共方法**：≥ 8 行注释
- 每个**复杂逻辑块**（循环、分支、异步）：≥ 5 行注释
- 每个**关键操作行**（锁、指针、网络、异步等待）：1 行行内注释

### 7.2 错误处理规范

**原则**：所有错误都通过回调通知，不抛 Java 异常。

**错误码定义**：

| 错误码 | 常量名 | 含义 |
|--------|--------|------|
| 1 | `ERR_INVALID_PARAMS` | 参数错误（host/clientId 为空等） |
| 2 | `ERR_INIT_FAILED` | 初始化失败（创建 NativeMqttCore 失败） |
| 3 | `ERR_PUBLISH_FAILED` | 发布失败（网络错误、未连接等） |

**异常处理**：JNI 0.22 采用闭包 + `?` 传播错误，由 `with_env().resolve::<LogErrorAndDefault>()` 统一处理异常。回调方法内部只需使用 `?` 返回错误，无需手动调用 `env.exception_clear()`。

```rust
pub fn on_msg(&self, topic: &str, message: &str) {
    self.attach(|env, cb| {
        let topic_jstr = env.new_string(topic)?;
        let msg_jstr = env.new_string(message)?;
        env.call_method(
            cb,
            jni::jni_str!("onMsg"),
            jni::jni_sig!((java.lang.String, java.lang.String) -> void),
            &[jni::JValue::Object(&topic_jstr), jni::JValue::Object(&msg_jstr)],
        )?;
        Ok(())
    });
}
```

### 7.3 锁的使用规范

**原则**：锁的作用域尽量小，绝不跨 `.await` 点持锁。

```rust
// ✅ 正确模式
let value = {
    let guard = mutex.lock().unwrap();
    guard.clone()
}; // guard 在这里被 drop
async_operation(value).await;

// ❌ 错误模式
let guard = mutex.lock().unwrap();
async_operation(guard.value).await; // guard 跨越了 await！
```

### 7.4 异步编程规范

**原则**：使用 `tokio::select!` 同时监听多个事件，确保可取消性。

```rust
// ✅ 正确：重连等待可取消
tokio::select! {
    _ = cancel_token.cancelled() => break,
    _ = tokio::time::sleep(Duration::from_secs(interval)) => {}
}

// ❌ 错误：sleep 期间无法响应取消信号
tokio::time::sleep(Duration::from_secs(interval)).await;
```

---

## 8. 构建与调试

### 8.1 编译命令

```bash
# 清理
cargo clean

# 编译 Android 64 位
cargo build --target aarch64-linux-android --release

# 编译 Android 64 和 32 位
cargo clean && \
  cargo build --target aarch64-linux-android --release && \
  cargo build --target armv7-linux-androideabi --release
```

### 8.2 交叉编译配置

`.cargo/config.toml`（macOS 示例）：

```toml
[target.aarch64-linux-android]
linker = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/aarch64-linux-android21-clang"

[target.armv7-linux-androideabi]
linker = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/armv7a-linux-androideabi21-clang"

[env]
CC_aarch64-linux-android = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/aarch64-linux-android21-clang"
AR_aarch64-linux-android = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
CC_armv7-linux-androideabi = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/armv7a-linux-androideabi21-clang"
AR_armv7-linux-androideabi = "/Users/itc/Library/Android/sdk/ndk/29.0.13599879/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
```

**注意：** 路径需要根据你的 NDK 安装位置调整。Windows 路径示例：
```toml
linker = "C:/app/asSdk/ndk/29.0.14206865/toolchains/llvm/prebuilt/windows-x86_64/bin/aarch64-linux-android21-clang.cmd"
```

### 8.3 调试技巧

**开启日志**：

```kotlin
val client = RumqttcClient(debug = true)
```

**Logcat 过滤**：

```bash
# 只看 MQTT 相关日志
adb logcat | grep -E "MQTT|JNI|Callback"

# 按线程过滤
adb logcat | grep "mqtt-io"
```

**常见问题排查**：

| 问题 | 可能原因 | 排查方法 |
|------|---------|---------|
| 连接不上 | 网络不通、服务器地址错误 | 检查 Logcat 中的连接日志 |
| 收不到消息 | 主题不匹配、QoS 问题 | 检查订阅主题和服务器发布的主题 |
| 发布失败 | 未连接、网络故障 | 检查 `onError` 回调的错误码 |
| 内存泄漏 | 忘记调用 `destroy()` | 检查 Activity 生命周期 |
| 线程 ID 持续增长 | attach/detach 循环 | 已修复：改用 `attach_current_thread_as_daemon` |

---

## 9. 后续开发建议

### 9.1 优先级 P0（关键问题）

1. **修复 publish_tx channel 容量不一致**
   - 将硬编码的 50 改为从 config 读取或硬编码为 1000
   - 考虑改用 `runtime.block_on(tx.send(...))` 实现背压

2. **修复 shutdown() 双重调用**
   - 添加 `is_shutdown: AtomicBool` 防止重复执行

### 9.2 优先级 P1（重要功能）

3. **支持 TLS/SSL 连接**
   - 在 `MqttConfig` 中添加 `use_tls: bool`
   - 配置 `MqttOptions::set_transport(Transport::tls_with_config(...))`
   - 处理证书验证（自签名证书 vs 系统 CA）

4. **支持 clean_session 配置**
   - 在 `nativeConnect` 参数列表中添加 `clean_session: jboolean`

### 9.3 优先级 P2（优化改进）

5. **实现重连指数退避**
   - 初始间隔 `reconnect_interval_secs`，每次失败翻倍，上限 5 分钟

6. **增强日志级别控制**
   - 将 `debug: jboolean` 改为 `log_level: jint`，支持多级日志

7. **添加连接状态监听**
   - 提供 `addConnectionListener()` / `removeConnectionListener()` API
   - 允许上层监听连接状态变化（连接成功、断开、重连中）

8. **添加统计信息**
   - 提供 `getStats()` 方法，返回已发送消息数、已接收消息数、重连次数等

---

## 10. 文件清单

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
├── README.md                    # 项目说明文档
└── DEVELOPMENT.md               # 本文件（开发指南）
```

---

## 11. 附录：关键代码片段

### 11.1 完整的事件循环伪代码

```rust
async fn event_loop(config, callbacks, client_arc, cancel_token, is_connected, msg_tx) {
    let mut was_ever_connected = false;

    loop {
        // 检查取消信号
        if cancel_token.is_cancelled() { break; }

        // 创建新的 MqttOptions 和 AsyncClient
        let mut mqttoptions = MqttOptions::new(&config.client_id, &config.host, config.port);
        mqttoptions.set_keep_alive(Duration::from_secs(config.keep_alive_secs));
        mqttoptions.set_clean_session(config.clean_session);
        if !config.username.is_empty() {
            mqttoptions.set_credentials(&config.username, &config.password);
        }

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        
        // 将新 client 存入共享 Arc<Mutex<>>
        {
            let mut guard = client_arc.lock().unwrap();
            *guard = Some(client);
        }

        log::info!("[MQTT] 正在连接 {}...", config.host);

        // 内层循环：事件处理
        let mut connected = false;
        loop {
            tokio::select! {
                // 分支 1：取消信号
                _ = cancel_token.cancelled() => {
                    log::info!("[MQTT] 收到取消信号，退出事件循环");
                    {
                        let mut guard = client_arc.lock().unwrap();
                        *guard = None;
                    }
                    return;
                }

                // 分支 2：MQTT 事件
                result = eventloop.poll() => {
                    match result {
                        // 连接成功
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            log::info!("[MQTT] 连接成功: {}", config.host);
                            connected = true;
                            is_connected.store(true, Ordering::Relaxed);
                            
                            // 回调通知
                            callbacks.connect_complete(was_ever_connected, &config.host);
                            was_ever_connected = true;

                            // 自动订阅所有 topics
                            for topic in &config.topics {
                                match client.subscribe(topic, config.qos).await {
                                    Ok(_) => log::info!("[MQTT] 订阅成功: {}", topic),
                                    Err(e) => log::error!("[MQTT] 订阅失败: {} - {}", topic, e),
                                }
                            }
                        }

                        // 收到消息
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            let topic = publish.topic;
                            let payload = String::from_utf8_lossy(&publish.payload).to_string();
                            
                            log::debug!("[MQTT] 收到消息: topic={}, size={}", topic, payload.len());

                            // 发送到消息队列（背压模式，不丢消息）
                            if let Err(_) = msg_tx.send(MqttMessage { topic, payload }).await {
                                log::error!("[MQTT] 消息队列已关闭，退出事件循环");
                                break;
                            }
                        }

                        // 其他事件（心跳等）忽略
                        Ok(_) => {}

                        // 连接错误
                        Err(e) => {
                            let err_msg = format!("{}", e);
                            log::warn!("[MQTT] 事件循环错误: {}", err_msg);

                            if connected {
                                // 之前是连接状态，说明是"断线"
                                is_connected.store(false, Ordering::Relaxed);
                                callbacks.connection_lost(&err_msg);
                            } else {
                                // 从未连接过，说明是"连接失败"
                                callbacks.connection_lost(&err_msg);
                            }

                            // 跳出内层循环，进入重连等待
                            break;
                        }
                    }
                }
            }
        }

        // 清理 client
        {
            let mut guard = client_arc.lock().unwrap();
            *guard = None;
        }

        log::info!("[MQTT] 等待 {} 秒后重连...", config.reconnect_interval_secs);

        // 等待重连间隔（可取消）
        tokio::select! {
            _ = cancel_token.cancelled() => {
                log::info!("[MQTT] 等待期间收到取消信号，退出");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(config.reconnect_interval_secs)) => {}
        }
    }

    log::info!("[MQTT] 事件循环已退出");
}
```

### 11.2 完整的 CallbackManager 实现

```rust
use jni::objects::{Global, JObject};
use jni::Env;
use jni::sys::jboolean;
use std::sync::Arc;

pub struct CallbackManager {
    java_vm: Arc<jni::JavaVM>,
    global_ref: Option<Global<JObject<'static>>>,
}

impl CallbackManager {
    pub fn new(env: &mut Env, callback_obj: JObject) -> jni::errors::Result<Self> {
        let java_vm = env.get_java_vm()?;
        let global_ref = env.new_global_ref(callback_obj)?;
        Ok(Self {
            java_vm: Arc::new(java_vm),
            global_ref: Some(global_ref),
        })
    }

    fn attach<F>(&self, f: F)
    where
        F: FnOnce(&mut Env, &Global<JObject<'static>>) -> jni::errors::Result<()>,
    {
        let Some(global_ref) = self.global_ref.as_ref() else { return };
        let result: Result<(), jni::errors::Error> =
            self.java_vm.attach_current_thread(|env| f(env, global_ref));
        if let Err(e) = result {
            log::error!("[Callback] 无法附加线程到 JVM: {}", e);
        }
    }

    pub fn connect_complete(&self, reconnect: bool, server_uri: &str) {
        self.attach(|env, cb| {
            let uri_jstr = env.new_string(server_uri)?;
            let reconnect_val: jboolean = reconnect;
            env.call_method(
                cb,
                jni::jni_str!("connectComplete"),
                jni::jni_sig!((boolean, java.lang.String) -> void),
                &[jni::JValue::Bool(reconnect_val), jni::JValue::Object(&uri_jstr)],
            )?;
            Ok(())
        });
    }

    pub fn connection_lost(&self, cause: &str) {
        self.attach(|env, cb| {
            let cause_jstr = env.new_string(cause)?;
            env.call_method(
                cb,
                jni::jni_str!("connectionLost"),
                jni::jni_sig!((java.lang.String) -> void),
                &[jni::JValue::Object(&cause_jstr)],
            )?;
            Ok(())
        });
    }

    pub fn on_msg(&self, topic: &str, message: &str) {
        self.attach(|env, cb| {
            let topic_jstr = env.new_string(topic)?;
            let msg_jstr = env.new_string(message)?;
            env.call_method(
                cb,
                jni::jni_str!("onMsg"),
                jni::jni_sig!((java.lang.String, java.lang.String) -> void),
                &[jni::JValue::Object(&topic_jstr), jni::JValue::Object(&msg_jstr)],
            )?;
            Ok(())
        });
    }

    pub fn on_error(&self, code: i32, message: &str) {
        self.attach(|env, cb| {
            let msg_jstr = env.new_string(message)?;
            env.call_method(
                cb,
                jni::jni_str!("onError"),
                jni::jni_sig!((int, java.lang.String) -> void),
                &[jni::JValue::Int(code), jni::JValue::Object(&msg_jstr)],
            )?;
            Ok(())
        });
    }

    pub fn release(&mut self) {
        if let Some(global_ref) = self.global_ref.take() {
            drop(global_ref);
            log::info!("[Callback] JNI 全局引用已释放");
        }
    }
}

impl Drop for CallbackManager {
    fn drop(&mut self) {
        self.release();
    }
}
```

---

**文档版本**：2026-08-24  
**维护者**：ccalywm  
**上游依赖**：基于 [rumqttc](https://github.com/bytebeamio/rumqttc) crate 实现
