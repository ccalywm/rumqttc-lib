//! # JNI 导出函数模块
//!
//! 这个模块定义了所有从 Kotlin 调用的 native 方法。
//!
//! ## JNI 命名规则
//!
//! JNI 函数名必须严格遵循特定格式，否则 Java 找不到对应的 native 方法：
//!
//! ```text
//! Java_包名_类名_方法名
//! ```
//!
//! 例如：`com.rust.rumqttc.RumqttcClient.nativeCreate` 对应的函数名是：
//! `Java_com_rust_rumqttc_RumqttcClient_nativeCreate`
//!
//! 注意：
//! - 包名中的 `.` 要替换成 `_`
//! - 不能有前导下划线
//! - 大小写要完全匹配
//!
//! ## 设计原则
//!
//! **所有错误都通过回调通知，不抛 Java 异常**
//!
//! 原因：
//! 1. JNI 异常会导致 Kotlin 端崩溃，用户体验差
//! 2. 异步操作的错误无法通过返回值传递
//! 3. 回调模式更符合 Android 开发习惯（类似 Retrofit、Room 等库）
//!
//! ## 错误码定义
//!
//! | 错误码 | 常量名 | 含义 |
//! |--------|--------|------|
//! | 1 | `ERR_INVALID_PARAMS` | 参数错误（host/clientId 为空等） |
//! | 2 | `ERR_INIT_FAILED` | 初始化失败（创建 NativeMqttCore 失败） |
//! | 3 | `ERR_PUBLISH_FAILED` | 发布失败（网络错误、未连接等） |
//!
//! ## 指针生命周期
//!
//! Kotlin 端持有一个 `Long` 类型的值，这个值实际上是 Rust 堆上的指针：
//!
//! ```text
//! Kotlin 端                    Rust 端
//! ────────                    ────────
//! nativeCreate()        →     Box::into_raw(Box::new(core))  分配堆内存，返回指针
//! ptr: Long = 0x7f...         0x7f... → NativeMqttCore
//!
//! nativeDestroy(ptr)    →     Box::from_raw(ptr)             回收堆内存
//! ptr = 0                     内存被释放，触发 Drop
//! ```

use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use std::sync::Arc;

use crate::callbacks::{CallbackManager, ERR_INVALID_PARAMS};
use crate::config::MqttConfig;
use crate::core::NativeMqttCore;

// ──────────────────────────────────────────────────────────
// 辅助函数
// ──────────────────────────────────────────────────────────

/// 从指针值获取 NativeMqttCore 引用
///
/// 这是一个**不安全**的操作，因为：
/// 1. 指针可能为空（0）
/// 2. 指针可能已经被释放（use-after-free）
/// 3. 指针可能指向无效内存
///
/// ## 参数
///
/// - `core_ptr` — Kotlin 端传入的指针值（Long 类型）
///
/// ## 返回值
///
/// - `Some(&mut NativeMqttCore)` — 指针有效，返回可变引用
/// - `None` — 指针为空或无效
///
/// ## 为什么用 `unsafe`？
///
/// Rust 的安全保证只适用于 Rust 代码内部。
/// 当数据跨越 FFI（Foreign Function Interface，外部函数接口）边界时，
/// Rust 无法验证指针的有效性，必须用 `unsafe` 来告诉编译器"我知道我在做什么"。
unsafe fn try_get_core<'a>(core_ptr: jlong) -> Option<&'a mut NativeMqttCore> {
    // 检查指针是否为空
    // Kotlin 端可能在 destroy 后继续调用其他方法，此时 ptr 是 0
    if core_ptr == 0 {
        log::warn!("[JNI] core_ptr 为空，操作已忽略");
        return None;
    }

    // 把 jlong（i64）转换为原始指针，再转换为可变引用
    //
    // `as *mut NativeMqttCore` — 类型转换：i64 → 原始指针
    // `&mut *(...)` — 解引用原始指针，获取可变引用
    //
    // 生命周期 `'a` 是"凭空"指定的，这是 unsafe 的常见模式。
    // 我们保证这个引用在 JNI 函数返回前都是有效的，
    // 因为 Kotlin 端会在 destroy 之前调用其他方法。
    Some(unsafe { &mut *(core_ptr as *mut NativeMqttCore) })
}

/// 安全地从 JNI 字符串中提取 Rust 字符串
///
/// JNI 字符串（`JString`）是 Java 虚拟机管理的对象，
/// 不能直接使用，必须通过 `env.get_string()` 转换为 Rust 的 `String`。
///
/// ## 参数
///
/// - `env` — JNI 环境，用于访问 JVM 对象
/// - `jstr` — JNI 字符串引用
///
/// ## 返回值
///
/// 转换后的 Rust 字符串。如果转换失败或字符串为空，返回空字符串。
///
/// ## 为什么可能失败？
///
/// - `jstr` 可能指向已被垃圾回收的对象
/// - JVM 内存不足，无法分配新的字符串
/// - 字符串包含非法 UTF-8 序列（极少见）
fn safe_get_string(env: &mut JNIEnv, jstr: &JString) -> String {
    // 检查是否为空引用
    if jstr.is_null() {
        return String::new();
    }

    // 调用 JNI 的 get_string 方法
    // 这个方法会：
    // 1. 从 JVM 获取字符串数据
    // 2. 转换为 Rust 的 Modified UTF-8 格式
    // 3. 返回 JavaStr（智能指针，自动释放）
    match env.get_string(jstr) {
        Ok(s) => s.into(), // JavaStr → String（自动复制数据）
        Err(e) => {
            log::error!("[JNI] get_string 失败: {}", e);
            String::new()
        }
    }
}

/// 安全地从 JNI 字节数组中提取 Rust 字节向量
///
/// 类似 `safe_get_string`，但处理的是字节数组（`byte[]`）。
///
/// ## 参数
///
/// - `env` — JNI 环境
/// - `arr` — JNI 字节数组引用
///
/// ## 返回值
///
/// 转换后的 Rust `Vec<u8>`。如果转换失败或数组为空，返回空向量。
fn safe_get_byte_array(env: &mut JNIEnv, arr: &JByteArray) -> Vec<u8> {
    if arr.is_null() {
        return Vec::new();
    }

    match env.convert_byte_array(arr) {
        Ok(v) => v,
        Err(e) => {
            log::error!("[JNI] convert_byte_array 失败: {}", e);
            Vec::new()
        }
    }
}

/// 安全地从 JNI 字符串数组中提取 Rust 字符串向量
///
/// JNI 数组（`jobjectArray`）需要逐个元素提取。
///
/// ## 参数
///
/// - `env` — JNI 环境
/// - `arr` — JNI 字符串数组引用
///
/// ## 返回值
///
/// 转换后的 Rust `Vec<String>`。空元素会被过滤掉。
fn safe_get_string_array(env: &mut JNIEnv, arr: &JObjectArray) -> Vec<String> {
    if arr.is_null() {
        return Vec::new();
    }

    let mut result = Vec::new();

    // 获取数组长度
    if let Ok(len) = env.get_array_length(arr) {
        // 逐个提取元素
        for i in 0..len {
            // 获取数组中第 i 个元素
            if let Ok(elem) = env.get_object_array_element(arr, i) {
                // 把 JObject 转换为 JString
                let jstr: JString = elem.into();
                let s = safe_get_string(env, &jstr);
                // 过滤掉空字符串
                if !s.is_empty() {
                    result.push(s);
                }
            }
        }
    }

    result
}

// ──────────────────────────────────────────────────────────
// JNI 导出函数
// ──────────────────────────────────────────────────────────

/// Kotlin 调用：`nativeCreate(debug: Boolean): Long`
///
/// 创建 NativeMqttCore 实例，返回指针值。
///
/// ## 参数
///
/// - `debug` — 是否启用调试日志
///   - `true` (1) — 输出 Info 级别日志（适合开发调试）
///   - `false` (0) — 关闭所有日志（适合生产环境）
///
/// ## 返回值
///
/// - 非零值 — 成功，返回指向 NativeMqttCore 的指针
/// - 0 — 失败（创建 Tokio 运行时失败）
///
/// ## 为什么返回 0 而不是抛异常？
///
/// 因为此时还没有回调通道（回调是在 connect 时才传入的），
/// 无法通过 `onError()` 通知 Kotlin 端，只能通过返回值表示失败。
///
/// Kotlin 端应该检查返回值是否为 0，如果为 0 则不继续调用其他方法。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_rust_rumqttc_RumqttcClient_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
    debug: jboolean,
) -> jlong {
    // 初始化日志系统
    //
    // `init_once` 保证只初始化一次，即使多次调用也不会报错。
    // 这对于库代码很重要，因为用户可能创建多个 RumqttcClient 实例。
    let level = if debug != 0 {
        log::LevelFilter::Info  // 输出 Info、Warn、Error
    } else {
        log::LevelFilter::Off   // 关闭所有日志
    };

    android_logger::init_once(
        android_logger::Config::default()
            .with_max_level(level),
    );

    log::info!("[JNI] nativeCreate (debug={})", debug != 0);

    // 创建 NativeMqttCore 实例
    match NativeMqttCore::new() {
        Ok(core) => {
            // 把 core 放入堆内存，并获取原始指针
            //
            // `Box::new(core)` — 在堆上分配内存，存放 core
            // `Box::into_raw(...)` — 把 Box 转换为原始指针，同时"忘记"释放内存
            //                         这样即使函数返回，堆上的 core 也不会被释放
            // `as jlong` — 把指针转换为 i64，可以安全地传递给 Kotlin
            //
            // 这个指针必须通过 `nativeDestroy()` 来释放，否则会内存泄漏。
            let ptr = Box::into_raw(Box::new(core)) as jlong;
            log::info!("[JNI] NativeMqttCore 创建成功，ptr={:#x}", ptr);
            ptr
        }
        Err(e) => {
            log::error!("[JNI] 创建 NativeMqttCore 失败: {}", e);
            0 // 返回 0 表示失败
        }
    }
}

/// Kotlin 调用：`nativeConnect(ptr, host, port, clientId, username, password, qos, keepAliveSecs, reconnectIntervalSecs, messageBufferSize, cleanSession, connectionTimeoutSecs, topics, callback)`
///
/// 启动 MQTT 连接。这是一个异步操作，结果通过回调通知。
///
/// ## 参数说明
///
/// | 参数 | 类型 | 说明 |
/// |------|------|------|
/// | `core_ptr` | `Long` | NativeMqttCore 指针 |
/// | `host` | `String` | MQTT 服务器地址（支持 `tcp://` 前缀，会自动剥离） |
/// | `port` | `Int` | 端口号（通常 1883） |
/// | `clientId` | `String` | 客户端 ID（建议用设备 MAC） |
/// | `username` | `String` | 用户名（空字符串表示无鉴权） |
/// | `password` | `String` | 密码 |
/// | `qos` | `Int` | QoS 等级（0/1/2） |
/// | `keepAliveSecs` | `Int` | 心跳间隔（秒），≤0 时使用默认值 10 |
/// | `reconnectIntervalSecs` | `Int` | 重连间隔（秒），≤0 时使用默认值 10 |
/// | `messageBufferSize` | `Int` | 消息队列容量，≤0 时使用默认值 10000 |
/// | `cleanSession` | `Boolean` | 是否清除会话（true=每次连接重新开始, false=保留会话） |
/// | `connectionTimeoutSecs` | `Int` | 连接超时（秒），≤0 时使用默认值 5 |
/// | `topics` | `Array<String>` | 订阅主题列表 |
/// | `callback` | `MqttCallback` | Kotlin 回调对象 |
///
/// ## 错误处理
///
/// - 参数错误 → `callback.onError(ERR_INVALID_PARAMS, message)`
/// - 回调创建失败 → 只记录日志（此时没有回调通道）
/// - 连接失败 → `callback.connectionLost(cause)` 或自动重连
///
/// ## tcp:// 前缀剥离
///
/// rumqttc 库要求 host 是纯 IP 或域名，不能包含协议前缀。
/// 如果 Kotlin 端传入的是 `"tcp://192.168.1.100"`，这里会自动剥离为 `"192.168.1.100"`。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_rust_rumqttc_RumqttcClient_nativeConnect(
    mut env: JNIEnv,
    _class: JClass,
    core_ptr: jlong,
    host: JString,
    port: jint,
    client_id: JString,
    username: JString,
    password: JString,
    qos: jint,
    keep_alive_secs: jint,
    reconnect_interval_secs: jint,
    message_buffer_size: jint,
    clean_session: jboolean,
    connection_timeout_secs: jint,
    topics_array: JObjectArray,
    callback_obj: JObject,
) {
    // 第一步：验证指针
    let Some(core) = (unsafe { try_get_core(core_ptr) }) else { return };

    // 第二步：验证回调对象
    // 必须在解析其他参数之前验证，因为后续的错误通知都依赖回调
    if callback_obj.is_null() {
        log::error!("[JNI] callback_obj 为空，无法连接");
        return;
    }

    // 第三步：创建回调管理器
    //
    // CallbackManager 会：
    // 1. 把 Kotlin 回调对象转换为 GlobalRef（防止被垃圾回收）
    // 2. 查找回调方法的签名（connectComplete、onMsg 等）
    // 3. 缓存 JavaVM 引用（用于跨线程调用）
    let callbacks = match CallbackManager::new(&mut env, callback_obj) {
        Ok(cb) => Arc::new(cb),
        Err(e) => {
            log::error!("[JNI] 创建 CallbackManager 失败: {}", e);
            // 此时没有可用的回调通道，只能记录日志
            return;
        }
    };

    // 第四步：解析配置参数

    // 先创建一个默认配置，然后逐个字段覆盖
    let mut config = MqttConfig::default();

    // 解析 host，自动剥离 tcp:// 前缀
    let host_str = safe_get_string(&mut env, &host);
    config.host = if host_str.starts_with("tcp://") {
        host_str[6..].to_string() // 去掉前 6 个字符 "tcp://"
    }else if host_str.starts_with("ssl://"){
        host_str[6..].to_string() // 去掉前 6 个字符 "ssl://"
    } else {
        host_str
    };

    config.port = port as u16;
    config.client_id = safe_get_string(&mut env, &client_id);
    config.username = safe_get_string(&mut env, &username);
    config.password = safe_get_string(&mut env, &password);

    // 把整数 QoS 转换为 rumqttc 的枚举
    config.qos = match qos {
        0 => rumqttc::QoS::AtMostOnce,    // 最多一次
        1 => rumqttc::QoS::AtLeastOnce,   // 至少一次
        2 => rumqttc::QoS::ExactlyOnce,   // 恰好一次
        _ => rumqttc::QoS::AtMostOnce,    // 其他值默认为 0
    };

    // 心跳间隔：如果传入的值 ≤0，使用默认值 10 秒
    config.keep_alive_secs = if keep_alive_secs > 0 {
        keep_alive_secs as u64
    } else {
        10
    };

    // 重连间隔：如果传入的值 ≤0，使用默认值 10 秒
    config.reconnect_interval_secs = if reconnect_interval_secs > 0 {
        reconnect_interval_secs as u64
    } else {
        10
    };

    // 消息队列容量：如果传入的值 ≤0，使用默认值 10000
    config.message_buffer_size = if message_buffer_size > 0 {
        message_buffer_size as usize
    } else {
        10000
    };

    // 清除会话标志：
    // - true（默认）：每次连接都从干净的状态开始，不保留之前的订阅和消息
    // - false：保留之前的会话状态（需要服务器支持持久会话）
    config.clean_session = clean_session != 0;

    // 连接超时：如果传入的值 ≤0，使用默认值 5 秒
    config.connection_timeout_secs = if connection_timeout_secs > 0 {
        connection_timeout_secs as u64
    } else {
        5
    };

    // 解析主题数组
    config.topics = safe_get_string_array(&mut env, &topics_array);

    // 第五步：参数校验
    // host 和 clientId 是必填项，如果为空则通过回调通知错误
    if config.host.is_empty() || config.client_id.is_empty() {
        let msg = format!(
            "host 或 clientId 为空，无法连接 (host='{}', clientId='{}')",
            config.host, config.client_id
        );
        log::error!("[JNI] {}", msg);
        callbacks.on_error(ERR_INVALID_PARAMS, &msg);
        return;
    }

    log::info!(
        "[JNI] 连接配置: host={}, port={}, clientId={}, qos={}, keepAlive={}s, reconnectInterval={}s, messageBuffer={}, cleanSession={}, connectionTimeout={}s, topics={:?}",
        config.host, config.port, config.client_id, qos,
        config.keep_alive_secs, config.reconnect_interval_secs,
        config.message_buffer_size,
        config.clean_session,
        config.connection_timeout_secs,
        config.topics
    );

    // 第六步：存入回调管理器并启动连接
    //
    // `set_callbacks()` 必须在 `connect()` 之前调用，
    // 因为 connect 启动的后台任务需要回调来通知上层
    core.set_callbacks(callbacks);
    core.connect(config);

    log::info!("[JNI] 连接请求已提交，等待回调...");
}

/// Kotlin 调用：`nativePublish(ptr, topic, payload, qos)`
///
/// 发布消息到指定主题。这是一个异步操作，不会阻塞调用线程。
///
/// ## 参数说明
///
/// - `core_ptr` — NativeMqttCore 指针
/// - `topic` — 目标主题（不能为空）
/// - `payload` — 消息内容（字节数组）
/// - `qos` — QoS 等级（0/1/2）
///
/// ## 错误处理
///
/// - topic 为空 → 记录日志，忽略此次发布
/// - 未连接或发布失败 → `callback.onError(ERR_PUBLISH_FAILED, message)`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_rust_rumqttc_RumqttcClient_nativePublish(
    mut env: JNIEnv,
    _class: JClass,
    core_ptr: jlong,
    topic: JString,
    payload: JByteArray,
    qos: jint,
) {
    let Some(core) = (unsafe { try_get_core(core_ptr) }) else { return };

    // 解析参数
    let topic_str = safe_get_string(&mut env, &topic);
    let payload_vec = safe_get_byte_array(&mut env, &payload);

    // 验证 topic 不为空
    if topic_str.is_empty() {
        log::error!("[JNI] topic 为空，发布已忽略");
        return;
    }

    // 转换 QoS
    let qos_val = match qos {
        0 => rumqttc::QoS::AtMostOnce,
        1 => rumqttc::QoS::AtLeastOnce,
        2 => rumqttc::QoS::ExactlyOnce,
        _ => rumqttc::QoS::AtMostOnce,
    };

    log::info!("[JNI] 发布: topic={}, size={}, qos={}", topic_str, payload_vec.len(), qos);

    // 调用 core.publish()，内部会异步执行
    core.publish(topic_str, payload_vec, qos_val);
}

/// Kotlin 调用：`nativeIsConnected(ptr): Boolean`
///
/// 查询当前是否已连接到 MQTT 服务器。
///
/// ## 参数
///
/// - `core_ptr` — NativeMqttCore 指针
///
/// ## 返回值
///
/// - `true` (1) — 已连接
/// - `false` (0) — 未连接或指针无效
///
/// ## 性能
///
/// 这个方法只是读取一个原子布尔值，开销在纳秒级别，
/// 可以在主线程安全调用，不会阻塞 UI。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_rust_rumqttc_RumqttcClient_nativeIsConnected(
    _env: JNIEnv,
    _class: JClass,
    core_ptr: jlong,
) -> jboolean {
    let Some(core) = (unsafe { try_get_core(core_ptr) }) else { return 0 };
    if core.is_connected() { 1 } else { 0 }
}

/// Kotlin 调用：`nativeDestroy(ptr)`
///
/// 销毁 NativeMqttCore 实例，释放所有资源。
///
/// ## 参数
///
/// - `core_ptr` — NativeMqttCore 指针（调用后此指针失效，不可再使用）
///
/// ## 执行流程
///
/// 1. 调用 `core.shutdown()` — 停止后台任务，等待清理完成
/// 2. 调用 `Box::from_raw(ptr)` — 从原始指针重新构造 Box
/// 3. 调用 `drop(boxed)` — 释放堆内存，触发 `Drop` trait
///
/// ## 为什么必须调用这个方法？
///
/// 因为 `nativeCreate()` 中使用了 `Box::into_raw()`，这会"忘记"释放内存。
/// 如果不手动调用 `nativeDestroy()`，会导致内存泄漏。
///
/// Kotlin 端应该在 Activity/Service 的 `onDestroy()` 中调用此方法。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_rust_rumqttc_RumqttcClient_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    core_ptr: jlong,
) {
    let Some(core) = (unsafe { try_get_core(core_ptr) }) else { return };

    log::info!("[JNI] 销毁 NativeMqttCore，ptr={:#x}", core_ptr);

    // 第一步：优雅关闭
    // 这会停止后台事件循环，等待最多 3 秒让任务完成清理
    core.shutdown();

    // 第二步：释放堆内存
    //
    // `Box::from_raw(ptr)` — 把原始指针重新转换为 Box
    //                        这告诉 Rust"这块内存由你管理了"
    // `drop(boxed)` — 显式释放 Box，触发 Drop trait
    //                 即使不写 drop，boxed 离开作用域时也会自动释放
    //
    // 注意：调用 Box::from_raw 后，core_ptr 指向的内存已经被释放，
    // Kotlin 端必须把 nativePtr 设为 0，防止再次使用已释放的指针。
    let boxed = unsafe { Box::from_raw(core_ptr as *mut NativeMqttCore) };
    drop(boxed);

    log::info!("[JNI] NativeMqttCore 已销毁");
}
