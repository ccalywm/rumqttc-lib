//! JNI 回调管理模块
//!
//! 本模块负责管理 Rust → Kotlin 的回调通知。
//! 当 MQTT 事件发生时（连接成功、收到消息、连接断开、发生错误），
//! 通过 JNI 调用 Kotlin 端实现的接口方法，把事件传递回去。

use jni::objects::{Global, JObject};
use jni::Env;
use jni::sys::jboolean;
use log::error;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════
// 错误码常量定义
//
// 这些错误码会在 onError 回调中传递给 Kotlin 端，
// Kotlin 可以根据错误码判断具体是什么类型的错误。
// ═══════════════════════════════════════════════════════════

/// 错误码 1：参数无效
/// 触发场景：host、clientId 等必填参数为空字符串
pub const ERR_INVALID_PARAMS: i32 = 1;

/// 错误码 2：初始化失败
/// 触发场景：创建 Tokio runtime 失败（极少发生，通常是系统资源不足）
pub const ERR_INIT_FAILED: i32 = 2;

/// 错误码 4：发布失败
/// 触发场景：MQTT 客户端未连接时调用 publish，或者网络发送失败
pub const ERR_PUBLISH_FAILED: i32 = 3;

// ═══════════════════════════════════════════════════════════
// CallbackManager 结构体
// ═══════════════════════════════════════════════════════════

/// JNI 回调管理器
///
/// ## 为什么需要这个结构体？
///
/// Rust 的 MQTT 事件循环运行在 Tokio 的后台线程中，
/// 而 Kotlin 的回调对象存在于 JVM（Java 虚拟机）中。
/// 要从 Rust 调用 Kotlin 的方法，需要解决两个问题：
///
/// 1. **GlobalRef（全局引用）**：
///    Kotlin 传入的 callback 对象是一个局部引用（LocalRef），
///    局部引用在 JNI 函数返回后就会被自动释放。
///    但我们需要在后台线程中长期持有这个对象，
///    所以必须把它转换成全局引用（GlobalRef），
///    告诉 JVM："这个对象不能回收，我还在用"。
///
/// 2. **JavaVM 引用**：
///    后台线程不是 JNI 线程，不能直接调用 Java 方法。
///    需要先通过 `java_vm.attach_current_thread()` 把当前线程"附加"到 JVM，
///    获得一个 JNIEnv（JNI 环境），然后才能调用 Java 方法。
///
/// ## 生命周期
///
/// - 在 `nativeConnect` 时创建
/// - 在 `nativeDestroy` 时通过 Drop 自动释放
/// - 释放时会调用 `delete_global_ref`，告诉 JVM 可以回收这个对象了
pub struct CallbackManager {
    /// JVM 引用，用于把后台线程附加到 JVM
    /// 使用 Arc 包装是因为 CallbackManager 可能被多个线程共享
    java_vm: Arc<jni::JavaVM>,

    /// Kotlin callback 对象的全局引用
    /// 使用 Option 包装是为了在 release 时可以 take 出来并释放
    global_ref: Option<Global<JObject<'static>>>,
}

impl CallbackManager {
    /// 创建回调管理器
    ///
    /// ## 参数
    /// - `env`：JNI 环境，用于获取 JavaVM 和创建全局引用
    /// - `callback_obj`：Kotlin 传入的回调对象（实现了 MqttCallback 接口）
    ///
    /// ## 返回值
    /// - `Ok(CallbackManager)`：创建成功
    /// - `Err(...)`：创建失败（callback_obj 为 null 或 JNI 调用失败）
    ///
    /// ## 内部做了什么？
    /// 1. 从 JNIEnv 中获取 JavaVM 引用（JavaVM 是全局的，可以在任何线程使用）
    /// 2. 把 callback_obj 从局部引用转换成全局引用（防止被 GC 回收）
    pub fn new(env: &mut Env, callback_obj: JObject) -> jni::errors::Result<Self> {
        let java_vm = env.get_java_vm()?;
        let global_ref = env.new_global_ref(callback_obj)?;
        Ok(Self {
            java_vm: Arc::new(java_vm),
            global_ref: Some(global_ref),
        })
    }

    /// 把当前线程附加到 JVM，获得 JNIEnv
    ///
    /// ## 为什么需要这个方法？
    ///
    /// Rust 的后台线程（Tokio worker thread）不是 JVM 线程，
    /// 不能直接调用 Java/Kotlin 的方法。
    /// 需要先调用 `attach_current_thread_as_daemon()` 把线程"注册"到 JVM，
    /// 之后才能通过返回的 JNIEnv 调用 Java 方法。
    ///
    /// ## 为什么用 as_daemon 而不是 attach_current_thread？
    ///
    /// - `attach_current_thread()` 返回 `AttachGuard`，guard 被 drop 时会 **自动 detach**
    ///   每次回调都 attach → callback → detach → 下次又 attach，
    ///   导致 JVM 端每次创建新的 Java Thread 对象（Thread-1, Thread-2, Thread-3...），
    ///   线程 ID 不断增长，虽然底层 OS 线程只有 2 个。
    ///
    /// - `attach_current_thread_as_daemon()` 返回 `JNIEnv`（无 guard），
    ///   **线程一旦附加就保持附加状态**，直到 JVM 退出。
    ///   同一个 OS 线程只附加一次，后续回调复用，不会再创建新的 Java Thread。
    ///
    /// ## 返回值
    /// - `Some(env)`：附加成功，可以用这个 env 调用 Java 方法
    /// - `None`：附加失败（极少发生）
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

    // ═══════════════════════════════════════════════════════
    // 以下四个方法是 Rust → Kotlin 的回调通知
    // 每个方法对应 Kotlin 端 MqttCallback 接口的一个方法
    // ═══════════════════════════════════════════════════════

    /// 通知 Kotlin：连接或重连成功
    ///
    /// 对应 Kotlin 方法：`fun connectComplete(reconnect: Boolean, serverURI: String)`
    ///
    /// ## 参数说明
    /// - `reconnect`：
    ///   - `false`：首次连接成功
    ///   - `true`：之前连过，断线后重新连接成功（重连）
    /// - `server_uri`：服务器地址，格式为 "host:port"，例如 "192.168.1.100:1883"
    ///
    /// ## JNI 方法签名
    /// `(ZLjava/lang/String;)V`
    /// - Z：第一个参数是 boolean
    /// - Ljava/lang/String;：第二个参数是 String
    /// - V：返回值是 void（没有返回值）
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

    /// 通知 Kotlin：连接断开了
    ///
    /// 对应 Kotlin 方法：`fun connectionLost(cause: String)`
    ///
    /// ## 参数说明
    /// - `cause`：断开原因的文本描述，例如：
    ///   - "Network is unreachable (os error 101)"：网络不可达
    ///   - "Connection reset by peer"：服务器主动断开
    ///   - "Software caused connection abort"：本地软件导致连接中断
    ///
    /// ## 注意
    /// 连接断开后，Rust 会自动等待 reconnect_interval_secs 秒后重新连接，
    /// 不需要 Kotlin 端做任何操作。
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

    /// 通知 Kotlin：收到了 MQTT 消息
    ///
    /// 对应 Kotlin 方法：`fun onMsg(topic: String, message: String)`
    ///
    /// ## 参数说明
    /// - `topic`：消息的主题，例如 "/device/host/30-3a-ba-12-0c-4e/get/status/change"
    /// - `message`：消息内容（UTF-8 编码的字符串），通常是 JSON 格式
    ///
    /// ## 注意
    /// 这个方法会在收到 MQTT PUBLISH 消息时立即调用，
    /// 回调发生在 Tokio 后台线程中，不要在里面做耗时操作。
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

    /// 通知 Kotlin：发生了错误
    ///
    /// 对应 Kotlin 方法：`fun onError(code: Int, message: String)`
    ///
    /// ## 参数说明
    /// - `code`：错误码，取值范围：
    ///   - 1 (ERR_INVALID_PARAMS)：参数无效
    ///   - 2 (ERR_INIT_FAILED)：初始化失败
    ///   - 3 (ERR_PUBLISH_FAILED)：发布失败
    /// - `message`：错误的详细描述文本
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

    /// 释放 JNI 全局引用
    ///
    /// ## 为什么需要手动释放？
    ///
    /// 全局引用（GlobalRef）会阻止 JVM 回收 callback 对象。
    /// 如果不释放，callback 对象会一直存在于 JVM 堆内存中，
    /// 即使 RumqttcClient 已经被 destroy 了也不会被回收，造成内存泄漏。
    ///
    /// jni 0.21 版本的 GlobalRef 内部使用 Arc 封装，
    /// 当最后一个引用被 drop 时会自动调用底层的 delete_global_ref。
    pub fn release(&mut self) {
        if let Some(global_ref) = self.global_ref.take() {
            drop(global_ref);
            log::info!("[Callback] JNI 全局引用已释放");
        }
    }
}

/// 当 CallbackManager 被销毁时，自动释放全局引用
///
/// 这是一个安全措施：即使忘记手动调用 release()，
/// Rust 的 drop 机制也会确保全局引用被释放，不会造成内存泄漏。
impl Drop for CallbackManager {
    fn drop(&mut self) {
        self.release();
    }
}
