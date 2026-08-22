package com.rust.rumqttc

import android.util.Log
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * 基于 rumqttc (Rust) 实现的高性能 MQTT 客户端封装
 *
 * 支持：
 * - 自动重连（可配置间隔，无限重试）
 * - 异步非阻塞（不卡 UI 线程）
 * - 工业级稳定性
 * - Debug/Release 日志控制
 * - 可选外部线程池
 *
 * ## 使用示例
 *
 * ### 最简用法（使用默认配置）
 * ```kotlin
 * val client = RumqttcClient()
 *
 * client.connect(
 *     host = "192.168.1.100",
 *     port = 1883,
 *     clientId = "device_mac",
 *     username = "user",
 *     password = "pass",
 *     qos = 0,
 *     topics = arrayOf("topic1", "topic2"),
 *     callback = object : MqttCallback {
 *         override fun connectComplete(reconnect: Boolean, serverURI: String) {
 *             Log.i("MQTT", "连接成功: $serverURI, 是否重连: $reconnect")
 *         }
 *         override fun connectionLost(cause: String) {
 *             Log.w("MQTT", "连接断开: $cause")
 *         }
 *         override fun onMsg(topic: String, payload: String) {
 *             Log.d("MQTT", "收到消息: $topic -> $payload")
 *         }
 *         override fun onError(code: Int, message: String) {
 *             Log.e("MQTT", "错误[$code]: $message")
 *         }
 *     }
 * )
 *
 * // 发布消息
 * client.publish("topic1", "hello".toByteArray(), qos = 0)
 *
 * // 查询连接状态
 * if (client.isConnected()) {
 *     Log.d("MQTT", "当前已连接")
 * }
 *
 * // 销毁（Activity/Service 销毁时调用）
 * client.destroy()
 * ```
 *
 * ### 高级用法（开启调试日志 + 自定义线程池 + 自定义心跳/重连间隔）
 * ```kotlin
 * // 使用外部线程池（例如你项目中已有的 SingleThreads.get()）
 * val client = RumqttcClient(
 *     debug = true,                           // 开启日志（release 包设为 false）
 *     executor = SingleThreads.get()          // 传入项目现有的线程池
 * )
 *
 * client.connect(
 *     host = "192.168.1.100",
 *     port = 1883,
 *     clientId = "device_mac",
 *     username = "user",
 *     password = "pass",
 *     qos = 1,                                // QoS 1: 至少送达一次
 *     topics = arrayOf("topic1", "topic2"),
 *     callback = myCallback,
 *     keepAliveSecs = 30,                     // 心跳间隔 30 秒
 *     reconnectIntervalSecs = 5               // 重连间隔 5 秒
 * )
 * ```
 */
class RumqttcClient @JvmOverloads constructor(
    private val debug: Boolean = false,
    executor: Executor? = null
) {
    companion object {
        private const val TAG = "RumqttcClient"

        init {
            try {
                System.loadLibrary("rumqttc")
                Log.i(TAG, "rumqttc 库加载成功")
            } catch (e: UnsatisfiedLinkError) {
                Log.e(TAG, "rumqttc 库加载失败: ${e.message}")
            }
        }

        // JNI 方法声明
        @JvmStatic
        private external fun nativeCreate(debug: Boolean): Long

        @JvmStatic
        private external fun nativeConnect(
            ptr: Long,
            host: String,
            port: Int,
            clientId: String,
            username: String,
            password: String,
            qos: Int,
            keepAliveSecs: Int,
            reconnectIntervalSecs: Int,
            messageBufferSize: Int,
            cleanSession: Boolean,
            connectionTimeoutSecs: Int,
            topics: Array<String>,
            callback: MqttCallback
        )

        @JvmStatic
        private external fun nativePublish(
            ptr: Long,
            topic: String,
            payload: ByteArray,
            qos: Int
        )

        @JvmStatic
        private external fun nativeIsConnected(ptr: Long): Boolean

        @JvmStatic
        private external fun nativeDestroy(ptr: Long)
    }

    /**
     * 原生指针，0 表示未初始化
     *
     * 这个值是 Rust 堆上 NativeMqttCore 实例的地址，
     * 由 nativeCreate() 返回，必须通过 nativeDestroy() 释放。
     *
     * 使用 @Volatile 保证多线程可见性（写入后其他线程能立即看到新值）。
     */
    @Volatile
    private var nativePtr: Long = 0

    /**
     * 是否已初始化
     */
    val isInitialized: Boolean
        get() = nativePtr != 0L

    /**
     * JNI 调用专用线程池
     *
     * 所有 JNI 调用（connect、publish、destroy）都在此线程池中执行，
     * 避免在主线程调用 JNI 导致 ANR 或阻塞 UI。
     *
     * 可以由外部传入（如 SingleThreads.get()），也可以不传（自动创建单线程池）。
     */
    private val executor: Executor

    /**
     * 内部创建的线程池引用（仅在未传入外部线程池时非空）
     *
     * 用于在 destroy() 时关闭内部线程池。
     * 如果线程池是外部传入的，这里为 null，destroy() 时不会关闭它。
     */
    private val internalExecutor: ExecutorService?

    init {
        // nativeCreate 必须在 init 中同步调用，拿到 ptr 后才能执行其他操作
        nativePtr = nativeCreate(debug)
        if (nativePtr == 0L) {
            Log.e(TAG, "初始化失败：nativeCreate 返回 0")
        } else if (debug) {
            Log.i(TAG, "初始化成功: ptr=$nativePtr")
        }

        // 初始化线程池
        if (executor != null) {
            // 使用外部传入的线程池
            this.executor = executor
            this.internalExecutor = null
        } else {
            // 创建内部单线程池
            // isDaemon = true 表示这是守护线程，主线程退出时会自动退出
            val internal = Executors.newSingleThreadExecutor { r ->
                Thread(r, "RumqttcClient-JNI").apply { isDaemon = true }
            }
            this.executor = internal
            this.internalExecutor = internal
        }
    }

    /**
     * 连接 MQTT 服务器
     *
     * 异步操作，连接结果通过回调通知：
     * - 成功：[MqttCallback.connectComplete]
     * - 失败：[MqttCallback.onError] 或 [MqttCallback.connectionLost]
     *
     * 连接断开后会自动重连，重连成功后回调 connectComplete(reconnect=true, ...)
     *
     * @param host 服务器地址（如 "192.168.1.100"，支持 tcp:// 前缀会自动剥离）
     * @param port 端口号（通常 1883）
     * @param clientId 客户端 ID（建议用设备 MAC）
     * @param username 用户名（空字符串表示无鉴权）
     * @param password 密码
     * @param qos QoS 等级：0=至多一次, 1=至少一次, 2=只有一次
     * @param topics 需要订阅的主题列表
     * @param callback 回调接口
     * @param keepAliveSecs 心跳间隔（秒），默认 10
     * @param reconnectIntervalSecs 重连间隔（秒），默认 10
     * @param messageBufferSize 消息队列容量，默认 10000
     * @param cleanSession 是否清除会话，默认 true（每次连接从干净状态开始）
     * @param connectionTimeoutSecs 连接超时（秒），默认 30
     */
    fun connect(
        host: String,
        port: Int,
        clientId: String,
        username: String,
        password: String,
        qos: Int,
        topics: Array<String>,
        callback: MqttCallback,
        keepAliveSecs: Int = 10,
        reconnectIntervalSecs: Int = 10,
        messageBufferSize: Int = 10000,
        cleanSession: Boolean = true,
        connectionTimeoutSecs: Int = 30
    ) {
        if (!isInitialized) {
            Log.e(TAG, "connect 失败：客户端未初始化")
            callback.onError(MqttError.INIT_FAILED, "客户端未初始化")
            return
        }

        if (debug) {
            Log.i(TAG, "开始连接: $host:$port, clientId=$clientId, qos=$qos, keepAlive=${keepAliveSecs}s, reconnectInterval=${reconnectIntervalSecs}s, messageBuffer=$messageBufferSize, cleanSession=$cleanSession, connectionTimeout=${connectionTimeoutSecs}s")
        }

        executor.execute {
            nativeConnect(
                ptr = nativePtr,
                host = host,
                port = port,
                clientId = clientId,
                username = username,
                password = password,
                qos = qos,
                keepAliveSecs = keepAliveSecs,
                reconnectIntervalSecs = reconnectIntervalSecs,
                messageBufferSize = messageBufferSize,
                cleanSession = cleanSession,
                connectionTimeoutSecs = connectionTimeoutSecs,
                topics = topics,
                callback = callback
            )
        }
    }

    /**
     * 发布消息
     *
     * @param topic 主题
     * @param payload 消息内容（字节数组）
     * @param qos QoS 等级：0=至多一次, 1=至少一次, 2=只有一次
     */
    fun publish(topic: String, payload: ByteArray, qos: Int = 0) {
        if (!isInitialized) {
            if (debug) Log.e(TAG, "publish 失败：客户端未初始化")
            return
        }

        executor.execute {
            if (debug) {
                Log.d(TAG, "发布消息: topic=$topic, size=${payload.size}, qos=$qos")
            }
            nativePublish(nativePtr, topic, payload, qos)
        }
    }

    /**
     * 查询连接状态
     *
     * 注意：此方法会直接调用 JNI，但开销极小（纳秒级指针读取），可安全在主线程调用
     *
     * @return true=已连接，false=未连接
     */
    fun isConnected(): Boolean {
        if (!isInitialized) return false
        return nativeIsConnected(nativePtr)
    }

    /**
     * 销毁客户端，释放所有资源
     *
     * 调用后客户端不可再用，必须在 Activity/Service 销毁时调用
     */
    fun destroy() {
        if (!isInitialized) {
            Log.w(TAG, "destroy 跳过：客户端未初始化")
            return
        }

        if (debug) {
            Log.i(TAG, "销毁客户端: ptr=$nativePtr")
        }

        executor.execute {
            nativeDestroy(nativePtr)
            nativePtr = 0
            internalExecutor?.shutdown()
        }
    }
}
