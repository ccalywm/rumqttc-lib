package com.rust.rumqttc

/**
 * MQTT 回调接口
 *
 * 对应 Rust 端的回调方法，所有连接状态变化和消息都通过此接口通知
 */
interface MqttCallback {
    /**
     * 连接或重连成功
     *
     * @param reconnect true=重连成功，false=首次连接成功
     * @param serverURI 服务器地址，格式为 "host:port"
     */
    fun connectComplete(reconnect: Boolean, serverURI: String)

    /**
     * 连接断开
     *
     * 断开后会自动尝试重连（10秒间隔）
     *
     * @param cause 断开原因
     */
    fun connectionLost(cause: String)

    /**
     * 收到消息
     *
     * @param topic 消息主题
     * @param payload 消息内容（UTF-8 字符串）
     */
    fun onMsg(topic: String, payload: String)

    /**
     * 错误回调
     *
     * @param code 错误码
     * @param message 错误描述
     */
    fun onError(code: Int, message: String)
}

/**
 * 错误码常量
 */
object MqttError {
    /** 参数无效 */
    const val INVALID_PARAMS = 1
    /** 初始化失败 */
    const val INIT_FAILED = 2
    /** 发布失败 */
    const val PUBLISH_FAILED = 3
}
