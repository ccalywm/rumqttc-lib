use rumqttc::QoS;

/// MQTT 连接配置
///
/// 这个结构体存放了连接 MQTT 服务器所需的所有参数。
/// 它由 JNI 层（jni.rs）根据 Kotlin 传入的参数构建，然后传递给核心层（core.rs）使用。
///
/// ## 使用示例
///
/// ```rust
/// let mut config = MqttConfig::default();
/// config.host = "192.168.1.100".to_string();
/// config.port = 1883;
/// config.client_id = "device-001".to_string();
/// config.topics = vec!["/device/data/#".to_string()];
/// ```
///
/// ## 字段说明
///
/// | 字段 | 说明 | 默认值 |
/// |------|------|--------|
/// | host | MQTT 服务器地址（纯 IP 或域名，不含 tcp:// 前缀） | "" |
/// | port | MQTT 服务器端口 | 1883 |
/// | client_id | 客户端唯一标识，通常使用设备 MAC 地址 | "" |
/// | username | 登录用户名，空字符串表示不需要鉴权 | "" |
/// | password | 登录密码 | "" |
/// | topics | 需要订阅的主题列表 | [] |
/// | qos | 消息服务质量等级（0/1/2） | 0 |
/// | keep_alive_secs | 心跳间隔（秒），超过此时间没有通信会发送心跳包 | 10 |
/// | reconnect_interval_secs | 断线后重连等待间隔（秒） | 10 |
/// | clean_session | 是否每次连接都使用全新的会话 | true |
/// | message_buffer_size | 消息队列容量（事件循环 → 回调分发器之间的缓冲区） | 10000 |
/// | connection_timeout_secs | TCP 连接超时时间（秒） | 5 |
pub struct MqttConfig {
    /// MQTT 服务器地址
    ///
    /// 只填纯 IP 或域名，例如 `"192.168.1.100"` 或 `"mqtt.example.com"`。
    /// 不要加 `tcp://` 或 `ssl://` 前缀。
    ///
    /// 如果你的 Kotlin 端传过来的是 `"tcp://192.168.1.100"`，
    /// JNI 层会自动把 `tcp://` 前缀剥离掉。
    pub host: String,

    /// MQTT 服务器端口
    ///
    /// 标准 MQTT 端口是 1883（明文）或 8883（TLS）。
    /// 本项目不支持 TLS，所以通常是 1883。
    pub port: u16,

    /// 客户端唯一标识（Client ID）
    ///
    /// MQTT 服务器通过这个 ID 来识别每个客户端。
    /// **不同的设备必须使用不同的 Client ID**，否则后连的会把先连的踢掉。
    ///
    /// 工业设备通常使用设备的 MAC 地址作为 Client ID，
    /// 例如 `"30-3a-ba-12-0c-4e"`。
    pub client_id: String,

    /// 登录用户名
    ///
    /// 如果 MQTT 服务器开启了身份验证，需要填写用户名。
    /// 如果不需要鉴权，传空字符串 `""` 即可。
    pub username: String,

    /// 登录密码
    ///
    /// 和 username 配合使用。如果 username 为空，password 也会被忽略。
    pub password: String,

    /// 需要订阅的主题列表
    ///
    /// 连接成功后，会自动订阅这些主题。
    /// 断线重连成功后，也会自动重新订阅。
    ///
    /// MQTT 主题支持通配符：
    /// - `+` 匹配一级，例如 `"/device/+/data"` 可以匹配 `"/device/001/data"`
    /// - `#` 匹配任意级，例如 `"/device/#"` 可以匹配所有子主题
    ///
    /// 示例：`vec!["/device/host/xxx/get/#".to_string()]`
    pub topics: Vec<String>,

    /// 消息服务质量等级（Quality of Service）
    ///
    /// MQTT 协议定义了三种 QoS 等级：
    ///
    /// | QoS | 名称 | 说明 | 适用场景 |
    /// |-----|------|------|---------|
    /// | 0 | AtMostOnce | 最多送达一次，可能丢失 | 传感器数据、心跳包 |
    /// | 1 | AtLeastOnce | 至少送达一次，不会丢但可能重复 | 报警通知、状态上报 |
    /// | 2 | ExactlyOnce | 恰好送达一次，最可靠但最慢 | 金融交易、订单处理 |
    pub qos: QoS,

    /// 心跳间隔（秒）
    ///
    /// MQTT 客户端每隔这么多秒向服务器发送一次心跳包（PINGREQ），
    /// 告诉服务器"我还活着，别断我的连接"。
    ///
    /// 如果服务器在 **1.5 倍心跳时间** 内没有收到任何数据包（包括心跳），
    /// 就会认为客户端已经断开，主动关闭连接。
    ///
    /// 默认值 10 秒，适合局域网内的工业设备。
    /// 如果网络不稳定，可以适当增大（比如 30 秒或 60 秒）。
    pub keep_alive_secs: u64,

    /// 断线重连间隔（秒）
    ///
    /// 连接断开后，等待这么多秒再尝试重新连接。
    /// 本项目采用**无限重连**策略，会一直重试直到连接成功。
    ///
    /// 重连流程：
    /// 1. 检测到连接断开 → 回调 `connectionLost(cause)`
    /// 2. 等待 `reconnect_interval_secs` 秒
    /// 3. 重新创建客户端并尝试连接
    /// 4. 如果还是失败，回到第 2 步继续等待
    /// 5. 连接成功 → 回调 `connectComplete(true, serverURI)`
    ///
    /// 默认值 10 秒。
    pub reconnect_interval_secs: u64,

    /// 是否清除会话（Clean Session）
    ///
    /// - **true**（默认）：每次连接都创建全新的会话。
    ///   服务器不会保留客户端断线期间的消息。
    ///   适合实时性要求高的场景（设备状态、传感器数据）。
    ///
    /// - **false**：使用持久会话。
    ///   服务器会保留客户端断线期间的消息，重连后一次性推送。
    ///   适合不允许丢消息的场景。
    ///   注意：使用持久会话时，MQTT 服务器可能会积累大量消息，占用内存。
    pub clean_session: bool,

    /// 消息队列容量（事件循环 → 回调分发器之间的缓冲区）
    ///
    /// ## 为什么需要消息队列？
    ///
    /// MQTT 事件循环运行在 Tokio 后台线程，收到消息后需要调用 JNI 通知 Kotlin。
    /// 如果 Kotlin 回调处理很慢（比如写数据库、更新 UI），会阻塞整个事件循环，
    /// 导致后续消息无法处理、心跳包发不出去、服务器主动断开连接。
    ///
    /// 消息队列的作用是**解耦**：事件循环收到消息后立即放入队列继续处理下一条，
    /// 由独立的回调分发器 task 从队列中取出消息并调用 JNI 回调。
    ///
    /// ## 容量选择
    ///
    /// - **默认值 10000**：适合工业场景高频消息（100~1000 条/秒）
    /// - **上层可通过 connect() 参数配置**，传 ≤0 时使用默认值
    /// - 队列满时，事件循环会短暂等待（背压机制），不会丢消息
    ///
    /// ## 内存占用估算
    ///
    /// 每条消息约占 200~500 字节（topic + payload + 结构体开销），
    /// 10000 条约 2~5 MB，对 Android 设备影响很小。
    pub message_buffer_size: usize,

    /// 连接超时时间（秒）
    ///
    /// TCP 连接建立的超时时间。如果在这个时间内没有成功建立 TCP 连接，
    /// 则认为连接失败，会触发重连。
    ///
    /// 默认值 5 秒（与 rumqttc 对齐）。
    pub connection_timeout_secs: u64,
}

/// 为 MqttConfig 提供默认值
///
/// 使用 `MqttConfig::default()` 可以创建一个所有字段都是默认值的配置，
/// 然后只需要修改你关心的字段，其他保持默认即可。
///
/// ## 默认值一览
///
/// - port: 1883
/// - qos: 0（AtMostOnce，最多送达一次）
/// - keep_alive_secs: 10 秒
/// - reconnect_interval_secs: 10 秒
/// - clean_session: true（每次新建会话）
/// - message_buffer_size: 10000（消息队列容量）
/// - connection_timeout_secs: 5 秒（TCP 连接超时）
impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 1883,
            client_id: String::new(),
            username: String::new(),
            password: String::new(),
            topics: Vec::new(),
            qos: QoS::AtMostOnce,
            keep_alive_secs: 10,
            reconnect_interval_secs: 10,
            clean_session: true,
            message_buffer_size: 10000,
            connection_timeout_secs: 5,
        }
    }
}
