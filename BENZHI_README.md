# PacketReplay

PacketReplay 是一个网络流量采集与回放服务：按抓包文件或实时源采集报文，
支持分片重组、时间戳对齐、过滤、检索索引与按时间顺序回放，回放进度通过
游标持久化，重启后从断点恢复。系统自包含，报文存储与索引都在进程内。

## 构建与运行

```bash
# 离线构建（vendor 已随仓库提供）
go build -mod=vendor -o packetreplay .

# 本地启动演示模式（自动生成演示抓包并采集）
./packetreplay -demo
```

服务默认监听 `127.0.0.1:8920`，可通过环境变量覆盖：

- `PACKETREPLAY_ADDR`：监听地址
- `PACKETREPLAY_CHUNK_SIZE`：存储块大小
- `PACKETREPLAY_REPLAY_CHUNK`：回放分块大小

## 接口

- `GET /web/monitor.html`：监控页面
- `GET /api/status`：聚合状态
- `GET /api/streams`：报文流列表
- `GET /api/search?q=...`：检索报文
- `POST /api/replay`：回放当前存储
- `POST /api/replay/all`：全量回放并推进游标
- `POST /api/replay/resume`：从游标恢复回放
- `GET /api/filter`：按规则过滤预览
- `GET /api/index/snapshot?version=N`：查看索引版本快照
- `POST /api/capture/start|stop`：启停实时采集

## 容器构建

```bash
./build_benzhi_docker.sh
docker run --rm -p 8920:8920 packetreplay:local
```
