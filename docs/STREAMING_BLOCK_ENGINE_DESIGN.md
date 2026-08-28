# TGX 系统架构与技术实现规范 (Master Specification)
## TGX Architecture & Production Technical Specification: SBE Engine & Network Subsystem

---

## 1. 架构定位与系统边界

### 1.1 系统定位
**TGX** 是专为大规模 Telegram 媒体 7x24h 自动化归档、高吞吐下载与 NAS / Linux 服务器部署而设计的工业级 Go 原生流式下载引擎与守护服务。系统主要由两大解耦子系统构成：

1. **Streaming Block Engine (SBE 流式分块存储引擎)**：
   - 将网络拉流与磁盘写入物理完全解耦，消除传统顺序下载导致的带宽断流与磁盘瓶颈；
   - 实行严格的双额度内存租赁背压（`BufferLease` 96 MiB + `DirtyLease` 48 MiB），通过流控信号量杜绝内存无界膨胀；
   - 实行 Small / Large 双车道加权差额轮询调度（DRR 3:1），保障海量小文件秒级流转与大文件平稳吞吐；
   - 实行全尺寸 Attempt 绑定的侧车预分配双槽位图（`.meta`）与单写者 Checkpoint，保障高抗崩溃能力与精确断点续传；
   - 实行 Linux `RENAME_NOREPLACE` 与 `linkat` 回退链的原子提交协议，杜绝同名文件静默覆盖。
2. **Network & Proxy Provider Subsystem (网络与代理适配子系统)**：
   - 抽象统一的 `DialerProvider` 接口，提供 `DirectProvider`、`ExternalProxyProvider`（默认标准 SOCKS5/HTTP）与可选的 `EmbeddedSingBoxProvider`（进程级内置 sing-box 核心）；
   - 配合 Proxy Watchdog 实现 30 秒自动心跳探活、指标持久化与 gotd 会话连接池安全重连。

### 1.2 系统边界与运行前提
- **单机单进程独占**：下载根目录由单实例通过 `flock` 文件锁独占写入，未取得锁拒绝启动。
- **同一物理分区**：临时数据文件（`.part`）与最终下载目标目录必须位于同一文件系统，严禁跨物理分区 rename。
- **状态分离原则**：SQLite 负责全局任务生命周期与元数据持久化，逐块下载进度由同目录的 `.meta` 侧车文件独立承担。
- **零静默覆盖**：当目标路径已存在文件时，必须通过原子非覆盖重命名阻断，绝不允许覆盖已有数据。
- **数据库掉电语义**：SQLite 必须显式配置为 WAL 模式与 `PRAGMA synchronous = FULL;`，所有状态变更事务严格断言 `RowsAffected == 1`。

---

## 2. 总体架构拓扑

```mermaid
flowchart TD
    subgraph NetworkSubsystem ["网络与代理子系统 (Network Subsystem)"]
        DP["DialerProvider 统一接口"]
        DP --> P1["DirectProvider (直连网络)"]
        DP --> P2["ExternalProxyProvider (标准 SOCKS5 / HTTP - 默认首选)"]
        DP --> P3["EmbeddedSingBoxProvider (可选内置 sing-box 核心)"]
        
        P1 --> Gotd["MTProto 会话连接池 (gotd/td Client Pool)"]
        P2 --> Gotd
        P3 --> Gotd
        Watchdog["Proxy Watchdog (30s 探活 / 连接池安全重连)"] -.-> DP
    end

    subgraph SBE_Pipeline ["SBE 流式分块流水线 (Streaming Block Engine)"]
        DB[(SQLite Task DB WAL FULL)] --> Orchestrator["Orchestrator 任务编排器"]
        Orchestrator --> DRR["DRR 双车道调度器 (Small : Large = 3:1, Work-Conserving)"]
        DRR --> ChunkChan["chunkChan 分片分发队列 (容量 128)"]

        subgraph NetStage ["网络拉流阶段"]
            ChunkChan --> AcquireBuf{"申请 BufferLease (上限 96 MiB)"}
            AcquireBuf -->|获取成功| NW["64 个逻辑网络 Worker"]
            NW -->|RPC 循环分片拉流| Gotd
            NW -->|填满 2MB 块| WriteChan["writeJobChan 写入队列 (容量 64)"]
        end

        subgraph DiskStage ["磁盘持久化阶段 (单文件串行 Checkpoint)"]
            WriteChan --> AcquireDirty{"申请 DirtyLease (上限 48 MiB)"}
            AcquireDirty -->|获取成功| DW["5 个磁盘 Writer"]
            DW -->|WriteAt 写入 .part.<Attempt>| TempDisk["临时数据文件"]
            DW -->|持锁标记 WRITTEN| ReleaseBuf["释放 BufferLease"]
            ReleaseBuf --> AcquireBuf
            
            TempDisk --> CheckpointLoop["FileCoordinator 串行 CheckpointLoop"]
            CheckpointLoop -->|累积 16MB 或 2s| Fdatasync["unix.Fdatasync(dataFD)"]
            Fdatasync --> WriteMeta[".meta.<Attempt> Slot A/B 覆写"]
            WriteMeta --> MetaSync["MetaFile.Sync()"]
            MetaSync --> AdvanceDurable["推进 DurableBitmap = old union snapshot"]
            AdvanceDurable --> ReleaseDirty["释放 DirtyLease"]
        end

        subgraph CommitStage ["原子提交流程 (Atomic Commit Protocol)"]
            AdvanceDurable -->|全部分片 DURABLE| FinalizingGate["进入 FINALIZING 门锁 (排空活跃写入)"]
            FinalizingGate --> CommittingCAS["SQLite CAS: RUNNING -> COMMITTING (RowsAffected==1)"]
            CommittingCAS --> AtomicRename{"unix.Renameat2(RENAME_NOREPLACE)"}
            AtomicRename -->|返回 ENOSYS/EINVAL| LinkFallback["linkat(temp, final) + unlinkat(temp)"]
            AtomicRename -->|成功| FsyncDir["fsync 父目录描述符"]
            LinkFallback -->|成功| FsyncDir
            FsyncDir --> SuccessCAS["SQLite CAS: COMMITTING -> SUCCESS (RowsAffected==1)"]
            SuccessCAS --> RemoveMeta["删除 .meta 并再次 fsync 目录"]
        end
    end
```

---

## 3. DRR (Deficit Round Robin) 调度算法与数学模型

### 3.1 车道划分与 Quantum 定量
- **分类阈值**：`SmallFileThreshold = 10 MiB`（10,485,760 字节）。即总分块数 $\le 5$ 归入 Small Lane，$> 5$ 归入 Large Lane。
- **调度计费单位**：按**分片任务数（Chunk Task Count）**计费，每派发一个 2MB 块消耗 1 个配额单位。
- **配额设定（Quantum）**：
  - `SmallQuantum = 3`（Small 车道每轮最多发 3 个 ChunkTask）；
  - `LargeQuantum = 1`（Large 车道每轮最多发 1 个 ChunkTask）；
  - **Work-Conserving（功耗守恒）特性**：当任一车道为空时，当前轮次配额不作废，活跃车道自动占满 100% 调度带宽；一旦空闲车道有新任务进入，下一轮立即恢复 3:1 比例。

### 3.2 大文件会话池与自适应并发公式
- **大文件活跃会话池**：固定单一默认值 **`ActiveLargeFiles = 12`**。超过 12 个大文件时，多余大文件在 SQLite 队列中排队。
- **单个大文件在途分片上限公式（FileInflightCap）**：
  $$\text{FileInflightCap} = \min\left(16, \max\left(4, \left\lfloor \frac{64}{\text{activeLargeCount}} \right\rfloor\right)\right)$$
  - 当系统中仅有 1 个大文件激活时：$\min(16, \max(4, 64)) = 16$ 个并发在途分片（跑满单文件极速）；
  - 当系统中同时有 4 个大文件激活时：每个大文件最多 $64 / 4 = 16$ 个并发在途分片；
  - 当系统中跑满 12 个大文件时：每个大文件最多 $64 / 12 = 5$ 个并发在途分片。

### 3.3 延迟任务与 FloodWait 退避
- 遇到 `FloodWait` 或网络退避的分片，从活跃调度队列移除，放入基于最小堆的 `TimerHeap` 中；
- 到达 `notBefore` 时间后，调度器自动将其弹出并直接插回所属车道的**队首（Head of Queue）**优先派发。

---

## 4. 网络拉流与 RPC 分片组装

### 4.1 存储分块与 RPC 实际返回循环
- **存储块尺寸**：标准块大小 `BlockSize = 2 MiB`（2,097,152 字节），尾块按实际文件剩余字节对齐。
- **动态循环填充规则**：网络 Worker 严禁假设“两次 RPC 必然填满 2 MiB”，必须按实际返回长度累加，以处理短读、尾块和网络截断：
  ```text
  while filled < ChunkTask.Length:
      partLength = min(1 MiB, ChunkTask.Length - filled)
      reqOffset = ChunkTask.Offset + filled
      resBytes = client.UploadGetFile(reqOffset, partLength)
      
      if len(resBytes) == 0:
          return EOFError / NetworkInterrupt
          
      copy(leasedBuffer[filled:], resBytes)
      filled += len(resBytes)

  // 严格验证：只有当 filled == ChunkTask.Length 时才生成 WriteJob
  ```

### 4.2 SingleFlight 文件引用刷新与异常自愈
- **`FILE_REFERENCE_EXPIRED`**：使用 `singleflight.Group` 按照 `FileKey` 对刷新请求进行收敛，配合单调递增的 `sourceGeneration`，确保同一文件的并发 Worker 共享单次刷新结果，避免风暴式重复请求。
- **`DC_MIGRATE_X` 与 CDN Redirect**：自动触发连接池对目标 DC 的长连接握手，迁移拉流通道。

---

## 5. 网络与代理适配子系统 (DialerProvider)

### 5.1 统一接口设计
```go
type DialerProvider interface {
    GetDialer(ctx context.Context, dcID int) (proxy.ContextDialer, error)
    ReportFailure(dcID int, err error)
    Close() error
}
```

### 5.2 适配器实现与构建规则
1. **`DirectProvider`**：标准 `net.Dialer` 直连。
2. **`ExternalProxyProvider`（生产默认模式）**：
   - 封装标准 SOCKS5 (`golang.org/x/net/proxy`) 与 HTTP Connect 代理；
   - 零额外依赖，跨平台 100% 纯 Go 编译。
3. **`EmbeddedSingBoxProvider`（可选内置插件）**：
   - 采用条件编译标签（`//go:build with_singbox`）；
   - 严格遵循官方 API 链：
     ```text
     include.Context(ctx)
     -> box.New(opts)
     -> instance.Start()
     -> instance.Outbound().Default()
     -> 转换为 dcs.DialFunc
     -> dcs.Plain(PlainOptions{Dial: dialFunc})
     -> 注入 gotd telegram.Options.Resolver
     ```
   - 支持 VLESS (Reality)、Hysteria2、TUIC、Shadowsocks、Trojan 等协议（QUIC 协议族依赖 `with_quic` 标签）。

### 5.3 Proxy Watchdog 故障转移与长连接迁移
- **心跳探活**：后台协程每 30 秒向目标 Telegram DC 接入点发起轻量握手探活；
- **连接池协调迁移**：当检测到节点故障触发 Failover 时，不仅切换默认 Outbound，**必须同步协调 gotd ClientPool 关闭当前不可用的旧 TCP 长连接**，将受影响的在途 `ChunkTask` 安全释放 `BufferLease` 并退回调度队列重新派发。

---

## 6. 有界内存与双额度租赁模型

```mermaid
stateDiagram-v2
    [*] --> IdleLeasePool
    IdleLeasePool --> BufferLeased: 网络 Worker 获取 BufferLease (96 MiB 额度池)
    BufferLeased --> Downloading: 循环填充 2MB 数据
    Downloading --> QueuedInWriteChan: 投递至 writeJobChan
    QueuedInWriteChan --> DirtyLeased: 磁盘 Writer 先获取 DirtyLease(Length)
    DirtyLeased --> WrittenToDisk: 执行 WriteAt 写入 .part
    WrittenToDisk --> BufferLeasedReleased: 持锁标记 WRITTEN，释放 BufferLease
    BufferLeasedReleased --> IdleLeasePool
    WrittenToDisk --> SyncedToDisk: CheckpointLoop 执行 fdatasync + meta 覆写
    SyncedToDisk --> DirtyLeasedReleased: 持锁标记 DURABLE，释放 DirtyLease (48 MiB 额度池)
    DirtyLeasedReleased --> [*]
```

### 6.1 租赁额度与背压流控
1. **`BufferBudget` (96 MiB)**：
   - 限制用户态数据缓冲总量，对应最多流通 $96 / 2 = 48$ 个满载 2MB 块；
   - 采用带 Context 取消与超时支持的加权信号量（Weighted Semaphore）。当 48 个 Lease 被占满时，多余 Worker 协程阻塞排队，产生精确网络背压。
2. **`DirtyBudget` (48 MiB)**：
   - 限制已调用 `WriteAt` 但尚未调用 `fdatasync` 的未落盘脏数据总量；
   - **严格时序**：磁盘 Writer **必须在执行 `WriteAt` 前** 成功申请 `DirtyLease(Length)`；写入完成后持锁标记 `WRITTEN` 并立即释放 `BufferLease`；落盘完成后释放 `DirtyLease`。

---

## 7. 磁盘写入、.meta 侧车文件与单写者 Checkpoint

### 7.1 MetaHeader 固定 128 字节显式二进制布局
为杜绝结构体内存对齐与尺寸覆盖错误，`.meta` 文件 Header 采用大端序（`binary.BigEndian`）固定为 **128 字节** 显式布局：

```text
+-----------------------------------------------------------------------+
| MetaHeader (Fixed 128 Bytes, BigEndian)                               |
+--------+--------+-------------------+---------------------------------+
| Offset | Length | Field             | Description                     |
+--------+--------+-------------------+---------------------------------+
| 0      | 4B     | Magic             | 固定 ASCII 字符 "SBM1"          |
| 4      | 4B     | Version           | 协议版本 (uint32 = 1)           |
| 8      | 32B    | FileKeyHash       | SHA256(FileKey)                 |
| 40     | 8B     | SourceFingerprint | SHA256(PeerID|MsgID|Hash)[:8]   |
| 48     | 16B    | AttemptID         | 当前 Attempt 的 UUID 字节       |
| 64     | 8B     | TotalSize         | 文件总字节数 (int64)            |
| 72     | 4B     | BlockSize         | 存储块大小 (uint32 = 2097152)   |
| 76     | 4B     | TotalBlocks       | 总分块数 (uint32)               |
| 80     | 4B     | HeaderCRC         | 前序 [0..79] 字节的 CRC32 校验和|
| 84     | 44B    | Reserved          | 预留与对齐填充 (填 0)           |
+--------+--------+-------------------+---------------------------------+
```

### 7.2 SlotHeader (32 字节) 与物理偏移对齐
每个快照 Slot 包含 32 字节的 `SlotHeader` 与紧随其后的位图字节数组：
```text
+-----------------------------------------------------------------------+
| SlotHeader (Fixed 32 Bytes, BigEndian)                                |
+--------+--------+-------------------+---------------------------------+
| Offset | Length | Field             | Description                     |
+--------+--------+-------------------+---------------------------------+
| 0      | 8B     | Generation        | 递增代次 (uint64)               |
| 8      | 4B     | Flags             | 0=INVALID, 1=VALID, 2=COMPLETE  |
| 12     | 4B     | BitmapLengthBytes | 位图有效字节数 (uint32)         |
| 16     | 4B     | SlotDataCRC       | [0..15] + BitmapBytes 的 CRC32  |
| 20     | 12B    | Reserved          | 预留与对齐填充 (填 0)           |
+--------+--------+-------------------+---------------------------------+
```

* **精确物理偏移计算**：
  $$\text{SlotSize} = 32\text{B} + \lceil \text{TotalBlocks} / 8 \rceil \text{B}$$
  - `Slot A` 起始物理偏移：固定为 **128**
  - `Slot B` 起始物理偏移：固定为 **$128 + \text{SlotSize}$**
  - `TotalMetaSize` 文件总尺寸：固定为 **$128 + 2 \times \text{SlotSize}$**
* **命名规范**：临时数据文件为 `<TargetDir>/<FileName>.part.<AttemptID>`，侧车位图为 `<TargetDir>/<FileName>.meta.<AttemptID>`。所有文件（包括单块小文件）在初始化时均一次性预分配完整尺寸并 `Sync()`。当且仅当两个槽位 CRC 均损坏时才判定元数据损坏并完整重置。

### 7.3 单写者 CheckpointLoop（互斥锁与 Durable Union）
每个 `FileCoordinator` 拥有唯一的串行 `CheckpointLoop`，彻底消除并发竞态：

```mermaid
flowchart TD
    Trigger["触发 Checkpoint (累积 16MB 脏数据 或 间隔满 2s)"] --> Lock1["1. FileCoordinator.Lock()"]
    Lock1 --> Snap["2. 快照 writtenSnapshot = writtenBitmap.Clone()"]
    Snap --> Unlock1["3. FileCoordinator.Unlock()"]
    Unlock1 --> Fdata["4. unix.Fdatasync(int(dataFile.Fd()))"]
    Fdata --> UnionCalc["5. nextDurable = oldDurable.Union(writtenSnapshot)"]
    UnionCalc --> WriteSlot["6. MetaFile.WriteAt 写入 Slot A (偏移 128) 或 Slot B 并计算 CRC"]
    WriteSlot --> MetaSync["7. MetaFile.Sync()"]
    MetaSync --> Lock2["8. FileCoordinator.Lock()"]
    Lock2 --> Advance["9. durableBitmap = nextDurable, 块状态置为 DURABLE"]
    Advance --> Unlock2["10. FileCoordinator.Unlock()"]
    Unlock2 --> RelDirty["11. 释放本批次对应的 DirtyLease 额度"]
```

---

## 8. 原子提交协议与生命周期门

### 8.1 Finalize 门禁与提交流程
```text
1. 调度器检测到所有分块均已分配完毕：
   a. 任务状态原子转移为 FINALIZING（关闭新分块申请门，BeginChunk 立即拒绝）
   b. 等待所有在途任务完全清空 (activeWorkers == 0 && activeWrites == 0)
   c. 触发最后一次 CheckpointLoop，确保 DurableBitmap 达到 100%
2. metaFile 写入 Flags = COMPLETE 的 Slot 并 MetaFile.Sync()
3. 关闭 dataFile 与 metaFile 描述符句柄
4. SQLite 乐观 CAS 事务：
   UPDATE tasks SET state = 'COMMITTING' 
   WHERE file_key = ? AND attempt_id = ? AND state = 'RUNNING';
   (严格校验 RowsAffected == 1，若为 0 终止并重试)
5. 执行非覆盖原子提交：
   a. 主路径：unix.Renameat2(parentDirFD, tempPartName, parentDirFD, finalName, unix.RENAME_NOREPLACE)
   b. 若返回 ENOSYS / EINVAL / EOPNOTSUPP，进入安全回退链：
      linkat(parentDirFD, tempPartName, parentDirFD, finalName, 0)
      -> unlinkat(parentDirFD, tempPartName, 0)
   c. 若仍不支持或返回 EEXIST (目标已存在)：明确报错阻断，严禁降级为 os.Rename
6. fsync 父目录文件描述符
7. SQLite 乐观 CAS 事务：
   UPDATE tasks SET state = 'SUCCESS' 
   WHERE file_key = ? AND attempt_id = ? AND state = 'COMMITTING';
   (严格校验 RowsAffected == 1)
8. 删除 .meta.<AttemptID> 侧车文件并再次 fsync 父目录
```

---

## 9. 崩溃对账与启动恢复矩阵

服务拉起或异常重启后，恢复器根据 SQLite 当前 Attempt 与文件系统物理状态进行严格对账：

| SQLite 状态 | 最终文件 | `.part.<Attempt>` | `.meta.<Attempt>` | 判定结果与自愈动作 |
| :--- | :--- | :--- | :--- | :--- |
| `SUCCESS` | 存在 | 不存在 | 不存在 | **正常完成**：直接跳过 |
| `SUCCESS` | 缺失 | 任意 | 任意 | **文件物理丢失**：记录严重告警，重置任务为 `QUEUED` 重新下载 |
| `SUCCESS` | 存在 | 存在 | 任意 | **提交后清理遗漏**：物理清理残余 `.part` 与 `.meta`，fsync 目录 |
| `COMMITTING` | 存在 | 不存在 | 存在有效 COMPLETE | **提交流程崩溃**：执行父目录 `fsync`，补交 SQLite 推进为 `SUCCESS`，清理残余 `.meta` |
| `COMMITTING` | 存在且与 part inode 相同 | 存在 | 任意 | **linkat 成功但 unlink 崩溃**：执行 `unlinkat(part)`，`fsync` 目录，推进 `SUCCESS` |
| `COMMITTING` | 存在但与 part inode 不同 | 存在 | 任意 | **路径严重冲突**：拒绝成功，记录 `PATH_CONFLICT` 错误并告警阻断 |
| `COMMITTING` | 不存在 | 存在 | 校验完整 (COMPLETE) | **提交前崩溃**：重新执行原子重命名与目录 `fsync`，推进 SQLite 为 `SUCCESS` |
| `COMMITTING` | 不存在 | 不存在 | 任意 | **异常提交中断**：标记任务损坏，重置为 `QUEUED` |
| `RUNNING` | 不存在 | 存在 | 有效 COMPLETE + 位图全满 | **提交前掉电**：直接进入 `FINALIZING`，跳过拉流，执行原子重命名与目录 `fsync`，推进为 `SUCCESS` |
| `RUNNING` | 不存在 | 存在 | 双槽 CRC 有效 (未满) | **断点续传**：加载最新合法 Generation 槽位的 `DurableBitmap`，仅下载未完成分块 |
| `RUNNING` | 不存在 | 存在 | 双槽损坏/不存在 | **位图损坏**：重置 `.meta` 和 `.part`，从第 0 块重新拉流 |
| `RUNNING` | 存在 | 任意 | 任意 | **外部同名文件已存在**：记录 `FILE_ALREADY_EXISTS`，拒绝覆盖已有文件 |
| 任意 | 任意 | 存在历史 Attempt | 存在历史 Attempt | **历史孤儿清理**：物理清除所有不属于当前 AttemptID 的残留文件 |

---

## 10. 优雅停机协议 (Orderly Shutdown / Drain Protocol)

```mermaid
flowchart TD
    Sig["接收到 SIGINT / SIGTERM / ctx.Done()"] --> Drain["1. Orchestrator 置全局状态为 DRAINING (停止准入新任务)"]
    Drain --> WaitSched["2. 等待所有调度器与分片生成协程退出"]
    WaitSched --> CloseChunk["3. 调度器 (唯一 Owner) 关闭 chunkChan"]
    CloseChunk --> WaitNet["4. 等待 64 个网络 Worker 退出 (未完成任务退还 BufferLease)"]
    WaitNet --> CloseWrite["5. 网络层关闭 writeJobChan"]
    CloseWrite --> WaitDisk["6. 等待 5 个磁盘 Writer 处理完在途任务退出"]
    WaitDisk --> FlushMeta["7. 各 Coordinator 触发终态 Checkpoint，关闭 data/meta 文件句柄"]
    FlushMeta --> BestEffortWAL["8. 带 5s 超时执行 PRAGMA wal_checkpoint(TRUNCATE) (Best-effort)"]
    BestEffortWAL --> CloseDB["9. 安全关闭 SQLite 数据库连接"]
    CloseDB --> Exit["10. 进程安全退出"]
```

---

## 11. 故障注入与自动化验收测试矩阵

在发布前必须通过以下场景的自动化故障注入与断言：

| 故障场景 | 注入方式 | 预期断言与系统行为 |
| :--- | :--- | :--- |
| **Slot 扇区撕裂** | 人为篡改 Slot A 的 CRC32 校验和 | 系统自动降级读取 Slot B，校验通过后恢复断点续传 |
| **双槽全部损坏** | 同时篡改 Slot A 与 Slot B 校验和 | 判定位图不可信，安全重置 `.meta` 和 `.part`，从 0 开始拉流 |
| **WriteAt 阶段强杀** | 在 Writer 刚写完 `.part` 尚未 Checkpoint 时 `kill -9` | 重启后未 Checkpoint 的块自动重新下载，数据无撕裂 |
| **Checkpoint 阶段强杀** | 在 `unix.Fdatasync` 与 `meta.Sync` 之间 `kill -9` | 重启后读取上一代合法 Slot，继续安全下载未持久化分块 |
| **Renameat2 前掉电** | 在 COMPLETE 写入后、重命名前 `kill -9` | 重启后依据 `RUNNING + COMPLETE` 状态秒级重试原子提交 |
| **linkat 成功后掉电** | 模拟 `linkat` 成功但 `unlinkat` 未完成前崩溃 | 重启后检测到 final 与 part 相同 inode，安全执行 `unlinkat` 并标记 `SUCCESS` |
| **同名目标已存在** | 在 `Renameat2` 前外部写入同名目标文件 | `Renameat2` 返回 `EEXIST`，系统明确报错阻断，严禁静默覆盖 |
| **网络 RPC 短读/截断** | MTProto 仅返回 512KB 数据后 EOF | Worker 循环继续发起 RPC 补齐 2MB，未满 2MB 绝不投递 WriteJob |
| **磁盘已满 (ENOSPC)** | 磁盘剩余空间归零触发写入失败 | Writer 暂停消费，释放对应 Lease，向调度器发出背压告警 |

---

## 12. 核心生产参数配置表

| 参数项 | 生产默认值 | 设计依据与说明 |
| :--- | :---:| :--- |
| **StorageBlockSize** | **2 MiB** | 存储与断点续传单位，末块按实际长度裁剪 |
| **RPCPartSize** | **1 MiB** | Telegram MTProto 标准单次请求对齐尺寸 |
| **NetworkWorkers** | **64** | 逻辑网络拉流协程数（跨文件复用 MTProto 长期会话） |
| **DiskWorkers** | **5** | 全局磁盘 Writer 协程数，按物理卷限流 |
| **BufferBudget** | **96 MiB** | 用户态数据缓冲区硬上限（最多流通 48 个 2MB 满载块） |
| **DirtyBudget** | **48 MiB** | 未刷盘脏数据硬上限，达到即暂停写盘并强制 Checkpoint |
| **ActiveLargeFiles** | **12** | 全局同时激活的大文件会话池上限（单一固定默认值） |
| **Small/Large DRR** | **3 : 1** | 小文件与大文件加权调度比（Work-Conserving 特性） |
| **CheckpointBytes** | **16 MiB** | 触发单文件 `fdatasync` 的累计脏数据量阈值 |
| **CheckpointInterval** | **2 秒** | 触发单文件 `fdatasync` 的最大时间延迟 |
| **ProxyWatchdogInterval**| **30 秒** | 代理心跳探活与故障转移周期 |
