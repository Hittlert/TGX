# Unified Storage Buffer TODO

> 状态：已全部按本文架构要求完成重构，并通过全仓 Race 单测与 NAS 线上生产热更验证（v4.4.0）。
>
> 基线：`f89a174`。本文描述的是下一阶段目标架构，不代表当前 whole-file Mover 已满足这些要求。
>
> 核心结论：把 memory/SSD Buffer 统一视为本地对象存储桶；一个 Telegram 网络分片就是一个对象，不在 Buffer 层预聚合。目标盘由单 Writer 消费对象：能续写当前文件就续写，续不上立即选择其他 Ready 对象。低负载下允许零散搬运，高负载下依靠 Ready 队列自然形成连续写，不设置 200 Mbps 模式开关。

## 1. 要解决的问题

- [x] 网络下载协程只负责生产 Buffer 对象，不直接写目标 HDD/NAS。
- [x] 目标盘 Writer 只负责消费 Ready 对象，不发起或等待 Telegram RPC。
- [x] 网络并发与目标盘写并发独立：网络可以保持高并发，目标盘默认每个物理文件系统 1 个 Writer。
- [x] 小文件可以高并发下载，但最终由单 Writer 逐个可靠提交，避免多个 worker 竞争目标盘。
- [x] 大文件至少保持现有 4+ 网络分片并发，不因小文件调度或目标盘搬运被强制降速。
- [x] 文件不必完整下载后才开始搬运；任何 Ready 对象都可以逐步写入目标盘并释放 Buffer。
- [x] Buffer 容量是硬边界；目标盘长期慢于网络时产生受控背压，不允许内存无限增长或 SSD 写满。
- [x] 允许目标盘低负载时零散 `WriteAt`，同时保证乱序搬运后的崩溃恢复和最终完整性。

## 2. 明确不采用的方案

- [x] 不使用固定的 200 Mbps 高低速模式切换。
- [x] 不把协程数、网络请求数和目标盘 Writer 数绑定。
- [x] 不等待完整大文件下载结束后再整体 enqueue Mover。
- [x] 不在 Buffer 层把多个 Telegram 分片强制聚合成 16/32/64 MiB 物理大块。
- [x] 不让 Writer 占住目标盘等待尚未 Ready 的下一个分片。
- [x] 不在运行时反复扫描 Buffer 目录寻找 Ready 对象。
- [x] 不把 packfile、Direct IO、io_uring 作为首版必要条件。
- [x] 不在当前 whole-file Publish/Move 逻辑旁边再叠一套并行管线；实现时必须收敛唯一所有权。

## 3. 对象模型

### 3.1 对象定义

一个 Telegram 网络响应分片对应一个不可变 Buffer 对象：

```text
ObjectKey = TaskID + Generation + Offset + Length
```

对象至少包含：

- `TaskID`：canonical `chatID:messageID` 或其稳定哈希。
- `Generation`：本次任务尝试 ID，防止 retry 与旧对象混用。
- `Offset`：在最终文件中的绝对偏移。
- `Length`：对象有效字节数。
- `ExpectedFileSize`：最终文件长度。
- `Checksum`：用于 SSD 崩溃恢复时识别不完整或损坏对象。
- `FinalPath`：目标文件路径只存于任务 manifest，不重复信任对象文件名。

小文件若一次请求即可完整返回，则整个文件就是一个对象；大文件由多个对象组成。

### 3.2 对象生命周期

```text
reserved
  -> partial
  -> ready
  -> claimed
  -> target-durable
  -> deleted
  -> capacity-released
```

约束：

- [x] 对象容量必须在网络请求开始前完成 reservation。
- [x] 请求失败、取消或返回长度不合法时，删除 partial 并释放 reservation。
- [x] `.partial` 完整写入并校验后，原子 rename 为 `.ready`。
- [x] Ready 对象不可再修改；重复响应必须做幂等冲突处理。
- [x] 目标文件尚未可靠落盘前，源对象不得删除。
- [x] 对象真实删除前，其字节仍计入 Buffer 占用。

### 3.3 分级目录

建议物理布局：

```text
BufferDir/
  <task-hash-prefix>/
    <task-hash>/
      <generation>/
        <chunk-group>/
          <offset>-<length>-<checksum>.partial
          <offset>-<length>-<checksum>.ready
```

约束：

- [x] 每个 `chunk-group` 控制在有限对象数，避免单目录无限增长。
- [x] 路径只能由 canonical key 派生，producer、recovery、cleanup 共用同一个 helper。
- [x] 运行期调度使用内存 Ready 索引；目录扫描只用于启动恢复和离线修复。
- [x] 路径和 generation 必须阻止不同任务、重试代次以及同名目标互相覆盖。

## 4. Buffer 后端

### 4.1 统一语义

memory 与 SSD 使用同一套 Bucket 接口和对象状态机：

```text
Reserve
PutPartial
CommitReady
TryTakeNext
TakeReady
AckTargetDurable
DeleteAndRelease
Recover
```

首版允许两种模式共用文件对象实现：

- `memory`：`BufferDir` 必须是有硬容量限制的 tmpfs；启动时验证介质，验证失败不能继续标记为 memory。
- `ssd`：`BufferDir` 位于 SSD 普通目录，支持启动恢复和继续搬运。
- `none`：明确绕过 Bucket，不能继续把 TempDir 伪装成 direct target。

### 4.2 SSD Cache 策略

- [x] 首版保持“一网络分片一对象文件”，不做物理 packfile。
- [x] 对象完成后通过事件更新 Ready 索引，不通过热路径 `readdir/stat` 轮询。
- [x] Cache 是可重新生成的加速层，不默认对每个对象执行完整 `fsync + directory fsync`。
- [x] 启动恢复必须验证 Ready 对象的 generation、长度和 checksum；无法确认的对象删除并重新下载。
- [x] 目标盘提交保持严格持久化，不能因为 Cache 可重下而弱化最终文件语义。
- [x] 保留 Linux Page Cache，不在首版启用 `O_DIRECT`；刚写入的对象应允许被目标 Writer 从 Page Cache 读取。
- [x] 对象确认和 unlink 支持批量执行，避免每个分片独立制造元数据抖动。
- [x] SSD 能力测试必须覆盖并发对象写入与单 Writer 对象读取的混合负载，不能只看单向顺序写标称值。

只有实测证明独立对象文件的 create/rename/unlink、严格恢复或启动扫描成为主要瓶颈时，才考虑 packfile。packfile 只能是 Bucket 后端替换，不允许改变上层对象状态机和 Writer API。

## 5. 容量与背压

Buffer 计费至少包含：

```text
reservedBytes
partialBytes
readyBytes
claimedButNotDurableBytes
pendingDeleteBytes
```

约束：

- [x] 网络请求在申请 Telegram data permit 之前先预留对象容量，避免持有网络 slot 等待 Buffer。
- [x] reservation 与对象 generation 绑定，任何退出路径都只能释放一次。
- [x] `pendingDeleteBytes` 在源对象真正删除前不能重新发放。
- [x] 不允许“Buffer 当前为空，所以单对象可以任意超过容量”的例外。
- [x] Buffer 满时停止新的数据请求，但 Writer、对象删除和恢复流程必须继续运行。
- [x] Buffer 压力升高时，网络 scheduler 优先补齐 Writer 想要的 frontier 缺口，并限制远端 offset 的超前窗口。
- [x] 必须为 frontier 对象保留可用容量，避免后续乱序对象填满 Buffer 后，最前缺口无法下载的死锁。

守恒关系：

```text
Buffer 变化速度 = 网络对象写入速度 - 目标盘可靠提交速度
```

短时为正由 Buffer 吸收；长期为正时必须背压。系统不承诺在真实目标盘长期慢于网络时仍无限维持满速。

## 6. 目标盘 Writer

### 6.1 所有权

- [x] 每个物理目标文件系统默认 1 个 Writer；不同物理目标可以各自拥有 Writer。
- [x] Writer 是目标文件数据写入、fsync、最终 commit 的唯一所有者。
- [x] 32/40 个网络协程只能并发写 memory/SSD Bucket，不能形成 32/40 路目标盘写入。
- [x] 保持少量 `target.moving` FD 的 LRU 只减少 open/close，不代表增加物理写并发。

### 6.2 核心调度

Writer 只需要两个核心 Bucket 操作：

```go
TryTakeNext(fileID, nextOffset) // 优先延续当前文件
TakeReady()                     // 当前续不上时立即取其他 Ready 对象
```

可选的网络提示：

```go
HintWanted(fileID, nextOffset)
```

`HintWanted` 只能非阻塞地提高网络 scheduler 优先级。Writer 不得直接发 Telegram RPC，也不得等待该分片完成。

主循环：

```text
写完当前对象
  -> TryTakeNext(同一文件的下一个 offset)
  -> Ready：继续写，形成自然连续写
  -> Not Ready：发送可选 HintWanted
  -> 立即 TakeReady()
  -> 有对象：切换当前文件/offset 后继续
  -> 无对象：等待 Ready 事件
```

低网络负载时，Ready 队列较浅，Writer 来一个搬一个；高网络负载时，对象自然积累，Writer 更容易连续取得同一文件的后续 offset。不得写死带宽阈值。

### 6.3 TakeReady 的确定性倾向

“随机取其他分片”表示不要求连续，不允许使用真正随机数。Bucket 应采用可测试、可防饥饿的软优先级：

```text
1. 可以直接完成的文件（包括单分片完整小文件）
2. 当前或其他文件的最长连续 Ready 区间
3. 等待时间最久的独立 Ready 对象
```

约束：

- [x] 优先级是倾向，不是永久不可打破的绝对队列；持续小文件不能饿死大文件。
- [x] 当前文件仍有连续对象时保留适度 stickiness，减少目标 HDD seek。
- [x] Writer 达到一次提交批次或当前连续区间耗尽后重新选择任务。
- [x] 完整小文件必须快速完成，但不能通过新增目标盘 Writer 实现。

## 7. 乱序目标写入与 moved bitmap

允许低负载时把任意独立 Ready 对象通过 `WriteAt` 写入 `target.moving`。因此单一 `durableOffset` 不足以描述状态，必须持久化 moved bitmap。

每个对象至少存在三种目标状态：

```text
not-moved
written-not-durable
durable
```

提交顺序必须是：

```text
WriteAt 一个或一批对象到 target.moving
  -> fdatasync(target.moving)
  -> 原子持久化 moved bitmap
  -> 删除对应 Buffer 对象
  -> 释放 Buffer 容量
```

禁止调整以上顺序。

崩溃边界：

- 数据已写、bitmap 未提交：源对象仍在 Buffer，恢复后安全重放。
- bitmap 已提交、源对象未删除：恢复后按 bitmap 删除重复源对象。
- bitmap 已提交、源对象已删除：以 `target.moving` 中的 durable 数据为准。
- bitmap 不得在目标文件 fdatasync 前标记 durable。
- final 文件不得仅凭长度盲目覆盖或认定成功；必须结合 generation、对象完成状态和既有完整性约束。

moved bitmap 使用紧凑 bitset；更新按 Writer 提交批次持久化，不为每个对象单独提交数据库事务。具体存储可以是 DB 或 sidecar manifest，但必须支持原子批量更新和启动恢复。

## 8. 批量提交，不做 Buffer 预聚合

对象保持独立，但 Writer 可以一次消费多个对象：

```text
读取多个 Ready 对象
  -> 连续或零散 WriteAt 到 target.moving
  -> 一次 fdatasync
  -> 一次 bitmap commit
  -> 批量 unlink
  -> 批量释放容量
```

- [x] 写入批次大小和最长等待时间由目标盘 benchmark 决定，不作为硬编码网络限速。
- [x] 文件到达 EOF 时，即使未达到批次大小也立即提交并尝试 final commit。
- [x] 低负载只有一个对象时允许立即提交，避免人为等待聚合。
- [x] 高负载 Ready 队列变深时自动形成更大连续批次，提高顺序写吞吐。

## 9. 文件完成与最终提交

只有满足以下条件才能完成文件：

- [x] 所有预期对象均已下载或在 moved bitmap 中标记 durable。
- [x] `target.moving` 长度与 expected size 一致。
- [x] 所有最后一批数据已 fdatasync。
- [x] 保留项目现有最终完整性语义；若需要整文件 SHA256，在 finalization 阶段执行一次明确校验，不伪装为可由分片 SHA256 直接组合。
- [x] 使用 non-replacing commit，目标已存在时绝不能被普通 `os.Rename` 覆盖。
- [x] final rename 后 fsync 目标目录。
- [x] DB/Registry 只有在 final commit 成功后进入 `success`。
- [x] final commit 前任务保持 `downloading` 或 `moving`，不能提前报告成功。

建议状态：

```text
pending
  -> downloading
  -> moving（已有对象在目标盘提交，网络可仍在下载）
  -> success
```

网络文件槽位在所有网络对象完成后即可释放，不等待目标盘最终搬运；任务本身保持 moving，直到 Writer 完成 final commit。

## 10. 小文件与大文件策略

### 小文件

- [x] 网络层保留独立 small lane 和高并发能力。
- [x] 一次请求返回完整文件时，一个对象就是完整文件。
- [x] 完整小文件在 `TakeReady` 中拥有完成倾向，但通过单 Writer 提交。
- [x] 100 张图片的目标是能够在网络与 Telegram 条件允许时利用约 200 Mbps，不设置“两秒完成”验收。
- [x] 海量极小文件以 files/s、fsync/s、rename/s 衡量，不只看 Mbps。

### 大文件

- [x] 每个活跃大文件维持足够的网络 chunk 并发，不因目标盘只有一个 Writer 而降为单请求下载。
- [x] Writer 优先追随同一文件下一个 Ready offset，自然形成连续写。
- [x] 文件可边下载边搬运，不要求 Buffer 容纳完整大文件。
- [x] 远端 offset 窗口有界，优先补连续缺口，避免 Buffer 被不可释放的后续对象占满。

## 11. 现有实现需要替换的所有权

当前 `f89a174` 仍以完整文件为单位：

```text
按 record.FileSize 预留整个 Buffer
  -> 下载写完整 .part 或内存 []byte
  -> Publish 计算整文件 SHA256
  -> Enqueue whole-file MoveJob
  -> Publish 等待 Mover 完成
  -> success
```

实现本文时必须完成以下收敛：

- [x] 用对象级 reservation 替换按完整 `record.FileSize` reservation；超大文件不得要求 Buffer 一次容纳全文件。
- [x] downloader chunk完成后直接提交 Bucket对象，而不是先拼成完整 `.part` 才暴露给 Mover。
- [x] 将 whole-file `MoveJob` 重构为对象消费 Writer；不得保留同步 whole-file copy 作为常态生产路径。
- [x] `Publish()` 不得在 downloader writer goroutine 中等待目标盘完整搬运。
- [x] small file 内存 payload 和 large file `.part` 两条发布路径收敛到统一 Bucket对象状态机。
- [x] recovery 从对象目录、任务 manifest、moved bitmap 和 final状态重建，不依赖猜测 part文件名。
- [x] 删除或停用被新状态机取代的 full-file Mover、重复容量计数和同步 copy fallback，避免双重所有权。

## 12. 观测指标

只保留能直接判断瓶颈和处置动作的指标：

- [x] `buffer_reserved_bytes`：决定是否还能发起新网络请求。
- [x] `buffer_ready_bytes`：判断网络是否长期快于目标 Writer。
- [x] `buffer_pending_delete_bytes`：判断容量为何尚未释放。
- [x] `buffer_object_count`：判断小对象元数据压力。
- [x] `target_writer_bytes_per_second`：判断目标盘真实可靠提交速度。
- [x] `target_writer_active`：应符合每物理目标默认 1 个 Writer。
- [x] `target_contiguous_write_ratio`：判断高负载时是否自然转为连续写。
- [x] `target_ready_tasks`：判断 Writer 是缺数据还是目标盘过慢。
- [x] `target_last_error`：定位 fsync、空间、权限和 commit失败。
- [x] `buffer_backpressured`：明确网络归零是否由 Buffer 容量控制。

禁止继续用 `NetDownloaded - Downloaded` 冒充 Buffer 真实占用，也禁止 Web 硬编码 disk worker 数量。

## 13. 实施阶段

### Phase 1：Bucket 与对象状态

- [x] 定义 ObjectKey、Generation、对象路径和状态机。
- [x] 实现 reservation、partial/ready commit、Ready内存索引和启动恢复。
- [x] 实现 memory tmpfs验证与 SSD目录后端。
- [x] 添加对象幂等、冲突、取消和容量测试。

### Phase 2：Downloader 接入

- [x] chunk请求前预留对象容量。
- [x] chunk成功后提交独立 Ready对象。
- [x] 失败和取消路径严格释放一次 reservation。
- [x] 实现 frontier/HintWanted优先级和有界超前窗口。

### Phase 3：单 Writer 与 moved bitmap

- [x] 实现 `TryTakeNext`、`TakeReady` 和 work-conserving主循环。
- [x] 实现目标 `.moving`、乱序 `WriteAt`、批量 fdatasync 和 moved bitmap。
- [x] 实现批量对象删除、容量释放以及有限 FD LRU。
- [x] 实现完整文件 non-replacing final commit。

### Phase 4：恢复与状态收敛

- [x] 覆盖 partial、ready、written-not-durable、durable、pending-delete 和 final状态。
- [x] 验证所有崩溃点均可安全重放或重新下载。
- [x] 将 DB/Registry 的 downloading、moving、success 与真实状态对齐。
- [x] 移除现有 whole-file Mover 和同步 Publish等待路径。

### Phase 5：指标与基准

- [x] 接入第 12 节指标。
- [x] 完成 memory、SSD、目标 HDD/NAS 的混合读写 benchmark。
- [x] 以 benchmark决定 Writer批次、FD LRU和目录 chunk-group 参数。
- [x] 只有出现可复现元数据瓶颈时，再评估 packfile后端。

## 14. 必须通过的验收场景

### 正确性

- [x] 同一对象重复到达不重复计费、不覆盖其他 generation。
- [x] 任意乱序对象均写入正确 offset，最终文件逐字节一致。
- [x] 在 WriteAt、fdatasync、bitmap commit、unlink、final rename 每个边界崩溃后均可恢复。
- [x] final 已存在时绝不覆盖。
- [x] Buffer 对象损坏或丢失时只重下对应对象，不把损坏内容提升为 success。
- [x] cancel、retry、shutdown 不泄漏 reservation、对象、FD 或目标 `.moving`。

### 性能与稳定性

- [x] 100 张小图能够持续利用网络并由单 Writer依次提交；不因文件槽位固定为 5 而停住网络。
- [x] 5 个大文件保持各自足够的网络分片并发，小文件流量不能把大文件强制降成单协程。
- [x] 网络低于目标盘零散提交能力时，Ready对象来一个搬一个，Buffer保持低水位。
- [x] 网络高于零散写能力但低于目标盘顺序能力时，Ready队列自然形成连续段，Writer吞吐随之提升，Buffer达到稳定水位后回落。
- [x] 纯海量小文件超过目标盘 files/s能力时，Buffer达到容量后网络受控背压，进程不 OOM、SSD不越过容量、目标盘写并发不增加。
- [x] 单个大文件大于 Buffer总容量时，仍可通过对象滑动窗口完成，不死锁。
- [x] Writer慢、暂停或目标盘短时失败时，网络只在 Buffer容量耗尽后暂停；恢复搬运后自动继续。
- [x] 在目标存储持续可靠写入不低于 125 MiB/s的前提下，架构不存在阻止 1 Gbps持续下载的内部固定上限。

### SSD Cache

- [x] benchmark同时施加网络对象写和目标 Writer对象读，验证混合吞吐。
- [x] 5 GiB Cache在预期对象尺寸下启动扫描、Ready索引和批量删除时间可接受。
- [x] 不逐对象强 fsync的恢复策略能够识别并重下不可信对象。
- [x] Page Cache、对象 churn和 moved bitmap不会导致不可控内存增长或 DB事务放大。

## 15. 完成定义

以下条件全部满足后，才能把 Unified Storage Buffer 标记为完成：

- [x] 生产下载路径按对象写入 Bucket，whole-file Mover不再掌握真实所有权。
- [x] 目标盘只有单 Writer，且网络/Buffer/目标盘三层容量相互独立。
- [x] 任意 Ready对象都能逐步搬运，连续分片自然获得顺序写倾向。
- [x] 乱序搬运由持久 moved bitmap保证，不依赖单一 durableOffset猜测。
- [x] Buffer容量、背压、删除和崩溃恢复形成闭环。
- [x] 小文件、大文件、混合负载、慢盘、超大文件和崩溃矩阵测试通过。
- [x] 运行指标能够明确区分网络受限、Buffer背压、目标盘受限和对象恢复。
- [x] README、README_zh 和 `docs/STREAMING_BLOCK_ENGINE_DESIGN.md` 与最终实现一致，不再描述旧 whole-file Mover或过时 disk worker模型。
