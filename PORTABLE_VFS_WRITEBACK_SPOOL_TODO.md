# Portable VFS Write-Back Spool TODO

> 状态：Architecture Direction GO / Implementation NO-GO。Gemini首轮评审已完成并合并；Phase 0原型、benchmark和剩余决策完成前，不得接入生产。
>
> 评审对象：以 rclone `vfs/vfscache` 的写缓存模型为参考，重构 TGX 当前 memory/SSD Bucket + TargetWriter。本文不是在现有对象管线旁边追加第三套路径，而是定义一个最终替代方案。
>
> 产品边界：TGX 运行在 Docker 中，宿主机可能是 Linux、NAS、macOS 或 Windows。核心实现只能依赖普通目录、普通文件 API、SQLite 和 Go 并发原语；不得依赖 RAID、LVM、Btrfs、APFS、NTFS、FUSE、块设备或特定内核能力。

## 0. Gemini评审结论与处理

评审结论拆成两层：

```text
Architecture Direction：GO
Implementation：NO-GO，直到Phase 0原型和benchmark关闭剩余决策
```

### 0.1 采纳

- 采用独立领域重写，参考rclone的Item/Dirty/expiry heap/reload，不直接import或暴力fork整个VFS cache。
- 大文件以可回收Segment承载，完整小文件作为单Item，统一走Spool和WriteBack合同。
- 默认先实现“完整Segment Ready后write-back”，前缀write-back仅在容量死锁或延迟benchmark证明必要时增加。
- 网络scheduler必须frontier-first，并为frontier预留容量，不能等缓存接近满后才补救。
- SQLite状态更新必须批处理，禁止逐网络chunk提交事务。
- Target Sink顺序写回时增量计算SHA，正常完成路径不得再次从目标盘完整读取。
- Memory模式明确为volatile，SQLite与Spool路径可独立配置。
- 旧`.part/.partial`必须由ownership-aware migration/cleaner处理。
- rename/remove前必须满足对应平台的句柄关闭合同。

### 0.2 修订后采纳

- Segment大小必须可配置并由benchmark选择；32MiB只作为候选，不是预先写死的领域常量。当前Telegram chunk为1MiB时，32MiB对应32个chunk，不是64个512KiB chunk。
- Target Writer总并发可配置，5-8是产品要求的验收范围，不以“HDD磁头”作为跨平台固定依据；small lane使用全局permit，不额外突破总并发。
- Streaming SHA要求同一文件按offset顺序写回。Crash/legacy场景允许受控全文件校验，禁止“绝对不读final”这种无法恢复的承诺。
- SQLite批处理使用时间/字节checkpoint和crash-loss预算决定，不预设“低于10 TPS”。
- Frontier水位由reservation不变量和benchmark决定，不写死80%。
- 历史SHA backfill只能生成`LegacyUnverified`候选，不能对当前文件自签为可信commit；只有既有可信source/DB intent可升级为Verified。
- Cleaner先quarantine再按grace period删除，禁止启动时直接删除所有“看起来无owner”的文件。
- Sync只用于durability boundary；普通cache reclaim只要求关闭句柄并保持cleanup ownership，不在每次remove前强制Sync。

### 0.3 不采纳为事实或完成标准

- 不接受“降低98%文件系统开销”“800行内完成”“彻底根除inode风险”等无benchmark结论。
- 不接受“5GiB必然平稳承载50GiB”作为证明；只能验收文件大于Spool仍可持续前进。
- 不接受固定32MiB、固定4-6并发、固定80%水位、固定10 TPS作为架构常量。
- 不接受后台扫描当前final并直接补SHA为可信commit，该做法会复现历史proof自签缺陷。

## 1. 核心决定

最终目标不是继续维护“一分片一对象文件”的应用级对象存储，而是：

```text
Bounded RAM
  -> Portable Spool Files
  -> Delayed Write-Back Queue
  -> Bounded Target Writers
  -> Final Commit
```

职责边界：

```text
Telegram scheduler：生成合法网络请求，控制账号/DC/FloodWait
Spool：保存尚未target-durable的字节，提供容量和crash恢复
WriteBack Queue：决定何时、以什么顺序、多少并发写向目标
Target Sink：执行顺序WriteAt、Sync、final non-replacing commit
SQLite：唯一任务、generation、range、dirty、commit和cleanup状态owner
```

- [ ] memory/SSD不再是两套业务状态机；它们只是同一Spool的不同Store实现。
- [ ] 用户通过`--buffer-dir`选择目录；TGX不判断该目录位于SSD、HDD、tmpfs、Docker volume还是宿主机bind mount。
- [ ] `memory`模式表示payload未持久，进程退出后按SQLite任务状态重新下载；不得伪装成durable cache。
- [ ] `disk`模式表示Segment文件完成Sync和manifest commit后可在重启后继续write-back。
- [ ] `none`模式也必须走相同final commit合同；它只能禁用持久Spool，不能保留另一套direct success/recovery逻辑。

## 2. rclone源码复用边界

参考源码：

- [`vfs/vfscache/item.go`](https://github.com/rclone/rclone/blob/master/vfs/vfscache/item.go)
- [`vfs/vfscache/cache.go`](https://github.com/rclone/rclone/blob/master/vfs/vfscache/cache.go)
- [`vfs/vfscache/writeback`](https://github.com/rclone/rclone/tree/master/vfs/vfscache/writeback)
- [VFS cache行为文档](https://rclone.org/commands/rclone_mount/#vfs-file-caching)
- [rclone MIT License](https://github.com/rclone/rclone/blob/master/docs/content/licence.md)

实现前必须固定一个rclone release/commit，不得长期跟随`master`复制未知变化。

### 2.1 可以提取或改写的逻辑

| rclone能力 | TGX对应 | 处理方式 |
|---|---|---|
| `Item` + local cache file | `SpoolItem` + Segment file | 按TGX identity重写 |
| `Info.Size/Rs/Dirty` | size/ranges/dirty/state | 存SQLite，不复制JSON meta |
| `WriteAt` + range tracking | 网络chunk写Segment | 复用模式和测试思想 |
| Close后延迟write-back | Segment Ready后排队 | 改成Segment级，不等整文件 |
| expiry heap + timer | WriteBack优先队列 | 可提取精简fork |
| cancel/rename/requeue | generation替换和任务取消 | 按attempt合同重写 |
| failed upload retry | target write失败重试 | 使用typed error和有界backoff |
| dirty item reload | 启动恢复Dirty/Queued Segment | 由SQLite驱动 |
| max-size/min-free cleaner | Spool容量和ENOSPC恢复 | 复用策略，不复制remote语义 |
| open/dirty item不可evict | leased Segment不可删除 | 保持资源守恒 |

### 2.2 不得带入TGX核心的逻辑

- [ ] 不引入rclone `fs.Object`、remote backend、`operations.Copy`。
- [ ] 不引入FUSE/VFS handle、目录cache、read cache和downloaders。
- [ ] 不引入remote fingerprint作为TGX任务identity。
- [ ] 不把rclone整文件Close后upload原样用于大文件。
- [ ] 不直接依赖`vfs/vfscache`包；它和rclone内部类型、锁顺序高度耦合。
- [ ] 不把NATS、MinIO、JetStream或另一个常驻服务引入单机下载主路径。

### 2.3 许可证

- 决策：独立重写TGX核心Item/Queue状态机；只有独立、依赖可控且测试价值明确的算法片段才允许复制并保留MIT声明。
- [ ] 若复制/修改rclone源码，保留原MIT版权和许可头。
- [ ] 新增`THIRD_PARTY_NOTICES.md`，记录来源仓库、固定commit、复制文件和主要修改。
- [ ] 不把“参考设计”写成“原创实现”或删除来源信息。

## 3. Portable Spool数据模型

### 3.1 Identity

```text
TaskKey    = CanonicalTaskID
AttemptKey = TaskKey + Generation
SegmentKey = AttemptKey + SegmentIndex + Start + Length
```

- [ ] TaskID、Generation、FinalPath、ExpectedSize必须由任务owner一次确定。
- [ ] worker数、网络并发数、Segment数不能改变逻辑range集合。
- [ ] 旧generation的Segment永远不能写入新attempt或final。
- [ ] 所有路径名称使用稳定hash，避免Windows保留名、大小写和路径长度问题。

### 3.2 SpoolItem

```go
type SpoolItem struct {
    Key           SegmentKey
    ExpectedSize  int64
    CachedRanges  RangeSet
    Durable       bool
    Dirty         bool
    State         SpoolState
    Attempts      int
    NextRetryAt   time.Time
}
```

状态只允许单向迁移：

```text
Reserved
  -> Receiving
  -> Ready
  -> Queued
  -> WritingBack
  -> TargetDurable
  -> Reclaimed
```

失败分支：

```text
Receiving/Ready/WritingBack
  -> RetryPending
  -> Queued

任何非终态
  -> Canceled / Failed / CleanupPending
```

- [ ] 不使用多个bool共同表达生命周期；`Dirty`仅表示payload尚未target-durable，不替代State。
- [ ] 状态迁移通过SQLite事务完成，不由文件存在性猜测业务状态。
- [ ] 每个状态必须定义payload owner、lease owner、cleanup owner和重启行为。

### 3.3 SQLite schema草案

```text
spool_attempts(
  task_id, generation, final_path, expected_size,
  state, created_at, updated_at,
  PRIMARY KEY(task_id, generation)
)

spool_segments(
  task_id, generation, segment_index,
  start_offset, expected_length, cached_ranges,
  state, dirty, attempts, next_retry_at,
  path, checksum,
  PRIMARY KEY(task_id, generation, segment_index)
)

target_commits(
  task_id, generation, final_path, expected_size,
  expected_sha256, committed_sha256,
  state, version, updated_at,
  PRIMARY KEY(task_id, generation)
)

spool_cleanup(
  path, bytes, reason, attempts, next_retry_at,
  PRIMARY KEY(path)
)
```

- [ ] schema必须有迁移版本。
- [ ] WAL、busy timeout、checkpoint策略需要独立benchmark。
- [ ] payload不得作为SQLite BLOB存储。
- [ ] DB success不得早于target commit和commit intent完成。

## 4. Small File与Large File统一模型

### 4.1 Small File

一个小文件对应一个完整SpoolItem：

```text
网络完整拉取
-> 有界RAM
-> 可选Spool文件Sync
-> Ready
-> Small WriteBack Queue
-> 单个small target writer
-> final commit
```

- [ ] small file不再经过`memBufferWriterAt -> Bytes -> Bucket`多次整文件复制。
- [ ] RAM budget在网络请求前取得，进入disk Spool或target-durable后释放。
- [ ] small writer按完整文件顺序提交，不为每个文件创建proof sidecar。
- [ ] small与large共享attempt、commit、retry、cleanup表。

### 4.2 Large File

rclone整文件Close后write-back不能直接用于大文件，因为单文件可能远大于Spool容量。大文件改用Segment：

```text
Telegram chunks -> Segment file -> Ready -> WriteBack -> Reclaim
```

- [ ] 一个Segment包含多个Telegram网络chunk，禁止恢复“一chunk一文件”。
- [ ] Segment大小可配置并由benchmark决定；Phase 0至少比较多个候选，32MiB不得在评测前升级为固定常量。
- [ ] 同一Segment允许多个chunk乱序WriteAt，并持久化range集合。
- [ ] 首版以Segment完整range或EOF作为Ready条件；只有容量/延迟证据证明必要时才设计连续前缀write-back。
- [ ] target writer优先同task的下一个连续Segment。
- [ ] Segment target-durable后立即回收，因此单文件可大于整个Spool容量。
- [ ] frontier缺口必须在admission阶段预留网络和Spool容量，避免后续Segment填满缓存造成死锁；高水位调度只是补充，不是根保证。

### 4.3 公平性

WriteBack选择倾向：

```text
1. 可直接完成的小文件
2. 当前target文件的下一个连续Segment
3. 其他文件的连续Segment
4. 等待时间最久的Ready Segment
```

- [ ] 倾向不是绝对优先级；小文件不能永久饿死大文件。
- [ ] 大文件必须维持独立网络chunk并发保底。
- [ ] WriteBack Queue和网络scheduler只通过非阻塞Hint协作，不互相调用RPC/target IO。

## 5. WriteBack Queue

借鉴rclone writeback heap：

```text
Ready Item
-> Add/Reset Expiry
-> Timer唤醒
-> 并发permit
-> TargetSink.WriteBack
-> success: clean/reclaim
-> retryable: backoff/requeue
-> permanent: failed/cleanup
```

- [ ] 每个Item只有一个稳定Handle，重复修改重置expiry，不重复入队。
- [ ] generation切换先取消旧writeback，再等待lease释放。
- [ ] queue并发控制的是target writeback，不是网络协程和连接数。
- [ ] target writer总并发可配置并在5-8范围完成验收；small lane默认串行但消耗全局permit，不额外突破总并发。
- [ ] retry使用typed error，不解析字符串。
- [ ] ENOSPC进入Backpressured/HealthProbe，不进行100ms热循环。
- [ ] queue状态和next_retry_at持久化，重启不丢任务。
- [ ] 修改中的Item不能被Cleaner删除。

## 6. Capacity与Cleaner

统一计费：

```text
reserved RAM
+ reserved Spool bytes
+ receiving bytes
+ ready bytes
+ writing-back bytes
+ cleanup-pending bytes
```

- [ ] 网络RPC前完成capacity reservation。
- [ ] 任何退出路径只能释放一次lease。
- [ ] Cleaner只能删除Clean/Reclaimed或明确可重新下载的Item。
- [ ] Dirty/Queued/WritingBack/CleanupPending Item不得因LRU直接删除。
- [ ] 支持`max_spool_bytes`和`min_free_space`。
- [ ] open/dirty文件可能使实际占用高于soft LRU目标，但不能越过hard admission boundary。
- [ ] 启动扫描区分managed与unmanaged bytes，旧`.part`不得隐藏在5GiB配置之外。
- [ ] Cleaner失败必须进入`spool_cleanup`，不能best-effort后丢失ownership。
- [ ] Cleaner采用`discover -> quarantine -> grace period -> delete`；未知文件不得在启动扫描中直接删除。
- [ ] frontier reservation是硬约束；高/低水位和调度比例由benchmark配置，不写死80%。
- [ ] SQLite range checkpoint按字节/时间/crash-loss预算批处理，不逐chunk提交，也不预设固定TPS上限。

## 7. Target Sink与Final Commit

Target Sink只拥有：

```text
WriteBack(segment)
SyncBatch()
CommitFinal()
```

- [ ] Segment按final offset写入`target.moving`。
- [ ] target Sync按批次/时间执行，不逐网络chunk执行。
- [ ] SQLite durable ranges只在target Sync成功后更新。
- [ ] Segment只在SQLite记录TargetDurable后回收。
- [ ] final使用non-replacing commit，同目录完成rename。
- [ ] final commit intent、SHA和DB terminal由SQLite统一持久化。
- [ ] 不再为每个媒体生成`.tgx_commit`；如需外部proof，使用集中manifest或可选导出。
- [ ] commit失败后必须区分pre-commit与post-commit状态，不能重新猜测final/moving。
- [ ] 同一文件的Target WriteBack必须按offset顺序推进frontier，正常路径在写回时增量计算SHA。
- [ ] SHA checkpoint/recovery格式必须版本化；crash或legacy缺少可恢复hash state时允许受控重读final，不得以“绝对禁止final reread”牺牲正确性。

## 8. Durability与Recovery

### 8.1 memory模式

- [ ] RAM Item不声明durable。
- [ ] 进程退出后清除其range，任务回到pending/继续下载。
- [ ] Registry/DB不得因为RAM中已有完整文件而提前success。

### 8.2 disk模式

提交顺序：

```text
WriteAt Segment
-> Sync Segment file
-> SQLite commit CachedRanges/Ready
-> enqueue WriteBack
```

目标写回：

```text
Read Segment
-> target WriteAt batch
-> target Sync
-> SQLite commit TargetDurable
-> delete Segment
-> SQLite commit Reclaimed
```

- [ ] crash发生在任意箭头前后均有唯一恢复动作。
- [ ] 文件存在不能自动提升状态；DB状态也必须验证对应payload。
- [ ] 恢复不得为当前final“自签”新的expected SHA。
- [ ] Verifier必须纯只读；repair由独立事务owner执行。
- [ ] corrupt/stale manifest不得阻断更可信的current attempt状态，也不得静默覆盖。
- [ ] SQLite与Spool可位于不同volume，但不得假设跨volume原子性；所有事务顺序必须允许DB先/文件先两种crash重放。
- [ ] `LegacyUnverified`与`Verified`是不同状态；后台hash backfill不能直接把前者提升为后者。

### 8.3 启动恢复顺序

```text
打开SQLite并执行migration
-> 恢复attempt/segment/cleanup状态
-> 验证Spool文件和range
-> 重建WriteBack heap
-> 安装callbacks
-> 启动网络producer与target consumer
```

- [ ] 任何Dirty/Queued Item必须在重启后重新入队。
- [ ] 不运行时扫描target媒体树猜测业务状态。
- [ ] target final只通过持久commit intent和SHA进入success。

## 9. 跨平台约束

核心只使用：

```text
os.OpenFile
File.ReadAt/WriteAt/Sync/Close
os.Rename/os.Remove
filepath
SQLite
context/channels/semaphore
```

- [ ] 不要求sparse file；支持时可优化，不支持时语义不变。
- [ ] 所有rename限定在同一目录/volume。
- [ ] 删除/rename前关闭Windows句柄，测试NTFS bind mount语义。
- [ ] 文件名使用hash，避免Windows保留名、大小写折叠和路径长度。
- [ ] 明确`File.Sync`是portable durability边界；directory Sync仅作为平台能力增强，不伪装所有平台完全相同。
- [ ] durability操作按状态迁移定义：data/proof/final commit需要Sync；clean cache reclaim只要求关闭句柄、检查Remove结果并保留cleanup owner。
- [ ] 禁止两个TGX实例共享同一Spool目录；启动时使用DB/process lease检测。
- [ ] 测试Linux named volume、Linux bind、Docker Desktop macOS bind、Docker Desktop Windows bind。

## 10. 迁移与删除清单

### 10.1 实现期

- [ ] 新Spool模块先以无生产调用的方式完成单元/chaos测试。
- [ ] 添加旧Bucket状态到Spool状态的一次性migration reader。
- [ ] migration只读旧对象，不允许新旧writer同时提交同一task。
- [ ] legacy扫描结果先进入quarantine清单；只有SQLite确认无owner且超过grace period后才能删除。
- [ ] 发布切换时生产constructor一次性从Bucket/TargetWriter切到Spool/WriteBack。
- [ ] 切换失败只能回滚到切换前DB/schema和旧二进制，不允许半数任务走新旧两套路径。

### 10.2 切换后必须删除或退役

- [ ] `core/bucket`的一chunk一对象文件后端。
- [ ] `.partial/.ready`对象目录和启动Recover猜测。
- [ ] `pendingDeleteBytes/objectCount/tombstoneOrder`重复资源账本。
- [ ] `TryTakeNext/TakeReady`的对象级实现，替换为Segment WriteBack Queue。
- [ ] TargetWriter中与Bucket对象、proof sidecar和多层AttemptPhase相关的补丁逻辑。
- [ ] `bucketWriterAt`、`memBufferWriterAt -> Bytes`重复copy路径。
- [ ] 每媒体`.moving.meta/.tgx_commit`；状态迁至SQLite。
- [ ] direct large、direct small、legacy recovery各自独立final producer。
- [ ] 旧whole-file Mover与任何同步Publish等待路径。

## 11. 实施阶段

### Phase 0：评审与原型

- [x] Gemini完成首轮方向评审：Architecture GO / Implementation NO-GO pending prototype。
- [ ] 固定rclone源码版本、许可证和复用文件清单。
- [ ] 用独立原型验证WriteAt、dirty reload、writeback heap、Cleaner、frontier reservation和SQLite checkpoint。
- [ ] 原型不得接入生产daemon。
- [ ] benchmark Segment候选大小、Target Writer并发、水位和checkpoint频率；结论落回本文后才能进入Phase 1。

### Phase 1：SQLite Manifest与Spool Store

- [ ] 实现schema、migration、attempt/segment事务API。
- [ ] 实现MemoryStore与FileStore同一接口。
- [ ] 实现range merge、Sync、Recover和capacity lease。
- [ ] 完成跨平台文件语义测试。

### Phase 2：WriteBack Queue与Target Sink

- [ ] 精简fork rclone expiry heap/retry/cancel模型。
- [ ] 实现Segment连续优先和公平性。
- [ ] 实现target batch Sync与Segment reclaim。
- [ ] 实现typed storage failure和CleanupPending。

### Phase 3：Downloader与Final Commit接入

- [ ] 网络RPC前reservation。
- [ ] small whole-file写入SpoolItem。
- [ ] large chunk写入Segment。
- [ ] SQLite commit intent和final non-replacing commit。
- [ ] 删除per-file proof sidecar路径。

### Phase 4：迁移与唯一owner切换

- [ ] 实现旧Bucket/part/meta只读migration。
- [ ] 验证rollback。
- [ ] 正式切换production constructor。
- [ ] 同一版本删除旧生产调用点。

### Phase 5：删除与文档

- [ ] 删除第10.2节旧模块和fallback。
- [ ] 更新README、README_zh、设计文档和CLI help。
- [ ] 将旧Unified Buffer TODO标记Superseded，不再保留“已全部完成”声明。

## 12. 必须通过的验收

### 12.1 正确性

- [ ] 同一logical task只有一个current generation。
- [ ] chunk乱序/重复/迟到不改变合法range集合。
- [ ] Segment WriteAt、Sync、SQLite commit、target Sync、reclaim各边界crash可恢复。
- [ ] target final逐字节正确，不凭size或当前文件自签SHA。
- [ ] retry/cancel/shutdown不泄漏RAM、Spool bytes、FD、queue handle或cleanup item。
- [ ] 大文件大于Spool总容量仍能持续完成。
- [ ] restart后所有dirty items重新入队，clean items不会重复writeback。
- [ ] Windows open-file rename/delete和macOS bind mount测试通过。

### 12.2 性能与稳定性

- [ ] 100张小图在网络/Telegram允许时可持续利用约200Mbps。
- [ ] 5个大文件维持各自足够的网络chunk并发，小文件不能将其降为单请求。
- [ ] target实际并发保持配置的5-8，small lane默认串行。
- [ ] 无每网络chunk文件create/rename/unlink/fsync。
- [ ] Spool满后网络受控背压，WriteBack和Cleaner仍可前进。
- [ ] 100/10000小文件测试记录files/s、metadata ops、SQLite事务和target durable BPS。
- [ ] 慢目标盘、ENOSPC、权限错误和临时IO错误不会形成100ms热循环。
- [ ] 在目标存储持续可靠写入不低于125MiB/s时，应用内部无阻止1Gbps的固定上限。

### 12.3 资源边界

- [ ] managed bytes与Spool目录实际bytes一致或差异可解释。
- [ ] unmanaged legacy文件有迁移/隔离指标。
- [ ] memory使用由全局byte budget控制，不按open file数线性无界增长。
- [ ] Cleaner从不删除dirty/leased item。
- [ ] cleanup失败可见、可重试、不重新发放capacity。

## 13. 已决策项与剩余实验项

| 主题 | 当前决策 | 仍需完成 |
|---|---|---|
| rclone复用 | 独立领域重写；不import整个vfscache | 固定参考commit和可能复制的独立片段 |
| Segment粒度 | 可配置；完整Segment Ready为首版 | benchmark候选大小和尾段策略 |
| 前缀write-back | 首版不实现 | 仅在容量/延迟证据成立时重开设计 |
| Target Writer | 全局可配置；5-8范围验收；small消耗同一permit | benchmark默认值和按目标路径分组 |
| SHA | 同文件按offset顺序写回时增量计算 | 定义可持久hash checkpoint；crash/legacy fallback |
| Memory模式 | volatile，可用于small和滑动large Segment | benchmark全局RAM budget和GC |
| SQLite/Spool位置 | 独立配置，不要求同盘 | 验证跨volume crash顺序和慢DB影响 |
| 历史final | 进入LegacyUnverified，后台限速hash | 定义可信升级条件、quarantine和人工确认 |
| Proof/Manifest | SQLite唯一owner；外部proof仅可选导出 | schema/version/export格式 |
| Power-loss合同 | portable file Sync + SQLite ordering；平台增强单独声明 | Linux/macOS/Windows crash矩阵 |

仍未通过实验关闭的实施门：

- [ ] Segment候选大小。
- [ ] Target Writer默认并发。
- [ ] SQLite checkpoint字节/时间阈值。
- [ ] Spool高/低水位与frontier保留比例。
- [ ] SHA状态持久化格式和恢复成本。
- [ ] 100/10000小文件metadata与SQLite吞吐。

## 14. 完成定义

以下全部满足后，才能宣称Portable VFS Write-Back Spool完成：

- [ ] rclone复用边界、许可证和固定commit清晰可审计。
- [ ] RAM/disk/none使用同一task、segment、writeback、commit状态机。
- [ ] small whole-file与large Segment共享唯一ownership和recovery。
- [ ] 网络、Spool、WriteBack、target容量相互独立且资源守恒。
- [ ] 生产运行时不存在旧Bucket与新Spool双owner。
- [ ] 大文件可超过Spool容量并持续回收。
- [ ] crash、retry、cancel、ENOSPC、cleanup和跨平台矩阵通过。
- [ ] 100小图约200Mbps与1Gbps capable大文件目标有真实数据面证据。
- [ ] 旧对象文件、proof sidecar和补丁状态机已删除，而非隐藏在fallback后。
- [ ] README/TODO/CLI与最终生产实现一致。
