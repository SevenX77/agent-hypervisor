# 0001 — master 席位按 provider 能力协商，不再绑定 claude

状态：已实施
日期：2026-08-03

## 决策

master 是一个**角色**，不是一个 provider。任何 provider 只要在自己的 manifest 中声明了 master 角色所需的能力集，就可以承担 master；不声明的 provider 在配置校验期被明确拒绝，并说明缺少哪一项能力。`ah` 不再在 master 路径上出现 provider 名称常量。

## 背景与事实

以下为实施前经核验的仓库事实，用于说明本决策要消除的具体状态。

- `master.provider` 已被解析并归一化（`src/cli/config.rs`），master 就绪模式已按 provider 名分叉（`src/rpc/handlers/master_cutover.rs` 的 `master_readiness_mode`）。
- master 沙箱物化的三处入参写死字面量 `"claude"`：bundle 解析、hook push 上下文、home layout 调用（`src/rpc/handlers/sessions.rs` 的 `prepare_master_pane_plan`）。
- 角色规则注入对非 claude 的 master 直接跳过（`src/provider/home_layout.rs` 的 `materialize_builtin_rules`），因此非 claude 的 master 拿不到 master kernel，也就拿不到"读取交接后运行 `ah master ack-ready`"这条指令。
- master 复活路径无条件写入 `CLAUDE_CONFIG_DIR`，并按命令名判断是否需要 claude 共享凭据（`src/monitor/master_reaper.rs`）。
- 两道 claude-only 校验闸：bundle 与 `master.settings`（`src/cli/config.rs`）。
- CLI 侧 master provider 缺省回退 `"claude"`（`src/cli/bundle.rs`）。
- 就绪的降级档是 pane 文本稳定性判定：cutover 路径连续三次相同 capture 即判就绪；复活路径同为 pane 探测。这与 1.5.0 "pane 文本不得进入生命周期状态"的既有结论冲突。

合并结论：当前"master 只能是 claude"不是一条被写下来的规则，而是 claude 这个字符串散落在 master 路径六处形成的既成事实；把 `master.provider` 配成非 claude 会得到"校验放行、实际拿 claude 形状家目录、且无任何角色规则"的破损组合。

## 关键设计决定

### D1 能力协商取代 provider 白名单

`ProviderManifest` 增加 `capabilities` 字段，声明该 provider 支持哪些可选能力：`rules_target`（ah 能为它物化角色规则文档）、`completion_signal`（能把回合结束信号推回 ahd）、`readiness_ack`（master 席位能显式上报就绪）、`bundles`、`settings`。master 角色所需能力集为 `{rules_target, completion_signal, readiness_ack}`。

能力检查按**使用点**执行，不做一刀切的准入拒绝：物化时缺 `rules_target` 就不注入规则，缺 `completion_signal` 就不接完成信号；cutover 与复活的就绪判定缺 `readiness_ack` 时以指明缺失能力的错误失败。配置校验期对 resolved master provider 做一次能力检查，缺项时发出**警告级**诊断，逐项列出缺失能力及随之失效的功能，使降级可见而非静默。

依据：CSI 用 `ControllerGetCapabilities` / `NodeGetCapabilities` 让插件显式声明可选能力，编排器据此决定可调用的操作；能力不足的插件仍可注册，失败发生在真正需要该能力的操作上，而不是注册时一律拒绝。LSP 在 `initialize` 握手交换 client/server capabilities，后续交互都在这份协定内进行。二者共同的性质是：能力由被集成方声明，集成方只做查询与在使用点拒绝，新增被集成方不需要修改集成方的判断分支。

放弃的替代：(a) 硬编码 provider allowlist——每接一个 provider 要改六处判断，且判断依据是名称而非事实；(b) 准入期一刀切拒绝能力不足的 provider——会让 `bash` 这类进程无法再作为 master pane 的测试替身，而它们在既有端到端测试中承担的正是"只要一个廉价进程占住 master pane"的角色，与是否具备 agent 能力无关；(c) 允许降级但不告知——那才是静默降级。

### D2 就绪只接受显式信号，pane 文本退出就绪判定

master 就绪只接受 master 自己发出的信号。cutover 路径的 ack 指继任 master 读取交接后运行 `ah master ack-ready`；复活路径的 ack 指该 provider 的会话记录（transcript）出现新的助手进展。两条路径的 pane 文本探测分支删除，pane 观察仍可用于告警，但不得产生就绪结论。

provider 不具备 `readiness_ack` 时，两条路径的处理不同，因为两者要证明的事实不同：

- **cutover 拒绝**。cutover 的成功判据就是"继任者已接受交接"，只有继任者本人能证明；没有 ack 能力就没有任何东西可以替代它，因此在建立任何状态之前以指明缺失能力的错误失败。
- **复活降级为 `started`**。复活是对既有席位的自动恢复，判据是"替代进程已起来且仍是运行时认定的 master"。这一档以 `readiness_mode = "started"`、`strength = "degraded"` 显式记录，不读取 pane 任何内容。

依据：systemd 的 `Type=notify` 让服务用 `sd_notify READY=1` 自己宣告就绪，`Type=simple` 那种"进程起来即就绪"是明确的降级档而非默认档；Kubernetes 中未声明 readinessProbe 的容器同样按"启动即就绪"处理，而不是拒绝运行——两者都把"没有就绪协议"处理成一个有名字的弱判据，而不是伪造一个强判据。ah 照此办理：`started` 是有名字的弱判据，pane 文本则连弱判据都不是。

放弃的替代：(a) 给 pane 探测开 1.5.0 红线的豁免——等于把已删除的 pane 推断从就绪这个后门放回生命周期；(b) 复活路径也一律拒绝——会让一个正在运行的系统因为 master 不会自报而永远无法恢复，代价高于收益。

### D3 `master.provider` 是唯一权威字段，`cmd` 降为可选覆盖

配置加载阶段产出 resolved master provider：显式 `master.provider` 优先并归一化；缺省时由 `master.cmd` 首词推导，首词不是已知 provider 时回落到默认 master provider（`claude`，保持既有默认行为）。下游一律读 resolved 值，不再各自 `unwrap_or("claude")`。作者未写 `master.cmd` 时，启动命令取自 resolved provider 的 manifest；`provider` 与作者显式写下的 `cmd` 冲突（例如 `provider = "codex"` 而 `cmd = "claude"`）在校验期报错。

复活路径例外，且是有意为之：它手上只有存下来的 master 命令，没有配置。该路径按命令严格解析 provider，命令不指向任何已知 provider 时即认定"无 provider"，不套用默认值——否则会给一个从未运行过 Claude 的席位安上 Claude 的家目录与凭据。

依据：Kubernetes API 的 defaulting 约定是在加载阶段补全成一份 resolved 对象、下游只读 resolved 值；Terraform 的 `ConflictsWith` 让互斥配置在 plan 期直接失败，其 kubernetes provider 曾因"环境变量悄悄压过显式配置"被当作 bug 修正。二者共同的性质是：缺省在入口补全，冲突在入口拒绝，下游不做二次裁决。

放弃的替代：维持"校验读 `provider`、实际跑 `cmd`"的双事实源。

### D4 凭据路径议题不并入本系列

`providers.claude.shared_credentials_dir` 的 `~` 展开与 WSL Windows 挂载点守卫单独处理。它们与 master 能力协商没有共享契约面，并入会污染本系列的验收判据。

## 实施范围

- P0：manifest 能力位与 master 最小能力集；config 层 resolved master provider 与冲突校验。
- P1：校验闸改写——bundle 与 `master.settings` 按能力判断；`shared_credentials_dir` 仅在 resolved master provider 或 agent provider 为 claude 时要求。
- P2：master 物化通用化——`prepare_master_pane_plan` 三处字面量改读 resolved provider；删除 `materialize_builtin_rules` 中的 master-only claude 闸；spawn 参数携带 provider。
- P3：复活路径通用化——环境变量按 provider 生成；claude 共享凭据仅对 claude 要求；复活就绪 ack 支持所有声明 `readiness_ack` 的 provider，会话记录根目录由单一 owner 计算。复活重启的是存下来的原命令、并向 pane 注入继续指令，本身不追加 resume 参数，因此这一项无需改动。
- P4：就绪重构——删除 cutover 与复活两处 pane 探测分支；cutover 缺 `readiness_ack` 即拒绝，复活缺 `readiness_ack` 降级为 `started`。
- P5：测试与文档——config 层能力与冲突用例、master 规则物化用例、复活就绪两档用例；README 与 CHANGELOG 同步。

## 例外与它们的依据

以下两处保留 provider 名称判断，且不属于 D1 要消除的那类判断：

1. **claude 共享凭据的传递**（spawn 与复活两处）。`shared_credentials_dir` 是 Claude 登录存储这一具体机制的参数，不是角色能力；它只能流向 claude 席位，其他 provider 收到它没有意义且构成凭据外泄面。判断的对象是"这个参数属于谁"，不是"这个 provider 能不能当 master"。
2. **默认 master provider 常量**。无 provider、无可识别 cmd 时回落到 `claude`，是为保持既有默认行为不变；该常量只有一个定义点。

## 验收判据

1. `ProviderManifest` 暴露 `capabilities`；`bash` 的 master 所需能力为假，`claude`/`codex`/`antigravity` 为真。
2. `[master] provider = "codex"`（或 `antigravity`）配合合法 agent 配置通过 `ah config validate`，且不要求 `providers.claude.shared_credentials_dir`。
3. `[master] provider = "bash"` 通过校验但产生警告级诊断，诊断文本列出缺失的能力名；该 master 的 cutover 以指明 `readiness_ack` 缺失的错误被拒绝，复活则以 `readiness_mode = "started"`、`strength = "degraded"` 完成。
4. `[master] provider = "codex"` 与 `cmd = "claude"` 同时显式给出时被校验拒绝。
5. 非 claude 的 master 沙箱物化后，其 provider 对应的规则文档存在且包含 master kernel 内容；不产生 `.claude` 家目录形状。
6. master 路径（物化、复活、就绪）中不再出现 provider 名称字面量判断，provider 相关分支一律经 resolved provider 或 manifest 能力查询。
7. 代码中不存在以 pane 文本作为 master 就绪依据的分支。
8. 现有测试全绿；新增测试覆盖判据 2、3、4、5。

## 验收证据

在 Linux（WSL Ubuntu-24.04，Linux 文件系统上的工作副本）执行：

- `cargo test --tests --no-fail-fast`：61 个测试目标通过，1 个失败，合计 1547 passed / 2 failed。两条失败为 `db::perception::phase1_acceptance` 的 CI grep 规则用例，原因是仓库缺少 `scripts/ci/check_state_write_gate.sh`；在本系列改动之前的 HEAD 上以同样方式执行，同样只失败这两条，故与本系列无关。
- `cargo check --all-targets`（Windows msvc 宿主）：无错误，与 CI 的 Windows 检查同形。
- `ah config validate` 实跑四份配置：`[master] provider = "codex"` 通过且不索要 claude 共享凭据；`provider = "bash"` 通过并输出列出 `rules_target, completion_signal, readiness_ack` 三项缺失能力及其后果的警告；`provider = "codex"` 配 `cmd = "claude"` 以退出码 3 被拒；仓库自身的 claude master 配置仍然通过。

注意：从 Windows 挂载点（`/mnt/d/...`）运行测试时，`tests/r1_master_exit_shutdown.rs` 等守护进程用例会因该文件系统无法承载 Unix socket 而失败；这与本系列无关，验收一律以 Linux 文件系统上的运行结果为准。

## 非目标

- 不改变 worker 侧的 provider 行为与契约。
- 不新增 provider。
- 不改动 claude 共享凭据机制本身。
