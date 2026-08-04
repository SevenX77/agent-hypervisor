# 0002 — 凭据路径可移植性、Windows 挂载守卫与状态写入闸

状态：已实施
日期：2026-08-03

## 决策

三条独立但同源的补齐，共同的判据是：**声称成立的性质必须有机制持续保证，且违反时的失败必须可读**。

1. `providers.claude.shared_credentials_dir` 支持以 `~` 开头，在配置加载阶段展开为调用者的家目录。
2. 该目录若位于 Windows 互操作文件系统（WSL 的 drvfs）上，物化阶段以指明证据的错误拒绝。
3. `agents.state` 的单写者闸补上 CI 检查脚本 `scripts/ci/check_state_write_gate.sh`，并接入 CI。

## 依据

### D1 `~` 展开

README 声明 `ah.toml` "safe to commit and share"，而 v1.7.0 起该文件必须携带一条机器本地的绝对路径——两者直接冲突，仓库自己的 `ah.toml` 就是证据（它硬编码了一台机器的 `/root/.claude`）。展开只处理开头的 `~` 与 `~/`；`~user` 指向他人家目录，ah 不解析。取不到 HOME 时保持原样，由既有校验以"必须是绝对路径"拒绝，而不是猜一个位置。

展开发生在配置加载的归一化阶段，与 provider 别名归一化同处一点，下游只看到已解析值——与 0001 的 D3 同一条规则：缺省在入口补全，下游不做二次裁决。

### D2 Windows 挂载守卫

Claude 的凭据存储在刷新时**就地重写**，依赖 POSIX 属主与原子 rename；WSL 的 drvfs 两者都不保证。gateway 路径早已拒绝 `/mnt/c` 下的凭据文件，而 v1.7.0 引入的共享凭据路径没有同等检查——同一类风险在一条路径上拦、另一条上放行。

判定依据取自 `/proc/self/mounts` 而非路径拼写：拼写只是约定（`/mnt/data` 可以是纯 Linux 目录），挂载表才是事实。规则是"fstype 为 drvfs，或 fstype 为 9p 且挂载选项含 `aname=drvfs`"，因此普通 9p 网络共享不受影响。读不到挂载表时不拒绝——没有证据就不下结论。

### D3 状态写入闸

`src/db/perception/mod.rs` 早已写明该脚本的契约，仓库里也早有两条测试断言它存在并通过，但脚本从未落地——即 1.6.0 "授权写者是唯一写者"这条性质当前只靠人工自觉维持。脚本补齐后，该性质由 CI 每次运行验证。

脚本按基线棘轮工作：新增或增多越权写入失败；已迁移导致写入减少也失败，直到基线被下调——使 Phase 2 的迁移进度必须被记录，而不能悄悄停滞。判定跳过行注释（讨论规则的文字不算违反规则），并在匹配前折叠换行（SQL 跨行书写的写入点正是该闸要抓的对象）。

### D4 失败必须可读

上述守卫在实跑中暴露了一个既有缺口：CLI 只打印错误码（`ENVIRONMENT_NOT_SUPPORTED`），守卫给出的原因句被丢弃。fail-closed 若不可读，等于把诊断成本转嫁给操作者。CLI 改为渲染 `data.details`，错误码保留为前缀。

## 验收证据

- `bash scripts/ci/check_state_write_gate.sh` 在当前树通过；`db::perception::phase1_acceptance` 的两条既有测试（此前一直失败）转绿：一条以临时目录验证"非闸文件中的直接写入被拦、闸文件被豁免"，一条验证当前树通过。
- `cargo test --lib`：1083 passed / 0 failed。
- 单元测试以本机真实 WSL 挂载表片段验证：`/mnt/c/...`、`/mnt/d/...` 判为 Windows 盘；`/root/.claude`、`/home/...`、以及 `/mnt/linux-share`（普通 9p）判为可用。
- 实跑：配置写 `~/.claude`，启动后真实 claude 席位的进程环境为 `CLAUDE_SECURESTORAGE_CONFIG_DIR=/root/.claude`，同时 `CLAUDE_CONFIG_DIR` 仍是各自沙箱目录。
- 实跑：配置指向 `/mnt/d/ah-credential-guard-test`（该目录真实存在于 9p 挂载上），`ah start` 拒绝并回滚会话，错误原文为：`ENVIRONMENT_NOT_SUPPORTED: providers.claude.shared_credentials_dir is on a Windows interop filesystem (9p mounted at /mnt/d): /mnt/d/ah-credential-guard-test. A token refresh rewrites this store in place, which needs POSIX ownership and atomic rename; keep it on the Linux filesystem, e.g. ~/.claude`。

## 非目标

- 不改变凭据共享机制本身（仍是一份宿主登录、各沙箱独立 `CLAUDE_CONFIG_DIR`）。
- 不迁移任何现有的越权状态写入点；本次只是把闸建起来并记录基线。
