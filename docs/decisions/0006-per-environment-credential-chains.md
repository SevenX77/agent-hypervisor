# 0006 — 每个环境自持凭据链，ah 是门卫不是代办

状态：已批准，实施中
日期：2026-08-05

## 决策

一条 OAuth token 链只能有一个活跃使用环境。**环境 = 一个操作系统里的一个 home 目录**：Windows 的 `C:\Users\<user>`、WSL 发行版里的 `/root`、macOS 的 `/Users/<user>` 是三个互不相通的环境。每个环境用 provider 官方登录流程建立**自己的链**；ah 负责**检测、引导、放行**（门卫），凭据值永远只经过 provider 官方登录程序和用户的浏览器，ah 不读取、不写入、不搬运凭据内容（代办是禁区）。

## 依据

1. **轮换实证**：同一账号同一次登录（相同 `sid`、相同 `auth_time`）派生的两份 codex auth.json，refresh_token 指纹不同——每次刷新都签发新 token、旧的作废。两个环境共用一条链（拷贝文件），先停止刷新的一侧必死（实测：WSL 的链 7 月 15 日被 Windows 侧轮换死）。
2. **Claude 的 #18 同类**：claude 用 temp+rename 写凭据。跨环境符号链接（WSL 路径 → `/mnt/c/...`）在 WSL 侧首次刷新时被 rename 顶掉，链分叉，另一侧随后死亡。实测本机 `/root/.claude/.credentials.json` 正是这样一枚未爆的引信。
3. **官方口径**：codex 的 CI/CD 指南允许"拷贝 auth.json 到 runner"，但其模型是**链的迁移**（拷过去之后只有 runner 刷新）；两侧同时活跃即分叉。多设备多链是 OAuth 的常态（手机+电脑同时登录同一账号），gh/claude/codex 都让每台机器各登录一次。

## 按操作系统区分

ah 的运行时（守护进程、沙箱、seats）目前只在 Linux（含 WSL2）上运行；本决议的检查在 Linux 实现并生效，但**规格按 OS 定义**，为 macOS 留下正确的接缝：

| | Linux（含 WSL） | macOS | Windows 原生 |
|---|---|---|---|
| codex 存储 | `~/.codex/auth.json`（常规文件） | `~/.codex/auth.json` | `%USERPROFILE%\.codex\auth.json`，**属于 Windows 环境，ah 不触碰** |
| claude 存储 | `<shared_credentials_dir>/.credentials.json`（常规文件） | **Keychain**——没有可静态检查的文件，只能用 `claude auth status` 探测 | Windows 凭据体系，同上不触碰 |
| antigravity 存储 | `~/.gemini/antigravity-cli/antigravity-oauth-token`（常规文件） | 同路径 | 同上不触碰 |
| 跨环境边界 | `/mnt/*`（9p/drvfs interop 挂载）——指向它的符号链接或位于其上的存储一律拒绝 | 无 interop 边界 | 不适用 |

要点：**"存储必须是常规文件"是 Linux 事实，不是通则**——macOS 上 claude 的存储在 Keychain 里，检查方式必须换成探测命令。规格表由 provider manifest 按 OS 给出，检查器据表行事，不把 Linux 的形状写死进通用逻辑。

## 关键设计决定

### D1 host 存储必须属于本环境

`ah start` 与 `ah doctor` 对项目用到的每个 provider 检查 host 存储：

- 存在且可解析；
- （Linux）是常规文件，不是符号链接指向 `/mnt/*`，也不位于 Windows interop 挂载上（复用 `windows_interop_mount_for_path` 的既有判定）；
- 未处于"已登出"终态（claude 的 `expiresAt: 0` 存根）。

**过期语义要克制**：claude 的 `expiresAt` 是 access token 的过期时刻，过期≠登录失效（refresh 会续）；codex 的 `last_refresh` 陈旧同理。静态检查只判"缺失 / 存根 / 不可解析 / 越界"，不冒充活性检查。链死（token 被外部轮换掉）静态查不出来——根除它靠本决议消灭跨环境共链，兜底靠运行期失败时 doctor 的处方。

发现跨边界符号链接时**报错并给出确切修复命令，不静默删除**——那个链接是用户建的，删除是用户的决定。

### D2 登录门卫

检查不过时：

- **交互终端**（stdin 和 stdout 都是 tty）：当场以继承终端的方式启动该 provider 的官方登录命令，用户在浏览器完成后 start 继续。登录命令由 manifest 声明：codex `codex login`、claude `claude auth login`、antigravity **无独立登录子命令**（provider 侧限制）——退化为打印指引（交互跑一次 `agy` 触发 OAuth），不冒充自动。
- **非交互**（Studio 拉起、CI、管道）：失败，错误信息包含可直接复制执行的 remedy 命令。

### D2b 浏览器桥是门卫的环境前置，由 `ah setup` 负责

门卫拉起的登录流程要开浏览器；WSL 里的 Linux 进程弹不出 Windows 浏览器，除非有桥（`xdg-open`/`wslview`）。这不是登录方法的可选优化，而是它的环境前置：没有桥，"当场拉起登录"退化成"打印 URL 等用户自己点"。因此桥进入产品：`ah doctor` 以 `wsl:browser-bridge` 检查它（缺失为 Warn——流程仍可用，只是要手点 URL），`ah setup --fix` 安装零依赖的 opener（`/usr/local/bin/xdg-open` → Windows `rundll32 url.dll,FileProtocolHandler`，即 wslview 的内核做法）；门卫启动登录命令时若可解析到 opener，则为尊重 `$BROWSER` 的 CLI 注入该变量。

### D3 持久性是澄清加守卫，不是新机制

"每次打开 WSL 都要重新登录"从来不是持久性问题——WSL 的 ext4 跨 `wsl --shutdown` 天然持久（本机那份死了 20 天的 auth.json 就是证明），过去的重登是**分叉病的症状**：文件还在，token 被另一侧轮换死了。每环境自持链后：登录一次，seats 正常使用即自动刷新、链靠使用保鲜。剩余风险只有长期闲置导致 refresh token 自然过期，由 D2 门卫兜住。

ah 侧配套保证（多为既有行为，用测试钉死）：销毁沙箱只删沙箱内的符号链接、不碰 host 存储；会话归档不跟随符号链接；`ah reclaim` 永不进入 host 凭据目录。

## 验收判据

1. 三个 provider 的 host 存储都健康时，`ah start` 零打扰。
2. codex 存储缺失时，交互式 `ah start` 当场拉起 `codex login`，完成后 start 继续、agent 正常工作（真机演示）。
3. claude 存储是指向 `/mnt/*` 的符号链接时，start/doctor 指名道姓报错并给出修复命令，不改动该链接。
4. 非交互路径的失败信息包含可复制执行的 remedy。
5. `wsl --shutdown` 后重启不需要任何重新登录。
6. 检查器的规格按 OS 给出；macOS 行把 claude 标为探测式（Keychain），Linux 行为由自动化测试覆盖。
7. `ah doctor` 按 provider 输出体检结果与处方（关闭 #4 的主体）。

## 非目标

- 不实现 macOS/Windows 原生运行时的检查执行（规格留接缝，实现随运行时支持走）。
- 不做任何形式的凭据代填、token 搬运或"帮用户登录"。
- 不做 token 保活定时器——链靠使用保鲜，闲置过期走门卫。
