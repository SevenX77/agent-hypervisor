# 0005 — 项目身份解析：一个函数、git 式发现、失败要响

状态：已批准（用户委托按领域成熟方案裁决），实施中
日期：2026-08-05

## 决策

`ah` 的每条项目级命令回答同一个问题——"我在跟哪个项目的栈说话"——因此这个问题只允许有一个答案函数。答案按 git/cargo 的项目发现模型给出：显式 `--config` 先绝对化再用；没有显式路径就从当前目录向上找 `ah.toml`；两者都没有就**报错**，不再静默回落到共享的 `default` 状态目录。

## 依据：领域成熟做法

同类工具里"命令作用于哪个项目"的判定只有两种成熟形态：

1. **项目发现型**（git、cargo）：从当前目录向上走找标记文件（`.git`、`Cargo.toml`），显式旗标（`-C`/`--git-dir`、`--manifest-path`）优先；找不到时**硬错误**——`fatal: not a git repository (or any of the parent directories)`。没有任何"全局默认仓库"。
2. **守护进程指向型**（docker、kubectl）：环境变量 → 显式旗标 → 众所周知的配置文件。它们指向的是一台守护进程/集群，不是一个项目。

ah 的 `start`/`ps`/`ask`/`stop` 都是项目级命令，适用形态 1。现状的 `default` 回落属于两种形态都不是的产物，且已造成实测事故：一个全新项目 `ah start` 报 `AGENT_ALREADY_EXISTS`，因为**另一个项目**的同名 agent 已在共享的 default 库里（#46）；`--config ah.toml`（裸文件名）哈希到空字符串，所有这样调用的项目共享 `e3b0c442`（#43）；`events` 与其他命令解析结果不一致（#15）。

## 关键设计决定

1. **一个解析函数。** `state_layout::resolve_cli_state_layout(cwd, config_path)` 是 CLI 侧唯一的项目身份判定，返回 `Result`。`rpc_client` 的 socket 解析、以及由它服务的全部命令共用它。`ah events` 原有的正确行为（绝对化 + 向上找）即由此函数统一提供。
2. **显式路径先绝对化、必须存在。** 相对 `--config` 以当前目录绝对化；指向的文件或目录不存在即报错。空字符串永远到不了哈希函数——`e3b0c442` 这一类共享目录从机制上不可能再产生。
3. **无显式路径就向上找。** 从当前目录逐级向上找 `ah.toml`（README 一直如此描述，此前是死代码）。`AH_CONFIG_PATH` 环境变量视同 `--config`，与 `ah events` 既有行为一致。
4. **找不到就报错，不回落。** git 式错误：`no ah.toml found in <cwd> or any parent directory; cd into a project or pass --config <path>`。环境覆盖（`AH_STATE_DIR`、`XDG_STATE_HOME`）与 `AH_ENV=dev` 的优先级保持不变。
5. **非项目命令不受解析约束。** `version`、`reclaim`、`setup`、`config validate`、`bundle`、`internal-bridge`、带显式 `--socket`/`AH_SOCKET` 的 `agent notify` 在任何目录都必须能跑；解析失败只在真正需要栈的命令上出现。
6. **守护进程侧不动。** `ahd` 由 `ah start` 以显式 `AH_STATE_DIR` 拉起，保留其原有解析与告警回落；本决议只修 CLI 侧的寻址。

## 迁移

`~/.local/state/ah/default` 与 `~/.local/state/ah/e3b0c442` 里的存量栈不被移动、不被删除；新版本解析到各自项目哈希后，旧目录成为死栈，由 `ah reclaim` 收走（决议 0004 D3）。CHANGELOG 记录该行为变更。

## 验收判据

1. 同一项目内，`--config ah.toml`、`--config /绝对路径/ah.toml`、不带 `--config`（在项目根或其任何子目录）解析到同一个状态目录。
2. 指向不存在路径的 `--config` 报错并指出路径；任何输入都不可能得到空字符串哈希 `e3b0c442`。
3. 在任何项目之外运行项目级命令，得到指明 cwd 与补救方式的错误，而不是 `default` 下的空栈。
4. `ah reclaim`、`ah version`、`ah setup`、`ah config validate` 在任何项目之外照常工作。
5. 上述行为由自动化测试覆盖，并在真实机器上以新旧版本同命令对比复核。
