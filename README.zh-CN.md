# 私人量化交易机器人（Rust，仅测试盘）

这是一个面向个人交易者的多市场量化机器人，默认仅支持模拟盘（Paper Trading）。

## 当前能力

- 多因子策略（动量、均值回归、低波动、量能）
- 策略插件注册表（`layered_multi_factor` / `momentum_guard`）
- 插件脚手架命令（`cargo run -- scaffold-plugin --id your_plugin --output-dir plugins`）
- 稳健性评估命令（`robustness`，含 OOS 稳定性与 PBO 代理指标）
- 数据质量检查命令（`validate-data`）
- 守护进程命令（`paper-daemon`，含状态快照与回撤告警）
- 运行注册表（`run_registry.csv/json/md`，每次命令自动追踪）
- 终端控制中心（`control-center`，可实时刷新）
- 公共排行榜构建（`leaderboard`，聚合 registry/benchmark/research）
- Strategy Plugin SDK（`sdk-init`/`sdk-check`）
- SDK 自动注册（`sdk-register`，注册后可直接用 `--strategy-plugin <id>`）
- 组合优化（`risk_parity` / `hrp`）+ 换手约束
- 跨市场风控（USD/CNY/JPY 货币敞口）
- 可选实时 FX（失败自动回退静态汇率）
- 支持美股 / A 股 / 日股
- 支持中英日三语 CLI 与 Dashboard

## 快速开始

```bash
# 一条命令跑 Demo（生成 outputs_rust/demo/run_<timestamp>/dashboard.html）
cargo run -- demo --config config/bot.toml --lang zh

# macOS：打开最近一次 Demo 的 dashboard
open "$(cat outputs_rust/demo/LATEST_DASHBOARD.txt)"

cargo run -- run --config config/bot.toml --output-dir outputs_rust --lang zh

# 可选：强制切换策略插件
cargo run -- run --config config/bot.toml --output-dir outputs_rust --strategy-plugin momentum_guard
```

只生成仪表盘：

```bash
cargo run -- dashboard --output-dir outputs_rust --lang zh
```

查看策略插件目录：

```bash
cargo run -- plugins
```

生成策略插件脚手架：

```bash
cargo run -- scaffold-plugin --id value_quality --output-dir plugins
```

生成 SDK 插件包：

```bash
cargo run -- sdk-init --id alpha_world --output-dir plugins_sdk
```

校验 SDK 插件包结构：

```bash
cargo run -- sdk-check --package-dir plugins_sdk/alpha_world
```

注册 SDK 插件到运行时：

```bash
cargo run -- sdk-register --package-dir plugins_sdk/alpha_world --name "Alpha World"
```

稳健性评估：

```bash
cargo run -- robustness --config config/bot.toml --output-dir outputs_rust/robustness
```

查看运行注册表 Top：

```bash
cargo run -- registry --output-dir outputs_rust --top 20
```

终端控制中心：

```bash
cargo run -- control-center --output-dir outputs_rust --refresh-secs 2 --cycles 30
```

生成公共排行榜：

```bash
cargo run -- leaderboard --output-dir outputs_rust --top 50
```

研究命令（同时比较 `risk_parity` 与 `hrp`）：

```bash
cargo run -- research \
  --config config/bot.toml \
  --output-dir outputs_rust/research \
  --markets US,A,JP \
  --short-windows 3 \
  --long-windows 7 \
  --vol-windows 5 \
  --top-ns 1,2 \
  --min-momentums=-0.01,0.0 \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp \
  --lang zh
```

英文文档见 [README.md](README.md)。
