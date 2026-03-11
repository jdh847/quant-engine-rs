# プライベート量的ボット（Rust / ペーパー取引専用）

個人トレーダー向けのマルチマーケット量的取引ボットです。デフォルトはペーパー取引のみです。

ドキュメント:

- データと回測の信頼性: `docs/DATA.md`
- チュートリアル: `docs/TUTORIALS.md`
- ペーパー専用セキュリティモデル: `docs/SECURITY_PAPER_ONLY.md`
- プラグイン SDK: `docs/PLUGIN_SDK.md`
- OSS 公開チェックリスト: `docs/OPEN_SOURCE_RELEASE_CHECKLIST.md`

## 安全性と免責

- デフォルトはペーパー取引のみ。実運用の売買システムではありません。
- 投資助言ではありません。
- もし PAT やブローカー認証情報をチャット/ログに貼った場合: 直ちに revoke/ローテーションしてください。

License: MIT OR Apache-2.0（[LICENSE](LICENSE) 参照）。

## 主要機能

- マルチファクター戦略（モメンタム、平均回帰、低ボラ、出来高）
- 戦略プラグインレジストリ（`layered_multi_factor` / `momentum_guard`）
- プラグイン雛形コマンド（`cargo run -- scaffold-plugin --id your_plugin --output-dir plugins`）
- ロバストネス評価コマンド（`robustness`: OOS 安定性 / PBO proxy）
- データ品質チェックコマンド（`validate-data`）
- デーモン運用コマンド（`paper-daemon`: 状態スナップショット / ドローダウン警告）
- 実行レジストリ（`run_registry.csv/json/md` を各コマンドで自動更新）
- ターミナル制御センター（`control-center`: ライブ更新）
- 公開ランキング生成（`leaderboard`: registry/benchmark/research を統合）
- Strategy Plugin SDK（`sdk-init` / `sdk-check`）
- SDK 自動登録（`sdk-register`; 登録後に `--strategy-plugin <id>` で実行可能）
- ポートフォリオ最適化（`risk_parity` / `hrp`）+ 回転率制約
- クロスマーケットリスク管理（USD/CNY/JPY 通貨エクスポージャー）
- 任意のライブ FX（失敗時は静的 FX にフォールバック）
- 米国株 / 中国A株 / 日本株
- CLI とダッシュボードの多言語対応（英語・中国語・日本語）

## クイックスタート

```bash
# ワンコマンドのデモ（outputs_rust/demo/run_<timestamp>/dashboard.html を生成）
cargo run -- demo --config config/bot.toml --lang ja

# macOS: 最新デモの dashboard を開く
open "$(cat outputs_rust/demo/LATEST_DASHBOARD.txt)"

# `file://` の fetch がブロックされる場合は localhost で提供:
cargo run -- serve --root outputs_rust/demo --bind 127.0.0.1:8787 --lang ja
# 次を開く: http://127.0.0.1:8787/

cargo run -- run --config config/bot.toml --output-dir outputs_rust --lang ja

# 任意: 実行時に戦略プラグインを上書き
cargo run -- run --config config/bot.toml --output-dir outputs_rust --strategy-plugin momentum_guard
```

ダッシュボードのみ生成：

```bash
cargo run -- dashboard --output-dir outputs_rust --lang ja
```

戦略プラグイン一覧を表示:

```bash
cargo run -- plugins
```

環境とデータの動作確認:

```bash
cargo run -- doctor --config config/bot.toml
```

戦略プラグインの雛形を生成:

```bash
cargo run -- scaffold-plugin --id value_quality --output-dir plugins
```

SDK プラグインパッケージを生成:

```bash
cargo run -- sdk-init --id alpha_world --output-dir plugins_sdk
```

SDK パッケージ構造を検証:

```bash
cargo run -- sdk-check --package-dir plugins_sdk/alpha_world
```

SDK プラグインをランタイムへ登録:

```bash
cargo run -- sdk-register --package-dir plugins_sdk/alpha_world --name "Alpha World"
```

ロバストネス評価:

```bash
cargo run -- robustness --config config/bot.toml --output-dir outputs_rust/robustness
```

実行レジストリ Top を表示:

```bash
cargo run -- registry --output-dir outputs_rust --top 20
```

ターミナル制御センター:

```bash
cargo run -- control-center --output-dir outputs_rust --refresh-secs 2 --cycles 30
```

公開ランキング生成:

```bash
cargo run -- leaderboard --output-dir outputs_rust --top 50
```

`risk_parity` と `hrp` を同時比較するリサーチ実行：

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
  --lang ja
```

英語ドキュメント: [README.md](README.md)
