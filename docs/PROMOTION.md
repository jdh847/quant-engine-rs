# Promotion Playbook

This doc is a lightweight playbook for promoting this repo in a way that matches the project's values:

- reproducible research
- paper-only safety by default
- auditability (hashes + manifests)
- clear disclaimers (not financial advice)

## What Worked In The Reference Post (Pattern)

- A simple milestone hook: `stars` count.
- A human motivation: learning Rust ecosystem + building a full-stack project.
- A concrete credibility claim: benchmarking / performance notes.
- A community invitation: "welcome issues" + real examples of issues found/fixed.
- Hashtags to reach the right audience.

## Our Differentiators (Use These In Copy)

- Paper-only by design; network calls are opt-in via env hard switches.
- Multi-market research: US / China A-share / Japan.
- Layered multi-factor alpha + HRP branch + turnover constraint.
- Cross-market FX exposure controls (USD/CNY/JPY caps).
- Reproducibility: dataset manifest hashing + audit snapshot hashing.
- Shareable artifacts: run `bundle` + `bundle-verify` + `bundle-extract`.
- Intuitive dashboard with i18n (EN/ZH/JA).
- Plugin SDK (strategy registry + SDK init/check/register).

## Launch Checklist (Before Posting)

- Repo is public.
- `README.md` quick start works.
- One fresh run produces:
  - `dashboard.html`
  - `audit_snapshot.json` + summary
  - `data_quality_report.csv` + summary
  - `run_bundle_*.tar.gz`
- Add at least 1 screenshot/GIF of the dashboard in README (optional but high impact).
- Pin 1 issue: "Roadmap" + "How to contribute".
- Create 5-10 labeled issues:
  - `good first issue`
  - `help wanted`
  - `bug`
  - `docs`

## Post Templates

### 中文 (适合小红书/微博/公众号短文)

标题:
> 开源 Rust 量化研究/回测系统（纸上交易）{stars}+⭐ 了

正文:
> 做了一个个人用的 Rust 量化 bot（默认 paper-only，不走实盘路径），主要为了把 Rust 生态常用库跑一遍，并把自己做工程化交易研究的流程固化下来。  
>  
> 亮点：
> - 美股 / A股 / 日股 多市场日频回测
> - 分层多因子（动量/均值回归/低波/量能）+ 行业中性化 + 去极值
> - 组合：HRP / risk parity + 换手约束
> - 跨市场 FX 敞口控制（USD/CNY/JPY 上限）
> - 可审计可复现：数据/配置哈希、audit snapshot、数据质量报告
> - 一键打包结果：bundle + verify + extract（带 SHA256 清单）
> - Dashboard 中英日三语
>  
> 说明：
> - 仅测试盘，非投资建议
> - 欢迎提 issue，尤其是数据/交易规则/日历等边界条件
>  
> Repo: <贴 GitHub 链接>

标签建议:
`#Rust #量化 #回测 #交易系统 #数据分析 #开源`

### English (X/Twitter, Reddit, HN-style)

Title:
> Open-source Rust paper-only quant research bot (US/A/JP) with auditability + reproducible bundles

Body:
> I built a personal quant bot in Rust with a strict paper-only model (no live-money path).  
> Focus: reproducible research + a practical engineering workflow.
>
> Highlights:
> - Multi-market daily backtests: US / CN A-share / JP
> - Layered multi-factor alpha (winsorization + industry neutralization)
> - Portfolio: HRP / risk parity + turnover constraint
> - Cross-market FX exposure caps (USD/CNY/JPY)
> - Audit + data quality reports (hashes, manifests)
> - Shareable artifacts: `bundle` + `bundle-verify` + `bundle-extract`
> - Intuitive dashboard with i18n (EN/ZH/JA)
>
> Looking for feedback/issues, especially on data handling and market rules.
>
> Repo: <paste GitHub link>

### 日本語 (Qiita/Zenn)

タイトル:
> Rust で作ったペーパー専用の量的リサーチ/回測ボット（US/A/JP）をOSS化しました

本文:
> Rust のエコシステムを実戦で学ぶ目的で、個人向けのペーパー取引用量的ボットを作りました（実運用の実弾売買はしない設計です）。  
>
> 特徴:
> - US / 中国A株 / 日本株のマルチマーケット日次回測
> - 分層マルチファクター + 業種中立化 + 外れ値処理
> - HRP / risk parity + 回転率制約
> - FX エクスポージャー制限（USD/CNY/JPY）
> - 監査/再現性: hash / audit snapshot / data quality
> - `bundle` で成果物をまとめて共有可能
>
> issue/PR 歓迎です（特にデータ処理や市場ルール周り）。
>
> Repo: <GitHubリンク>

## Where To Post (High Signal)

- Rust communities (EN/ZH/JA).
- Algo trading / backtesting communities (paper-only emphasis).
- Data engineering / reproducibility communities (hashes + manifests + bundles).

## What To Ask For (So People Engage)

- "Does my data-quality gate miss a real-world edge case?"
- "Any improvements to market rules (holidays, halts, limit-up/down)?"
- "Thoughts on the dashboard UX or bundle verification?"

