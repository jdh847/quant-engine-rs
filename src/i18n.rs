#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    En,
    Zh,
    Ja,
}

impl Language {
    pub fn from_tag(tag: &str) -> Self {
        match tag.to_ascii_lowercase().as_str() {
            "zh" | "zh-cn" | "zh-hans" => Self::Zh,
            "ja" | "jp" | "ja-jp" => Self::Ja,
            _ => Self::En,
        }
    }

    pub fn html_lang(self) -> &'static str {
        match self {
            Language::En => "en",
            Language::Zh => "zh-CN",
            Language::Ja => "ja",
        }
    }
}

pub struct DashboardText {
    pub title: &'static str,
    pub subtitle: &'static str,
    pub generated_from: &'static str,
    pub overview: &'static str,
    pub series: &'static str,
    pub equity: &'static str,
    pub cash: &'static str,
    pub gross_exposure: &'static str,
    pub net_exposure: &'static str,
    pub equity_curve: &'static str,
    pub run_summary: &'static str,
    pub kpi_start_equity: &'static str,
    pub kpi_end_equity: &'static str,
    pub kpi_pnl: &'static str,
    pub kpi_pnl_ratio: &'static str,
    pub kpi_max_drawdown: &'static str,
    pub kpi_cagr: &'static str,
    pub kpi_sharpe: &'static str,
    pub kpi_trades: &'static str,
    pub kpi_rejections: &'static str,
    pub recent_trades: &'static str,
    pub filters: &'static str,
    pub all: &'static str,
    pub search: &'static str,
    pub date: &'static str,
    pub market: &'static str,
    pub symbol: &'static str,
    pub side: &'static str,
    pub qty: &'static str,
    pub price: &'static str,
    pub fees: &'static str,
    pub rejections: &'static str,
    pub reason: &'static str,
    pub factors: &'static str,
    pub avg_selected_symbols: &'static str,
    pub start: &'static str,
    pub end: &'static str,
    pub buy: &'static str,
    pub sell: &'static str,
    pub live_on: &'static str,
    pub live_fallback: &'static str,
    pub live_init: &'static str,
}

pub fn dashboard_text(lang: Language) -> DashboardText {
    match lang {
        Language::Zh => DashboardText {
            title: "私人量化机器人",
            subtitle: "模拟盘仪表盘（Rust）",
            generated_from: "由本地输出自动生成",
            overview: "总览",
            series: "序列",
            equity: "权益",
            cash: "现金",
            gross_exposure: "总敞口",
            net_exposure: "净敞口",
            equity_curve: "权益曲线",
            run_summary: "运行摘要",
            kpi_start_equity: "起始权益",
            kpi_end_equity: "结束权益",
            kpi_pnl: "盈亏",
            kpi_pnl_ratio: "收益率",
            kpi_max_drawdown: "最大回撤",
            kpi_cagr: "CAGR",
            kpi_sharpe: "夏普",
            kpi_trades: "成交数",
            kpi_rejections: "拒单数",
            recent_trades: "最近成交",
            filters: "筛选",
            all: "全部",
            search: "搜索",
            date: "日期",
            market: "市场",
            symbol: "代码",
            side: "方向",
            qty: "数量",
            price: "价格",
            fees: "费用",
            rejections: "拒单",
            reason: "原因",
            factors: "因子",
            avg_selected_symbols: "平均入选数",
            start: "起始",
            end: "结束",
            buy: "买入",
            sell: "卖出",
            live_on: "实时刷新: 开",
            live_fallback: "实时刷新: 回退",
            live_init: "实时刷新: 初始化",
        },
        Language::Ja => DashboardText {
            title: "プライベート量的ボット",
            subtitle: "ペーパー取引ダッシュボード（Rust）",
            generated_from: "ローカル出力から生成",
            overview: "概要",
            series: "系列",
            equity: "エクイティ",
            cash: "現金",
            gross_exposure: "総エクスポージャー",
            net_exposure: "純エクスポージャー",
            equity_curve: "エクイティ曲線",
            run_summary: "実行サマリー",
            kpi_start_equity: "開始エクイティ",
            kpi_end_equity: "終了エクイティ",
            kpi_pnl: "損益",
            kpi_pnl_ratio: "リターン",
            kpi_max_drawdown: "最大DD",
            kpi_cagr: "CAGR",
            kpi_sharpe: "シャープ",
            kpi_trades: "約定数",
            kpi_rejections: "拒否数",
            recent_trades: "最近の約定",
            filters: "フィルター",
            all: "すべて",
            search: "検索",
            date: "日付",
            market: "市場",
            symbol: "銘柄",
            side: "売買",
            qty: "数量",
            price: "価格",
            fees: "手数料",
            rejections: "拒否",
            reason: "理由",
            factors: "ファクター",
            avg_selected_symbols: "平均採用数",
            start: "開始",
            end: "終了",
            buy: "買い",
            sell: "売り",
            live_on: "ライブ更新: ON",
            live_fallback: "ライブ更新: fallback",
            live_init: "ライブ更新: init",
        },
        Language::En => DashboardText {
            title: "Private Quant Bot",
            subtitle: "Paper Trading Dashboard (Rust)",
            generated_from: "Generated from local outputs",
            overview: "Overview",
            series: "Series",
            equity: "Equity",
            cash: "Cash",
            gross_exposure: "Gross Exposure",
            net_exposure: "Net Exposure",
            equity_curve: "Equity Curve",
            run_summary: "Run Summary",
            kpi_start_equity: "Start Equity",
            kpi_end_equity: "End Equity",
            kpi_pnl: "PnL",
            kpi_pnl_ratio: "PnL %",
            kpi_max_drawdown: "Max DD",
            kpi_cagr: "CAGR",
            kpi_sharpe: "Sharpe",
            kpi_trades: "Trades",
            kpi_rejections: "Rejections",
            recent_trades: "Recent Trades",
            filters: "Filters",
            all: "All",
            search: "Search",
            date: "Date",
            market: "Market",
            symbol: "Symbol",
            side: "Side",
            qty: "Qty",
            price: "Price",
            fees: "Fees",
            rejections: "Rejections",
            reason: "Reason",
            factors: "Factors",
            avg_selected_symbols: "Avg Selected",
            start: "Start",
            end: "End",
            buy: "BUY",
            sell: "SELL",
            live_on: "Live refresh: on",
            live_fallback: "Live refresh: fallback",
            live_init: "Live refresh: init",
        },
    }
}

pub fn msg_run_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "运行完成",
        Language::Ja => "実行完了",
        Language::En => "run completed",
    }
}

pub fn msg_dashboard(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "仪表盘",
        Language::Ja => "ダッシュボード",
        Language::En => "dashboard",
    }
}

pub fn msg_demo_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "Demo 完成",
        Language::Ja => "デモ完了",
        Language::En => "demo completed",
    }
}

pub fn msg_open_dashboard_hint(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "用浏览器打开（macOS 可用 open 命令）",
        Language::Ja => "ブラウザで開く（macOS は open コマンド）",
        Language::En => "open in a browser (macOS: open command)",
    }
}

pub fn msg_server_started(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "本地服务已启动",
        Language::Ja => "ローカルサーバー起動",
        Language::En => "server started",
    }
}

pub fn msg_server_root(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "根目录",
        Language::Ja => "ルート",
        Language::En => "root",
    }
}

pub fn msg_server_url(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "地址",
        Language::Ja => "URL",
        Language::En => "url",
    }
}

pub fn msg_server_ctrl_c(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "按 Ctrl+C 停止",
        Language::Ja => "Ctrl+C で停止",
        Language::En => "Press Ctrl+C to stop",
    }
}

pub fn msg_walk_forward_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "滚动优化完成",
        Language::Ja => "ウォークフォワード最適化完了",
        Language::En => "walk-forward completed",
    }
}

pub fn msg_research_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "研究完成",
        Language::Ja => "リサーチ完了",
        Language::En => "research completed",
    }
}

pub fn msg_benchmark_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "基准测试完成",
        Language::Ja => "ベンチマーク完了",
        Language::En => "benchmark completed",
    }
}

pub fn msg_replay_completed(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "事件回放导出完成",
        Language::Ja => "イベントリプレイ出力完了",
        Language::En => "replay completed",
    }
}

#[cfg(test)]
mod tests {
    use super::{dashboard_text, Language};

    #[test]
    fn from_tag_maps_supported_languages() {
        assert_eq!(Language::from_tag("en"), Language::En);
        assert_eq!(Language::from_tag("zh"), Language::Zh);
        assert_eq!(Language::from_tag("zh-CN"), Language::Zh);
        assert_eq!(Language::from_tag("ja"), Language::Ja);
        assert_eq!(Language::from_tag("jp"), Language::Ja);
    }

    #[test]
    fn dashboard_text_has_expected_localized_markers() {
        assert_eq!(dashboard_text(Language::En).equity_curve, "Equity Curve");
        assert_eq!(dashboard_text(Language::Zh).equity_curve, "权益曲线");
        assert_eq!(dashboard_text(Language::Ja).equity_curve, "エクイティ曲線");
        assert_eq!(dashboard_text(Language::En).overview, "Overview");
    }
}
