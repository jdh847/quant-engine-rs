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
    pub equity_curve: &'static str,
    pub run_summary: &'static str,
    pub recent_trades: &'static str,
    pub date: &'static str,
    pub market: &'static str,
    pub symbol: &'static str,
    pub side: &'static str,
    pub qty: &'static str,
    pub price: &'static str,
    pub start: &'static str,
    pub end: &'static str,
    pub buy: &'static str,
    pub sell: &'static str,
}

pub fn dashboard_text(lang: Language) -> DashboardText {
    match lang {
        Language::Zh => DashboardText {
            title: "私人量化机器人",
            subtitle: "模拟盘仪表盘（Rust）",
            generated_from: "由本地输出自动生成",
            equity_curve: "权益曲线",
            run_summary: "运行摘要",
            recent_trades: "最近成交",
            date: "日期",
            market: "市场",
            symbol: "代码",
            side: "方向",
            qty: "数量",
            price: "价格",
            start: "起始",
            end: "结束",
            buy: "买入",
            sell: "卖出",
        },
        Language::Ja => DashboardText {
            title: "プライベート量的ボット",
            subtitle: "ペーパー取引ダッシュボード（Rust）",
            generated_from: "ローカル出力から生成",
            equity_curve: "エクイティ曲線",
            run_summary: "実行サマリー",
            recent_trades: "最近の約定",
            date: "日付",
            market: "市場",
            symbol: "銘柄",
            side: "売買",
            qty: "数量",
            price: "価格",
            start: "開始",
            end: "終了",
            buy: "買い",
            sell: "売り",
        },
        Language::En => DashboardText {
            title: "Private Quant Bot",
            subtitle: "Paper Trading Dashboard (Rust)",
            generated_from: "Generated from local outputs",
            equity_curve: "Equity Curve",
            run_summary: "Run Summary",
            recent_trades: "Recent Trades",
            date: "Date",
            market: "Market",
            symbol: "Symbol",
            side: "Side",
            qty: "Qty",
            price: "Price",
            start: "Start",
            end: "End",
            buy: "BUY",
            sell: "SELL",
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
    }
}
