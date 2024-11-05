""" XiaoYuan Key Metrics Model."""

# pylint: disable=unused-argument

from typing import Any, Dict, List, Optional, Literal
from warnings import warn

import pandas as pd
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.key_metrics import (
    KeyMetricsData,
    KeyMetricsQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field

from openbb_xiaoyuan.utils.references import (
    get_report_month,
    get_query_finance_sql,
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
    get_specific_daily_sql,
)


class XiaoYuanKeyMetricsQueryParams(KeyMetricsQueryParams):
    """
    XiaoYuan Key Metrics Query.
    """

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "period": {
            "choices": ["annual", "ytd"],
        },
    }

    period: Literal["annual", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanKeyMetricsData(KeyMetricsData):
    """XiaoYuan Key Metrics Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "eps_ttm": "每股收益EPSTTM（元）",
        "working_capital": "营运资本",
        "gross_margin": "毛利",
        "ebit": "息税前利润",
        "free_cash_flow_to_firm": "企业自由现金流量",
        "eps": "每股收益",
        "inventory_turnover": "存货周转率",
        "days_of_inventory_on_hand": "存货周转天数",
        "receivables_turnover": "应收账款周转率（含应收票据）",
        "days_sales_outstanding": "应收账款周转天数（含应收票据）",
        "payables_turnover": "应付账款周转率",
        "days_payables_outstanding": "应付账款周转天数（含应付票据）",
        "return_on_equity": "净资产收益率ROE（摊薄）（百分比）",
        "return_on_assets": "总资产净利率ROA（百分比）",
        "return_on_invested_capital": "投入资本回报率ROIC（百分比）",
        "current_ratio": "流动比率",
        "quick_ratio": "速动比率",
        "ebitda": "息税折旧摊销前利润",
        "market_cap": "总市值",
        "pe_ratio": "市盈率（静态）",
        "price_to_book": "市净率（静态）",
        "dividend_yield": "股息率",
    }
    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))

    eps_ttm: Optional[float] = Field(description="Eps ttm.", default=None)
    working_capital: Optional[float] = Field(
        description="Working capital.", default=None
    )
    gross_margin: Optional[float] = Field(description="Gross margin.", default=None)
    ebit: Optional[float] = Field(description="Ebit.", default=None)
    free_cash_flow_to_firm: Optional[float] = Field(
        description="Free cash flow to firm.", default=None
    )
    eps: Optional[float] = Field(description="Eps.", default=None)
    inventory_turnover: Optional[float] = Field(
        description="Inventory turnover.", default=None
    )
    days_of_inventory_on_hand: Optional[float] = Field(
        description="Days of inventory on hand.", default=None
    )
    receivables_turnover: Optional[float] = Field(
        description="Receivables turnover.", default=None
    )
    days_sales_outstanding: Optional[float] = Field(
        description="Days sales outstanding.", default=None
    )
    payables_turnover: Optional[float] = Field(
        description="Payables turnover.", default=None
    )
    days_payables_outstanding: Optional[float] = Field(
        description="Days payables outstanding.", default=None
    )
    return_on_equity: Optional[float] = Field(
        description="Return on equity.", default=None
    )
    return_on_assets: Optional[float] = Field(
        description="Return on assets.", default=None
    )
    return_on_invested_capital: Optional[float] = Field(
        description="Return on invested capital.", default=None
    )
    current_ratio: Optional[float] = Field(description="Current ratio.", default=None)
    quick_ratio: Optional[float] = Field(description="Quick ratio.", default=None)
    ebitda: Optional[float] = Field(description="Ebitda.", default=None)
    market_cap: Optional[float] = Field(description="Market cap.", default=None)
    pe_ratio: Optional[float] = Field(description="Pe ratio.", default=None)
    price_to_book: Optional[float] = Field(description="Price to book.", default=None)
    dividend_yield: Optional[float] = Field(description="Dividend yield.", default=None)


class XiaoYuanKeyMetricsFetcher(
    Fetcher[
        XiaoYuanKeyMetricsQueryParams,
        List[XiaoYuanKeyMetricsData],
    ]
):
    """Transform the query, extract and transform the data from the  XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanKeyMetricsQueryParams:
        """Transform the query params."""

        if params.get("period") is not None and params.get("period") != "annual":
            warn(
                "The period parameter is not available for this  XiaoYuan endpoint, it will be ignored."
            )
        return XiaoYuanKeyMetricsQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanKeyMetricsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the  XiaoYuan endpoint."""
        from jinniuai_data_store.reader import get_jindata_reader

        factors = [
            "每股收益EPSTTM（元）",
            "营运资本",
            "毛利",
            "息税前利润",
            "企业自由现金流量",
            "每股收益",
            "存货周转率",
            "存货周转天数",
            "应收账款周转率（含应收票据）",
            "应收账款周转天数（含应收票据）",
            "应付账款周转率",
            "应付账款周转天数（含应付票据）",
            "净资产收益率ROE（摊薄）（百分比）",
            "总资产净利率ROA（百分比）",
            "投入资本回报率ROIC（百分比）",
            "流动比率",
            "速动比率",
            "息税折旧摊销前利润",
            "总市值",
            "市盈率（静态）",
            "市净率（静态）",
            "股息率",
        ]
        reader = get_jindata_reader()
        symbols = query.symbol.split(",")
        stock_listing_info = reader.get_stocks().symbol.tolist()
        symbols = [s for s in symbols if s in stock_listing_info]
        if not symbols:
            raise EmptyDataError()
        report_month = get_report_month(query.period, -query.limit)
        finance_sql = get_query_finance_sql(factors, symbols, report_month)
        df = reader._run_query(
            script=extractMonthDayFromTime + getFiscalQuarterFromTime + finance_sql,
        )
        df = df.sort_values(by=["报告期"])
        if df is None or df.empty:
            raise EmptyDataError()
        date_list = df["报告期"].tolist()
        date_list = [
            reader.get_adjacent_trade_day(i, -1).strftime("%Y.%m.%d") for i in date_list
        ]

        daily_sql = get_specific_daily_sql(factors, symbols, date_list)
        df_daily = reader._run_query(daily_sql)
        df = pd.merge_asof(
            df,
            df_daily,
            left_on=["报告期"],
            right_on=["timestamp"],
            direction="backward",
        )
        # 删除不必要的列
        df = df.drop(columns=["timestamp_y", "symbol_y"])
        df = df.rename(columns={"timestamp_x": "timestamp", "symbol_x": "symbol"})
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df.sort_values(by="报告期", ascending=False, inplace=True)
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
        query: XiaoYuanKeyMetricsQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanKeyMetricsData]:
        """Validate and transform the data."""

        # Sort the results by the order of the symbols in the query.
        symbols = query.symbol.split(",")
        data = sorted(
            data,
            key=(
                lambda item: (
                    symbols.index(item["symbol"])
                    if item["symbol"] in symbols
                    else len(symbols)
                )
            ),
        )

        results: List[XiaoYuanKeyMetricsData] = []
        for item in data:

            if item.get("总市值") is None or isinstance(item.get("总市值"), dict):
                warn(f"Symbol Error: No data found for {item.get('symbol')}")
                continue

            for key, value in item.copy().items():
                # A bad response in a field will return a dict here, so we remove it.
                if isinstance(value, dict):
                    _ = item.pop(key)

            results.append(XiaoYuanKeyMetricsData.model_validate(item))

        return results
