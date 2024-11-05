"""XiaoYuan Equity Valuation Multiples Model."""

from typing import Any, Dict, List, Optional

import pandas as pd
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_valuation_multiples import (
    EquityValuationMultiplesData,
    EquityValuationMultiplesQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field

from openbb_xiaoyuan.utils.references import (
    get_specific_daily_sql,
    get_recent_1q_query_finance_sql,
)


# pylint: disable=unused-argument


class XiaoYuanEquityValuationMultiplesQueryParams(EquityValuationMultiplesQueryParams):
    """XiaoYuan Equity Valuation Multiples Query."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class XiaoYuanEquityValuationMultiplesData(EquityValuationMultiplesData):
    """XiaoYuan Equity Valuation Multiples Data."""

    __alias_dict__ = {
        "roic_ttm": "投入资本回报率ROIC（TTM）（百分比）",
        "pe_ratio_ttm": "市盈率（滚动）",
        "price_to_sales_ratio_ttm": "市销率（滚动）",
    }
    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))
    roic_ttm: Optional[float] = Field(description="Roic ttm", default=None)
    pe_ratio_ttm: Optional[float] = Field(description="Pe ratio ttm", default=None)
    price_to_sales_ratio_ttm: Optional[float] = Field(
        description="Price to sales ratio ttm", default=None
    )


class XiaoYuanEquityValuationMultiplesFetcher(
    Fetcher[
        XiaoYuanEquityValuationMultiplesQueryParams,
        List[XiaoYuanEquityValuationMultiplesData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanEquityValuationMultiplesQueryParams:
        """Transform the query params."""
        return XiaoYuanEquityValuationMultiplesQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanEquityValuationMultiplesQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        from jinniuai_data_store.reader import get_jindata_reader

        symbols = query.symbol.split(",")
        factors = [
            "市盈率（滚动）",
            "市销率（滚动）",
            "投入资本回报率ROIC（TTM）（百分比）",
        ]
        reader = get_jindata_reader()
        stock_listing_info = reader.get_stocks().symbol.tolist()
        symbols = [s for s in symbols if s in stock_listing_info]
        if not symbols:
            raise EmptyDataError()
        # 获取当前时间
        cur_date = pd.Timestamp.now().strftime("%Y.%m.%d")
        # 获取最近一个报告期的财务数据
        df_sql = get_recent_1q_query_finance_sql(
            factors, symbols, reader.convert_to_db_date_format(cur_date)
        )
        df = reader._run_query(df_sql)
        if df is None or df.empty:
            raise EmptyDataError()
        date_list = df["报告期"].tolist()
        date_list = [
            (
                reader.get_adjacent_trade_day(i, 0).strftime("%Y.%m.%d")
                if reader.get_adjacent_trade_day(i, 0).strftime("%Y.%m.%d") == i
                else reader.get_adjacent_trade_day(i, 1).strftime("%Y.%m.%d")
            )
            for i in date_list
        ]

        daily_sql = get_specific_daily_sql(factors, symbols, date_list)
        df_daily = reader._run_query(daily_sql)
        df = pd.merge_asof(
            df,
            df_daily,
            left_on=["报告期"],
            right_on=["timestamp"],
            direction="forward",
        )

        df = df.drop(columns=["timestamp_y", "symbol_y", "报告期", "timestamp_x"])
        df = df.rename(columns={"symbol_x": "symbol"})
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
        query: XiaoYuanEquityValuationMultiplesQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanEquityValuationMultiplesData]:
        """Return the transformed data."""
        return [XiaoYuanEquityValuationMultiplesData.model_validate(d) for d in data]
