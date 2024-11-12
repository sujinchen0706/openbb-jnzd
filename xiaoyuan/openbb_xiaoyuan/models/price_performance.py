"""XiaoYuan Price Performance Model."""


# pylint: disable=unused-argument
from typing import Any, Dict, List, Optional
from warnings import warn

from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.recent_performance import (
    RecentPerformanceData,
    RecentPerformanceQueryParams,
)
from openbb_xiaoyuan.utils.references import convert_stock_code_format
from pydantic import Field, model_validator


class XiaoYuanPricePerformanceQueryParams(RecentPerformanceQueryParams):
    """XiaoYuan Price Performance Query.

    Source: https://site.financialmodelingprep.com/developer/docs/stock-split-calendar-api/
    """

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class XiaoYuanPricePerformanceData(RecentPerformanceData):
    """XiaoYuan Price Performance Data."""

    symbol: str = Field(description="The ticker symbol.")

    __alias_dict__ = {
        "one_day": "1D",
        "one_week": "5D",
        "one_month": "1M",
        "three_month": "3M",
        "six_month": "6M",
        "one_year": "1Y",
        "three_year": "3Y",
        "five_year": "5Y",
        "ten_year": "10Y",
    }

    @model_validator(mode="before")
    @classmethod
    def replace_zero(cls, values):  # pylint: disable=no-self-argument
        """Replace zero with None and convert percents to normalized values."""
        if isinstance(values, dict):
            for k, v in values.items():
                if k != "symbol":
                    values[k] = None if v == 0 else float(v) / 100
        return values


class XiaoYuanPricePerformanceFetcher(
    Fetcher[
        XiaoYuanPricePerformanceQueryParams,
        List[XiaoYuanPricePerformanceData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanPricePerformanceQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanPricePerformanceQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanPricePerformanceQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        reader = get_jindata_reader()
        factors = ["收盘价（不复权）"]
        symbol_list = query.symbol.split(",")

        sc = f"""
        use mytt
        t = select timestamp, symbol, factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} and symbol in {symbol_list}
            
        t = select value from t pivot by timestamp, symbol, factor_name;
        update t set one_day = (收盘价（不复权） - REF(收盘价（不复权）,1))/REF(收盘价（不复权）,1) context by symbol;
        update t set first_week = weekBegin(timestamp)
        update t set wtd = (收盘价（不复权） - REF(收盘价（不复权）,weekBegin(timestamp)))/REF(收盘价（不复权）,1) context by symbol;
        update t set a = select 收盘价（不复权） from t where t.zhou in t.timestamp
        """

        df = reader._run_query(sc)
        return df

    @staticmethod
    def transform_data(
        query: XiaoYuanPricePerformanceQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanPricePerformanceData]:
        """Return the transformed data."""

        symbols = query.symbol.upper().split(",")
        symbols = list(dict.fromkeys(symbols))
        if len(data) != len(symbols):
            data_symbols = [d["symbol"] for d in data]
            missing_symbols = [
                symbol for symbol in symbols if symbol not in data_symbols
            ]
            warn(f"Missing data for symbols: {missing_symbols}")

        return [XiaoYuanPricePerformanceData.model_validate(i) for i in data]
