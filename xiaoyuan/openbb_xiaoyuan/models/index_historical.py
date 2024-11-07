"""XiaoYuan Index Historical Price Model."""

# pylint: disable=unused-argument

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from dateutil.relativedelta import relativedelta
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_historical import (
    IndexHistoricalData,
    IndexHistoricalQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field


class XiaoYuanIndexHistoricalQueryParams(IndexHistoricalQueryParams):
    """XiaoYuan Index Historical Price Query.

    Source: https://financialmodelingprep.com/developer/docs/#Stock-Historical-Price
    """

    __alias_dict__ = {"start_date": "from", "end_date": "to"}
    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": ["1d"]},
    }

    interval: Literal["1d"] = Field(default="1d", description="only return daily data")


class XiaoYuanIndexHistoricalData(IndexHistoricalData):
    """XiaoYuan Index Historical Price Data."""

    __alias_dict__ = {
        "date": "timestamp",
        "open": "开盘价",
        "close": "收盘价",
        "high": "最高价",
        "low": "最低价",
        "volume": "成交量",
    }

    change: Optional[float] = Field(
        default=None,
        description="Change in the price from the previous close.",
    )
    change_percent: Optional[float] = Field(
        default=None,
        description="Change in the price from the previous close, as a normalized percent.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    turnover: Optional[float] = Field(
        default=None,
        description="The turnover the total value of a stock's trades to its market value.",
    )


class XiaoYuanIndexHistoricalFetcher(
    Fetcher[
        XiaoYuanIndexHistoricalQueryParams,
        List[XiaoYuanIndexHistoricalData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanIndexHistoricalQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        transformed_params = params

        now = datetime.now().date()
        if params.get("start_date") is None:
            transformed_params["start_date"] = now - relativedelta(years=1)

        if params.get("end_date") is None:
            transformed_params["end_date"] = now

        return XiaoYuanIndexHistoricalQueryParams(**transformed_params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanIndexHistoricalQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanIndexHistoricalData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()

        historical_start = reader.convert_to_db_date_format(
            reader.get_adjacent_trade_day(query.start_date, -1)
        )
        historical_end = reader.convert_to_db_date_format(query.end_date)

        symbols_list = query.symbol.split(",")

        factors = list(XiaoYuanIndexHistoricalData.__alias_dict__.values())
        factors.remove(XiaoYuanIndexHistoricalData.__alias_dict__["date"])

        historical_sql = f"""
            use mytt
            t = select timestamp, symbol, factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} 
            and timestamp between {historical_start} 
            and {historical_end} 
            and symbol in {symbols_list};
            t = select value from t pivot by timestamp, symbol, factor_name;
            update t set ref_close = REF({XiaoYuanIndexHistoricalData.__alias_dict__["close"]}, 1) context by symbol;
            update t set change = {XiaoYuanIndexHistoricalData.__alias_dict__["close"]} - ref_close context by symbol;
            update t set changeOverTime = change / ref_close  context by symbol;
            select * from t where timestamp > {reader.convert_to_db_date_format(query.start_date)};
        """
        df = reader._run_query(
            script=historical_sql,
        )
        if df is None or df.empty:
            raise EmptyDataError()
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanIndexHistoricalQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanIndexHistoricalData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)

        return [XiaoYuanIndexHistoricalData.model_validate(d) for d in data]
