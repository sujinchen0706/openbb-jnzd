"""XiaoYuan Equity Historical Price Model."""

# pylint: disable=unused-argument

from dateutil.relativedelta import relativedelta
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
)
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import QUERY_DESCRIPTIONS
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field


class XiaoYuanEquityHistoricalQueryParams(EquityHistoricalQueryParams):
    """XiaoYuan Equity Historical Price Query.

    Source: https://financialmodelingprep.com/developer/docs/#Stock-Historical-Price
    """

    __alias_dict__ = {"start_date": "from", "end_date": "to"}
    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": ["1d"]},
    }

    interval: Literal["1d"] = Field(
        default="1d", description=QUERY_DESCRIPTIONS.get("interval", "")
    )


class XiaoYuanEquityHistoricalData(EquityHistoricalData):
    """XiaoYuan Equity Historical Price Data."""

    __alias_dict__ = {
        "date": "timestamp",
        "open": "开盘价（不复权）",
        "close": "收盘价（不复权）",
        "high": "最高价（不复权）",
        "low": "最低价（不复权）",
        "volume": "成交量（不复权）",
        "adj_close": "收盘价（前复权）",
    }
    adj_close: Optional[float] = Field(
        default=None, description=DATA_DESCRIPTIONS.get("adj_close", "")
    )
    change: Optional[float] = Field(
        default=None,
        description="Change in the price from the previous close.",
    )
    change_percent: Optional[float] = Field(
        default=None,
        description="Change in the price from the previous close, as a normalized percent.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )


class XiaoYuanEquityHistoricalFetcher(
    Fetcher[
        XiaoYuanEquityHistoricalQueryParams,
        List[XiaoYuanEquityHistoricalData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanEquityHistoricalQueryParams:
        """Transform the query params."""
        transformed_params = params

        now = datetime.now().date()
        if params.get("start_date") is None:
            transformed_params["start_date"] = now - relativedelta(years=1)

        if params.get("end_date") is None:
            transformed_params["end_date"] = now

        return XiaoYuanEquityHistoricalQueryParams(**transformed_params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanEquityHistoricalQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanEquityHistoricalData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()

        historical_start = reader.convert_to_db_date_format(
            reader.get_adjacent_trade_day(query.start_date, -1)
        )
        historical_end = reader.convert_to_db_date_format(query.end_date)

        symbols_list = query.symbol.split(",")

        factors = list(XiaoYuanEquityHistoricalData.__alias_dict__.values())
        factors.remove(XiaoYuanEquityHistoricalData.__alias_dict__["date"])

        historical_sql = f"""
            use mytt
            t = select timestamp, symbol, factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} 
            and timestamp between {historical_start} 
            and {historical_end} 
            and symbol in {symbols_list};

            t = select value from t pivot by timestamp, symbol, factor_name;
            update t set ref_close = REF({XiaoYuanEquityHistoricalData.__alias_dict__["close"]}, 1) context by symbol;
            update t set change = {XiaoYuanEquityHistoricalData.__alias_dict__["close"]} - ref_close context by symbol;
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
        query: XiaoYuanEquityHistoricalQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanEquityHistoricalData]:
        """Return the transformed data."""

        return [XiaoYuanEquityHistoricalData.model_validate(d) for d in data]
