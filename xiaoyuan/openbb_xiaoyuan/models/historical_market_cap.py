"""XiaoYuan Historical Market Cap Model."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.historical_market_cap import (
    HistoricalMarketCapData,
    HistoricalMarketCapQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError


class XiaoYuanHistoricalMarketCapQueryParams(HistoricalMarketCapQueryParams):
    """XiaoYuan Historical Market Cap Query.

    Source: https://site.financialmodelingprep.com/developer/docs#historical-market-cap-company-information

    """

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
    }


class XiaoYuanHistoricalMarketCapData(HistoricalMarketCapData):
    """XiaoYuan Historical Market Cap Data."""

    __alias_dict__ = {
        "date": "timestamp",
        "market_cap": "总市值",
    }


class XiaoYuanHistoricalMarketCapFetcher(
    Fetcher[
        XiaoYuanHistoricalMarketCapQueryParams,
        List[XiaoYuanHistoricalMarketCapData],
    ]
):
    """XiaoYuan Historical Market Cap Fetcher."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanHistoricalMarketCapQueryParams:
        """Transform the query params."""
        # pylint: disable=import-outside-toplevel
        from dateutil.relativedelta import relativedelta

        transformed_params = params
        now = datetime.now().date()

        if params.get("start_date") is None:
            transformed_params["start_date"] = now - relativedelta(years=5)

        if params.get("end_date") is None:
            transformed_params["end_date"] = now

        return XiaoYuanHistoricalMarketCapQueryParams(**transformed_params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanHistoricalMarketCapQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanHistoricalMarketCapData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        reader = get_jindata_reader()

        historical_start = reader.convert_to_db_date_format(query.start_date)
        historical_end = reader.convert_to_db_date_format(query.end_date)

        symbols_list = query.symbol.split(",")

        factors = list(XiaoYuanHistoricalMarketCapData.__alias_dict__.values())
        factors.remove(XiaoYuanHistoricalMarketCapData.__alias_dict__["date"])

        historical_sql = f"""
            t = select timestamp, symbol, factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} 
            and timestamp between {historical_start} 
            and {historical_end} 
            and symbol in {symbols_list};

            select value from t pivot by timestamp, symbol, factor_name;
        """
        df = reader._run_query(
            script=historical_sql,
        )
        if df is None or df.empty:
            raise EmptyDataError()
        df.sort_values(by="timestamp", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanHistoricalMarketCapQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanHistoricalMarketCapData]:
        """Return the transformed data."""

        return [XiaoYuanHistoricalMarketCapData.model_validate(d) for d in data]
