"""XiaoYuan ETF Search Model."""

# pylint: disable=unused-argument

from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.etf_search import (
    EtfSearchData,
    EtfSearchQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field


class XiaoYuanEtfSearchQueryParams(EtfSearchQueryParams):
    """
    XiaoYuan ETF Search Query Params.

    Source: https://docs.XiaoYuan.com/documentation/web_api/search_etfs_v2
    """

    is_active: Optional[Literal[True, False]] = Field(
        description="Whether the ETF is actively trading.",
        default=None,
    )


class XiaoYuanEtfSearchData(EtfSearchData):
    """XiaoYuan ETF Search Data."""

    __alias_dict__ = {
        "country": "exchange",
        "price": "收盘价（不复权）",
        "volume": "成交量（不复权）",
        "investment_style": "invest_style",
    }
    list_date: Optional[dateType] = Field(
        default=None,
        description="The date on which the stock was listed on the exchange.",
    )
    exchange: Optional[str] = Field(
        default=None,
        description="The exchange on which the stock is listed.",
    )
    end_date: Optional[dateType] = Field(
        default=None,
        description="The date on which the stock was delisted from the exchange.",
    )


class XiaoYuanEtfSearchFetcher(
    Fetcher[XiaoYuanEtfSearchQueryParams, List[XiaoYuanEtfSearchData]]
):
    """XiaoYuan ETF Search Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanEtfSearchQueryParams:
        """Transform query."""
        if params.get("query", ""):
            params["query"] = convert_stock_code_format(params.get("query", ""))
        return XiaoYuanEtfSearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanEtfSearchQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        # pylint: disable=import-outside-toplevel

        from jinniuai_data_store.reader import get_jindata_reader

        now = datetime.now().date()
        factors = ["收盘价（不复权）", "成交量（不复权）"]
        reader = get_jindata_reader()
        start_date = reader.get_adjacent_trade_day(now, -1)
        reader = get_jindata_reader()
        etf_listing_info = """
        t = select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,
        name,exchange,list_date,end_date from loadTable("dfs://cn_zvt", `etf_detail) 
        """
        query_etf = f"""where upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {query.query.split(",")}"""

        other_data = f"""
        t
        update t set country = "CH"
        update t set actively_trading = iif(end_date is null,1,0)
        daily_t = select timestamp, symbol,factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} and timestamp = {reader.convert_to_db_date_format(start_date)} and symbol in t.symbol 
        daily_t =  select value from daily_t where value is not null pivot by timestamp, symbol, factor_name;
        select * from aj(daily_t,t,`symbol`timestamp,`symbol`list_date)
        """
        if query.query:
            df = reader._run_query(etf_listing_info + query_etf + other_data)
        else:
            df = reader._run_query(etf_listing_info + other_data)
        if query.is_active:
            df = df.query("end_date.isnull()")
        if df is None or df.empty:
            raise EmptyDataError()
        df.drop(columns=["list_date", "end_date", "exchange"], inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanEtfSearchQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanEtfSearchData]:
        """Transform data."""
        # pylint: disable=import-outside-toplevel
        data = revert_stock_code_format(data)
        return [XiaoYuanEtfSearchData.model_validate(d) for d in data]
