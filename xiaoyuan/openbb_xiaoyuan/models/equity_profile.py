"""XiaoYuan Equity Profile Model."""

# pylint: disable=unused-argument

from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta
from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.data import ForceInt
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_info import (
    EquityInfoData,
    EquityInfoQueryParams,
)
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field, field_validator, model_validator


class XiaoYuanEquityProfileQueryParams(EquityInfoQueryParams):
    """XiaoYuan Equity Profile Query.

    Source: https://site.financialmodelingprep.com/developer/docs/companies-key-stats-free-api/
    """

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class XiaoYuanEquityProfileData(EquityInfoData):
    """XiaoYuan Equity Profile Data."""

    __alias_dict__ = {
        "stock_exchange": "exchange",
        "last_price": "收盘价（不复权）",
        "first_stock_price_date": "list_date",
        "last_stock_price_date": "end_date",
        "market_cap": "总市值",
    }
    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}

    is_actively_trading: bool = Field(description="If the company is actively trading.")

    market_cap: Optional[ForceInt] = Field(
        default=None,
        description="Market capitalization of the company.",
    )
    last_price: Optional[float] = Field(
        default=None,
        description="The last traded price.",
    )
    year_high: Optional[float] = Field(
        default=None, description="The one-year high of the price."
    )
    year_low: Optional[float] = Field(
        default=None, description="The one-year low of the price."
    )
    volume_avg: Optional[ForceInt] = Field(
        default=None,
        description="The one-year average daily trading volume",
    )

    @field_validator("first_stock_price_date", mode="before", check_fields=False)
    @classmethod
    def validate_date(cls, v):  # pylint: disable=E0213
        """Return the date as a datetime object."""
        if isinstance(v, dateType) or v is None:
            return v
        return dateType.fromisoformat(v) if v else None

    @model_validator(mode="before")
    @classmethod
    def replace_empty_strings(cls, values):
        """Check for empty strings and replace with None."""
        return (
            {k: None if v in ("", "NA") else v for k, v in values.items()}
            if isinstance(values, dict)
            else values
        )


class XiaoYuanEquityProfileFetcher(
    Fetcher[
        XiaoYuanEquityProfileQueryParams,
        List[XiaoYuanEquityProfileData],
    ]
):
    """XiaoYuan Equity Profile Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanEquityProfileQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanEquityProfileQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanEquityProfileQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        reader = get_jindata_reader()
        now = datetime.now().date()
        factors = ["收盘价（不复权）", "总市值", "成交量（不复权）"]
        symbol_list = query.symbol.split(",")
        start_date = now - relativedelta(years=1)
        sc = f"""
        t = select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,
        name,exchange,list_date,end_date,* from loadTable("dfs://cn_zvt", `stock)
        where upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {symbol_list}
        
        update t set is_actively_trading = iif( end_date is null,1,0)

       
        fun_t = select timestamp, min(报告期) as first_fundamental_date, max(报告期) as last_fundamental_date,symbol
        from loadTable("dfs://finance_factors_1Y", `cn_finance_factors_1Q)
        where symbol in {symbol_list} and factor_name in {["营业收入"]} context by symbol limit -1
        
        daily_t = select timestamp, symbol,factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} and timestamp between {reader.convert_to_db_date_format(start_date)} 
            and {reader.convert_to_db_date_format(now)} and symbol in {symbol_list}
        daily_t =  select value from daily_t where value is not null pivot by timestamp, symbol, factor_name;
        
        update daily_t set year_high = max(收盘价（不复权）) context by symbol
        update daily_t set year_low = min(收盘价（不复权）) context by symbol
        update daily_t set volume_avg = avg(成交量（不复权）) context by symbol
        
        stockData = select timestamp,symbol,string(value[0]) as industry_category
        from loadTable("dfs://vectors_6M", `cn_vectors_1D)
        where factor_name = `申万行业分类信息
        and symbol in {symbol_list};
        t2 = select * from aj(daily_t,stockData,`symbol`timestamp)
        t2 = select * from aj(t2,fun_t,`symbol`timestamp)
        t = last(aj(t2,t,`symbol`timestamp))
        update t set end_date = timestamp where end_date is null
        t
        """
        df = reader._run_query(sc)
        df.drop(
            columns=[
                "timestamp",
                "成交量（不复权）",
                "stockData_timestamp",
                "fun_t_timestamp",
                "id",
                "entity_id",
                "t_timestamp",
                "entity_type",
            ],
            inplace=True,
        )
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanEquityProfileQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanEquityProfileData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        return [XiaoYuanEquityProfileData.model_validate(d) for d in data]
