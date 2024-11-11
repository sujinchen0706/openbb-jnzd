"""XiaoYuan Etf Profile Model."""

# pylint: disable=unused-argument

from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.etf_info import (
    EtfInfoData,
    EtfInfoQueryParams,
)
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field, model_validator


class XiaoYuanEtfInfoQueryParams(EtfInfoQueryParams):
    """XiaoYuan Etf Profile Query.

    Source: https://site.financialmodelingprep.com/developer/docs/companies-key-stats-free-api/
    """

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class XiaoYuanEtfInfoData(EtfInfoData):
    """XiaoYuan Etf Profile Data."""

    __alias_dict__ = {
        "stock_exchange": "exchange",
        "close": "收盘价（不复权）",
        "open": "开盘价（不复权）",
        "high": "最高价（不复权）",
        "low": "最低价（不复权）",
        "volume": "成交量（不复权）",
    }
    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}

    is_listed: bool = Field(
        default=None, description="If true, the ETF is still listed on an exchange."
    )
    category: Optional[str] = Field(
        default=None,
        description="The etf category.",
    )
    exchange: Optional[str] = Field(
        default=None,
        description="The exchange the etf is listed on.",
    )
    last_price: Optional[float] = Field(
        default=None,
        description="The last traded price.",
    )
    year_high: Optional[float] = Field(
        default=None,
        description="The one-year high of the price.",
    )
    year_low: Optional[float] = Field(
        default=None,
        description="The one-year low of the price.",
    )
    ma_50d: Optional[float] = Field(
        default=None,
        description="50-day moving average price.",
    )
    ma_200d: Optional[float] = Field(
        default=None,
        description="200-day moving average price.",
    )

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


class XiaoYuanEtfInfoFetcher(
    Fetcher[
        XiaoYuanEtfInfoQueryParams,
        List[XiaoYuanEtfInfoData],
    ]
):
    """XiaoYuan Etf Profile Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanEtfInfoQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanEtfInfoQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanEtfInfoQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        reader = get_jindata_reader()
        now = datetime.now().date()
        factors = [
            "开盘价（不复权）",
            "收盘价（不复权）",
            "最高价（不复权）",
            "最低价（不复权）",
            "成交量（不复权）",
        ]
        symbol_list = query.symbol.split(",")
        start_date = now - relativedelta(years=1)
        sc = f"""
        use mytt
        t1 = select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,invest_style from loadTable("dfs://cn_zvt", `etf_detail)
        where upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {symbol_list}
        
        t2 = select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,
        upper(split(underlying_index_code,'_')[1])+split(underlying_index_code,'_')[2] as index_symbol,
        name,list_date,end_date,* from loadTable("dfs://cn_zvt", `etf)
        where upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {symbol_list}
        
        index_data = select upper(split(entity_id, "_")[1]) + split(entity_id, "_")[2] as index_symbol, 
        name as index_name from loadTable("dfs://cn_zvt", `index) 
        where upper(split(entity_id, "_")[1]) + split(entity_id, "_")[2] in t2.index_symbol;

        t = lj(t2,t1,`symbol)
        t = lj(t, index_data, `index_symbol);
        update t set is_listed = iif(end_date is null,1,0)
        alter table t rename list_date to inception_date;
        alter table t rename end_date to close_date	;
        alter table t rename invest_style to investment_style;
       
        daily_t = select timestamp, symbol,factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} and timestamp between {reader.convert_to_db_date_format(start_date)} 
            and {reader.convert_to_db_date_format(now)} and symbol in {symbol_list}
        daily_t =  select value from daily_t where value is not null pivot by timestamp, symbol, factor_name;
        
        update daily_t set year_high = max(收盘价（不复权）) context by symbol
        update daily_t set year_low = min(收盘价（不复权）) context by symbol
        update daily_t set prev_close = REF(成交量（不复权）,1) context by symbol
        
        update daily_t set ma_50d = MA(收盘价（不复权）,50) context by symbol
        update daily_t set ma_200d = MA(收盘价（不复权）,200) context by symbol
        daily_t = last(daily_t)
        res = select * from lj(t,daily_t,`symbol)
        res
        """
        df = reader._run_query(sc)
        df.drop(
            columns=[
                "timestamp",
                "id",
                "entity_id",
                "entity_type",
                "choice_category_1",
                "choice_category_2",
                "daily_t_timestamp",
                "code",
                "underlying_index_code",
            ],
            inplace=True,
        )
        df["inception_date"] = df["inception_date"].dt.strftime("%Y-%m-%d")
        df["is_listed"] = df["is_listed"].astype(bool)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanEtfInfoQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanEtfInfoData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        data = [
            {
                **d,
                "close_date": None if pd.isna(d.get("close_date")) else d["close_date"],
            }
            for d in data
        ]
        for i in data:
            if "SH" in i["index_symbol"]:
                i["index_symbol"] = i["index_symbol"].replace("SH", "") + ".SS"
            elif "SZ" in i["index_symbol"]:
                i["index_symbol"] = i["index_symbol"].replace("SZ", "") + ".SZ"
            elif "OF" in i["index_symbol"]:
                i["index_symbol"] = i["index_symbol"].replace("OF", "") + ".OF"
        return [XiaoYuanEtfInfoData.model_validate(d) for d in data]
