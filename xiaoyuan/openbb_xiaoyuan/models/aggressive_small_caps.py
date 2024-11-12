"""XiaoYuan Asset Performance AggressiveSmallCaps Model."""

from datetime import datetime

# pylint: disable=unused-argument
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta
from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_performance import (
    EquityPerformanceData,
    EquityPerformanceQueryParams,
)
from pydantic import Field

if TYPE_CHECKING:
    from pandas import DataFrame


class XiaoYuanAggressiveSmallCapsQueryParams(EquityPerformanceQueryParams):
    """XiaoYuan Asset Performance AggressiveSmallCaps Query.

    Source: https://finance.yahoo.com/screener/predefined/day_AggressiveSmallCaps
    """


class XiaoYuanAggressiveSmallCapsData(EquityPerformanceData):
    """XiaoYuan Asset Performance AggressiveSmallCaps Data."""

    __alias_dict__ = {
        "volume": "成交量（不复权）",
        "change": "change",
        "price": "收盘价（不复权）",
        "market_cap": "总市值",
        "avg_volume_3_months": "Avg Vol (3 month)",
        "pe_ratio_ttm": "市盈率（滚动）",
    }

    avg_volume_3_months: Optional[float] = Field(
        default=None,
        description="Average volume over the last 3 months in millions.",
    )
    market_cap: Optional[float] = Field(
        default=None,
        description="Market Cap.",
        json_schema_extra={"x-unit_measurement": "currency"},
    )
    pe_ratio_ttm: Optional[float] = Field(
        default=None,
        description="PE Ratio (TTM).",
    )


class XiaoYuanAggressiveSmallCapsFetcher(
    Fetcher[
        XiaoYuanAggressiveSmallCapsQueryParams, List[XiaoYuanAggressiveSmallCapsData]
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanAggressiveSmallCapsQueryParams:
        """Transform query params."""
        return XiaoYuanAggressiveSmallCapsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: XiaoYuanAggressiveSmallCapsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> "DataFrame":
        """Get data from XiaoYuan."""
        # pylint: disable=import-outside-toplevel
        reader = get_jindata_reader()
        now = datetime.now().date()
        start_date = now - relativedelta(months=3)
        symbol_list = reader.get_symbols().symbol.to_list()
        factors = [
            "收盘价（不复权）",
            "成交量（不复权）",
            "总市值",
            "市盈率（滚动）",
            "基本每股收益同比增长率（百分比）",
        ]
        sc = f"""
            use mytt
            t = select timestamp, symbol, factor_name ,value 
            from loadTable("dfs://factors_6M", `cn_factors_1D) 
            where factor_name in {factors} and timestamp between {reader.convert_to_db_date_format(start_date)} 
            and {reader.convert_to_db_date_format(now)} and symbol in {symbol_list}
            

            t = select value from t pivot by timestamp, symbol, factor_name;
            update t set ref_close = REF(收盘价（不复权）, 1) context by symbol;
            update t set change = 收盘价（不复权） - ref_close context by symbol;
            update t set percent_change = change / ref_close 
            update t set avg_volume_3_months = mean(成交量（不复权）) context by symbol;
            t = select * from t context by symbol limit -1
            
            t2 = select timestamp,报告期, symbol, factor_name ,value 
            from loadTable("dfs://finance_factors_1Y", `cn_finance_factors_1Q) 
            where factor_name in {factors} and timestamp between {reader.convert_to_db_date_format(start_date)} 
            and {reader.convert_to_db_date_format(now)} and symbol in {symbol_list}
            t2 = select value from t2 pivot by timestamp,报告期, symbol, factor_name;
            
            t = select * from aj(t,t2,`symbol`timestamp)
            
            
            t = select * from t where 基本每股收益同比增长率（百分比） > 25 and 总市值 < 1000000000

            
            symbol_name = select trim(upper(substr(entity_id, 6, 2)) + substr(entity_id, 9)) as symbol,
             new_name as name, change_date as timestamp
                from loadTable("dfs://cn_zvt", `stock_name_change) where
                exchange in `sh`sz  and change_date <= {reader.convert_to_db_date_format(now)} and 
                trim(upper(substr(entity_id, 6, 2)) + substr(entity_id, 9)) in t.symbol;
            t = select * from aj(t,symbol_name,`symbol`timestamp)
            select symbol, name, change, 收盘价（不复权）,成交量（不复权）,percent_change, 总市值, avg_volume_3_months, 市盈率（滚动） from t;
            """
        df = reader._run_query(
            script=sc,
        )
        return df

    @staticmethod
    def transform_data(
        query: EquityPerformanceQueryParams,
        data: "DataFrame",
        **kwargs: Any,
    ) -> List[XiaoYuanAggressiveSmallCapsData]:
        """Transform data."""
        # pylint: disable=import-outside-toplevel

        data = data.where(data.notna(), None)
        if query.sort == "desc":
            ascending = False
        else:
            ascending = True
        return [
            XiaoYuanAggressiveSmallCapsData.model_validate(d)
            for d in data.sort_values("percent_change", ascending=ascending).to_dict(
                orient="records"
            )
        ]
