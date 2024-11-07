"""XiaoYuan Equity Search Model."""

# pylint: disable=unused-argument

from datetime import date as dateType
from typing import Any, Dict, List, Optional

import pandas as pd
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_search import (
    EquitySearchData,
    EquitySearchQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field


class XiaoYuanEquitySearchQueryParams(EquitySearchQueryParams):
    """XiaoYuan Equity Search Query.

    Source: https://www.XiaoYuan.com/
    """

    ...


class XiaoYuanEquitySearchData(EquitySearchData):
    """XiaoYuan Equity Search Data."""

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

    @classmethod
    def date_validate(cls, v: str):  # pylint: disable=E0213
        """Validate dates."""
        if not isinstance(v, str):
            return v
        return dateType.fromisoformat(v) if v else None


class XiaoYuanEquitySearchFetcher(
    Fetcher[
        XiaoYuanEquitySearchQueryParams,
        List[XiaoYuanEquitySearchData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanEquitySearchQueryParams:
        """Transform the query."""
        if params.get("is_symbol", False):
            params["query"] = convert_stock_code_format(params.get("query", ""))

        return XiaoYuanEquitySearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanEquitySearchQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> Dict:
        """Return the raw data from the XiaoYuan endpoint."""
        # pylint: disable=import-outside-toplevel

        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        stock_listing_info = """
        select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,
        name,exchange,list_date,end_date from loadTable("dfs://cn_zvt", `stock)
        where exchange in ['sh','sz'] 
        """
        query_symbol = f"""and upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {query.query.split(",")}"""
        if query.is_symbol:
            df = reader._run_query(stock_listing_info + query_symbol)
        else:
            df = reader._run_query(stock_listing_info)
        if df is None or df.empty:
            raise EmptyDataError()
        df["list_date"] = df["list_date"].dt.strftime("%Y-%m-%d")
        df["end_date"] = df["end_date"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanEquitySearchQueryParams, data: Dict, **kwargs: Any
    ) -> List[XiaoYuanEquitySearchData]:
        """Transform the data to the standard format."""
        data = revert_stock_code_format(data)
        data = [
            {**d, "end_date": None if pd.isna(d.get("end_date")) else d["end_date"]}
            for d in data
        ]
        return [XiaoYuanEquitySearchData.model_validate(d) for d in data]
