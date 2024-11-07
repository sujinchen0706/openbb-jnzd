"""XiaoYuan Index Search Model."""

# pylint: disable=unused-argument
from datetime import date as dateType
from typing import Any, Dict, List, Optional

import pandas as pd
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_search import (
    IndexSearchData,
    IndexSearchQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    revert_stock_code_format,
)
from pydantic import Field


class XiaoYuanIndexSearchQueryParams(IndexSearchQueryParams):
    """
    XiaoYuan Index Search Query Params.

    Source: https://docs.XiaoYuan.com/documentation/web_api/search_Indexs_v2
    """

    ...


class XiaoYuanIndexSearchData(IndexSearchData):
    """XiaoYuan Index Search Data."""

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


class XiaoYuanIndexSearchFetcher(
    Fetcher[XiaoYuanIndexSearchQueryParams, List[XiaoYuanIndexSearchData]]
):
    """XiaoYuan Index Search Fetcher."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanIndexSearchQueryParams:
        """Transform query."""
        if params.get("query", ""):
            params["query"] = convert_stock_code_format(params.get("query", ""))
        return XiaoYuanIndexSearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanIndexSearchQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        # pylint: disable=import-outside-toplevel

        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        Index_listing_info = """
        select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol,
        name,exchange,list_date,end_date from loadTable("dfs://cn_zvt", `index)
        """
        query_Index = f"""where upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] in {query.query.split(",")}"""
        if query.query and query.is_symbol:
            df = reader._run_query(Index_listing_info + query_Index)
        else:
            df = reader._run_query(Index_listing_info)
        if df is None or df.empty:
            raise EmptyDataError()
        df["list_date"] = df["list_date"].dt.strftime("%Y-%m-%d")
        df["end_date"] = df["end_date"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanIndexSearchQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanIndexSearchData]:
        """Transform data."""
        # pylint: disable=import-outside-toplevel
        data = revert_stock_code_format(data)
        data = [
            {**d, "end_date": None if pd.isna(d.get("end_date")) else d["end_date"]}
            for d in data
        ]
        return [XiaoYuanIndexSearchData.model_validate(d) for d in data]
