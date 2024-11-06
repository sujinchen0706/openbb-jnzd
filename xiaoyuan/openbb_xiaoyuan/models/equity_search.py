"""XiaoYuan Equity Search Model."""

# pylint: disable=unused-argument

from datetime import date as dateType
from typing import Any, Dict, List, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_search import (
    EquitySearchData,
    EquitySearchQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError


class XiaoYuanEquitySearchQueryParams(EquitySearchQueryParams):
    """XiaoYuan Equity Search Query.

    Source: https://www.XiaoYuan.com/
    """

    ...


class XiaoYuanEquitySearchData(EquitySearchData):
    """XiaoYuan Equity Search Data."""

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
            select code as symbol,name,exchange,list_date,end_date from loadTable("dfs://cn_zvt", `stock)
        """
        df = reader._run_query(stock_listing_info)
        if df is None or df.empty:
            raise EmptyDataError()
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanEquitySearchQueryParams, data: Dict, **kwargs: Any
    ) -> List[XiaoYuanEquitySearchData]:
        """Transform the data to the standard format."""
        return [XiaoYuanEquitySearchData.model_validate(d) for d in data]
