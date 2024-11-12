"""XiaoYuan Index Constituents Model."""

# pylint: disable=unused-argument
from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Literal, Optional, Union

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_constituents import (
    IndexConstituentsData,
    IndexConstituentsQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    get_index_constituents_sql,
)
from pydantic import Field, field_validator


class XiaoYuanIndexConstituentsQueryParams(IndexConstituentsQueryParams):
    """XiaoYuan Index Constituents Query.
    """


class XiaoYuanIndexConstituentsData(IndexConstituentsData):
    """XiaoYuan Index Constituents Data."""

    __alias_dict__ = {
        "headquarter": "headQuarter",
        "date_first_added": "dateFirstAdded",
        "sub_sector": "subSector",
    }

    sector: str = Field(
        description="Sector the constituent company in the index belongs to."
    )
    sub_sector: Optional[str] = Field(
        default=None,
        description="Sub-sector the constituent company in the index belongs to.",
    )
    headquarter: Optional[str] = Field(
        default=None,
        description="Location of the headquarter of the constituent company in the index.",
    )
    date_first_added: Optional[Union[dateType, str]] = Field(
        default=None, description="Date the constituent company was added to the index."
    )
    cik: Optional[int] = Field(
        description=DATA_DESCRIPTIONS.get("cik", ""), default=None
    )
    founded: Optional[Union[dateType, str]] = Field(
        default=None,
        description="Founding year of the constituent company in the index.",
    )

    @field_validator("dateFirstAdded", mode="before", check_fields=False)
    @classmethod
    def date_first_added_validate(cls, v):  # pylint: disable=E0213
        """Return the date_first_added date as a datetime object for valid cases."""
        try:
            return datetime.strptime(v, "%Y-%m-%d") if v else None
        except Exception:
            # For returning string in case of mismatched dates
            return v

    @field_validator("founded", mode="before", check_fields=False)
    @classmethod
    def founded_validate(cls, v):  # pylint: disable=E0213
        """Return the founded date as a datetime object for valid cases."""
        try:
            return datetime.strptime(v, "%Y-%m-%d") if v else None
        except Exception:
            # For returning string in case of mismatched dates
            return v


class XiaoYuanIndexConstituentsFetcher(
    Fetcher[
        XiaoYuanIndexConstituentsQueryParams,
        List[XiaoYuanIndexConstituentsData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanIndexConstituentsQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanIndexConstituentsQueryParams(**params)

    @staticmethod
    async def aextract_data(
            query: XiaoYuanIndexConstituentsQueryParams,
            credentials: Optional[Dict[str, str]],
            **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        from jinniuai_data_store.reader import get_jindata_reader
        reader = get_jindata_reader()

        df = reader._run_query(
            script=get_index_constituents_sql(query.symbol)
        )
        if df is None or df.empty:
            raise EmptyDataError()
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
            query: XiaoYuanIndexConstituentsQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanIndexConstituentsData]:
        """Return the raw data from the XiaoYuan endpoint."""
        return [XiaoYuanIndexConstituentsData.model_validate(d) for d in data]
