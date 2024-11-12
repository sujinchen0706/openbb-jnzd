"""XiaoYuan Country Profile."""

# pylint: disable=unused-argument

from typing import Any, Dict, List, Optional

from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.country_profile import (
    CountryProfileData,
    CountryProfileQueryParams,
)
from pydantic import Field, field_validator


class XiaoYuanCountryProfileQueryParams(CountryProfileQueryParams):
    """Country Profile Query."""

    __json_schema_extra__ = {"country": {"multiple_items_allowed": True}}
    country: str = Field(default="china", description="Can only read china data")

    latest: bool = Field(
        default=True,
        description="If True, return only the latest data."
        + " If False, return all available data for each indicator.",
    )


class XiaoYuanCountryProfileData(CountryProfileData):
    """XiaoYuan Country Profile Data."""

    __alias_dict__ = {
        "date": "timestamp",
        "real_growth_qoq": "环比",
        "real_growth_yoy": "GDP：当季同比",
        "gdp_cny": "GDP：当季值",
    }
    real_growth_qoq: float = Field(
        description="Real GDP growth rate quarter over quarter.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    real_growth_yoy: float = Field(
        description="Real GDP growth rate year over year.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )

    @field_validator(
        "gdp_qoq",
        "gdp_yoy",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def normalize_percent(cls, v):
        """Normalize the percent values."""
        return float(v) / 100 if v is not None else None


class XiaoYuanCountryProfileFetcher(
    Fetcher[XiaoYuanCountryProfileQueryParams, List[XiaoYuanCountryProfileData]]
):
    """XiaoYuan Country Profile Fetcher."""

    require_credentials = False

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanCountryProfileQueryParams:
        """Transform the query parameters."""
        return XiaoYuanCountryProfileQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanCountryProfileQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Extract the data."""
        # pylint: disable=import-outside-toplevel
        reader = get_jindata_reader()
        factor_names = ["GDP：当季同比", "GDP：当季值"]

        sc = f"""
               use mytt
               t = select timestamp, factor_name ,value 
               from loadTable("dfs://factors_1Y", `cn_factors_1Q) 
               where factor_name in {factor_names}  
               t = select value from t pivot by timestamp,factor_name;
               update t set 环比 = (GDP：当季值 - REF(GDP：当季值,1) ) / REF(GDP：当季值,1) ;
               update t set GDP：当季同比 = GDP：当季同比 / 100;
               t
               """
        filter_sc = """select top 1 * from t  order by timestamp desc;"""
        if query.latest:
            df = reader._run_query(sc + filter_sc)
        else:
            df = reader._run_query(sc)
        df["country"] = "china"
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanCountryProfileQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanCountryProfileData]:
        """Transform the data."""
        # pylint: disable=import-outside-toplevel

        return [XiaoYuanCountryProfileData.model_validate(d) for d in data]
