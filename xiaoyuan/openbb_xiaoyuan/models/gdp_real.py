"""XiaoYuan GDP Real Model."""

from datetime import datetime

# pylint: disable=unused-argument
from typing import Any, Dict, List, Literal, Optional

from dateutil.relativedelta import relativedelta
from jinniuai_data_store.reader import get_jindata_reader
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.gdp_nominal import (
    GdpNominalData,
    GdpNominalQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field


class XiaoYuanGdpRealQueryParams(GdpNominalQueryParams):
    """XiaoYuan GDP Real Query."""

    frequency: Literal["quarter", "annual"] = Field(
        description="Frequency of the data.",
        default="quarter",
        json_schema_extra={"choices": ["quarter", "annual"]},
    )


class XiaoYuanGdpRealData(GdpNominalData):
    """XiaoYuan GDP Real Data."""

    __alias_dict__ = {
        "date": "timestamp",
        "real_growth_qoq": "环比",
        "real_growth_yoy": "GDP：当季同比",
        "value": "GDP：当季值",
    }
    real_growth_qoq: float = Field(
        description="Real GDP growth rate quarter over quarter.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    real_growth_yoy: float = Field(
        description="Real GDP growth rate year over year.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )


class XiaoYuanGdpRealFetcher(
    Fetcher[
        XiaoYuanGdpRealQueryParams,
        List[XiaoYuanGdpRealData],
    ]
):
    """XiaoYuan GDP Real Fetcher."""

    require_credentials = False

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanGdpRealQueryParams:
        """Transform the query parameters."""
        now = datetime.now().date()

        if params.get("start_date") is None:
            params["start_date"] = now

        if params.get("end_date") is None:
            params["end_date"] = now

        return XiaoYuanGdpRealQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanGdpRealQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs,
    ) -> List[Dict]:
        """Extract the data."""
        # pylint: disable=import-outside-toplevel
        reader = get_jindata_reader()
        factor_names = ["GDP：当季同比", "GDP：当季值"]
        first_date = query.start_date - relativedelta(years=1)

        sc = f"""
        use mytt
        t = select timestamp, factor_name ,value 
        from loadTable("dfs://factors_1Y", `cn_factors_1Q) 
        where factor_name in {factor_names} and timestamp between {reader.convert_to_db_date_format(first_date)}
        and {reader.convert_to_db_date_format(query.end_date)}  
        t = select value from t pivot by timestamp,factor_name;
        update t set 环比 = (GDP：当季值 - REF(GDP：当季值,1) ) / REF(GDP：当季值,1) ;
        t = select * from t where timestamp between {reader.convert_to_db_date_format(query.start_date)}
        and {reader.convert_to_db_date_format(query.end_date)}  
        t
        """
        filter_sc = """ select * from t where monthOfYear(timestamp) = 12"""
        if query.frequency == "annual":
            df = reader._run_query(sc + filter_sc)
        else:
            df = reader._run_query(sc)
        if df is None or df.empty:
            raise EmptyDataError(
                "No data was found for the supplied date range and countries."
            )
        df["GDP：当季值"] = (df["GDP：当季值"] * 100000000).astype("int64")
        df["GDP：当季同比"] = df["GDP：当季同比"] / 100
        df = df.sort_values(by=["timestamp", "GDP：当季值"], ascending=[True, False])
        df["country"] = "china"
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanGdpRealQueryParams,
        data: List[Dict],
        **kwargs,
    ) -> List[XiaoYuanGdpRealData]:
        """Transform the data."""
        # pylint: disable=import-outside-toplevel
        # type: ignore
        return [XiaoYuanGdpRealData.model_validate(d) for d in data]
