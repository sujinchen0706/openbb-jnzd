"""XiaoYuan Historical Dividends Model."""

from datetime import (
    date as dateType,
    datetime,
)
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.historical_dividends import (
    HistoricalDividendsData,
    HistoricalDividendsQueryParams,
)
from pandas.errors import EmptyDataError
from pydantic import Field, field_validator

from openbb_xiaoyuan.utils.references import get_dividend_sql


class XiaoYuanHistoricalDividendsQueryParams(HistoricalDividendsQueryParams):
    """XiaoYuan Historical Dividends Query.

    Source: https://site.financialmodelingprep.com/developer/docs/#Historical-Dividends
    """


class XiaoYuanHistoricalDividendsData(HistoricalDividendsData):
    """XiaoYuan Historical Dividends Data."""

    __alias_dict__ = {
        "ex_dividend_date": "date",
        "amount": "dividend",
    }

    record_date: Optional[dateType] = Field(
        default=None,
        description="Record date of the historical dividends.",
    )
    payment_date: Optional[dateType] = Field(
        default=None,
        description="Payment date of the historical dividends.",
    )

    @field_validator(
        "record_date",
        "payment_date",
        "ex_dividend_date",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def date_validate(cls, v: str):  # pylint: disable=E0213
        """Validate dates."""
        if not isinstance(v, str):
            return v
        return dateType.fromisoformat(v) if v else None


class XiaoYuanHistoricalDividendsFetcher(
    Fetcher[
        XiaoYuanHistoricalDividendsQueryParams,
        List[XiaoYuanHistoricalDividendsData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanHistoricalDividendsQueryParams:
        """Transform the query params."""
        transformed_params = params

        now = datetime.now().date()
        if params.get("start_date") is None:
            transformed_params["start_date"] = now - relativedelta(year=1)
        if params.get("end_date") is None:
            transformed_params["end_date"] = now

        return XiaoYuanHistoricalDividendsQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanHistoricalDividendsQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanHistoricalDividendsData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()

        historical_start = reader.convert_to_db_date_format(query.start_date)
        historical_end = reader.convert_to_db_date_format(query.end_date)
        dividend_sql = get_dividend_sql(
            historical_start, historical_end, query.symbol[-6:]
        )

        df = reader._run_query(dividend_sql)
        if df is None or df.empty:
            raise EmptyDataError()
        df.sort_values(by="date", ascending=False, inplace=True)
        date_columns = ["date", "recordDate", "paymentDate"]
        for col in date_columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanHistoricalDividendsQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanHistoricalDividendsData]:
        """Return the transformed data."""

        return [XiaoYuanHistoricalDividendsData.model_validate(d) for d in data]
