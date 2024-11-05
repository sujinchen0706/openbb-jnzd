"""XiaoYuan Dividend Calendar Model."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.calendar_dividend import (
    CalendarDividendData,
    CalendarDividendQueryParams,
)
from pandas.errors import EmptyDataError
from pydantic import field_validator
from openbb_xiaoyuan.utils.references import get_dividend_sql


class XiaoYuanCalendarDividendQueryParams(CalendarDividendQueryParams):
    """XiaoYuan Dividend Calendar Query.

    Source: https://site.financialmodelingprep.com/developer/docs/dividend-calendar-api/

    The maximum time interval between the start and end date can be 3 months.
    """

    __alias_dict__ = {"start_date": "from", "end_date": "to"}


class XiaoYuanCalendarDividendData(CalendarDividendData):
    """XiaoYuan Dividend Calendar Data."""

    __alias_dict__ = {
        "amount": "dividend",
        "ex_dividend_date": "date",
        "record_date": "recordDate",
        "payment_date": "paymentDate",
    }

    @field_validator(
        "ex_dividend_date",
        "record_date",
        "payment_date",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def date_validate(cls, v: str):  # pylint: disable=E0213
        """Return the date as a datetime object."""
        return datetime.strptime(v, "%Y-%m-%d") if v else None


class XiaoYuanCalendarDividendFetcher(
    Fetcher[
        XiaoYuanCalendarDividendQueryParams,
        List[XiaoYuanCalendarDividendData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanCalendarDividendQueryParams:
        """Transform the query params."""
        transformed_params = params

        now = datetime.now().date()
        if params.get("start_date") is None:
            transformed_params["start_date"] = now
        if params.get("end_date") is None:
            transformed_params["end_date"] = now + relativedelta(days=30)

        return XiaoYuanCalendarDividendQueryParams(**transformed_params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanCalendarDividendQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanCalendarDividendData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()

        historical_start = reader.convert_to_db_date_format(query.start_date)
        historical_end = reader.convert_to_db_date_format(query.end_date)
        dividend_sql = get_dividend_sql(historical_start, historical_end)

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
        query: XiaoYuanCalendarDividendQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanCalendarDividendData]:
        """Return the transformed data."""
        return [XiaoYuanCalendarDividendData.model_validate(d) for d in data]
