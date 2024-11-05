"""XiaoYuan Finance Cash Flow Statement Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.cash_flow import (
    CashFlowStatementData,
    CashFlowStatementQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    QUERY_DESCRIPTIONS,
    DATA_DESCRIPTIONS,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, model_validator

from openbb_xiaoyuan.utils.references import (
    get_report_month,
    get_query_finance_sql,
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
)


class XiaoYuanCashFlowStatementQueryParams(CashFlowStatementQueryParams):
    """XiaoYuan Finance Cash Flow Statement Query."""

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "ytd"],
        }
    }

    period: Literal["annual", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanCashFlowStatementData(CashFlowStatementData):
    """XiaoYuan Finance Cash Flow Statement Data."""

    __alias_dict__ = {
        "net_cash_from_operating_activities": "经营活动产生的现金流量净额",
        "net_cash_from_investing_activities": "投资活动产生的现金流量净额",
        "issuance_of_debt": "发行债券收到的现金",
        "repayment_of_debt": "偿还债务支付的现金",
        "net_cash_from_financing_activities": "筹资活动产生的现金流量净额",
        "depreciation_and_amortization": "折旧与摊销",
        "period_ending": "报告期",
    }
    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))

    net_cash_from_operating_activities: Optional[float] = Field(
        description="Net cash from operating activities.", default=None
    )
    net_cash_from_investing_activities: Optional[float] = Field(
        description="Net cash from investing activities.", default=None
    )
    issuance_of_debt: Optional[float] = Field(
        description="Issuance of debt.", default=None
    )

    repayment_of_debt: Optional[float] = Field(
        description="Repayment of debt.", default=None
    )
    net_cash_from_financing_activities: Optional[float] = Field(
        description="Net cash from financing activities.", default=None
    )
    depreciation_and_amortization: Optional[float] = Field(
        description="Depreciation and amortization.", default=None
    )

    @model_validator(mode="before")
    @classmethod
    def replace_zero(cls, values):  # pylint: disable=no-self-argument
        """Check for zero values and replace with None."""
        return (
            {k: None if v == 0 else v for k, v in values.items()}
            if isinstance(values, dict)
            else values
        )


class XiaoYuanCashFlowStatementFetcher(
    Fetcher[
        XiaoYuanCashFlowStatementQueryParams,
        List[XiaoYuanCashFlowStatementData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan Finance endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanCashFlowStatementQueryParams:
        """Transform the query parameters."""
        return XiaoYuanCashFlowStatementQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanCashFlowStatementQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanCashFlowStatementData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        factors = [
            "经营活动产生的现金流量净额",
            "投资活动产生的现金流量净额",
            "发行债券收到的现金",
            "偿还债务支付的现金",
            "筹资活动产生的现金流量净额",
            "折旧与摊销",
        ]
        reader = get_jindata_reader()
        report_month = get_report_month(query.period, -query.limit)
        finance_sql = get_query_finance_sql(factors, [query.symbol], report_month)
        df = reader._run_query(
            script=extractMonthDayFromTime + getFiscalQuarterFromTime + finance_sql,
        )
        if df is None or df.empty:
            raise EmptyDataError()
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df.sort_values(by="报告期", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        # pylint: disable=unused-argument
        query: XiaoYuanCashFlowStatementQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanCashFlowStatementData]:
        """Transform the data."""
        return [XiaoYuanCashFlowStatementData.model_validate(d) for d in data]
