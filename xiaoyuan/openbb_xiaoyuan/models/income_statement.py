""" XiaoYuan Income Statement Model."""

# pylint: disable=unused-argument
from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.income_statement import (
    IncomeStatementData,
    IncomeStatementQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    convert_stock_code_format,
    extractMonthDayFromTime,
    get_query_finance_sql,
    get_report_month,
    getFiscalQuarterFromTime,
    revert_stock_code_format,
    get_query_cnzvt_sql,
)
from pydantic import Field, model_validator


class XiaoYuanIncomeStatementQueryParams(IncomeStatementQueryParams):
    """XiaoYuan Income Statement Query."""

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "quarter", "ytd"],
        }
    }

    period: Literal["annual", "quarter", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanIncomeStatementData(IncomeStatementData):
    """XiaoYuan Income Statement Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "total_operating_income": "营业总收入",
        "total_operating_expenses": "营业总成本",
        "operating_cost_of_revenue": "营业成本",
        "research_and_development_expense": "研发费用",
        "basic_earnings_per_share": "每股收益",
        "diluted_earnings_per_share": "稀释每股收益",
        "total_comprehensive_income": "综合收益总额",
        "interest_income": "其中：利息收入",
        "total_interest_expense": "利息支出",
        "other_income": "其他收益",
        "net_income_continuing_operations": "持续经营净利润",
        "net_income_discontinued_operations": "终止经营净利润",
        "ebitda": "息税折旧摊销前利润",
        "depreciation_and_amortization": "折旧与摊销",
    }
    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))

    total_operating_income: Optional[float] = Field(
        description="Total operating income.", default=None
    )
    total_operating_expenses: Optional[float] = Field(
        description="Total operating expenses.", default=None
    )
    operating_cost_of_revenue: Optional[float] = Field(
        description="Operating cost of revenue.", default=None
    )
    research_and_development_expense: Optional[float] = Field(
        description="Research and development expense.", default=None
    )
    basic_earnings_per_share: Optional[float] = Field(
        description="Basic earnings per share.", default=None
    )
    diluted_earnings_per_share: Optional[float] = Field(
        description="Diluted earnings per share.", default=None
    )
    total_comprehensive_income: Optional[float] = Field(
        description="Total comprehensive income.", default=None
    )
    interest_income: Optional[float] = Field(
        description="Interest income.", default=None
    )
    total_interest_expense: Optional[float] = Field(
        description="Total interest expense.", default=None
    )
    other_income: Optional[float] = Field(description="Other income.", default=None)
    net_income_continuing_operations: Optional[float] = Field(
        description="Net income from continuing operations.", default=None
    )
    net_income_discontinued_operations: Optional[float] = Field(
        description="Net income from discontinued operations.", default=None
    )
    ebitda: Optional[float] = Field(
        description="Earnings before interest, taxes, depreciation, and amortization.",
        default=None,
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


class XiaoYuanIncomeStatementFetcher(
    Fetcher[
        XiaoYuanIncomeStatementQueryParams,
        List[XiaoYuanIncomeStatementData],
    ]
):
    """Transform the query, extract and transform the data from the  XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanIncomeStatementQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanIncomeStatementQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanIncomeStatementQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        from jinniuai_data_store.reader import get_jindata_reader

        factors = [
            "营业总收入",
            "营业总成本",
            "营业成本",
            "研发费用",
            "每股收益",
            "稀释每股收益",
            "综合收益总额",
            "其中：利息收入",
            "利息支出",
            "其他收益",
            "持续经营净利润",
            "终止经营净利润",
            "息税折旧摊销前利润",
            "折旧与摊销",
        ]
        quarter_factors = {
            "total_op_income": "营业总收入",
            "total_operating_costs": "营业总成本",
            "operating_costs": "营业成本",
            "rd_costs": "研发费用",
            "eps": "每股收益",
            "diluted_eps": "稀释每股收益",
            "total_comprehensive_income": "综合收益总额",
            "fi_interest_income": "其中：利息收入",
            "fi_other_income": "其他收益",
            "fi_net_profit_continuing_operations": "持续经营净利润",
            "fi_iscontinued_operating_net_profit": "终止经营净利润",
        }

        reader = get_jindata_reader()
        if query.period == "quarter":
            cnzvt_sql = get_query_cnzvt_sql(quarter_factors, [query.symbol], "income_statement_qtr", -query.limit)
            df = reader._run_query(
                script=extractMonthDayFromTime + getFiscalQuarterFromTime + cnzvt_sql
            )
        else:
            report_month = get_report_month(query.period, -query.limit)
            finance_sql = get_query_finance_sql(factors, [query.symbol], report_month)
            df = reader._run_query(
                script=extractMonthDayFromTime + getFiscalQuarterFromTime + finance_sql,
            )
        if df is None or df.empty:
            raise EmptyDataError()
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df.sort_values(by="报告期", ascending=False, inplace=True)
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
        query: XiaoYuanIncomeStatementQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanIncomeStatementData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        return [XiaoYuanIncomeStatementData.model_validate(d) for d in data]
