"""XiaoYuan Income Statement Growth Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.income_statement_growth import (
    IncomeStatementGrowthData,
    IncomeStatementGrowthQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from pandas.errors import EmptyDataError
from pydantic import Field, model_validator

from openbb_xiaoyuan.utils.references import (
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
    get_query_finance_sql,
    get_report_month,
)


class XiaoYuanIncomeStatementGrowthQueryParams(IncomeStatementGrowthQueryParams):
    """XiaoYuan Income Statement Growth Query.

    Source: https://site.financialmodelingprep.com/developer/docs/financial-statements-growth-api/
    """

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "quarter"],
        }
    }

    period: Literal["annual", "quarter"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanIncomeStatementGrowthData(IncomeStatementGrowthData):
    """XiaoYuan Income Statement Growth Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "growth_revenue": "营业总收入同比增长率（百分比）",
        # "growth_cost_of_revenue": "收入成本增长率",
        # "growth_gross_profit": "毛利增长率",
        # "growth_gross_profit_margin": "毛利率增长率",
        # "growth_general_and_admin_expense": "一般及行政费用增长率",
        # "growth_research_and_development_expense": "研发费用增长率",
        # "growth_selling_and_marketing_expense": "销售和市场费用增长率",
        # "growth_other_expenses": "其他费用增长率",
        # "growth_operating_expenses": "营业费用增长率",
        # "growth_cost_and_expenses": "总成本和费用增长率",
        # "growth_interest_expense": "利息支出增长率",
        # "growth_depreciation_and_amortization": "折旧与摊销费用增长率",
        # "growth_ebitda": "息税折旧及摊销前利润（EBITDA）增长率",
        # "growth_ebitda_margin": "EBITDA利润率增长率",
        "growth_operating_income": "营业收入同比增长率",
        # "growth_operating_income_margin": "营业收入利润率增长率",
        # "growth_total_other_income_expenses_net": "其他收入净额增长率",
        # "growth_income_before_tax": "税前收入增长率",
        # "growth_income_before_tax_margin": "税前收入利润率增长率",
        # "growth_income_tax_expense": "所得税费用增长率",
        # "growth_consolidated_net_income": "合并净利润增长率",
        # "growth_net_income_margin": "净利润率增长率",
        "growth_basic_earings_per_share": "基本每股收益同比增长率（百分比）",
        "growth_diluted_earnings_per_share": "稀释每股收益同比增长率（百分比）",
        # "growth_weighted_average_basic_shares_outstanding": "加权平均基本股本增长率",
        # "growth_weighted_average_diluted_shares_outstanding": "加权平均稀释股本增长率"
    }

    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))
    growth_revenue: Optional[float] = Field(
        default=None,
        description="Growth rate of total revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_cost_of_revenue: Optional[float] = Field(
        default=None,
        description="Growth rate of cost of goods sold.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_gross_profit: Optional[float] = Field(
        default=None,
        description="Growth rate of gross profit.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_gross_profit_margin: Optional[float] = Field(
        default=None,
        description="Growth rate of gross profit as a percentage of revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_general_and_admin_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of general and administrative expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_research_and_development_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of expenses on research and development.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_selling_and_marketing_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of expenses on selling and marketing activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_expenses: Optional[float] = Field(
        default=None,
        description="Growth rate of other operating expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_operating_expenses: Optional[float] = Field(
        default=None,
        description="Growth rate of total operating expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_cost_and_expenses: Optional[float] = Field(
        default=None,
        description="Growth rate of total costs and expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_interest_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of interest expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_depreciation_and_amortization: Optional[float] = Field(
        default=None,
        description="Growth rate of depreciation and amortization expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_ebitda: Optional[float] = Field(
        default=None,
        description="Growth rate of Earnings Before Interest, Taxes, Depreciation, and Amortization.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_ebitda_margin: Optional[float] = Field(
        default=None,
        description="Growth rate of EBITDA as a percentage of revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_operating_income: Optional[float] = Field(
        default=None,
        description="Growth rate of operating income.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_operating_income_margin: Optional[float] = Field(
        default=None,
        description="Growth rate of operating income as a percentage of revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_total_other_income_expenses_net: Optional[float] = Field(
        default=None,
        description="Growth rate of net total other income and expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_income_before_tax: Optional[float] = Field(
        default=None,
        description="Growth rate of income before taxes.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_income_before_tax_margin: Optional[float] = Field(
        default=None,
        description="Growth rate of income before taxes as a percentage of revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_income_tax_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of income tax expenses.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_consolidated_net_income: Optional[float] = Field(
        default=None,
        description="Growth rate of net income.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_income_margin: Optional[float] = Field(
        default=None,
        description="Growth rate of net income as a percentage of revenue.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_basic_earings_per_share: Optional[float] = Field(
        default=None,
        description="Growth rate of Earnings Per Share (EPS).",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_diluted_earnings_per_share: Optional[float] = Field(
        default=None,
        description="Growth rate of diluted Earnings Per Share (EPS).",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_weighted_average_basic_shares_outstanding: Optional[float] = Field(
        default=None,
        description="Growth rate of weighted average shares outstanding.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_weighted_average_diluted_shares_outstanding: Optional[float] = Field(
        default=None,
        description="Growth rate of diluted weighted average shares outstanding.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )

    @model_validator(mode="before")
    @classmethod
    def replace_zero(cls, values):
        """Check for zero values and replace with None."""
        return (
            {k: None if v == 0 else v for k, v in values.items()}
            if isinstance(values, dict)
            else values
        )


class XiaoYuanIncomeStatementGrowthFetcher(
    Fetcher[
        XiaoYuanIncomeStatementGrowthQueryParams,
        List[XiaoYuanIncomeStatementGrowthData],
    ]
):
    """XiaoYuan Income Statement Growth Fetcher."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanIncomeStatementGrowthQueryParams:
        """Transform the query params."""
        return XiaoYuanIncomeStatementGrowthQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanIncomeStatementGrowthQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanIncomeStatementGrowthData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        FIN_METRICS_PER_SHARE = [
            "营业总收入同比增长率（百分比）",
            "营业收入同比增长率",
            "基本每股收益同比增长率（百分比）",
            "稀释每股收益同比增长率（百分比）",
        ]
        report_month = get_report_month(query.period, -query.limit)
        finance_sql = get_query_finance_sql(
            FIN_METRICS_PER_SHARE, [query.symbol], report_month
        )
        df = reader._run_query(
            script=extractMonthDayFromTime + getFiscalQuarterFromTime + finance_sql,
        )
        if df is None or df.empty:
            raise EmptyDataError()
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        columns_to_divide = FIN_METRICS_PER_SHARE
        df[columns_to_divide] /= 100
        df.sort_values(by="报告期", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanIncomeStatementGrowthQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanIncomeStatementGrowthData]:
        """Return the transformed data."""
        return [XiaoYuanIncomeStatementGrowthData.model_validate(d) for d in data]
