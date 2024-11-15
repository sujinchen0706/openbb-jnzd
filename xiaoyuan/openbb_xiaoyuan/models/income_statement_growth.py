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
from openbb_xiaoyuan.utils.references import (
    caculate_sql,
    calculateGrowth,
    convert_stock_code_format,
    extractMonthDayFromTime,
    get_query_finance_sql,
    get_report_month,
    getFiscalQuarterFromTime,
    revert_stock_code_format, get_query_cnzvt_sql,
)
from pandas.errors import EmptyDataError
from pydantic import Field, model_validator


class XiaoYuanIncomeStatementGrowthQueryParams(IncomeStatementGrowthQueryParams):
    """XiaoYuan Income Statement Growth Query.

    Source: https://site.financialmodelingprep.com/developer/docs/financial-statements-growth-api/
    """

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "ytd","quarter"],
        }
    }

    period: Literal["annual", "ytd","quarter"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanIncomeStatementGrowthData(IncomeStatementGrowthData):
    """XiaoYuan Income Statement Growth Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "growth_revenue": "营业总收入同比增长率（百分比）",
        "growth_operating_income": "营业收入同比增长率",
        "growth_basic_earings_per_share": "基本每股收益同比增长率（百分比）",
        "growth_diluted_earnings_per_share": "稀释每股收益同比增长率（百分比）",
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

    growth_research_and_development_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of expenses on research and development.",
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

    growth_operating_income: Optional[float] = Field(
        default=None,
        description="Growth rate of operating income.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )

    growth_income_before_tax: Optional[float] = Field(
        default=None,
        description="Growth rate of income before taxes.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )

    growth_income_tax_expense: Optional[float] = Field(
        default=None,
        description="Growth rate of income tax expenses.",
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
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
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
        DDB_DATA = [
            "营业总收入同比增长率（百分比）",
            "营业收入同比增长率",
            "基本每股收益同比增长率（百分比）",
            "稀释每股收益同比增长率（百分比）",
        ]
        metrics_mapping = {
            "growth_cost_of_revenue": "营业成本",
            "growth_gross_profit": "毛利",
            "growth_research_and_development_expense": "研发费用",
            "growth_depreciation_and_amortization": "折旧与摊销",
            "growth_ebitda": "息税折旧摊销前利润",
            "growth_income_tax_expense": "减：所得税费用",
        }

        CACULATE_DATA_QTR_DIC = {
            "operating_costs": "营业成本",
            "total_op_income": "营业总收入",
            "operating_income": "营业收入",
            "eps": "基本每股收益",
            "diluted_eps": "稀释每股收益",
            "rd_costs": "研发费用",
            "tax_expense": "所得税费用",
        }
        if query.period == "quarter":
            cnzvt_sql = get_query_cnzvt_sql(
                CACULATE_DATA_QTR_DIC,
                [query.symbol],
                "income_statement_qtr",
                -query.limit - 1,
            )
            cash_flow_caculate = calculateGrowth + caculate_sql(CACULATE_DATA_QTR_DIC)
            df = reader._run_query(
                script=extractMonthDayFromTime
                + getFiscalQuarterFromTime
                + cnzvt_sql
                + cash_flow_caculate
            )
            df.drop(columns=list((CACULATE_DATA_QTR_DIC).values()), inplace=True)

        else:
            income_caculate = calculateGrowth + caculate_sql(metrics_mapping)

            report_month = get_report_month(query.period, -query.limit - 1)
            finance_sql = get_query_finance_sql(
                DDB_DATA + list(metrics_mapping.values()),
                [query.symbol],
                report_month,
            )
            df = reader._run_query(
                script=extractMonthDayFromTime
                       + getFiscalQuarterFromTime
                       + finance_sql
                       + income_caculate
            )
            print(finance_sql)
            df = df.drop(columns=list(metrics_mapping.values()))
            df[DDB_DATA] /= 100
        if df is None or df.empty:
            raise EmptyDataError()
        df = df.iloc[-query.limit:]
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df.sort_values(by="报告期", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
            query: XiaoYuanIncomeStatementGrowthQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanIncomeStatementGrowthData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        return [XiaoYuanIncomeStatementGrowthData.model_validate(d) for d in data]
