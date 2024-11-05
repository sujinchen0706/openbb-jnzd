"""XiaoYuan Cash Flow Statement Growth Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_core.provider.standard_models.cash_flow_growth import (
    CashFlowStatementGrowthData,
    CashFlowStatementGrowthQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)

from pydantic import Field

from openbb_xiaoyuan.utils.references import (
    get_report_month,
    get_query_finance_sql,
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
)


class XiaoYuanCashFlowStatementGrowthQueryParams(CashFlowStatementGrowthQueryParams):
    """XiaoYuan Cash Flow Statement Growth Query.

    Source: https://site.financialmodelingprep.com/developer/docs/financial-statements-growth-api/
    """

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "ytd"],
        }
    }

    period: Literal["annual", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanCashFlowStatementGrowthData(CashFlowStatementGrowthData):
    """XiaoYuan Cash Flow Statement Growth Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "growth_net_income": "净利润同比增长率（百分比）",
        # "growth_depreciation_and_amortization": "折旧与摊销增长率",
        # "growth_deferred_income_tax": "递延所得税增长率",
        # "growth_stock_based_compensation": "基于股票的薪酬增长率",
        # "growth_change_in_working_capital": "营运资本变动增长率",
        # "growth_account_receivables": "应收账款增长率",
        # "growth_inventory": "库存增长率",
        # "growth_account_payable": "应付账款增长率",
        # "growth_other_working_capital": "其他营运资本增长率",
        # "growth_other_non_cash_items": "其他非现金项目增长率",
        # "growth_net_cash_from_operating_activities": "经营活动现金流增长率",
        # "growth_purchase_of_property_plant_and_equipment": "购买固定资产增长率",
        # "growth_acquisitions": "收购增长率",
        # "growth_purchase_of_investment_securities": "投资证券购买增长率",
        # "growth_sale_and_maturity_of_investments": "投资出售及到期增长率",
        # "growth_other_investing_activities": "其他投资活动增长率",
        # "growth_net_cash_from_investing_activities": "投资活动现金流增长率",
        # "growth_repayment_of_debt": "偿债增长率",
        # "growth_common_stock_issued": "普通股发行增长率",
        # "growth_common_stock_repurchased": "普通股回购增长率",
        # "growth_dividends_paid": "支付的股息增长率",
        # "growth_other_financing_activities": "其他融资活动增长率",
        # "growth_net_cash_from_financing_activities": "融资活动现金流增长率",
        # "growth_effect_of_exchange_rate_changes_on_cash": "汇率变动对现金的影响增长率",
        # "growth_net_change_in_cash_and_equivalents": "现金及等价物净变动增长率",
        # "growth_cash_at_beginning_of_period": "期初现金增长率",
        # "growth_cash_at_end_of_period": "期末现金增长率",
        "growth_operating_cash_flow": "经营活动产生的现金流量净额同比增长率（百分比）",
        # "growth_capital_expenditure": "资本支出增长率",
        # "growth_free_cash_flow": "自由现金流增长率"
    }

    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))
    growth_net_income: Optional[float] = Field(
        default=None,
        description="Growth rate of net income.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_depreciation_and_amortization: Optional[float] = Field(
        default=None,
        description="Growth rate of depreciation and amortization.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_deferred_income_tax: Optional[float] = Field(
        default=None,
        description="Growth rate of deferred income tax.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_stock_based_compensation: Optional[float] = Field(
        default=None,
        description="Growth rate of stock-based compensation.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_change_in_working_capital: Optional[float] = Field(
        default=None,
        description="Growth rate of change in working capital.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_account_receivables: Optional[float] = Field(
        default=None,
        description="Growth rate of accounts receivables.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_inventory: Optional[float] = Field(
        default=None,
        description="Growth rate of inventory.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_account_payable: Optional[float] = Field(
        default=None,
        description="Growth rate of account payable.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_working_capital: Optional[float] = Field(
        default=None,
        description="Growth rate of other working capital.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_non_cash_items: Optional[float] = Field(
        default=None,
        description="Growth rate of other non-cash items.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_cash_from_operating_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of net cash provided by operating activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_purchase_of_property_plant_and_equipment: Optional[float] = Field(
        default=None,
        description="Growth rate of investments in property, plant, and equipment.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_acquisitions: Optional[float] = Field(
        default=None,
        description="Growth rate of net acquisitions.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_purchase_of_investment_securities: Optional[float] = Field(
        default=None,
        description="Growth rate of purchases of investments.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_sale_and_maturity_of_investments: Optional[float] = Field(
        default=None,
        description="Growth rate of sales maturities of investments.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_investing_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of other investing activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_cash_from_investing_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of net cash used for investing activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_repayment_of_debt: Optional[float] = Field(
        default=None,
        description="Growth rate of debt repayment.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_common_stock_issued: Optional[float] = Field(
        default=None,
        description="Growth rate of common stock issued.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_common_stock_repurchased: Optional[float] = Field(
        default=None,
        description="Growth rate of common stock repurchased.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_dividends_paid: Optional[float] = Field(
        default=None,
        description="Growth rate of dividends paid.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_financing_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of other financing activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_cash_from_financing_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of net cash used/provided by financing activities.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_effect_of_exchange_rate_changes_on_cash: Optional[float] = Field(
        default=None,
        description="Growth rate of the effect of foreign exchange changes on cash.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_change_in_cash_and_equivalents: Optional[float] = Field(
        default=None,
        description="Growth rate of net change in cash.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_cash_at_beginning_of_period: Optional[float] = Field(
        default=None,
        description="Growth rate of cash at the beginning of the period.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_cash_at_end_of_period: Optional[float] = Field(
        default=None,
        description="Growth rate of cash at the end of the period.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_operating_cash_flow: Optional[float] = Field(
        default=None,
        description="Growth rate of operating cash flow.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_capital_expenditure: Optional[float] = Field(
        default=None,
        description="Growth rate of capital expenditure.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_free_cash_flow: Optional[float] = Field(
        default=None,
        description="Growth rate of free cash flow.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )


class XiaoYuanCashFlowStatementGrowthFetcher(
    Fetcher[
        XiaoYuanCashFlowStatementGrowthQueryParams,
        List[XiaoYuanCashFlowStatementGrowthData],
    ]
):
    """XiaoYuan Cash Flow Statement Growth Fetcher."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanCashFlowStatementGrowthQueryParams:
        """Transform the query params."""
        return XiaoYuanCashFlowStatementGrowthQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanCashFlowStatementGrowthQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanCashFlowStatementGrowthData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        FIN_METRICS_PER_SHARE = [
            "净利润同比增长率（百分比）",
            "经营活动产生的现金流量净额同比增长率（百分比）",
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
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
        query: XiaoYuanCashFlowStatementGrowthQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanCashFlowStatementGrowthData]:
        """Return the transformed data."""
        return [XiaoYuanCashFlowStatementGrowthData.model_validate(d) for d in data]
