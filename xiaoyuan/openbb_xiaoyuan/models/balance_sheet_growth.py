"""XiaoYuan Balance Sheet Growth Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.balance_sheet_growth import (
    BalanceSheetGrowthData,
    BalanceSheetGrowthQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_xiaoyuan.utils.references import (
    caculate_sql,
    calculateGrowth,
    convert_stock_code_format,
    extractMonthDayFromTime,
    get_query_finance_sql,
    get_report_month,
    getFiscalQuarterFromTime,
    revert_stock_code_format,
)
from pydantic import Field, model_validator


class XiaoYuanBalanceSheetGrowthQueryParams(BalanceSheetGrowthQueryParams):
    """XiaoYuan Balance Sheet Growth Query.

    Source:  https://site.financialmodelingprep.com/developer/docs/#Financial-Statements-Growth
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


class XiaoYuanBalanceSheetGrowthData(BalanceSheetGrowthData):
    """XiaoYuan Balance Sheet Growth Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "growth_total_assets": "总资产同比增长率（百分比）",
    }

    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))
    growth_cash_and_cash_equivalents: Optional[float] = Field(
        default=None,
        description="Growth rate of cash and cash equivalents.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_short_term_investments: Optional[float] = Field(
        default=None,
        description="Growth rate of short-term investments.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_cash_and_short_term_investments: Optional[float] = Field(
        default=None,
        description="Growth rate of cash and short-term investments.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_net_receivables: Optional[float] = Field(
        default=None,
        description="Growth rate of net receivables.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_inventory: Optional[float] = Field(
        default=None,
        description="Growth rate of inventory.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_current_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of other current assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_total_current_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of total current assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_property_plant_equipment_net: Optional[float] = Field(
        default=None,
        description="Growth rate of net property, plant, and equipment.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_goodwill: Optional[float] = Field(
        default=None,
        description="Growth rate of goodwill.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_intangible_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of intangible assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_goodwill_and_intangible_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of goodwill and intangible assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_long_term_investments: Optional[float] = Field(
        default=None,
        description="Growth rate of long-term investments.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_tax_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of tax assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_non_current_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of other non-current assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_total_non_current_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of total non-current assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_other_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of other assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_total_assets: Optional[float] = Field(
        default=None,
        description="Growth rate of total assets.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_account_payables: Optional[float] = Field(
        default=None,
        description="Growth rate of accounts payable.",
        json_schema_extra={"x-unit_measurement": "percent", "x-frontend_multiply": 100},
    )
    growth_short_term_debt: Optional[float] = Field(
        default=None,
        description="Growth rate of short-term debt.",
    )
    growth_tax_payables: Optional[float] = Field(
        default=None,
        description="Growth rate of tax payables.",
    )
    growth_deferred_revenue: Optional[float] = Field(
        default=None,
        description="Growth rate of deferred revenue.",
    )
    growth_other_current_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of other current liabilities.",
    )
    growth_total_current_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of total current liabilities.",
    )
    growth_long_term_debt: Optional[float] = Field(
        default=None,
        description="Growth rate of long-term debt.",
    )
    growth_deferred_revenue_non_current: Optional[float] = Field(
        default=None,
        description="Growth rate of non-current deferred revenue.",
    )
    growth_deferrred_tax_liabilities_non_current: Optional[float] = Field(
        default=None,
        description="Growth rate of non-current deferred tax liabilities.",
    )
    growth_other_non_current_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of other non-current liabilities.",
    )
    growth_total_non_current_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of total non-current liabilities.",
    )
    growth_other_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of other liabilities.",
    )
    growth_total_liabilities: Optional[float] = Field(
        default=None,
        description="Growth rate of total liabilities.",
    )
    growth_common_stock: Optional[float] = Field(
        default=None,
        description="Growth rate of common stock.",
    )
    growth_retained_earnings: Optional[float] = Field(
        default=None,
        description="Growth rate of retained earnings.",
    )
    growth_accumulated_other_comprehensive_income: Optional[float] = Field(
        default=None,
        description="Growth rate of accumulated other comprehensive income/loss.",
    )
    growth_other_total_shareholders_equity: Optional[float] = Field(
        default=None,
        description="Growth rate of other total stockholders' equity.",
    )
    growth_total_shareholders_equity: Optional[float] = Field(
        default=None,
        description="Growth rate of total stockholders' equity.",
    )
    growth_total_liabilities_and_shareholders_equity: Optional[float] = Field(
        default=None,
        description="Growth rate of total liabilities and stockholders' equity.",
    )
    growth_total_investments: Optional[float] = Field(
        default=None,
        description="Growth rate of total investments.",
    )
    growth_total_debt: Optional[float] = Field(
        default=None,
        description="Growth rate of total debt.",
    )
    growth_net_debt: Optional[float] = Field(
        default=None,
        description="Growth rate of net debt.",
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


class XiaoYuanBalanceSheetGrowthFetcher(
    Fetcher[
        XiaoYuanBalanceSheetGrowthQueryParams,
        List[XiaoYuanBalanceSheetGrowthData],
    ]
):
    """XiaoYuan Balance Sheet Growth Fetcher."""

    @staticmethod
    def transform_query(
        params: Dict[str, Any]
    ) -> XiaoYuanBalanceSheetGrowthQueryParams:
        """Transform the query params."""
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
        return XiaoYuanBalanceSheetGrowthQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanBalanceSheetGrowthQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanBalanceSheetGrowthData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        FIN_METRICS_PER_SHARE = [
            "总资产同比增长率（百分比）",
        ]
        metrics_mapping = {
            "growth_inventory": "存货",
            "growth_other_current_assets": "其他流动资产",
            "growth_total_current_assets": "流动资产合计",
            "growth_goodwill": "商誉",
            "growth_intangible_assets": "无形资产",
            "growth_other_non_current_assets": "其他非流动资产",
            "growth_total_non_current_assets": "非流动资产合计",
            "growth_account_payables": "应付账款",
            "growth_other_current_liabilities": "其他流动负债",
            "growth_total_current_liabilities": "流动负债合计",
            "growth_other_non_current_liabilities": "其他非流动负债",
            "growth_total_non_current_liabilities": "非流动负债合计",
            "growth_total_liabilities": "负债合计",
            "growth_retained_earnings": "留存收益",
            "growth_total_shareholders_equity": "股东权益合计",
            "growth_total_liabilities_and_shareholders_equity": "负债和股东权益合计",
            "growth_net_debt": "净债务",
        }
        report_month = get_report_month(query.period, -query.limit - 1)
        balance_growth_caculate = calculateGrowth + caculate_sql(metrics_mapping)

        finance_sql = get_query_finance_sql(
            FIN_METRICS_PER_SHARE + list(metrics_mapping.values()),
            [query.symbol],
            report_month,
        )
        df = reader._run_query(
            script=extractMonthDayFromTime
            + getFiscalQuarterFromTime
            + finance_sql
            + balance_growth_caculate,
        )

        if df is None or df.empty:
            raise EmptyDataError()
        df.drop(columns=list(metrics_mapping.values()), inplace=True)
        df = df.iloc[-query.limit :]
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        columns_to_divide = FIN_METRICS_PER_SHARE
        df[columns_to_divide] /= 100
        df.sort_values(by="报告期", ascending=False, inplace=True)
        data = df.to_dict(orient="records")
        return data

    @staticmethod
    def transform_data(
        query: XiaoYuanBalanceSheetGrowthQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanBalanceSheetGrowthData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        return [XiaoYuanBalanceSheetGrowthData.model_validate(d) for d in data]
