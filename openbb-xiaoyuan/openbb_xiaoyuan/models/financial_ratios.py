"""XiaoYuan Financial Ratios Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.financial_ratios import (
    FinancialRatiosData,
    FinancialRatiosQueryParams,
)
from openbb_core.provider.utils.descriptions import QUERY_DESCRIPTIONS
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, model_validator

from openbb_xiaoyuan.utils.references import (
    get_report_month,
    get_query_finance_sql,
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
)


class XiaoYuanFinancialRatiosQueryParams(FinancialRatiosQueryParams):
    """XiaoYuan Financial Ratios Query.

    Source: https://financialmodelingprep.com/developer/docs/#Company-Financial-Ratios
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


class XiaoYuanFinancialRatiosData(FinancialRatiosData):
    """XiaoYuan Financial Ratios Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        # "timestamp": "公告日",
        "current_ratio": "流动比率",
        "quick_ratio": "速动比率",
        # "cash_ratio": "现金比率", 东财有，zvt没有
        "days_of_sales_outstanding": "应收账款周转天数（含应收票据）",
        "days_of_inventory_outstanding": "存货周转天数",
        "operating_cycle": "营业周期",
        "days_of_payables_outstanding": "应付账款周转天数（含应付票据）",
        # "cash_conversion_cycle": "现金转换周期",
        "gross_profit_margin": "销售毛利率（百分比）",
        "operating_profit_margin": "营业利润比营业总收入（百分比）",  # 营业利润率
        # "pretax_profit_margin": "税前利润率",
        "net_profit_margin": "净利润比营业总收入（百分比）",  # 净利润率
        # "effective_tax_rate": "有效税率",
        "return_on_assets": "总资产净利率ROA（百分比）",
        "return_on_equity": "净资产收益率ROE（摊薄）（百分比）",
        "return_on_capital_employed": "投入资本回报率ROIC（百分比）",
        "net_income_per_ebt": "净利润比利润总额",
        "ebt_per_ebit": "利润总额比息税前利润",
        "ebit_per_revenue": "息税前利润比营业总收入",
        "debt_ratio": "资产负债率",
        "debt_equity_ratio": "产权比率",
        # "long_term_debt_to_capitalization": "长期负债资本化率",
        # "total_debt_to_capitalization": "总负债资本化率",
        # "interest_coverage": "利息覆盖率",
        # "cash_flow_to_debt_ratio": "现金流/债务比率",
        # "company_equity_multiplier": "公司权益乘数",
        "receivables_turnover": "应收账款周转率（含应收票据）",
        "payables_turnover": "应付账款周转率",
        "inventory_turnover": "存货周转率",
        # "fixed_asset_turnover": "固定资产周转率",
        # "asset_turnover": "总资产周转率",
        # "operating_cash_flow_per_share": "每股营业现金流",
        # "free_cash_flow_per_share": "每股自由现金流",
        # "cash_per_share": "每股现金",
        # "payout_ratio": "派息率",
        # "operating_cash_flow_sales_ratio": "营业现金流销售比率",
        # "free_cash_flow_operating_cash_flow_ratio": "自由现金流/营业现金流比率",
        # "cash_flow_coverage_ratios": "现金流覆盖率",
        # "short_term_coverage_ratios": "短期覆盖率",
        # "capital_expenditure_coverage_ratio": "资本支出覆盖率",
        # "dividend_paid_and_capex_coverage_ratio": "股息和资本支出覆盖率",
        # "dividend_payout_ratio": "股息支付率",
        # "price_book_value_ratio": "市净率",
        # "price_to_book_ratio": "市价/账面价值比率",
        # "price_to_sales_ratio": "市销率",
        # "price_earnings_ratio": "市盈率",
        # "price_to_free_cash_flows_ratio": "市值/自由现金流比率",
        # "price_to_operating_cash_flows_ratio": "市值/营业现金流比率",
        # "price_cash_flow_ratio": "市现率",
        # "price_earnings_to_growth_ratio": "市盈率增长比率",
        # "price_sales_ratio": "市销率",
        # "dividend_yield": "股息收益率",
        # "dividend_yield_percentage": "股息收益率百分比",
        # "dividend_per_share": "每股股息",
        # "enterprise_value_multiple": "企业价值倍数",
        # "price_fair_value": "市价/公允价值比率"
    }

    current_ratio: Optional[float] = Field(default=None, description="Current ratio.")
    quick_ratio: Optional[float] = Field(default=None, description="Quick ratio.")
    cash_ratio: Optional[float] = Field(default=None, description="Cash ratio.")
    days_of_sales_outstanding: Optional[float] = Field(
        default=None, description="Days of sales outstanding."
    )
    days_of_inventory_outstanding: Optional[float] = Field(
        default=None, description="Days of inventory outstanding."
    )
    operating_cycle: Optional[float] = Field(
        default=None, description="Operating cycle."
    )
    days_of_payables_outstanding: Optional[float] = Field(
        default=None, description="Days of payables outstanding."
    )
    cash_conversion_cycle: Optional[float] = Field(
        default=None, description="Cash conversion cycle."
    )
    gross_profit_margin: Optional[float] = Field(
        default=None, description="Gross profit margin."
    )
    operating_profit_margin: Optional[float] = Field(
        default=None, description="Operating profit margin."
    )
    pretax_profit_margin: Optional[float] = Field(
        default=None, description="Pretax profit margin."
    )
    net_profit_margin: Optional[float] = Field(
        default=None, description="Net profit margin."
    )
    effective_tax_rate: Optional[float] = Field(
        default=None, description="Effective tax rate."
    )
    return_on_assets: Optional[float] = Field(
        default=None, description="Return on assets."
    )
    return_on_equity: Optional[float] = Field(
        default=None, description="Return on equity."
    )
    return_on_capital_employed: Optional[float] = Field(
        default=None, description="Return on capital employed."
    )
    net_income_per_ebt: Optional[float] = Field(
        default=None, description="Net income per EBT."
    )
    ebt_per_ebit: Optional[float] = Field(default=None, description="EBT per EBIT.")
    ebit_per_revenue: Optional[float] = Field(
        default=None, description="EBIT per revenue."
    )
    debt_ratio: Optional[float] = Field(default=None, description="Debt ratio.")
    debt_equity_ratio: Optional[float] = Field(
        default=None, description="Debt equity ratio."
    )
    long_term_debt_to_capitalization: Optional[float] = Field(
        default=None, description="Long term debt to capitalization."
    )
    total_debt_to_capitalization: Optional[float] = Field(
        default=None, description="Total debt to capitalization."
    )
    interest_coverage: Optional[float] = Field(
        default=None, description="Interest coverage."
    )
    cash_flow_to_debt_ratio: Optional[float] = Field(
        default=None, description="Cash flow to debt ratio."
    )
    company_equity_multiplier: Optional[float] = Field(
        default=None, description="Company equity multiplier."
    )
    receivables_turnover: Optional[float] = Field(
        default=None, description="Receivables turnover."
    )
    payables_turnover: Optional[float] = Field(
        default=None, description="Payables turnover."
    )
    inventory_turnover: Optional[float] = Field(
        default=None, description="Inventory turnover."
    )
    fixed_asset_turnover: Optional[float] = Field(
        default=None, description="Fixed asset turnover."
    )
    asset_turnover: Optional[float] = Field(default=None, description="Asset turnover.")
    operating_cash_flow_per_share: Optional[float] = Field(
        default=None, description="Operating cash flow per share."
    )
    free_cash_flow_per_share: Optional[float] = Field(
        default=None, description="Free cash flow per share."
    )
    cash_per_share: Optional[float] = Field(default=None, description="Cash per share.")
    payout_ratio: Optional[float] = Field(default=None, description="Payout ratio.")
    operating_cash_flow_sales_ratio: Optional[float] = Field(
        default=None, description="Operating cash flow sales ratio."
    )
    free_cash_flow_operating_cash_flow_ratio: Optional[float] = Field(
        default=None, description="Free cash flow operating cash flow ratio."
    )
    cash_flow_coverage_ratios: Optional[float] = Field(
        default=None, description="Cash flow coverage ratios."
    )
    short_term_coverage_ratios: Optional[float] = Field(
        default=None, description="Short term coverage ratios."
    )
    capital_expenditure_coverage_ratio: Optional[float] = Field(
        default=None, description="Capital expenditure coverage ratio."
    )
    dividend_paid_and_capex_coverage_ratio: Optional[float] = Field(
        default=None, description="Dividend paid and capex coverage ratio."
    )
    dividend_payout_ratio: Optional[float] = Field(
        default=None, description="Dividend payout ratio."
    )
    price_book_value_ratio: Optional[float] = Field(
        default=None, description="Price book value ratio."
    )
    price_to_book_ratio: Optional[float] = Field(
        default=None, description="Price to book ratio."
    )
    price_to_sales_ratio: Optional[float] = Field(
        default=None, description="Price to sales ratio."
    )
    price_earnings_ratio: Optional[float] = Field(
        default=None, description="Price earnings ratio."
    )
    price_to_free_cash_flows_ratio: Optional[float] = Field(
        default=None, description="Price to free cash flows ratio."
    )
    price_to_operating_cash_flows_ratio: Optional[float] = Field(
        default=None, description="Price to operating cash flows ratio."
    )
    price_cash_flow_ratio: Optional[float] = Field(
        default=None, description="Price cash flow ratio."
    )
    price_earnings_to_growth_ratio: Optional[float] = Field(
        default=None, description="Price earnings to growth ratio."
    )
    price_sales_ratio: Optional[float] = Field(
        default=None, description="Price sales ratio."
    )
    dividend_yield: Optional[float] = Field(default=None, description="Dividend yield.")
    dividend_yield_percentage: Optional[float] = Field(
        default=None, description="Dividend yield percentage."
    )
    dividend_per_share: Optional[float] = Field(
        default=None, description="Dividend per share."
    )
    enterprise_value_multiple: Optional[float] = Field(
        default=None, description="Enterprise value multiple."
    )
    price_fair_value: Optional[float] = Field(
        default=None, description="Price fair value."
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


class XiaoYuanFinancialRatiosFetcher(
    Fetcher[
        XiaoYuanFinancialRatiosQueryParams,
        List[XiaoYuanFinancialRatiosData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanFinancialRatiosQueryParams:
        """Transform the query params."""
        return XiaoYuanFinancialRatiosQueryParams(**params)

    @staticmethod
    def extract_data(
        # pylint: disable=unused-argument
        query: XiaoYuanFinancialRatiosQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[XiaoYuanFinancialRatiosData]:
        """Extract the data from the XiaoYuan Finance endpoints."""
        from jinniuai_data_store.reader import get_jindata_reader

        reader = get_jindata_reader()
        FIN_METRICS_PER_SHARE = [
            "流动比率",
            "速动比率",
            "固定资产周转率",
            "总资产周转率",
            "存货周转率",
            "存货周转天数",
            "应收账款周转率（含应收票据）",
            "应收账款周转天数（含应收票据）",
            "营业周期",
            "应付账款周转率",
            "应付账款周转天数（含应付票据）",
            "净资产收益率ROE（摊薄）（百分比）",
            "总资产净利率ROA（百分比）",
            "投入资本回报率ROIC（百分比）",
            "销售毛利率（百分比）",
            "净利润比营业总收入（百分比）",
            "营业利润比营业总收入（百分比）",
            "净利润比利润总额",
            "利润总额比息税前利润",
            "息税前利润比营业总收入",
            "资产负债率",
            "产权比率",
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
        columns_to_divide = [
            "净资产收益率ROE（摊薄）（百分比）",
            "总资产净利率ROA（百分比）",
            "投入资本回报率ROIC（百分比）",
            "销售毛利率（百分比）",
            "净利润比营业总收入（百分比）",
            "营业利润比营业总收入（百分比）",
            "净利润比利润总额",
            "利润总额比息税前利润",
            "息税前利润比营业总收入",
            "资产负债率",
        ]
        df[columns_to_divide] /= 100
        df.sort_values(by="报告期", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanFinancialRatiosQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanFinancialRatiosData]:
        """Return the transformed data."""
        return [XiaoYuanFinancialRatiosData.model_validate(d) for d in data]
