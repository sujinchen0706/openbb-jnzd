"""openbb_xiaoyuan OpenBB Platform Provider."""

from openbb_core.provider.abstract.provider import Provider

from openbb_xiaoyuan.models.balance_sheet import XiaoYuanBalanceSheetFetcher
from openbb_xiaoyuan.models.balance_sheet_growth import (
    XiaoYuanBalanceSheetGrowthFetcher,
)
from openbb_xiaoyuan.models.calendar_dividend import XiaoYuanCalendarDividendFetcher
from openbb_xiaoyuan.models.cash_flow import XiaoYuanCashFlowStatementFetcher
from openbb_xiaoyuan.models.cash_flow_growth import (
    XiaoYuanCashFlowStatementGrowthFetcher,
)
from openbb_xiaoyuan.models.equity_valuation_multiples import (
    XiaoYuanEquityValuationMultiplesFetcher,
)
from openbb_xiaoyuan.models.equity_historical import XiaoYuanEquityHistoricalFetcher
from openbb_xiaoyuan.models.financial_ratios import XiaoYuanFinancialRatiosFetcher
from openbb_xiaoyuan.models.historical_dividends import (
    XiaoYuanHistoricalDividendsFetcher,
)
from openbb_xiaoyuan.models.historical_market_cap import (
    XiaoYuanHistoricalMarketCapFetcher,
)
from openbb_xiaoyuan.models.income_statement import XiaoYuanIncomeStatementFetcher
from openbb_xiaoyuan.models.key_metrics import XiaoYuanKeyMetricsFetcher

from openbb_xiaoyuan.models.income_statement_growth import (
    XiaoYuanIncomeStatementGrowthFetcher,
)

# mypy: disable-error-code="list-item"


openbb_xiaoyuan_provider = Provider(
    name="xiaoyuan",
    description="Data provider for openbb-xiaoyuan.",
    # credentials=["api_key"],
    website="https://openbb-xiaoyuan.com",
    fetcher_dict={
        "CashFlowStatement": XiaoYuanCashFlowStatementFetcher,
        "FinancialRatios": XiaoYuanFinancialRatiosFetcher,
        "CashFlowStatementGrowth": XiaoYuanCashFlowStatementGrowthFetcher,
        "BalanceSheetGrowth": XiaoYuanBalanceSheetGrowthFetcher,
        "BalanceSheet": XiaoYuanBalanceSheetFetcher,
        "IncomeStatement": XiaoYuanIncomeStatementFetcher,
        "IncomeStatementGrowth": XiaoYuanIncomeStatementGrowthFetcher,
        "EquityHistorical": XiaoYuanEquityHistoricalFetcher,
        "HistoricalMarketCap": XiaoYuanHistoricalMarketCapFetcher,
        "KeyMetrics": XiaoYuanKeyMetricsFetcher,
        "EquityValuationMultiples": XiaoYuanEquityValuationMultiplesFetcher,
        "CalendarDividend": XiaoYuanCalendarDividendFetcher,
        "HistoricalDividends": XiaoYuanHistoricalDividendsFetcher,
    },
)
