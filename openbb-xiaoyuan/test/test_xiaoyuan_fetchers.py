"""Tests for XiaoYuan fetchers."""

import pytest
from datetime import date
from openbb_core.app.service.user_service import UserService

from openbb_xiaoyuan import (
    XiaoYuanEquityValuationMultiplesFetcher,
    XiaoYuanKeyMetricsFetcher,
)
from openbb_xiaoyuan.models.balance_sheet import XiaoYuanBalanceSheetFetcher
from openbb_xiaoyuan.models.calendar_dividend import XiaoYuanCalendarDividendFetcher
from openbb_xiaoyuan.models.cash_flow import XiaoYuanCashFlowStatementFetcher
from openbb_xiaoyuan.models.cash_flow_growth import (
    XiaoYuanCashFlowStatementGrowthFetcher,
)
from openbb_xiaoyuan.models.equity_historical import XiaoYuanEquityHistoricalFetcher
from openbb_xiaoyuan.models.financial_ratios import (
    XiaoYuanFinancialRatiosFetcher,
)
from openbb_xiaoyuan.models.balance_sheet_growth import (
    XiaoYuanBalanceSheetGrowthFetcher,
)
from openbb_xiaoyuan.models.historical_dividends import (
    XiaoYuanHistoricalDividendsFetcher,
)
from openbb_xiaoyuan.models.historical_market_cap import (
    XiaoYuanHistoricalMarketCapFetcher,
)
from openbb_xiaoyuan.models.income_statement_growth import (
    XiaoYuanIncomeStatementGrowthFetcher,
)
from openbb_xiaoyuan.models.income_statement import XiaoYuanIncomeStatementFetcher

test_credentials = UserService().default_user_settings.credentials.model_dump(
    mode="json"
)


@pytest.fixture(scope="module")
def vcr_config():
    """VCR configuration."""
    return {
        "filter_headers": [
            ("User-Agent", None),
            ("Cookie", "MOCK_COOKIE"),
            ("crumb", "MOCK_CRUMB"),
        ],
        "filter_query_parameters": [
            ("period1", "MOCK_PERIOD_1"),
            ("period2", "MOCK_PERIOD_2"),
            ("crumb", "MOCK_CRUMB"),
            ("date", "MOCK_DATE"),
        ],
    }


def test_xiaoyuan_financial_ratios_fetcher(credentials=test_credentials):
    """Test XiaoYuanFinancialRatiosFetcher."""
    params = {"symbol": "SH600519", "period": "annual", "limit": 4}
    fetcher = XiaoYuanFinancialRatiosFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_cash_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanCashFlowStatementGrowthFetcher."""
    params = {"symbol": "SH600519", "period": "annual", "limit": 4}
    fetcher = XiaoYuanCashFlowStatementGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_balance_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanBalanceSheetGrowthFetcher."""
    params = {"symbol": "SH600519", "period": "annual", "limit": 4}
    fetcher = XiaoYuanBalanceSheetGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_cash_flow_fetcher(credentials=test_credentials):
    params = {
        "symbol": "SH600519",
        "period": "annual",
    }

    fetcher = XiaoYuanCashFlowStatementFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_balance_sheet_fetcher(credentials=test_credentials):
    """Test XiaoYuanBalanceSheetFetcher."""
    params = {"symbol": "SH600519", "period": "ytd"}

    fetcher = XiaoYuanBalanceSheetFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_income_statement_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementFetcher."""
    params = {"symbol": "SH600519", "period": "ytd"}

    fetcher = XiaoYuanIncomeStatementFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_key_metrics_fetcher(credentials=test_credentials):
    """Test XiaoYuanKeyMetricsFetcher."""
    params = {"symbol": "SH600519,SZ002415", "period": "ytd"}

    fetcher = XiaoYuanKeyMetricsFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_income_statement_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementGrowthFetcher."""
    params = {"symbol": "SH600519", "period": "annual", "limit": 4}

    fetcher = XiaoYuanIncomeStatementGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_equity_historical_fetcher(credentials=test_credentials):
    """Test XiaoYuanEquityHistoricalFetcher."""
    params = {
        "symbol": "SH600519",
        "start_date": date(2023, 1, 1),
        "end_date": date(2023, 1, 10),
        "interval": "1d",
    }

    fetcher = XiaoYuanEquityHistoricalFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_historical_market_cap_fetcher(credentials=test_credentials):
    """Test XiaoYuanHistoricalMarketCapFetcher."""
    params = {
        "symbol": "SH600519",
        "start_date": date(2023, 1, 1),
        "end_date": date(2023, 1, 10),
        "interval": "1d",
    }

    fetcher = XiaoYuanHistoricalMarketCapFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_equity_valuation_multiples_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementGrowthFetcher."""
    params = {"symbol": "SH600519,SZ002415"}

    fetcher = XiaoYuanEquityValuationMultiplesFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_calendar_dividend_fetcher(credentials=test_credentials):
    """Test XiaoYuanCalendarDividendFetcher."""
    params = {
        "start_date": date(2023, 1, 1),
        "end_date": date(2023, 5, 1),
    }

    fetcher = XiaoYuanCalendarDividendFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_historical_dividends_fetcher(credentials=test_credentials):
    """Test XiaoYuanHistoricalDividendsFetcher."""
    params = {
        "symbol": "SH600519",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 10, 1),
    }

    fetcher = XiaoYuanHistoricalDividendsFetcher()
    result = fetcher.test(params, credentials)
    assert result is None
