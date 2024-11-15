"""Tests for XiaoYuan fetchers."""

from datetime import date

import pytest
from openbb_core.app.service.user_service import UserService
from openbb_xiaoyuan import (
    XiaoYuanAggressiveSmallCapsFetcher,
    XiaoYuanCountryProfileFetcher,
    XiaoYuanEquityProfileFetcher,
    XiaoYuanEquitySearchFetcher,
    XiaoYuanEquityValuationMultiplesFetcher,
    XiaoYuanGainersFetcher,
    XiaoYuanIndexHistoricalFetcher,
    XiaoYuanIndexSearchFetcher,
    XiaoYuanKeyMetricsFetcher,
    XiaoYuanLosersFetcher,
)
from openbb_xiaoyuan.models.balance_sheet import XiaoYuanBalanceSheetFetcher
from openbb_xiaoyuan.models.balance_sheet_growth import (
    XiaoYuanBalanceSheetGrowthFetcher,
)
from openbb_xiaoyuan.models.calendar_dividend import XiaoYuanCalendarDividendFetcher
from openbb_xiaoyuan.models.cash_flow import XiaoYuanCashFlowStatementFetcher
from openbb_xiaoyuan.models.cash_flow_growth import (
    XiaoYuanCashFlowStatementGrowthFetcher,
)
from openbb_xiaoyuan.models.equity_historical import XiaoYuanEquityHistoricalFetcher
from openbb_xiaoyuan.models.etf_info import XiaoYuanEtfInfoFetcher
from openbb_xiaoyuan.models.etf_search import XiaoYuanEtfSearchFetcher
from openbb_xiaoyuan.models.financial_ratios import (
    XiaoYuanFinancialRatiosFetcher,
)
from openbb_xiaoyuan.models.gdp_real import XiaoYuanGdpRealFetcher
from openbb_xiaoyuan.models.historical_dividends import (
    XiaoYuanHistoricalDividendsFetcher,
)
from openbb_xiaoyuan.models.historical_market_cap import (
    XiaoYuanHistoricalMarketCapFetcher,
)
from openbb_xiaoyuan.models.income_statement_growth import (
    XiaoYuanIncomeStatementGrowthFetcher,
)

from xiaoyuan.openbb_xiaoyuan import XiaoYuanIncomeStatementFetcher

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
    params = {"symbol": "600519.SS", "period": "annual", "limit": 4}
    fetcher = XiaoYuanFinancialRatiosFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_cash_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanCashFlowStatementGrowthFetcher."""
    params = {"symbol": "600519.SS", "period": "annual", "limit": 4}
    fetcher = XiaoYuanCashFlowStatementGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_balance_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanBalanceSheetGrowthFetcher."""
    params = {"symbol": "600519.SS", "period": "annual", "limit": 4}
    fetcher = XiaoYuanBalanceSheetGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_cash_flow_fetcher(credentials=test_credentials):
    params = {
        "symbol": "600519.SS",
        "period": "quarter",
    }

    fetcher = XiaoYuanCashFlowStatementFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_balance_sheet_fetcher(credentials=test_credentials):
    """Test XiaoYuanBalanceSheetFetcher."""
    params = {"symbol": "600519.SS", "period": "ytd"}

    fetcher = XiaoYuanBalanceSheetFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_income_statement_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementFetcher."""
    params = {"symbol": "600519.SS", "period": "annual"}

    fetcher = XiaoYuanIncomeStatementFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_key_metrics_fetcher(credentials=test_credentials):
    """Test XiaoYuanKeyMetricsFetcher."""
    params = {"symbol": "600519.SS,002415.SZ", "period": "ytd"}

    fetcher = XiaoYuanKeyMetricsFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_income_statement_growth_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementGrowthFetcher."""
    params = {"symbol": "600519.SS", "period": "quarter", "limit": 4}

    fetcher = XiaoYuanIncomeStatementGrowthFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_equity_historical_fetcher(credentials=test_credentials):
    """Test XiaoYuanEquityHistoricalFetcher."""
    params = {
        "symbol": "600519.SS",
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
        "symbol": "600519.SS",
        "start_date": date(2023, 1, 1),
        "end_date": date(2023, 1, 10),
        "interval": "1d",
    }

    fetcher = XiaoYuanHistoricalMarketCapFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiaoyuan_equity_valuation_multiples_fetcher(credentials=test_credentials):
    """Test XiaoYuanIncomeStatementGrowthFetcher."""
    params = {"symbol": "600519.SS,002415.SZ"}

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
        "symbol": "600519.SS",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 10, 1),
    }

    fetcher = XiaoYuanHistoricalDividendsFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_equity_search_fetcher(credentials=test_credentials):
    """Test XiaoYuanEquitySearchFetcher."""
    params = {"query": "600519.SS", "is_symbol": True}

    fetcher = XiaoYuanEquitySearchFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_etf_search_fetcher(credentials=test_credentials):
    """Test XiaoYuanEtfSearchFetcher."""
    params = {"query": "510300.SS"}
    fetcher = XiaoYuanEtfSearchFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_etf_historical_fetcher(credentials=test_credentials):
    """Test XiaoYuanEtfSearchFetcher."""
    params = {"symbol": "510300.SS"}
    fetcher = XiaoYuanEquityHistoricalFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_index_search_fetcher(credentials=test_credentials):
    """Test XiaoYuanIndexSearchFetcher."""
    params = {"query": "000300.SS"}
    fetcher = XiaoYuanIndexSearchFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_index_historical_fetcher(credentials=test_credentials):
    """Test XiaoYuanIndexHistoricalFetcher."""
    params = {
        "symbol": "000300.SS",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 10, 1),
    }
    fetcher = XiaoYuanIndexHistoricalFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_gainers_fetcher(credentials=test_credentials):
    """Test XiaoYuanGainersFetcher."""
    params = {
        "sort": "desc",
    }
    fetcher = XiaoYuanGainersFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_losers_fetcher(credentials=test_credentials):
    """Test XiaoYuanLosersFetcher."""
    params = {
        "sort": "desc",
    }
    fetcher = XiaoYuanLosersFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_aggressive_small_caps_fetcher(credentials=test_credentials):
    """Test XiaoYuanAggressiveSmallCapsFetcher."""
    params = {
        "sort": "desc",
    }
    fetcher = XiaoYuanAggressiveSmallCapsFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


# def test_xiao_yuan_price_performance_fetcher(credentials=test_credentials):
#     """Test XiaoYuanPricePerformanceFetcher."""
#     params = {
#         "symbol": "600519.SS",
#     }
#     fetcher = XiaoYuanPricePerformanceFetcher()
#     result = fetcher.test(params, credentials)
#     assert result is None


def test_xiao_yuan_equity_profile_fetcher(credentials=test_credentials):
    """Test XiaoYuanEquityProfileFetcher."""
    params = {
        "symbol": "600519.SS",
    }
    fetcher = XiaoYuanEquityProfileFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_etf_info_fetcher(credentials=test_credentials):
    """Test XiaoYuanEtfInfoFetcher."""
    params = {
        "symbol": "510300.SS",
    }
    fetcher = XiaoYuanEtfInfoFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_gdp_real_fetcher(credentials=test_credentials):
    """Test XiaoYuanEtfInfoFetcher."""
    params = {
        "start_date": date(2023, 1, 1),
        "end_date": date(2024, 10, 10),
        "frequency": "annual",
    }
    fetcher = XiaoYuanGdpRealFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_xiao_yuan_country_profile_fetcher(credentials=test_credentials):
    """Test XiaoYuanCountryProfileFetcher."""
    params = {
        "latest": True,
    }
    fetcher = XiaoYuanCountryProfileFetcher()
    result = fetcher.test(params, credentials)
    assert result is None
