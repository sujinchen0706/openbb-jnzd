import datetime

from openbb_core.app.service.user_service import UserService
from openbb_fmp_extension.models.dcf import FMPDcfFetcher
from openbb_fmp_extension.models.form_13f import FMPForm13FHRFetcher
from openbb_fmp_extension.models.government_trades import FMPGovernmentTradesFetcher
from openbb_fmp_extension.models.historical_rating import FMPHistoricalRatingFetcher
from openbb_fmp_extension.models.rating import FMPRatingFetcher

test_credentials = UserService().default_user_settings.credentials.model_dump(
    mode="json"
)


def test_fmp_form_13f_fetcher(credentials=test_credentials):
    """Test FMP form 13f fetcher."""
    params = {
        "symbol": "0001388838",
        "date": datetime.date(2021, 9, 30),
        "limit": 1,
    }
    fetcher = FMPForm13FHRFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_fmp_government_trades_fetcher(credentials=test_credentials):
    """Test FMP government trades fetcher.
    params limit only functions when there is no parameter symbol
    """
    params = {
        "chamber": "all",
        "symbol": "AAPL",
    }
    fetcher = FMPGovernmentTradesFetcher()
    result = fetcher.test(params, credentials)
    assert result is None
    params = {
        "chamber": "all",
        "limit": 300,
    }
    fetcher = FMPGovernmentTradesFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_fmp_dcf_fetcher(credentials=test_credentials):
    """Test Dcf fetcher."""
    params = {
        "symbol": "AAPL,A",
    }
    fetcher = FMPDcfFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_fmp_rating_fetcher(credentials=test_credentials):
    """Test FMP Advanced Dcf fetcher.
    params limit only functions when there is no parameter symbol
    """
    params = {
        "symbol": "AAPL,600519.SS",
    }
    fetcher = FMPRatingFetcher()
    result = fetcher.test(params, credentials)
    assert result is None


def test_fmp_historical_rating_fetcher(credentials=test_credentials):
    """Test FMP Advanced Dcf fetcher.
    params limit only functions when there is no parameter symbol
    """
    params = {
        "symbol": "AAPL",
    }
    fetcher = FMPHistoricalRatingFetcher()
    result = fetcher.test(params, credentials)
    assert result is None
