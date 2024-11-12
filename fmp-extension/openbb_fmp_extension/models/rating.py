"""Rating Model."""

import asyncio
from typing import Any, Dict, List, Optional
from warnings import warn

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_core.provider.utils.helpers import to_snake_case

from openbb_fmp_extension.utils.helpers import get_jsonparsed_data
from openbb_fmp_extension.standard_models.rating import (
    RatingData,
    RatingQueryParams,
)
from openbb_fmp.utils.helpers import create_url




class FMPRatingQueryParams(RatingQueryParams):
    """Rating Query Parameters.

    Source: https://financialmodelingprep.com/api/v3/rating/AAPL
    """


class FMPRatingData(RatingData):
    """Rating Data Model."""

    __alias_dict__ = {
        "symbol": "ticker",
    }


class FMPRatingFetcher(
    Fetcher[
        FMPRatingQueryParams,
        List[FMPRatingData],
    ]
):
    """Fetches and transforms data from the Rating endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> FMPRatingQueryParams:
        """Transform the query params."""
        return FMPRatingQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: FMPRatingQueryParams,
        credentials: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the Rating endpoint."""
        symbols = query.symbol.split(",")
        results: List[Dict] = []

        async def get_one(symbol):
            api_key = credentials.get("fmp_api_key") if credentials else ""
            url = create_url(
                3, f"rating/{symbol}", api_key, query, exclude=["symbol"]
            )
            result = get_jsonparsed_data(url)
            if not result or len(result) == 0:
                warn(f"Symbol Error: No data found for symbol {symbol}")
            if result:
                results.extend(result)

        await asyncio.gather(*[get_one(symbol) for symbol in symbols])

        if not results:
            raise EmptyDataError("No data returned for the given symbol.")
        results = [{to_snake_case(key): value for key, value in d.items()} for d in results if isinstance(d, dict)]

        return results

    @staticmethod
    def transform_data(
        query: FMPRatingQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[FMPRatingData]:
        """Return the transformed data."""
        return [FMPRatingData(**d) for d in data]
