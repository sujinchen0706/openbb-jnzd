"""XiaoYuan Balance Sheet Model."""

# pylint: disable=unused-argument

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.balance_sheet import (
    BalanceSheetData,
    BalanceSheetQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    QUERY_DESCRIPTIONS,
    DATA_DESCRIPTIONS,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, field_validator, model_validator

from openbb_xiaoyuan.utils.references import (
    get_report_month,
    get_query_finance_sql,
    extractMonthDayFromTime,
    getFiscalQuarterFromTime,
)


class XiaoYuanBalanceSheetQueryParams(BalanceSheetQueryParams):
    """XiaoYuan Balance Sheet Query."""

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "ytd"],
        }
    }

    period: Literal["annual", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )

    @field_validator("symbol", mode="after", check_fields=False)
    @classmethod
    def handle_symbol(cls, v) -> str:
        """Handle symbols with a dash and replace it with a dot for XiaoYuan."""
        return v.replace("-", ".")


class XiaoYuanBalanceSheetData(BalanceSheetData):
    """XiaoYuan Balance Sheet Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "accounts_receivable": "应收账款",
        "prepaid_expenses": "预付款项",
        "inventory": "存货",
        "other_current_assets": "其他流动资产",
        "total_current_assets": "流动资产合计",
        "plant_property_equipment_gross": "固定资产",
        "intangible_assets": "无形资产",
        "goodwill": "商誉",
        "other_non_current_assets": "其他非流动资产",
        "total_non_current_assets": "非流动资产合计",
        "total_assets": "资产总计",
        "accounts_payable": "应付账款",
        "accrued_interest_payable": "应付利息",
        "other_current_liabilities": "其他流动负债",
        "total_current_liabilities": "流动负债合计",
        "other_non_current_liabilities": "其他非流动负债",
        "total_non_current_liabilities": "非流动负债合计",
        "total_liabilities": "负债合计",
        "minority_interest": "少数股东权益",
        "total_shareholders_equity": "股东权益合计",
        "total_liabilities_and_shareholders_equity": "负债和股东权益合计",
        "dividends_payable": "应付股利",
        "treasury_stock": "减：库存股",
        "accumulated_other_comprehensive_income": "其他综合收益",
        "net_debt": "净债务",
    }
    symbol: str = Field(description=DATA_DESCRIPTIONS.get("symbol", ""))

    accounts_receivable: Optional[float] = Field(
        description="Accounts receivable.", default=None
    )
    prepaid_expenses: Optional[float] = Field(
        description="Prepaid expenses.", default=None
    )
    inventory: Optional[float] = Field(description="Inventory.", default=None)
    other_current_assets: Optional[float] = Field(
        description="Other current assets.", default=None
    )
    total_current_assets: Optional[float] = Field(
        description="Total current assets.", default=None
    )
    plant_property_equipment_gross: Optional[float] = Field(
        description="Plant property equipment gross.", default=None
    )
    intangible_assets: Optional[float] = Field(
        description="Intangible assets.", default=None
    )
    goodwill: Optional[float] = Field(description="Goodwill.", default=None)
    other_non_current_assets: Optional[float] = Field(
        description="Other non current assets.", default=None
    )
    total_non_current_assets: Optional[float] = Field(
        description="Total non current assets.", default=None
    )
    total_assets: Optional[float] = Field(description="Total assets.", default=None)
    accounts_payable: Optional[float] = Field(
        description="Accounts payable.", default=None
    )
    accrued_interest_payable: Optional[float] = Field(
        description="Accrued interest payable.", default=None
    )
    other_current_liabilities: Optional[float] = Field(
        description="Other current liabilities.", default=None
    )
    total_current_liabilities: Optional[float] = Field(
        description="Total current liabilities.", default=None
    )
    other_non_current_liabilities: Optional[float] = Field(
        description="Other non current liabilities.", default=None
    )
    total_non_current_liabilities: Optional[float] = Field(
        description="Total non current liabilities.", default=None
    )
    total_liabilities: Optional[float] = Field(
        description="Total liabilities.", default=None
    )
    minority_interest: Optional[float] = Field(
        description="Minority interest.", default=None
    )
    total_shareholders_equity: Optional[float] = Field(
        description="Total shareholders equity.", default=None
    )
    total_liabilities_and_shareholders_equity: Optional[float] = Field(
        description="Total liabilities and shareholders equity.", default=None
    )
    dividends_payable: Optional[float] = Field(
        description="Dividends payable.", default=None
    )
    treasury_stock: Optional[float] = Field(description="Treasury stock.", default=None)
    accumulated_other_comprehensive_income: Optional[float] = Field(
        description="Accumulated other comprehensive income.", default=None
    )
    net_debt: Optional[float] = Field(description="Net debt.", default=None)

    @model_validator(mode="before")
    @classmethod
    def replace_zero(cls, values):  # pylint: disable=no-self-argument
        """Check for zero values and replace with None."""
        return (
            {k: None if v == 0 else v for k, v in values.items()}
            if isinstance(values, dict)
            else values
        )


class XiaoYuanBalanceSheetFetcher(
    Fetcher[
        XiaoYuanBalanceSheetQueryParams,
        List[XiaoYuanBalanceSheetData],
    ]
):
    """Transform the query, extract and transform the data from the XiaoYuan endpoints."""

    @staticmethod
    def transform_query(params: Dict[str, Any]) -> XiaoYuanBalanceSheetQueryParams:
        """Transform the query params."""
        return XiaoYuanBalanceSheetQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: XiaoYuanBalanceSheetQueryParams,
        credentials: Optional[Dict[str, str]],
        **kwargs: Any,
    ) -> List[Dict]:
        """Return the raw data from the XiaoYuan endpoint."""
        from jinniuai_data_store.reader import get_jindata_reader

        factors = [
            "应收账款",
            "预付款项",
            "存货",
            "其他流动资产",
            "流动资产合计",
            "固定资产",
            "无形资产",
            "商誉",
            "其他非流动资产",
            "非流动资产合计",
            "资产总计",
            "应付账款",
            "应付利息",
            "其他流动负债",
            "流动负债合计",
            "其他非流动负债",
            "非流动负债合计",
            "负债合计",
            "少数股东权益",
            "股东权益合计",
            "负债和股东权益合计",
            "应付股利",
            "减：库存股",
            "其他综合收益",
            "净债务",
        ]
        reader = get_jindata_reader()
        report_month = get_report_month(query.period, -query.limit)
        finance_sql = get_query_finance_sql(factors, [query.symbol], report_month)
        df = reader._run_query(
            script=extractMonthDayFromTime + getFiscalQuarterFromTime + finance_sql,
        )
        if df is None or df.empty:
            raise EmptyDataError()
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df.sort_values(by="报告期", ascending=False, inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanBalanceSheetQueryParams, data: List[Dict], **kwargs: Any
    ) -> List[XiaoYuanBalanceSheetData]:
        """Return the transformed data."""

        return [XiaoYuanBalanceSheetData.model_validate(d) for d in data]
