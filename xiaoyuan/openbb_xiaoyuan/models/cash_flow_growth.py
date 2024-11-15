"""XiaoYuan Cash Flow Statement Growth Model."""

from typing import Any, Dict, List, Literal, Optional

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.cash_flow_growth import (
    CashFlowStatementGrowthData,
    CashFlowStatementGrowthQueryParams,
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
    get_query_cnzvt_sql,
    get_query_finance_sql,
    get_report_month,
    getFiscalQuarterFromTime,
    revert_stock_code_format,
)
from pydantic import Field


class XiaoYuanCashFlowStatementGrowthQueryParams(CashFlowStatementGrowthQueryParams):
    """XiaoYuan Cash Flow Statement Growth Query.

    Source: https://site.financialmodelingprep.com/developer/docs/financial-statements-growth-api/
    """

    __json_schema_extra__ = {
        "period": {
            "choices": ["annual", "quarter", "ytd"],
        }
    }

    period: Literal["annual", "quarter", "ytd"] = Field(
        default="annual",
        description=QUERY_DESCRIPTIONS.get("period", ""),
    )


class XiaoYuanCashFlowStatementGrowthData(CashFlowStatementGrowthData):
    """XiaoYuan Cash Flow Statement Growth Data."""

    __alias_dict__ = {
        "period_ending": "报告期",
        "growth_net_income": "净利润同比增长率（百分比）",
        "growth_operating_cash_flow": "net_op_cash_flows",
        "growth_net_cash_from_investing_activities": "net_investing_cash_flows",
        "growth_effect_of_exchange_rate_changes_on_cash": "foreign_exchange_rate_effect",
        "growth_net_change_in_cash_and_equivalents": "net_cash_increase",
        "growth_cash_at_beginning_of_period": "cash_at_beginning",
        "growth_cash_at_end_of_period": "cash",
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
    growth_net_cash_from_operating_activities: Optional[float] = Field(
        default=None,
        description="Growth rate of net cash provided by operating activities.",
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
        params["symbol"] = convert_stock_code_format(params.get("symbol", ""))
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
        DDB_DATA = [
            "净利润同比增长率（百分比）",
        ]

        CACULATE_DATA_DIC = {
            "growth_depreciation_and_amortization": "折旧与摊销",
            "growth_account_receivables": "应收账款",
            "growth_inventory": "存货",
            "growth_account_payable": "应付账款",
        }
        CACULATE_DATA_QTR_DIC = {
            "net_op_cash_flows": "经营活动产生的现金流量净额",
            "net_investing_cash_flows": "投资活动产生的现金流量净额",
            "foreign_exchange_rate_effect": "汇率变动对现金及现金等价物的影响",
            "net_cash_increase": "现金及现金等价物净增加额",
            "cash_at_beginning": "加：期初现金及现金等价物余额",
            "cash": "期末现金及现金等价物余额",
        }

        if query.period == "quarter":
            columns_to_divide = []
            cnzvt_sql = get_query_cnzvt_sql(
                CACULATE_DATA_QTR_DIC,
                [query.symbol],
                "cash_flow_statement_qtr",
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

            report_month = get_report_month("annual", -query.limit - 1)
            cash_flow_caculate = calculateGrowth + caculate_sql(CACULATE_DATA_DIC)

            finance_sql = get_query_finance_sql(
                list(CACULATE_DATA_DIC.values()),
                [query.symbol],
                report_month,
            )
            df2 = reader._run_query(
                script=extractMonthDayFromTime
                + getFiscalQuarterFromTime
                + finance_sql
                + cash_flow_caculate,
            )
            df2.drop(columns=list((CACULATE_DATA_DIC).values()), inplace=True)
            df = df.merge(df2, on=["报告期", "symbol", "timestamp"], how="left")
        else:
            columns_to_divide = DDB_DATA
            report_month = get_report_month(query.period, -query.limit - 1)
            cash_flow_caculate = calculateGrowth + caculate_sql(
                CACULATE_DATA_QTR_DIC | CACULATE_DATA_DIC
            )

            finance_sql = get_query_finance_sql(
                DDB_DATA + list(((CACULATE_DATA_QTR_DIC | CACULATE_DATA_DIC)).values()),
                [query.symbol],
                report_month,
            )
            df = reader._run_query(
                script=extractMonthDayFromTime
                + getFiscalQuarterFromTime
                + finance_sql
                + cash_flow_caculate,
            )
            df.drop(
                columns=list((CACULATE_DATA_QTR_DIC | CACULATE_DATA_DIC).values()),
                inplace=True,
            )
        if df is None or df.empty:
            raise EmptyDataError()
        df = df.iloc[-query.limit :]
        df["报告期"] = df["报告期"].dt.strftime("%Y-%m-%d")
        df[columns_to_divide] /= 100
        df.sort_values(by="报告期", ascending=False, inplace=True)
        df.set_index(["报告期", "timestamp", "symbol"], inplace=True)
        df = df.loc[
            :,
            df.columns.isin(
                DDB_DATA
                + list(CACULATE_DATA_DIC.keys())
                + list(CACULATE_DATA_QTR_DIC.keys())
            ),
        ]
        df.reset_index(inplace=True)
        return df.to_dict(orient="records")

    @staticmethod
    def transform_data(
        query: XiaoYuanCashFlowStatementGrowthQueryParams,
        data: List[Dict],
        **kwargs: Any,
    ) -> List[XiaoYuanCashFlowStatementGrowthData]:
        """Return the transformed data."""
        data = revert_stock_code_format(data)
        return [XiaoYuanCashFlowStatementGrowthData.model_validate(d) for d in data]
