extractMonthDayFromTime = """
def extractMonthDayFromTime(time) {
    return substr(string(time), 5)
};
"""

getFiscalQuarterFromTime = """
def getFiscalQuarterFromTime(time) {
    i = monthOfYear(time);
    if (i==3){
        return 'q1'
    }
    if (i==6){
        return 'q2'
    }
    if (i==9){
        return 'q3'
    }
    if (i==12){
        return 'q4'
    }
    };
"""


def get_query_cnzvt_sql(
    factor_names: dict, symbol: list, table_name: str, limit: int
) -> str:
    return f"""
        t = select timestamp,report_date as 报告期, (upper(split(id,"_")[1])+split(id,"_")[2]) as symbol,
        {', '.join(f"{key} as {value}" for key, value in factor_names.items())} 
        from loadTable("dfs://cn_zvt","{table_name}") where (upper(split(id,"_")[1])+split(id,"_")[2]) in {symbol};
        t = t.unpivot(keyColNames=["timestamp","报告期","symbol"],valueColNames={list(factor_names.values())});
        rename!(t,`timestamp`报告期`symbol`factor_name`value);
        t = select timestamp, 报告期, symbol, factor_name, value from t context by symbol, factor_name order by 报告期 limit {limit};
        t = select value from t pivot by timestamp,symbol,报告期,factor_name;
        select *,getFiscalQuarterFromTime(报告期) as fiscal_period,year(报告期) as fiscal_year 
        from t context by symbol,报告期;
        """


# Script 组合
def get_query_finance_sql(factor_names: list, symbol: list, report_month: str) -> str:
    return f"""
        t = select timestamp,报告期, symbol, factor_name ,value 
        from loadTable("dfs://finance_factors_1Y", `cn_finance_factors_1Q) 
        where factor_name in {factor_names} 
            and symbol in {symbol} 
            {report_month} 
        t = select value from t pivot by timestamp,symbol,报告期,factor_name;
        select *,getFiscalQuarterFromTime(报告期) as fiscal_period,year(报告期) as fiscal_year 
        from t context by symbol,报告期;
        """


def get_recent_1q_query_finance_sql(
    factor_names: list, symbol: list, cur_date: str
) -> str:
    return f"""
        t = select timestamp,报告期, symbol, factor_name ,value 
        from loadTable("dfs://finance_factors_1Y", `cn_finance_factors_1Q) 
        where factor_name in {factor_names} 
            and symbol in {symbol} and timestamp <= {cur_date} context by symbol order by timestamp limit -1;

        t = select value from t where value is not null pivot by 报告期,timestamp,symbol, factor_name;
        t
        """


def get_report_month(period: str, limit=-4) -> str:
    period_to_month = {
        "ytd": "",
        "annual": "12",
    }
    if period not in period_to_month:
        raise ValueError(f"Invalid period: {period}")
    month = period_to_month[period]
    return (
        (
            f" and monthOfYear(报告期) = {month}  context by symbol,factor_name,extractMonthDayFromTime(报告期) "
            f"order by 报告期 limit {limit} ;"
        )
        if month
        else f"context by symbol,factor_name,extractMonthDayFromTime(报告期) "
        f"order by 报告期 limit {limit};"
    )


def get_specific_daily_sql(factor_names: list, symbol: list, date_list: list) -> str:
    return f"""
        timestamp = {date_list};
        date_list_table = table(timestamp);
        timestamp_table = select datetime(date(timestamp)) from date_list_table;
        t = select timestamp, symbol, factor_name,value
            from loadTable("dfs://factors_6M", `cn_factors_1D)
            where factor_name in {factor_names} and
            timestamp in timestamp_table
            and symbol in {symbol};
        t = select value from t where value is not null pivot by timestamp, symbol, factor_name;
        t
        """


def get_dividend_sql(
    start_date: str,
    end_date: str,
    code: str = None,
    table_name: str = "dividend_detail",
) -> str:
    dividend_sql = f"""
        select upper(split(entity_id,'_')[1])+split(entity_id,'_')[2] as symbol, 
        dividend_per_share_before_tax as dividend,
        record_date as recordDate,
        dividend_date as paymentDate,
        dividend_date as date 
        from loadTable("dfs://cn_zvt", `{table_name}) 
        where dividend_date between {start_date} and {end_date} 
        """
    if code:
        dividend_sql += f" and code = '{code[-6:]}'"
    return dividend_sql


def convert_stock_code_format(symbol):
    # 将.SS转换为SH前缀 .SZ后缀转换为SZ前缀
    symbol = symbol.split(",")
    symbol = [s.strip() for s in symbol if s.strip()]
    converted_symbol = []
    for s in symbol:
        if "SS" in s:
            s = "SH" + s.replace(".SS", "")
        elif "SZ" in s:
            s = "SZ" + s.replace(".SZ", "")
        elif "OF" in s:
            s = "OF" + s.replace(".OF", "")
        converted_symbol.append(s)

    return ",".join(converted_symbol)


def revert_stock_code_format(data):
    for i in data:
        if "SH" in i["symbol"]:
            i["symbol"] = i["symbol"].replace("SH", "") + ".SS"
        elif "SZ" in i["symbol"]:
            i["symbol"] = i["symbol"].replace("SZ", "") + ".SZ"
        elif "OF" in i["symbol"]:
            i["symbol"] = i["symbol"].replace("OF", "") + ".OF"
    return data
