from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable, Base

import akshare as ak


def stock_zh_a_hist(**kwargs):
    print(f"stock_zh_a_hist {kwargs}")
    df = ak.stock_zh_a_hist(**kwargs)
    if df is not None:
        df["代码"] = kwargs["symbol"]
        print(f"stock_zh_a_hist {kwargs['symbol']}, size {len(df)}")
    return df


class KlineDailyHFQ(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "kline_daily_hfq"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class StockKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "stock_kline_5m"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BaoStockKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "baostock_kline_5m"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BaoStockKline1dHfq(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "baostock_kline_1d_hfq"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")
    turn = Column(types.Float64, comment="turn")
    peTTM = Column(types.Float64, comment="peTTM")
    psTTM = Column(types.Float64, comment="psTTM")
    pcfNcfTTM = Column(types.Float64, comment="pcfNcfTTM")
    # 2026-10 新增：proxy 一直在拉这两个字段，此前因表无对应列被静默丢弃。
    # 历史行为空的区间统一为 NaN（本项目「空值 = NaN」约定），故建表/加列用 DEFAULT nan。
    pbMRQ = Column(types.Float64, comment="pbMRQ")
    isST = Column(types.Float64, comment="isST")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BondKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "bond_kline_5m"

    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    close_time = Column(types.DateTime64, comment="close_time")
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(open_time),
            order_by=(open_time, code),
            primary_key=(open_time, code),
        ),
    )


class StockKline15m(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "stock_kline_15m"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class StockKlineHFQ15M(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "stock_kline_hfq_15m"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class KlineDailyNFQ(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "kline_daily_nfq"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureInfo(TsTable):
    """
    合约信息表
    """

    __tablename__ = "future_info"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="jj_code")
    total_supply = Column(types.Float64, comment="total_supply")
    market_cap_fdv_ratio = Column(types.Float64, comment="market_cap_fdv_ratio")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class TokenHolders(TsTable):
    """
    合约信息表
    """

    __tablename__ = "token_holders"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    chain = Column(types.String, comment="chain")
    wallet_address = Column(types.String, comment="wallet_address")
    amount = Column(types.Float64, comment="amount")
    usd_value = Column(types.Float64, comment="usd_value")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code, chain, wallet_address),
            primary_key=(datetime, code, chain, wallet_address),
        ),
    )


class FutureLongShortRatio(TsTable):
    """
    合约多空账户数比信息表

    按保证金余额（账户资金规模）排名前 20% 的用户（即 “大户”），仅统计有未平仓合约的账户。

    longAccount: 有净多头头寸的大户账户数 ÷ 有未平仓头寸的大户总账户数
    shortAccount: 有净空头头寸的大户账户数 ÷ 有未平仓头寸的大户总账户数
    longShortRatio: 多空账户数比
    """

    __tablename__ = "future_long_short_ratio"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    longAccount = Column(types.Float64, comment="longAccount")
    shortAccount = Column(types.Float64, comment="shortAccount")
    longShortRatio = Column(types.Float64, comment="longShortRatio")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureLongShortPositionRatio(TsTable):
    """
    合约多空持仓量比信息表

    前 20% 拥有最高保证金余额的用户的净多头和净空头持仓占其总未平仓持仓的比例。

    longAccount: 高级交易者的多头持仓 / 高级交易者的总未平仓持仓
    shortAccount: 高级交易者的空头持仓 / 高级交易者的总未平仓持仓
    longShortRatio: 多空比率（持仓） = 多头持仓百分比 / 空头持仓百分比
    """

    __tablename__ = "future_long_short_position_ratio"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    longAccount = Column(types.Float64, comment="longAccount")
    shortAccount = Column(types.Float64, comment="shortAccount")
    longShortRatio = Column(types.Float64, comment="longShortRatio")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureOpenInterest(TsTable):
    """
    合约持仓信息表
    sumOpenInterest: 总持仓量
    sumOpenInterestValue: 总持仓价值
    CMCCirculatingSupply: CMC 统计的总流通量
        总流通量: 排除项目方未解锁、团队锁仓、基金会储备等 “暂不可交易” 的部分，仅统计当前能在交易所、钱包间自由流通的代币数量
        数据来源：由 CMC 统计并提供，币安 API 仅做同步展示，其统计逻辑可能与项目方官方数据略有差异（如是否计入跨链流通的代币、解锁进度更新时效等）。
        非实时更新：CMC 流通供应量通常每日更新一次，并非实时变动，因此不适合作为实时交易决策的直接依据，仅用于中长期趋势参考。
    """

    __tablename__ = "future_open_interest"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    sumOpenInterest = Column(types.Float64, comment="sumOpenInterest")
    sumOpenInterestValue = Column(types.Float64, comment="sumOpenInterestValue")
    CMCCirculatingSupply = Column(types.Float64, comment="CMCCirculatingSupply")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureFundingRate(TsTable):
    """
    合约资金费率表
    """

    __tablename__ = "future_funding_rate"

    datetime = Column(types.DateTime64, comment="fundingTime", primary_key=True)
    code = Column(types.String, comment="symbol")
    fundingRate = Column(types.Float64, comment="fundingRate")
    markPrice = Column(types.Float64, comment="markPrice")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline1H(TsTable):
    __tablename__ = "kline_futures_1h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class SpotKline1H(TsTable):
    __tablename__ = "kline_spot_1h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline4H(TsTable):
    __tablename__ = "kline_futures_4h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline15m(TsTable):
    __tablename__ = "kline_futures_15m"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline5m(TsTable):
    __tablename__ = "kline_futures_5m"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline1d(TsTable):
    __tablename__ = "kline_futures_1d"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FundKlineDailyHFQ(TsTable):
    """
    基金日K线，后复权
    """

    __tablename__ = "fund_kline_daily_hfq"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.ReplacingMergeTree(
            # ⚠️ 必须按年分区。原为 toYYYYMM：ETF 全历史（2015→今）单标的就是 140+ 个分区，
            # 超过 max_partitions_per_insert_block(100)，StockHelper 的单 symbol INSERT 会失败。
            # 与 BaoStockKline1dHfq 口径一致。该表此前为空，重建无数据损失。
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FundNetValue(TsTable):
    """
    基金净值数据
    """

    __tablename__ = "fund_value"

    datetime = Column(types.DateTime64, comment="净值日期", primary_key=True)
    code = Column(types.String, comment="代码")
    net_value = Column(types.Float64, comment="单位净值")
    cumsum_value = Column(types.Float64, comment="累计净值")
    tradable = Column(types.Boolean, comment="tradable")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class TokenUnlock(Base):
    """
    代币解锁信息表

    每个解锁事件全局唯一一行，由 ReplacingMergeTree 按
    (symbol, platform, contract_address, next_unlock_time) 四字段去重。

    不继承 TsTable，因为 TsTable 强制 datetime 为主键和排序列，
    而此表需要自定义 ORDER BY 以支持按解锁事件去重。
    """

    __tablename__ = "token_unlock"

    symbol = Column(types.String, comment="CMC 代币符号 (如 ZRO)", primary_key=True)
    platform = Column(
        types.String, comment="区块链平台 (如 ethereum)", primary_key=True
    )
    contract_address = Column(
        types.String, comment="合约地址 (原生币填 0x0)", primary_key=True
    )
    next_unlock_time = Column(
        types.DateTime64, comment="下次解锁时间 (UTC)", primary_key=True
    )

    slug = Column(types.String, comment="CMC slug (如 layerzero)")
    crypto_id = Column(types.Int64, comment="CMC cryptoId")
    name = Column(types.String, comment="项目全称")

    total_unlocked_pct = Column(types.Float64, comment="已解锁占总供应量 %")
    next_unlock_amount = Column(types.Float64, comment="下次解锁数量")
    next_unlock_amount_usd = Column(types.Float64, comment="下次解锁价值 USD")
    next_unlock_pct = Column(types.Float64, comment="下次解锁占锁仓 %")

    circulating_supply = Column(types.Float64, comment="当前流通量")
    price = Column(types.Float64, comment="当前价格 USD")
    market_cap = Column(types.Float64, comment="当前市值 USD")

    binance_code = Column(
        types.String, comment="币安合约代码 (如 ZROUSDT，无映射则为空)"
    )

    synced_at = Column(types.DateTime64, comment="本次同步时间 (UTC)")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(next_unlock_time),
            order_by=(symbol, platform, contract_address, next_unlock_time),
            primary_key=(symbol, platform, contract_address, next_unlock_time),
            version=synced_at,
        ),
    )


class TokenLiquidity(Base):
    """
    代币 DEX 流动性快照表

    每个代币在各链各 DEX 上的流动性池全局唯一一行，
    由 ReplacingMergeTree 按 (symbol, chain, dex_id, pair_address) 四字段去重。
    """

    __tablename__ = "token_liquidity"

    symbol          = Column(types.String,    comment="代币符号 (如 ZRO)", primary_key=True)
    chain           = Column(types.String,    comment="链标识 (ethereum, bsc, solana)", primary_key=True)
    dex_id          = Column(types.String,    comment="DEX 标识 (uniswap_v3, pancakeswap)", primary_key=True)
    pair_address    = Column(types.String,    comment="池合约地址", primary_key=True)

    base_token      = Column(types.String,    comment="base token 地址")
    base_token_name = Column(types.String,    comment="base token 名称")
    quote_token     = Column(types.String,    comment="quote token 地址")
    quote_token_name = Column(types.String,   comment="quote token 名称")
    pair_created_at = Column(types.DateTime64, comment="池创建时间")

    liquidity_usd   = Column(types.Float64,   comment="池总流动性 USD")
    liquidity_base  = Column(types.Float64,   comment="base token 流动性数量")
    liquidity_quote = Column(types.Float64,   comment="quote token 流动性数量")

    volume_h24      = Column(types.Float64,   comment="24h 交易量 USD")
    volume_h6       = Column(types.Float64,   comment="6h 交易量 USD")
    volume_h1       = Column(types.Float64,   comment="1h 交易量 USD")
    txns_buys_h24   = Column(types.Int64,     comment="24h 买单数")
    txns_sells_h24  = Column(types.Int64,     comment="24h 卖单数")

    price_usd       = Column(types.Float64,   comment="当前价格 USD")
    price_native    = Column(types.Float64,   comment="当前价格 (原生代币)")
    fdv             = Column(types.Float64,   comment="全稀释估值 USD")
    market_cap      = Column(types.Float64,   comment="市值 USD")

    price_change_h24 = Column(types.Float64,  comment="24h 价格变化 %")
    price_change_h6  = Column(types.Float64,  comment="6h 价格变化 %")
    price_change_h1  = Column(types.Float64,  comment="1h 价格变化 %")
    price_change_m5  = Column(types.Float64,  comment="5m 价格变化 %")

    synced_at       = Column(types.DateTime64, comment="本次同步时间 (UTC)")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(synced_at),
            order_by=(symbol, chain, dex_id, pair_address),
            primary_key=(symbol, chain, dex_id, pair_address),
            version=synced_at,
        ),
    )


class CnFuturesKline1d(TsTable):
    """
    国内商品期货【主力连续】日线（新浪源 akshare.futures_main_sina）

    code 形如 RB0 / I0 / MA0（交易所主力连续代码），非具体合约。
    注意：主力连续在换月处有跳空，回测需按换月规则另行拼接处理。
    """

    __tablename__ = "cn_futures_kline_1d"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘价")
    high = Column(types.Float64, comment="最高价")
    low = Column(types.Float64, comment="最低价")
    close = Column(types.Float64, comment="收盘价")
    volume = Column(types.Float64, comment="成交量")
    open_interest = Column(types.Float64, comment="持仓量")
    settle = Column(types.Float64, comment="动态结算价")

    __table_args__ = (
        engines.ReplacingMergeTree(
            # 长历史日线必须按年分区：单标的全历史（2015→今）按月分会有 140+ 个分区，
            # 超过 ClickHouse max_partitions_per_insert_block（100）导致单次 INSERT 失败。
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class CnFuturesContract1d(TsTable):
    """
    国内商品期货【逐合约】日线（交易所源 get_futures_daily + DCE 走新浪逐合约）

    code 形如 RB2405 / M2409（具体合约），variety 形如 RB / M。

    存在理由：主力连续（cn_futures_kline_1d）在换月处【价格拼接】有跳空，实测全表
    3455 天（2.1%）是假跳空，平均 |收益| 476bp —— 例：RB0 在 2015-06-24 单日跳空
    +980bp 而当日高低区间只有 76bp。直接拿主力连续算收益，波动率/回撤/趋势信号全错。
    本表保留逐合约 OHLC + 持仓量 + 结算价，用于：
      ① 每日按【持仓量最大】定主力合约；
      ② 记录精确换月日（用于显式计提换月成本）；
      ③ 用【同一合约内】的收益拼接出无跳空的连续收益序列。
    """

    __tablename__ = "cn_futures_contract_1d"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="合约代码")
    variety = Column(types.String, comment="品种")
    market = Column(types.String, comment="交易所")
    open = Column(types.Float64, comment="开盘价")
    high = Column(types.Float64, comment="最高价")
    low = Column(types.Float64, comment="最低价")
    close = Column(types.Float64, comment="收盘价")
    volume = Column(types.Float64, comment="成交量")
    open_interest = Column(types.Float64, comment="持仓量")
    settle = Column(types.Float64, comment="结算价")

    __table_args__ = (
        engines.ReplacingMergeTree(
            # 同 CnFuturesKline1d：长历史日线必须按年分区（按月会超 100 分区上限）
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BondKlineDaily(TsTable):
    """
    可转债日线（新浪源 akshare.bond_zh_hs_cov_daily）

    🚫 【暂缓 · 用户 2026-10-14 决定】本机无可用的可转债日线源，此表保留但不要使用：
    新浪该接口对 2021 年后上市的活跃券覆盖极差 —— 12 只实测新券仅 3 只可取，
    且只返回最近数日（6/9/23 行），sh 前缀还会返回错数据（127118 -> 2021-01-14 的 4 行）。
    对「可转债轮动」这种要当前活跃券的策略是致命的。
    后续若要用，三条路：① 装通达信（pond/tdx/ 有现成实现）② 买数据源 token（Tushare/聚宽/米筐）
    ③ 每日快照累积（只有 go-forward 数据，无历史）。

    该表仅含源实际提供的字段（无 amount / turn），不造空列。
    """

    __tablename__ = "bond_kline_1d"

    datetime = Column(types.DateTime64, comment="date", primary_key=True)
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            # 同 CnFuturesKline1d：长历史日线按年分区，避免单次 INSERT 超 100 分区
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )
