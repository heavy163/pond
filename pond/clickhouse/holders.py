from sqlalchemy import Column, func
from clickhouse_sqlalchemy import (
    types,
    engines,
)
from pond.clickhouse import TsTable

# ══════════════════════════════════════════════════════════════════════════════
# 本模块的公共约定（2026-10 修订，改动原因见下）
# ══════════════════════════════════════════════════════════════════════════════
#
# ① `datetime` = 【公告日】（信息可观测时点），不是报告期。
#    全库其他表都是这个口径（kline 的 datetime = bar 收盘时刻）。
#    若把 datetime 设成报告期，最自然的写法 `GROUP BY datetime` 会直接变成
#    未来函数（报告期末时点该信息还没公开）。报告期单独放 report_date。
#    `publish_date` 与 datetime 恒等，保留是为了与源头字段（UPDATE_DATE）对账；
#    loader 必须断言两者相等。
#
# ② 主键必须含【股东身份】。同一只股票在同一公告日有最多 10+ 个流通股东，
#    只按 (datetime, code) 做 ORDER BY 会让 ReplacingMergeTree 把 10 行压成 1 行
#    —— 沙箱实测丢 90% 且不报错（复现：report/manual/institution_follow/
#    scripts/sandbox_holder_ddl.py）。源不提供 HOLDER_ID，故用 name 作身份键。
#
# ③ partition_by 用 toYear 而非 toYYYYMM：单表跨 20 年（>240 个月）时，
#    一次 INSERT 会撞 ClickHouse `max_partitions_per_insert_block=100`。


class FreeHoldingDetail(TsTable):
    """
    持股明细-十大流通股东
    """

    __tablename__ = "FreeHoldingDetail"

    datetime = Column(types.DateTime64, comment="时间(公告日)", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    volume = Column(types.Float64, comment="期末持股-数量")
    free_amount = Column(types.Float64, comment="期末持股-流通市值")
    volume_change = Column(types.Float64, comment="期末持股-数量变化")
    volume_change_percent = Column(types.Float64, comment="期末持股-数量变化比例")
    report_date = Column(types.DateTime64, comment="报告期")
    publish_date = Column(types.DateTime64, comment="公告日")
    # ── 2026-10 新增：分析必需、但原 schema 缺失 ────────────────────────────
    holder_rank = Column(types.Float64, comment="股东排名")
    holder_newtype = Column(types.String, comment="股东类型(粗分)")
    shares_type = Column(types.String, comment="股份类型")
    free_holdnum_ratio = Column(types.Float64, comment="持股占流通股比")
    holder_code = Column(types.String, comment="股东代码")
    security_name = Column(types.String, comment="股票简称")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code, name),
            primary_key=(datetime, code, name),
        ),
    )


class HoldingDetail(TsTable):
    __tablename__ = "HoldingDetail"

    """
    持股明细-十大股东
    """

    datetime = Column(types.DateTime64, comment="时间(公告日)", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    volume = Column(types.Float64, comment="期末持股-数量")
    amount = Column(types.Float64, comment="期末持股-流通市值")
    volume_change = Column(types.Float64, comment="期末持股-数量变化")
    volume_change_percent = Column(types.Float64, comment="期末持股-数量变化比例")
    holder_rank = Column(types.Float64, comment="股东排名")
    report_date = Column(types.DateTime64, comment="报告期")
    publish_date = Column(types.DateTime64, comment="公告日")
    # ── 2026-10 新增 ─────────────────────────────────────────────────────────
    holder_newtype = Column(types.String, comment="股东类型(粗分)")
    shares_type = Column(types.String, comment="股份类型")
    holder_code = Column(types.String, comment="股东代码")
    security_name = Column(types.String, comment="股票简称")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code, name),
            primary_key=(datetime, code, name),
        ),
    )


class FreeHoldingStatistic(TsTable):
    """
    持股统计-十大流通股东

    ⚠️ 主键必须含 name：同一 datetime（公告日）下有【多个机构】，
    原 ORDER BY=(datetime) 会把所有机构压成 1 行。表为空时修正成本为零。
    """

    __tablename__ = "FreeHoldingStatistic"

    datetime = Column(types.DateTime64, comment="时间(公告日)", primary_key=True)
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    stat_counts = Column(types.Float64, comment="统计次数")
    avg_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-平均涨幅"
    )
    max_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-最大涨幅"
    )
    min_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-最小涨幅"
    )
    avg_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-平均涨幅"
    )
    max_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-最大涨幅"
    )
    min_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-最小涨幅"
    )
    avg_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-平均涨幅"
    )
    max_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-最大涨幅"
    )
    min_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-最小涨幅"
    )
    holding_codes = Column(types.String, comment="持有个股")
    # ── 2026-10 新增 ─────────────────────────────────────────────────────────
    report_date = Column(types.DateTime64, comment="报告期")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, name),
            primary_key=(datetime, name),
        ),
    )


class HoldingStatistic(TsTable):
    """
    持股统计-十大股东
    """

    __tablename__ = "HoldingStatistic"

    datetime = Column(types.DateTime64, comment="时间(公告日)", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    stat_counts = Column(types.Float64, comment="统计次数")
    avg_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-平均涨幅"
    )
    max_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-最大涨幅"
    )
    min_chg_pct_day10 = Column(
        types.Float64, comment="公告日后涨幅统计-10个交易日-最小涨幅"
    )
    avg_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-平均涨幅"
    )
    max_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-最大涨幅"
    )
    min_chg_pct_day30 = Column(
        types.Float64, comment="公告日后涨幅统计-30个交易日-最小涨幅"
    )
    avg_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-平均涨幅"
    )
    max_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-最大涨幅"
    )
    min_chg_pct_day60 = Column(
        types.Float64, comment="公告日后涨幅统计-60个交易日-最小涨幅"
    )
    holding_codes = Column(types.String, comment="持有个股")
    # ── 2026-10 新增 ─────────────────────────────────────────────────────────
    report_date = Column(types.DateTime64, comment="报告期")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code, name),
            primary_key=(datetime, code, name),
        ),
    )


class HolderCounts(TsTable):
    """
    股东户数

    主键 (datetime, code) 本来就唯一（一行一个 code），无需改动；
    仅把 partition_by 从 toYYYYMM 改为 toYear，与同组表保持一致。
    """

    __tablename__ = "HolderCounts"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="代码")
    report_date = Column(types.DateTime64, comment="股东户数统计截止日-本次")
    last_report_date = Column(types.DateTime64, comment="股东户数统计截止日-上次")
    period_chg_pct = Column(types.Float64, comment="区间涨跌幅")
    counts = Column(types.Float64, comment="股东户数-本次")
    last_counts = Column(types.Float64, comment="股东户数-上次")
    counts_change = Column(types.Float64, comment="股东户数-增减")
    counts_change_percent = Column(types.Float64, comment="股东户数-增减比例")
    amount_avg = Column(types.Float64, comment="户均持股市值")
    volume_avg = Column(types.Float64, comment="户均持股数量")
    amount_toal = Column(types.Float64, comment="总市值")
    volume_total = Column(types.Float64, comment="总股本")
    publish_date = Column(types.DateTime64, comment="公告日期")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class StockRestrictedReleaseDetail(TsTable):
    """
    限售股解禁明细

    ⚠️ 主键必须含 release_date：同一只股票在不同解禁日有多条记录，
    原 ORDER BY=(datetime, code) 会把同股多次解禁压成 1 行。
    """

    __tablename__ = "StockRestrictedReleaseDetail"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="股票代码")
    release_date = Column(types.DateTime64, comment="解禁时间")
    restriction = Column(types.String, comment="限售股类型")
    release_volume = Column(types.Float64, comment="解禁数量")
    release_percent = Column(types.Float64, comment="占解禁前流通市值比例")
    release_volume_actual = Column(types.Float64, comment="实际解禁数量")
    release_amount_actual = Column(types.Float64, comment="实际解禁市值")
    chg_pct_before_day20 = Column(types.Float64, comment="解禁前20日涨跌幅")
    chg_pct_afer_day20 = Column(types.Float64, comment="解禁后20日涨跌幅")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code, release_date),
            primary_key=(datetime, code, release_date),
        ),
    )
