"""同步完成通知（MQTT）。

与 CryptoTrade 的 data_service 保持同一套传输约定：
  - paho-mqtt，QoS 2
  - jsonpickle 序列化 dove.event.Event
  - 话题 /event/trade/{plan} 为"整批完成"

新增两个粒度：
  - /event/data/{plan}/{venue}   每所写完立刻发（细粒度）
  - /state/data/{plan}/{venue}   retained 状态（迟到消费者订阅即知）

dove 未安装时自动降级为 LogNotifier，不影响采集本身。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

try:  # dove 是 CryptoTrade 侧的共享库，采集容器里可能没装
    from dove.client import MqttClient
    from dove.event import DataEvent
    from dove.topic import topic_trade_event

    _DOVE_OK = True
except Exception:  # pragma: no cover
    _DOVE_OK = False
    MqttClient = None  # type: ignore
    DataEvent = object  # type: ignore
    topic_trade_event = "/event/trade/"

TOPIC_DATA = "/event/data/"
TOPIC_STATE = "/state/data/"

SYNCED = "synced"
PARTIAL = "partial"
FAILED = "failed"
TIMEOUT = "timeout"


class VenueDataEvent(DataEvent):  # type: ignore[misc,valid-type]
    """单所同步结果。字段与 doc §12.3 一致。"""

    SYNCED = SYNCED
    PARTIAL = PARTIAL
    FAILED = FAILED
    TIMEOUT = TIMEOUT

    def __init__(self, **kw: Any):
        super().__init__()
        self.venue = kw.get("venue")
        self.window_start = kw.get("window_start")
        self.window_end = kw.get("window_end")
        self.status = kw.get("status")
        self.rows = kw.get("rows") or {}
        self.codes = kw.get("codes", 0)
        self.errors = kw.get("errors") or []
        self.duration_ms = kw.get("duration_ms", 0)
        self.ingested_at = kw.get("ingested_at")
        self.name = kw.get("status")


class Notifier:
    def publish_venue(self, ev: VenueDataEvent) -> None: ...
    def publish_aggregate(self, events: list[VenueDataEvent], plan: str) -> None: ...
    def close(self) -> None: ...


class LogNotifier(Notifier):
    def __init__(self, plan: str = "crypto"):
        self.plan = plan

    def publish_venue(self, ev: VenueDataEvent) -> None:
        logger.info(
            f"[notify] {self.plan}/{ev.venue} {ev.status} "
            f"rows={ev.rows} codes={ev.codes} {ev.duration_ms}ms "
            f"errors={len(ev.errors)}"
        )

    def publish_aggregate(self, events: list[VenueDataEvent], plan: str) -> None:
        ok = sum(1 for e in events if e.status == SYNCED)
        logger.info(f"[notify] {plan} 汇总: {ok}/{len(events)} 所成功")

    def close(self) -> None:
        pass


class DoveNotifier(Notifier):
    def __init__(
        self,
        plan: str,
        host: str = "localhost",
        port: int = 1883,
        service_name: str = "crypto_data_service",
        retain_state: bool = True,
    ):
        if not _DOVE_OK:
            raise RuntimeError("dove 未安装")
        self.plan = plan
        self.retain_state = retain_state
        self.client = MqttClient(
            {
                "host": host,
                "port": port,
                "client_id": f"crypto_collector_{service_name}_{os.getpid()}",
            }
        )
        self.client.connect()
        self.client.mqtt_client.loop_start()
        logger.info(f"[notify] MQTT 已连接 {host}:{port} plan={plan}")

    def publish_venue(self, ev: VenueDataEvent) -> None:
        import jsonpickle

        payload = jsonpickle.encode(ev)
        self.client.publish(f"{TOPIC_DATA}{self.plan}/{ev.venue}", payload)
        if self.retain_state:
            # retained：新启动的消费端订阅即拿到最后一轮状态
            self.client.mqtt_client.publish(
                f"{TOPIC_STATE}{self.plan}/{ev.venue}", payload, qos=2, retain=True
            )

    def publish_aggregate(self, events: list[VenueDataEvent], plan: str) -> None:
        import jsonpickle

        agg = DataEvent()
        agg.service_name = "crypto_data_service"
        agg.name = DataEvent.DOWNLOADED
        agg.data = ",".join(sorted({k for e in events for k in e.rows}))
        agg.finished_signal = datetime.now().astimezone().isoformat()
        self.client.publish(f"{topic_trade_event}{plan}", jsonpickle.encode(agg))

    def close(self) -> None:
        try:
            self.client.mqtt_client.loop_stop()
            self.client.mqtt_client.disconnect()
        except Exception:
            pass


def build_notifier(
    mode: str = "auto",
    plan: str = "crypto",
    host: str | None = None,
    port: int | None = None,
) -> Notifier:
    """mode: auto | mqtt | log | off。auto 表示有 dove 就发 MQTT。"""
    if mode in ("off", "log"):
        return LogNotifier(plan)
    if mode == "auto" and not _DOVE_OK:
        logger.warning("dove 未安装，通知降级为本地日志（安装 dove 后自动启用 MQTT）")
        return LogNotifier(plan)
    if not _DOVE_OK:
        raise RuntimeError("要求 mqtt 通知但 dove 未安装")
    return DoveNotifier(
        plan=plan,
        host=host or os.environ.get("MQTT_HOST", "localhost"),
        port=int(port or os.environ.get("MQTT_PORT", "1883")),
    )
