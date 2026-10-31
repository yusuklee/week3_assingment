"""
일 배치 DAG — 하루치 이벤트를 정리하고, 집계하고, 검사하고, 대시보드 테이블에 올린다.

  check_partition → compact_parquet → daily_agg → quality_check → publish
  (어느 Task 든 최종 실패하면 on_failure_callback 으로 알림)

처리 날짜 (target_date)
  - 스케줄 실행: 매일 02:00 UTC 에 전날(UTC) 하루치. = data_interval_start 의 날짜
  - 리플레이 데이터(2015년)는 실행 날짜와 데이터 날짜가 다르므로 직접 지정한다
      airflow dags trigger daily_events -c '{"target_date": "2015-06-10"}'
      airflow dags backfill daily_events -s 2015-06-01 -e 2015-06-10
  - 시간대는 UTC 로 통일. Spark 가 dt 파티션을 UTC 날짜로 만들기 때문

멱등성 — 같은 날짜를 몇 번 돌려도 결과가 같다
  - Parquet        : dt 폴더를 통째로 다시 써서 교체 (append 아님)
  - item_stats_daily: 한 트랜잭션 안에서 DELETE WHERE dt + INSERT
  - daily_summary  : PK(dt) 로 INSERT … ON CONFLICT DO UPDATE
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

EVENTS_DIR = Path(os.environ.get("PARQUET_EVENTS_DIR", "/opt/airflow/data/parquet/events"))
# Spark 파티션 탐색은 '_' 로 시작하는 폴더를 무시한다 → 작업용 폴더가 테이블에 섞이지 않음
TMP_DIR = EVENTS_DIR / "_compact_tmp"
OLD_DIR = EVENTS_DIR / "_compact_old"
PG_CONN_ID = "clickstream_db"
EVENT_TYPES = ("view", "addtocart", "transaction")
REQUIRED = ["event_id", "event_type", "event_time", "visitor_id", "item_id"]

# Spark 가 쓴 파일과 같은 컬럼 · 타입. 시각은 UTC 기준 timestamp(마이크로초)
ARROW_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("produced_at", pa.timestamp("us", tz="UTC")),
    ("visitor_id", pa.int64()),
    ("item_id", pa.int64()),
    ("transaction_id", pa.int64()),
    ("schema_version", pa.int32()),
    ("source", pa.string()),
])


def notify_failure(context) -> None:
    """재시도를 다 쓰고 최종 실패했을 때(또는 재시도 없이 바로 실패했을 때) 호출된다."""
    ti = context["task_instance"]
    msg = (f":rotating_light: `{ti.dag_id}.{ti.task_id}` 실패 "
           f"(target_date={context['params'].get('target_date') or context['data_interval_start'].date()}, "
           f"try={ti.try_number}) — {context.get('exception')}")
    log.error("ALERT %s", msg)
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if url:  # 웹훅이 설정된 경우에만 Discord 로 보낸다
        req = urllib.request.Request(url, data=json.dumps({"content": msg}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)


def partition_dir(d: str) -> Path:
    return EVENTS_DIR / f"dt={d}"


def read_day(d: str) -> pd.DataFrame:
    # Spark 파일(INT96 시각)과 합친 파일이 섞여 있어도 같은 타입으로 읽는다
    return pq.read_table(partition_dir(d), schema=ARROW_SCHEMA).to_pandas()


default_args = {
    "owner": "clickstream",
    # 파일 · DB 작업은 일시 장애(잠금, 연결 끊김)가 대부분이라 재시도한다. 1분 → 2분 → 4분
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="daily_events",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2015, 5, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # 같은 dt 폴더를 두 실행이 동시에 교체하지 않도록
    default_args=default_args,
    params={"target_date": Param(None, type=["null", "string"], format="date",
                                 description="처리할 날짜 (YYYY-MM-DD). 비우면 data_interval_start 의 날짜")},
    tags=["clickstream"],
)
def daily_events():

    @task(retries=0)
    def check_partition(**context) -> str:
        """처리할 날짜를 정하고, 그 날짜 Parquet 폴더가 있는지 본다. 없으면 이후 Task 를 건너뛴다."""
        d = context["params"].get("target_date") or context["data_interval_start"].date().isoformat()
        date.fromisoformat(d)  # 형식이 틀리면 여기서 바로 실패
        p = partition_dir(d)
        # 지난 실행이 교체 도중 죽었으면 원래 폴더를 되돌려 놓는다
        if not p.exists() and (OLD_DIR / p.name).exists():
            (OLD_DIR / p.name).rename(p)
        if not p.exists() or not any(p.glob("*.parquet")):
            raise AirflowSkipException(f"{p} 에 데이터가 없음 → 처리할 것 없음")
        log.info("target_date=%s files=%d", d, len(list(p.glob("*.parquet"))))
        return d

    @task
    def compact_parquet(d: str) -> dict:
        """작은 파일 여러 개를 하나로 합치고 event_id 중복을 지운 뒤 dt 폴더를 통째로 교체한다."""
        src = partition_dir(d)
        files_before = len(list(src.glob("*.parquet")))
        table = pq.read_table(src, schema=ARROW_SCHEMA)
        rows_before = table.num_rows

        df = (table.to_pandas()
              .drop_duplicates("event_id")
              .sort_values(["event_time", "event_id"], ignore_index=True))  # 정렬해서 다시 돌려도 같은 파일

        tmp = TMP_DIR / src.name
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        pq.write_table(pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False),
                       tmp / "part-00000-compacted.snappy.parquet", compression="snappy")

        # 교체: 원본 → _compact_old, 새 폴더 → dt=…, 원본 삭제
        old = OLD_DIR / src.name
        shutil.rmtree(old, ignore_errors=True)
        OLD_DIR.mkdir(exist_ok=True)
        src.rename(old)
        tmp.rename(src)
        shutil.rmtree(old)

        result = {"files_before": files_before, "files_after": 1,
                  "rows_before": rows_before, "rows_after": len(df), "duplicates_removed": rows_before - len(df)}
        log.info("compact %s %s", d, result)
        return result

    @task
    def daily_agg(d: str) -> int:
        """하루치 상품별 집계를 다시 계산해 item_stats_daily 의 그 날짜 행을 갈아 끼운다."""
        df = read_day(d)
        counts = (pd.crosstab(df["item_id"], df["event_type"])
                  .reindex(columns=list(EVENT_TYPES), fill_value=0))
        visitors = df.groupby("item_id")["visitor_id"].nunique()
        agg = counts.join(visitors.rename("unique_visitors")).reset_index()
        rows = [(d, int(r.item_id), int(r.view), int(r.addtocart), int(r.transaction), int(r.unique_visitors))
                for r in agg.itertuples(index=False)]

        conn = PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()
        try:
            with conn, conn.cursor() as cur:  # with conn → 끝나면 commit, 예외면 rollback
                cur.execute("DELETE FROM item_stats_daily WHERE dt = %s", (d,))
                execute_values(cur, """
                    INSERT INTO item_stats_daily
                        (dt, item_id, view_cnt, addtocart_cnt, transaction_cnt, unique_visitors)
                    VALUES %s""", rows, page_size=5000)
        finally:
            conn.close()
        log.info("daily_agg %s items=%d events=%d", d, len(rows), len(df))
        return len(rows)

    @task(retries=0)
    def quality_check(d: str) -> dict:
        """건수 · null · 중복 · 값 범위 · 집계 합계를 검사한다. 데이터가 틀린 건 재시도해도 같으니 바로 실패 → 알림."""
        df = read_day(d)
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT coalesce(sum(view_cnt + addtocart_cnt + transaction_cnt), 0)
                FROM item_stats_daily WHERE dt = %s""", (d,))
            agg_total = int(cur.fetchone()[0])

        checks = {
            "row_count": len(df),
            "null_required": int(df[REQUIRED].isna().sum().sum()),
            "duplicate_event_id": int(df["event_id"].duplicated().sum()),
            "bad_event_type": int((~df["event_type"].isin(EVENT_TYPES)).sum()),
            "transaction_without_id": int(((df["event_type"] == "transaction") & df["transaction_id"].isna()).sum()),
            "wrong_date": int((df["event_time"].dt.tz_convert("UTC").dt.date.astype(str) != d).sum()),
            "agg_total": agg_total,
        }
        log.info("quality %s %s", d, checks)

        failed = [k for k in ("null_required", "duplicate_event_id", "bad_event_type",
                              "transaction_without_id", "wrong_date") if checks[k] > 0]
        if checks["row_count"] == 0:
            failed.append("row_count")
        if agg_total != checks["row_count"]:
            failed.append("agg_total != row_count")
        if failed:
            raise AirflowFailException(f"품질 검사 실패 {d}: {failed} {checks}")
        return checks

    @task
    def publish(d: str) -> dict:
        """검사를 통과한 날짜만 대시보드 테이블(daily_summary)에 올린다. PK(dt) 업서트."""
        df = read_day(d)
        n = df["event_type"].value_counts()
        views, carts, txs = (int(n.get(t, 0)) for t in EVENT_TYPES)
        row = {
            "dt": d,
            "events": len(df),
            "view_cnt": views,
            "addtocart_cnt": carts,
            "transaction_cnt": txs,
            "unique_visitors": int(df["visitor_id"].nunique()),
            "unique_items": int(df["item_id"].nunique()),
            "cart_rate": round(carts / views, 6) if views else None,
            "purchase_rate": round(txs / views, 6) if views else None,
        }
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_summary
                    (dt, events, view_cnt, addtocart_cnt, transaction_cnt,
                     unique_visitors, unique_items, cart_rate, purchase_rate, updated_at)
                VALUES (%(dt)s, %(events)s, %(view_cnt)s, %(addtocart_cnt)s, %(transaction_cnt)s,
                        %(unique_visitors)s, %(unique_items)s, %(cart_rate)s, %(purchase_rate)s, now())
                ON CONFLICT (dt) DO UPDATE SET
                    events = EXCLUDED.events, view_cnt = EXCLUDED.view_cnt,
                    addtocart_cnt = EXCLUDED.addtocart_cnt, transaction_cnt = EXCLUDED.transaction_cnt,
                    unique_visitors = EXCLUDED.unique_visitors, unique_items = EXCLUDED.unique_items,
                    cart_rate = EXCLUDED.cart_rate, purchase_rate = EXCLUDED.purchase_rate,
                    updated_at = now()""", row)
        log.info("publish %s", row)
        return row

    d = check_partition()
    compacted = compact_parquet(d)
    agg = daily_agg(d)
    checked = quality_check(d)
    published = publish(d)
    compacted >> agg >> checked >> published


daily_events()
