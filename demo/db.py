from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from demo.config import settings


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(
        settings.database_url, autocommit=autocommit, row_factory=dict_row
    )


def get_config(key: str, default: str | None = None) -> str | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM meta.demo_config WHERE key = %s", (key,))
        row = cur.fetchone()
        return row["value"] if row else default


def set_config(key: str, value: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.demo_config(key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key) DO UPDATE
            SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value),
        )


@contextmanager
def stage_run(stage: str, trading_date: date) -> Iterator[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.pipeline_runs(stage, trading_date) VALUES (%s, %s) RETURNING run_id",
            (stage, trading_date),
        )
        run_id = cur.fetchone()["run_id"]
    outcome: dict = {"run_id": run_id, "row_count": None, "message": None}
    try:
        yield outcome
    except Exception as exc:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE meta.pipeline_runs
                SET finished_at = now(), status = 'FAILED', message = %s
                WHERE run_id = %s
                """,
                (str(exc)[:4000], run_id),
            )
        raise
    else:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE meta.pipeline_runs
                SET finished_at = now(), status = 'SUCCESS', row_count = %s, message = %s
                WHERE run_id = %s
                """,
                (outcome["row_count"], outcome["message"], run_id),
            )

