"""
db/database.py
──────────────
Supabase / PostgreSQL database access layer for the Trade Idea Engine.

All pipeline modules import this single class for DB access.
Uses psycopg2 for direct Postgres queries (faster than the Supabase
REST client for bulk inserts) and supabase-py for realtime features.

Dependencies:
    pip install psycopg2-binary supabase python-dotenv loguru

Environment variables required (.env):
    SUPABASE_URL        = https://xxxx.supabase.co
    SUPABASE_KEY        = your-service-role-key   (NOT the anon key)
    DATABASE_URL        = postgresql://postgres:password@db.xxxx.supabase.co:5432/postgres

Where to find these:
    Supabase dashboard → Project Settings → API  (URL + keys)
    Supabase dashboard → Project Settings → Database → Connection string (URI)
"""

from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras        # for RealDictCursor
from dotenv import load_dotenv
from loguru import logger
from supabase import create_client, Client

#load_dotenv()

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "")   # service-role key
DATABASE_URL  = os.getenv("DATABASE_URL", "")

if not all([SUPABASE_URL, SUPABASE_KEY, DATABASE_URL]):
    raise EnvironmentError(
        "Missing required env vars: SUPABASE_URL, SUPABASE_KEY, DATABASE_URL. "
        "Check your .env file."
    )


# ─────────────────────────────────────────────
# Database class
# ─────────────────────────────────────────────
class Database:
    """
    Singleton database access layer.

    Uses two clients:
      - psycopg2 (direct Postgres) for all inserts, updates, and analytical queries.
        Faster for bulk operations and gives full SQL flexibility.
      - supabase-py for realtime subscriptions and simple table inserts.

    Usage:
        from db.database import Database
        db = Database.get()
        db.insert_signal(signal.to_dict())
    """

    _instance: Database | None = None

    def __init__(self) -> None:
        self._conn: psycopg2.extensions.connection | None = None
        self._supabase: Client | None = None
        logger.info("Database initialised (Supabase / Postgres)")

    # ── Singleton ───────────────────────────────────────────────────────────

    @classmethod
    def get(cls) -> Database:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── Connections ─────────────────────────────────────────────────────────

    @property
    def conn(self) -> psycopg2.extensions.connection:
        """
        Lazy psycopg2 connection with auto-reconnect.
        Postgres connections can drop after idle periods — this handles it.
        """
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                DATABASE_URL,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=10,
            )
            self._conn.autocommit = False
            logger.debug("Postgres connection established")
        return self._conn

    @property
    def supabase(self) -> Client:
        """Lazy Supabase client (for realtime subscriptions and simple inserts)."""
        if self._supabase is None:
            self._supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        return self._supabase

    @contextmanager
    def transaction(self):
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            # Force reset so next call gets a fresh connection
            self._conn = None
            logger.error("Transaction rolled back: {}", e)
            raise

    def _execute(
        self,
        sql: str,
        params: tuple | list | None = None,
        fetch: str | bool = "none",   # "none" | "one" | "all" | "df" | True (=all)
    ) -> Any:
        """
        Execute a SQL statement with error handling and optional fetch.

        fetch="df"  — returns a pandas DataFrame (dashboard use)
        fetch="all" / fetch=True — returns list[dict]
        fetch="one" — returns a single dict or None
        """
        # Normalise legacy True → "all"
        if fetch is True:
            fetch = "all"

        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

                if fetch == "one":
                    row = cur.fetchone()
                    return dict(row) if row else None
                elif fetch in ("all", "df"):
                    rows = cur.fetchall()
                    result = [dict(r) for r in rows]
                    if fetch == "df":
                        try:
                            import pandas as pd
                            return pd.DataFrame(result)
                        except ImportError:
                            return result
                    return result
                return None

    # ── Raw query (legacy / dashboard use) ──────────────────────────────────

    def query(self, sql: str, params: tuple | list | None = None) -> list[dict]:
        """Execute a raw SELECT and return list[dict]. No transaction wrapper."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    # ── Signals ─────────────────────────────────────────────────────────────

    def insert_signal(self, signal: dict) -> None:
        """
        Insert a new signal. Silently ignores duplicates (ON CONFLICT DO NOTHING)
        because the dedup check happens upstream, but this is a safety net.
        """
        self._execute(
            """
            INSERT INTO signals
                (signal_id, source, feed_topic, raw_text, url,
                 published_at, ingested_at, status,
                 fp_event, fp_subject, fp_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (signal_id) DO NOTHING
            """,
            (
                signal["signal_id"],
                signal["source"],
                signal.get("feed_topic"),
                signal["raw_text"],
                signal.get("url"),
                signal.get("published_at"),
                signal.get("ingested_at"),
                signal.get("status", "ingested"),
                signal.get("_fp_event"),
                signal.get("_fp_subject"),
                signal.get("_fp_key"),
            ),
        )

    def insert_signals_bulk(self, signals: list[dict]) -> int:
        """
        Bulk insert a list of signals in a single round-trip.
        Returns the number of rows actually inserted (duplicates skipped).
        """
        if not signals:
            return 0

        rows = [
            (
                s["signal_id"], s["source"], s.get("feed_topic"),
                s["raw_text"], s.get("url"), s.get("published_at"),
                s.get("ingested_at"), s.get("status", "ingested"),
                s.get("_fp_event"), s.get("_fp_subject"), s.get("_fp_key"),
            )
            for s in signals
        ]

        with self.transaction() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO signals
                        (signal_id, source, feed_topic, raw_text, url,
                         published_at, ingested_at, status,
                         fp_event, fp_subject, fp_key)
                    VALUES %s
                    ON CONFLICT (signal_id) DO NOTHING
                    """,
                    rows,
                    page_size=100,
                )
                inserted = cur.rowcount

        logger.info("Bulk inserted {} / {} signals", inserted, len(signals))
        return inserted

    def update_signal_enrichment(self, signal_id: str, data: dict) -> None:
        """Update enrichment columns after filtering / entity extraction."""
        self._execute(
            """
            UPDATE signals SET
                relevance_score = %s,
                entities        = %s,
                tickers         = %s,
                sectors         = %s,
                sentiment       = %s,
                macro_context   = %s,
                supply_chain    = %s,
                status          = %s
            WHERE signal_id = %s
            """,
            (
                data.get("relevance_score", 0),
                json.dumps(data.get("entities", [])),
                json.dumps(data.get("tickers", [])),
                json.dumps(data.get("sectors", [])),
                json.dumps(data.get("sentiment", {})),
                json.dumps(data.get("macro_context", {})),
                json.dumps(data.get("supply_chain", [])),
                data.get("status", "enriched"),
                signal_id,
            ),
        )

    def get_signals_for_analysis(
        self,
        min_relevance: float = 55,
        limit: int = 50,
        hours_back: int = 24,
    ) -> list[dict]:
        """
        Fetch enriched signals ready for LLM idea generation.
        Only returns signals above the relevance threshold that haven't
        been analyzed yet within the lookback window.
        """
        rows = self._execute(
            """
            SELECT *
            FROM signals
            WHERE relevance_score >= %s
              AND status = 'enriched'
              AND ingested_at > NOW() - (%s * INTERVAL '1 hour')
            ORDER BY relevance_score DESC
            LIMIT %s
            """,
            (min_relevance, hours_back, limit),
            fetch="all",
        )
        return rows or []

    def get_signals_summary(self, hours_back: int = 24) -> dict:
        """Quick counts for dashboard metrics."""
        row = self._execute(
            """
            SELECT
                COUNT(*)                                            AS total,
                COUNT(*) FILTER (WHERE status = 'ingested')        AS ingested,
                COUNT(*) FILTER (WHERE status = 'filtered')        AS filtered,
                COUNT(*) FILTER (WHERE status = 'enriched')        AS enriched,
                COUNT(*) FILTER (WHERE status = 'analyzed')        AS analyzed,
                ROUND(AVG(relevance_score)::numeric, 1)            AS avg_relevance
            FROM signals
            WHERE ingested_at > NOW() - (%s * INTERVAL '1 hour')
            """,
            (hours_back,),
            fetch="one",
        )
        return row or {}

    # ── Trade Ideas ─────────────────────────────────────────────────────────

    def insert_idea(self, idea: dict) -> None:
        """Insert a generated trade idea, persisting the thesis embedding if present."""
        # Embedding may be attached by IdeaDeduplicator — persist it so restarts
        # don't need to re-encode (avoids sentence-transformers cold start cost)
        embedding = idea.get("thesis_embedding")
        embedding_json = json.dumps(embedding) if embedding is not None else None

        self._execute(
            """
            INSERT INTO trade_ideas
                (idea_id, signal_id, ticker, direction, instrument_type,
                 time_horizon, thesis, confidence, key_risks, contrarian_flag,
                 size_tier, pricing_risk, raw_llm_output, thesis_embedding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idea_id) DO NOTHING
            """,
            (
                idea.get("idea_id", str(uuid.uuid4())[:16]),
                idea.get("signal_id"),
                idea["ticker"],
                idea["direction"],
                idea.get("instrument_type"),
                idea.get("time_horizon"),
                idea.get("thesis"),
                idea.get("confidence"),
                json.dumps(idea.get("key_risks", [])),
                idea.get("contrarian_flag", False),
                idea.get("size_tier"),
                idea.get("pricing_risk"),
                idea.get("raw_llm_output"),
                embedding_json,
            ),
        )
        logger.info(
            "Idea saved: {} {} (conf: {}, tier: {})",
            idea["direction"], idea["ticker"],
            idea.get("confidence"), idea.get("size_tier"),
        )

    def get_recent_ideas(
        self,
        hours: int | None = 48,
        min_confidence: int = 40,
        direction: list[str] | None = None,
    ) -> list[dict]:
        """
        Fetch recent ideas joined with their source signal.
        hours=None returns all ideas regardless of age.
        Returns list[dict] — includes thesis_embedding for dedup cache reload.
        """
        params: list = [min_confidence]
        time_filter = ""
        if hours is not None:
            time_filter = "AND i.generated_at > NOW() - (%s * INTERVAL '1 hour')"
            params.append(hours)

        dir_filter = ""
        if direction:
            placeholders = ", ".join(["%s"] * len(direction))
            dir_filter = f"AND i.direction IN ({placeholders})"
            params.extend(direction)

        rows = self._execute(
            f"""
            SELECT
                i.idea_id, i.generated_at, i.ticker, i.direction,
                i.instrument_type, i.confidence, i.size_tier,
                i.time_horizon, i.thesis, i.contrarian_flag,
                i.pricing_risk, i.key_risks, i.outcome_return_pct,
                i.alerted,
                s.raw_text AS signal_text,
                s.feed_topic, s.source, s.url, s.relevance_score,
                ta.score               AS ta_score,
                ta.overall_confidence  AS ta_confidence,
                ta.bias                AS ta_bias,
                ta.entry_price         AS ta_entry,
                ta.stop_loss           AS ta_stop_loss,
                ta.invalidation_price  AS ta_invalidation,
                ta.risk_reward_ratio   AS ta_rr,
                ta.trend_alignment     AS ta_trend_alignment,
                ta.volume_confirmation AS ta_volume_confirmation,
                ta.pattern_quality     AS ta_pattern_quality,
                ta.risk_reward         AS ta_risk_reward_score,
                ta.summary             AS ta_summary,
                ta.full_result         AS ta_full_result
            FROM trade_ideas i
            LEFT JOIN signals s              ON i.signal_id = s.signal_id
            LEFT JOIN technical_analysis ta  ON i.idea_id   = ta.idea_id
            WHERE i.confidence >= %s
              {time_filter}
              {dir_filter}
            ORDER BY i.confidence DESC, i.generated_at DESC
            """,
            params,
            fetch="all",
        )
        return rows or []

    def get_unalerted_ideas(self, min_confidence: int = 50) -> list[dict]:
        """Fetch ideas not yet sent to Telegram, with TA summary if available."""
        rows = self._execute(
            """
            SELECT
                ti.*,
                s.url,
                ta.score            AS ta_score,
                ta.overall_confidence AS ta_confidence,
                ta.bias             AS ta_bias,
                ta.entry_price      AS ta_entry,
                ta.stop_loss        AS ta_stop_loss,
                ta.risk_reward_ratio AS ta_rr,
                ta.summary          AS ta_summary
            FROM trade_ideas ti
            LEFT JOIN signals s           ON ti.signal_id = s.signal_id
            LEFT JOIN technical_analysis ta ON ti.idea_id  = ta.idea_id
            WHERE ti.alerted = FALSE
              AND ti.confidence >= %s
            ORDER BY ti.confidence DESC
            """,
            (min_confidence,),
            fetch="all",
        )
        return rows or []

    def mark_idea_alerted(self, idea_id: str) -> None:
        self._execute(
            "UPDATE trade_ideas SET alerted = TRUE WHERE idea_id = %s",
            (idea_id,),
        )

    def get_idea_embeddings(self, hours: int = 336) -> dict[str, list]:
        """
        Fetch thesis_embedding for recent ideas — used by IdeaDeduplicator on reload.
        Separate method so it fails gracefully if the column doesn't exist yet
        (before migration 003 is run).
        Returns {idea_id: embedding_list}.
        """
        try:
            rows = self._execute(
                """
                SELECT idea_id, thesis_embedding
                FROM trade_ideas
                WHERE thesis_embedding IS NOT NULL
                  AND generated_at > NOW() - (%s * INTERVAL '1 hour')
                """,
                (hours,),
                fetch="all",
            )
            result = {}
            for row in (rows or []):
                emb = row.get("thesis_embedding")
                if emb is not None:
                    if isinstance(emb, str):
                        import json as _j
                        try:
                            emb = _j.loads(emb)
                        except Exception:
                            continue
                    result[row["idea_id"]] = emb
            return result
        except Exception:
            return {}   # column doesn't exist yet — silently return empty

    def update_idea_outcome(
        self,
        idea_id: str,
        return_pct: float,
        days: int,
        notes: str = "",
    ) -> None:
        """Record the outcome of an idea for the feedback loop."""
        self._execute(
            """
            UPDATE trade_ideas SET
                outcome_return_pct = %s,
                outcome_days       = %s,
                outcome_at         = NOW(),
                outcome_notes      = %s
            WHERE idea_id = %s
            """,
            (return_pct, days, notes, idea_id),
        )

    # ── Price Snapshots ──────────────────────────────────────────────────────

    def insert_price_snapshot(self, data: dict) -> list:
        """Insert a price snapshot via Supabase client."""
        response = (
            self.supabase
            .table("price_snapshots")
            .insert(data)
            .execute()
        )
        if response.data is None:
            raise Exception(response)
        logger.info(
            "Price snapshot saved: {} ({})",
            data.get("ticker", ""), data.get("fetch_reason", ""),
        )
        return response.data

    # ── LLM Messages ─────────────────────────────────────────────────────────

    def insert_llm_message(self, data: dict) -> list:
        """Insert an LLM message log entry via Supabase client."""
        response = (
            self.supabase
            .table("llm_messages")
            .insert(data)
            .execute()
        )
        if response.data is None:
            raise Exception(response)
        logger.info("Inserted LLM message id {}", data.get("message_id", ""))
        return response.data

    # ── Deduplication ───────────────────────────────────────────────────────

    def is_duplicate(self, signal_id: str) -> bool:
        row = self._execute(
            "SELECT 1 FROM dedup_store WHERE signal_id = %s",
            (signal_id,),
            fetch="one",
        )
        return row is not None

    def mark_seen(self, signal_id: str) -> None:
        self._execute(
            "INSERT INTO dedup_store (signal_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (signal_id,),
        )

    def mark_seen_bulk(self, signal_ids: list[str]) -> None:
        """Bulk insert dedup entries — call after a successful ingest run."""
        if not signal_ids:
            return
        with self.transaction() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dedup_store (signal_id) VALUES %s ON CONFLICT DO NOTHING",
                    [(sid,) for sid in signal_ids],
                )

    def purge_old_dedup(self, days: int = 7) -> int:
        """Delete dedup entries older than N days. Returns rows deleted."""
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM dedup_store WHERE seen_at < NOW() - (%s * INTERVAL '1 day')",
                    (days,),
                )
                deleted = cur.rowcount
        logger.info("Purged {} old dedup entries (>{} days)", deleted, days)
        return deleted

    # ── Macro Snapshots ─────────────────────────────────────────────────────

    def insert_macro_snapshot(self, snapshot: dict) -> None:
        signals  = snapshot.get("signals") or {}
        zscores  = snapshot.get("zscores") or {}

        self._execute(
            """
            INSERT INTO macro_snapshots
                (snapshot_id, captured_at, fed_funds_rate, cpi_yoy, unemployment,
                dxy_level, hy_spread, ten_yr_yield, regime, raw_data,
                confidence,
                signal_risk_off, signal_risk_on, signal_stagflation, signal_disinflation,
                z_cpi, z_unemployment, z_spread)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id) DO NOTHING
            """,
            (
                snapshot.get("snapshot_id", str(uuid.uuid4())[:16]),
                snapshot.get("captured_at"),
                snapshot.get("fed_funds_rate"),
                snapshot.get("cpi_yoy"),
                snapshot.get("unemployment"),
                snapshot.get("dxy_level"),
                snapshot.get("hy_spread"),
                snapshot.get("ten_yr_yield"),
                snapshot.get("regime"),
                json.dumps(snapshot.get("raw_data", {})),
                snapshot.get("confidence"),
                signals.get("risk_off"),
                signals.get("risk_on"),
                signals.get("stagflation"),
                signals.get("disinflation"),
                zscores.get("cpi"),
                zscores.get("unemployment"),
                zscores.get("spread"),
            ),
        )

    def get_latest_macro(self) -> dict:
        """Returns the most recent macro snapshot as a plain dict."""
        row = self._execute(
            "SELECT * FROM macro_snapshots ORDER BY captured_at DESC LIMIT 1",
            fetch="one",
        )
        return row or {}

    def load_macro_history(self, limit: int = 60) -> list[dict]:
        """Returns macro snapshots in chronological order for charting."""
        rows = self._execute(
            """
            SELECT captured_at, cpi_yoy, unemployment, hy_spread
            FROM macro_snapshots
            ORDER BY captured_at DESC
            LIMIT %s
            """,
            (limit,),
            fetch="all",
        )
        return list(reversed(rows)) if rows else []

    # ── Run Log ─────────────────────────────────────────────────────────────

    def log_run_start(self) -> str:
        run_id = str(uuid.uuid4())[:16]
        self._execute(
            "INSERT INTO run_log (run_id, status) VALUES (%s, 'running')",
            (run_id,),
        )
        return run_id

    def log_run_finish(
        self,
        run_id: str,
        signals_found: int,
        signals_filtered: int,
        ideas_generated: int,
        errors: list | None = None,
        status: str = "success",
    ) -> None:
        self._execute(
            """
            UPDATE run_log SET
                finished_at      = NOW(),
                signals_found    = %s,
                signals_filtered = %s,
                ideas_generated  = %s,
                errors           = %s,
                status           = %s
            WHERE run_id = %s
            """,
            (
                signals_found,
                signals_filtered,
                ideas_generated,
                json.dumps(errors or []),
                status,
                run_id,
            ),
        )

    def get_pipeline_health(self, days: int = 7) -> list[dict]:
        """Returns daily pipeline stats for the dashboard health view."""
        rows = self._execute(
            """
            SELECT
                DATE(started_at)                                        AS run_date,
                COUNT(*)                                                AS total_runs,
                SUM(signals_found)                                      AS total_signals,
                SUM(ideas_generated)                                    AS total_ideas,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)    AS failed_runs,
                ROUND(AVG(EXTRACT(EPOCH FROM (finished_at - started_at)))::numeric, 1)
                                                                        AS avg_duration_secs
            FROM run_log
            WHERE started_at > NOW() - (%s * INTERVAL '1 day')
            GROUP BY DATE(started_at)
            ORDER BY run_date DESC
            """,
            (days,),
            fetch="all",
        )
        return rows or []

    # ── Maintenance ─────────────────────────────────────────────────────────

    def run_maintenance(self) -> None:
        """
        Weekly maintenance tasks. Call from a scheduled job.
        Purges old dedup entries, stale low-relevance signals, and old run logs.
        """
        purged_dedup = self.purge_old_dedup(days=7)

        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM signals
                    WHERE relevance_score < 30
                      AND created_at < NOW() - INTERVAL '30 days'
                    """
                )
                purged_signals = cur.rowcount

                cur.execute(
                    "DELETE FROM run_log WHERE started_at < NOW() - INTERVAL '90 days'"
                )
                purged_runs = cur.rowcount

        logger.info(
            "Maintenance complete — dedup: {}, signals: {}, run_log: {}",
            purged_dedup, purged_signals, purged_runs,
        )

    # ── Realtime (Supabase-specific) ────────────────────────────────────────

    def subscribe_to_ideas(self, callback) -> None:
        """
        Subscribe to new trade ideas in realtime.
        Use this in the dashboard to push updates without polling.

        Example:
            def on_new_idea(payload):
                st.rerun()

            db.subscribe_to_ideas(on_new_idea)
        """
        self.supabase.realtime.channel("trade_ideas").on(
            "postgres_changes",
            event="INSERT",
            schema="public",
            table="trade_ideas",
            callback=callback,
        ).subscribe()
        logger.info("Subscribed to realtime trade_ideas inserts")


# ─────────────────────────────────────────────
# DB-backed deduplication store
# Drop-in replacement for the in-memory version in google_news_rss.py
# ─────────────────────────────────────────────
class DBDeduplicationStore:
    """
    Persistent dedup store backed by the dedup_store table.
    Pass this to GoogleNewsRSSIngestor() as the dedup_store argument.

    Example:
        from db.database import DBDeduplicationStore
        ingestor = GoogleNewsRSSIngestor(dedup_store=DBDeduplicationStore())
    """

    def is_duplicate(self, signal_id: str) -> bool:
        return Database.get().is_duplicate(signal_id)

    def mark_seen(self, signal_id: str) -> None:
        Database.get().mark_seen(signal_id)

    def __len__(self) -> int:
        row = Database.get()._execute(
            "SELECT COUNT(*) AS n FROM dedup_store", fetch="one"
        )
        return row["n"] if row else 0


# ─────────────────────────────────────────────
# Connection test (run directly to verify setup)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    print("Testing Supabase connection...")
    try:
        db = Database.get()
        summary = db.get_signals_summary()
        print(f"✓ Connected successfully")
        print(f"  Signals in DB: {summary.get('total', 0)}")
        print(f"  Avg relevance: {summary.get('avg_relevance', 'n/a')}")

        macro = db.get_latest_macro()
        print(f"  Latest macro snapshot: {macro.get('captured_at', 'none yet')}")

        ideas = db.get_recent_ideas(hours=336, min_confidence=0)
        print(f"  Ideas in DB (14d): {len(ideas)}")

        print("\n✓ All checks passed. Database is ready.")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nCheck your .env file:")
        print("  SUPABASE_URL  — found in Supabase → Settings → API")
        print("  SUPABASE_KEY  — use the service_role key, NOT the anon key")
        print("  DATABASE_URL  — found in Supabase → Settings → Database → URI")
        sys.exit(1)