import logging
from contextlib import contextmanager
from typing import Any, Iterable, Sequence, Tuple
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import psycopg2.extras
from psycopg2 import sql
from functools import lru_cache

logger = logging.getLogger(__name__)

class PostgresClient:

    def __init__(self, postgres_conn_id: str = "fpl_postgres_conn"):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    @contextmanager
    def connection(self):
        """Transaction context manager.

        Commits on success, rolls back on exception.
        """
        conn = self.hook.get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_one(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        conn: Any | None = None,
    ):
        """Fetches a single row. Supports running inside an active transaction."""
        if conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters)
                return cursor.fetchone()
        return self.hook.get_first(sql, parameters=parameters)

    def fetch_all(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        conn: Any | None = None,
    ) -> list[Tuple[Any, ...]]:
        if conn is not None:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                return cursor.fetchall()
        return self.hook.get_records(query, parameters=parameters)

    def fetch_df(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        conn: Any | None = None,
    ) -> pd.DataFrame:
        if conn is not None:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else None
                return pd.DataFrame(rows, columns=columns)
        return self.hook.get_pandas_df(query, parameters=parameters)

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        conn: Any | None = None,
    ) -> None:
        if conn is not None:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
            return

        with self.connection() as active_conn:
            with active_conn.cursor() as cursor:
                cursor.execute(query, parameters)

    def executemany(
        self,
        query: str,
        rows: Iterable[Sequence[Any]],
        conn: Any | None = None,
    ) -> None:
        if conn is not None:
            with conn.cursor() as cursor:
                cursor.executemany(query, rows)
            return

        with self.connection() as active_conn:
            with active_conn.cursor() as cursor:
                cursor.executemany(query, rows)

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        conflict_fields: list[str] | None = None,
        conn: Any | None = None,
    ) -> None:
        if df.empty:
            logger.warning("Passed DataFrame for '%s' is empty. Skipping.", table)
            return

        columns = list(df.columns)
        rows = list(df.itertuples(index=False, name=None))

        if conflict_fields:
            missing = [field for field in conflict_fields if field not in columns]
            if missing:
                raise ValueError(
                    f"Conflict fields {missing} are not present in DataFrame columns for '{table}'."
                )
        
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.SQL(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )

        if conflict_fields:
            conflict_sql = sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
                sql.SQL(", ").join(sql.Identifier(column) for column in conflict_fields)
            )
            insert_sql = insert_sql + conflict_sql

        if conn is not None:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor,
                    insert_sql.as_string(conn),
                    rows,
                )
            logger.info("Staged %s rows for table '%s'.", len(rows), table)
            return

        with self.connection() as active_conn:
            with active_conn.cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor,
                    insert_sql.as_string(active_conn),
                    rows,
                )

        logger.info("Staged %s rows for table '%s'.", len(rows), table)

@lru_cache(maxsize=1)
def get_db(postgres_conn_id: str = "fpl_postgres_conn") -> PostgresClient:
    return PostgresClient(postgres_conn_id=postgres_conn_id)