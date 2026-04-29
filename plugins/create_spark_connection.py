"""Airflow plugin to auto-create the Spark connection on startup.

Creates a ``spark_docker`` connection pointing to the Spark master
if it doesn't already exist. This avoids manual connection setup
in the Airflow UI.
"""

import logging

from airflow.models import Connection
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session

logger = logging.getLogger(__name__)


@provide_session
def create_spark_connection(session=None) -> None:
    """Create the spark_docker connection if it doesn't exist.

    Args:
        session: SQLAlchemy session (injected by @provide_session).
    """
    conn_id = "spark_docker"

    existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing:
        logger.info("Connection '%s' already exists, skipping creation", conn_id)
        return

    conn = Connection(
        conn_id=conn_id,
        conn_type="spark",
        host="spark://spark-master",
        port=7077,
        extra='{"deploy_mode": "client"}',
    )

    session.add(conn)
    session.commit()
    logger.info("Connection '%s' created successfully", conn_id)


class SparkConnectionPlugin(AirflowPlugin):
    """Plugin that creates the Spark connection on Airflow load."""

    name = "spark_connection_plugin"

    def on_load(self, *args, **kwargs) -> None:
        """Initialize the Spark connection when the plugin loads."""
        create_spark_connection()
