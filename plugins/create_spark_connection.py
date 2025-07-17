from airflow.models import Connection
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
import logging

@provide_session
def create_spark_connection(session=None):
    conn_id = "spark_docker"

    existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing:
        logging.info(f"Conexão '{conn_id}' já existe.")
        return

    conn = Connection(
        conn_id=conn_id,
        conn_type="spark",
        host="spark://spark-master",
        port=7077,
        extra='{"deploy_mode": "client", "spark_home": "/opt/spark"}'
    )

    session.add(conn)
    session.commit()
    logging.info(f"Conexão '{conn_id}' criada com sucesso.")

class InitConnectionsPlugin(AirflowPlugin):
    name = "init_connections_plugin"

    def on_load(self, *args, **kwargs):
        create_spark_connection()
