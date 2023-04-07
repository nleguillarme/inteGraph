from airflow import settings
from airflow.models import Connection


def create_connection(conn_id, config):
    session = settings.Session()  # get the session
    conn_name = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if str(conn_name) != str(conn_id):
        print("create connection", conn_id)
        conn = Connection(
            conn_id=conn_id,
            conn_type=config.get("conn_type"),
            host=config.get("host"),
            port=config.get("port"),
            login=config.get("login"),
            password=config.get("password"),
            schema=config.get("schema"),
            extra=config.get("extra"),
        )  # create a connection object
        session.add(conn)
        session.commit()
    else:
        print("connection", conn_id, "exists")


def register_connections(config):
    for conn_id in config:
        create_connection(conn_id, config[conn_id])
