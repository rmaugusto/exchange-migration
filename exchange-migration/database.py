
import psycopg2.extras
#from utils import EmThreadedConnectionPool
from psycopg2.pool import ThreadedConnectionPool


class Database:
    """
    This class allows to connect to the database and query it

    CREATE TABLE public.email_migration (
        id uuid DEFAULT uuid_generate_v4() NOT NULL,
        date_time timestamp DEFAULT now() NULL,
        account varchar NULL,
        "type" varchar NULL,
        subject varchar NULL,
        from_id varchar NULL,
        to_id varchar NULL,
        "event" varchar NULL,
        "path" varchar NULL,
        CONSTRAINT newtable_pk PRIMARY KEY (id)
    );

    CREATE TABLE public.migration_queue (
        id uuid NOT NULL,
        email_origin varchar NULL,
        email_destination varchar NULL,
        "action" varchar NULL,
        priority numeric NULL,
        status varchar NULL,
        "instance" varchar NULL,
        date_begin timestamp NULL,
        date_end timestamp NULL,
        last_update timestamp NULL,
        CONSTRAINT migraiton_queue_pk PRIMARY KEY (id)
    );


    """

    def __init__(self, config):
        self.config = config
        self.db_pool = None

    def connect(self):

        db_config = {
            'host': self.config['general']['db_host'],
            'database': self.config['general']['db_name'],
            'user': self.config['general']['db_user'],
            'password': self.config['general']['db_pass']
        }

        self.db_pool = ThreadedConnectionPool(minconn=1, maxconn=1, **db_config)


    def insert_migration(self, account, type, subject, from_id, to_id, event, path):

        with self.db_pool.getconn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO email_migration (account, type, subject, from_id, to_id, event, path) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (account, type, subject, from_id, to_id, event, path)
                )
                conn.commit()
        self.db_pool.putconn(conn)

    def get_next_migration(self, instance_name):

        migration = None

        with self.db_pool.getconn() as conn:
            with conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM migration_queue WHERE status = 'pending' or (status = 'processing' and instance = %s) ORDER BY priority DESC LIMIT 1",
                    (instance_name,)
                )
                migration = cursor.fetchone()
        self.db_pool.putconn(conn)
        
        return migration
    
    def try_lock_migration(self, id, instance_name):

        with self.db_pool.getconn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE migration_queue SET status = 'processing', instance = %s, last_update = now() WHERE id = %s and status = 'pending'",
                    (instance_name, id)
                )
                conn.commit()
        self.db_pool.putconn(conn)

    def get_migration_by_id(self, id):

        migration = None

        with self.db_pool.getconn() as conn:
            with conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM migration_queue WHERE id = %s LIMIT 1",
                    (id,)
                )
                migration = cursor.fetchone()
        self.db_pool.putconn(conn)
        
        return migration
    
    def update_migration_done(self, id):

        with self.db_pool.getconn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE migration_queue SET status = 'done', last_update = now() WHERE id = %s",
                    (id,)
                )
                conn.commit()
        self.db_pool.putconn(conn)