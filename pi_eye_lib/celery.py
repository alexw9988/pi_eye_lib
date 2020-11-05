
import logging
import os

from celery import Celery

log = logging.getLogger('celery')

# rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
# rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "guest")
# rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
# rabbitmq_vhost = os.getenv("RABBITMQ_VHOST", "/")

postgres_user = os.getenv("POSTGRES_USER", "postgres")
postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres123")
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_db = os.getenv("POSTGRES_CELERY_DB", "celery_results")

app = Celery('tasks', enable_utc=True,
            #  broker=f'amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}/{rabbitmq_vhost}',
            #  broker='amqp://guest:guest@localhost//',
             broker="pyamqp://rabbitmq//",
             backend=f'db+postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
)
