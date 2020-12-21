from celery import Celery
import os

RABBITMQ_DEFAULT_USER = os.getenv("RABBITMQ_DEFAULT_USER", 'admin')
RABBITMQ_DEFAULT_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", 'mypass')
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", '5672')

celery_app = Celery('app', broker=f"amqp://{RABBITMQ_DEFAULT_USER}:{RABBITMQ_DEFAULT_PASS}@rabbit:{RABBITMQ_PORT}",
                    backend=f"rpc://",
                    include=['sender', 'parser'],)

# определяет максимальное время хранения результатов на бекенде
celery_app.conf.update(
    result_expires=3600,
)
