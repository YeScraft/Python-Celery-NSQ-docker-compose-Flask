import nsq
from parser import parsing_sites
import os

NSQD_LH = os.getenv("NSQD_LH", '127.0.0.1')
NSQD_P = os.getenv("NSQD_P", '4150')


def parsing_handler(data):
    """
    запускает асинхранную задачу по обработке url в celery
    """
    data = data.body
    j_data = data.decode()
    ok = 'ok'
    # даем предпочтение обработке url, тк это более важная и трудоемкая задача
    parsing_sites.apply_async(queue='high_priority', args=(j_data, ok))
    return True


# слушает топик send_task в ожидании данных на обработку
r = nsq.Reader(
    message_handler=parsing_handler,
    nsqd_tcp_addresses=[f"{NSQD_LH}:{NSQD_P}"],
    topic='send_task',
    channel='parser',
    lookupd_poll_interval=15
)


if __name__ == '__main__':
    nsq.run()
