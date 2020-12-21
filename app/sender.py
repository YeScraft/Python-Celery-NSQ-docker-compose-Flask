# модуль по пересылке сообщений в NSQ
import functools
import tornado.ioloop
import nsq
import tornado
from my_celery import celery_app

writer = nsq.Writer(["nsqd:4150"])


def finish_pub(conn, data):
    print('data: ', data)
    print('conn: ', conn)


# без @tornado.gen.coroutine костыль не работает
@tornado.gen.coroutine
def send_msg(b_data, topic):
    # без yield костыль даёт ошибки
    yield tornado.gen.sleep(1)
    writer.pub(topic, b_data, finish_pub)
    yield tornado.gen.sleep(1)


# этот костыль шлёт данные в NSQ
@celery_app.task(serializer='json')
def send(data, topic):
    b_data = data.encode()
    return tornado.ioloop.IOLoop.instance().run_sync(func=functools.partial(send_msg, b_data=b_data, topic=topic))


@celery_app.task
def send_data(data, topic):
    print("data: ", data)
    return send.apply_async(queue='low_priority', args=(data, topic))


if __name__ == '__main__':
    nsq.run()