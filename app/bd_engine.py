from sqlalchemy import Enum
import nsq
from sender import send_data
import datetime
import enum
import json
import os
from flask_sqlalchemy import SQLAlchemy
from initialisation import app

POSTGRES_USER = os.getenv("POSTGRES_USER", 'postgres')
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", 'password')
POSTGRES_DB = os.getenv("POSTGRES_DB", 'results')
POSTGRES_LH = os.getenv("POSTGRES_LH", '0.0.0.0')
PORT = os.getenv("PORT", '5432')

app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_LH}:{PORT}/{POSTGRES_DB}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

NSQD_LH = os.getenv("NSQD_LH", '127.0.0.1')
NSQD_P = os.getenv("NSQD_P", '4150')


class Results(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(300), unique=False, nullable=True)
    words_count = db.Column(db.Integer, unique=False, nullable=True)
    http_status_code = db.Column(db.Integer)

    def __repr__(self):
        return f"<address {self.address}"[0:30]+'>'


class TaskStatus (enum.Enum):
    NOT_HTTP = 1
    PENDING = 2
    FINISHED = 3
    TIME_IS_OUT = 4
    NOT_ADDRESS = 5
    NOT_HOST = 6


class Tasks(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address = db.Column(db.String(300), unique=False, nullable=True)
    timestamp = db.Column(db.DateTime())
    task_status = db.Column(Enum(TaskStatus))
    http_status = db.Column(db.Integer)

    def __repr__(self):
        return f"<address {self.address}"[0:30]+'>'


def new_url(parsing_url):
    """
    Создает в БД запись о новом URL и передаёт его значение в очередь NSQ топик send_task, который
    слушает parser_listener и забирает от туда задачи
    """
    parsing_url = parsing_url.body
    n_url = parsing_url.decode()
    print('parsing_url_1: ', n_url)
    t = Tasks(address=n_url,
              timestamp=datetime.datetime.now(),
              task_status='PENDING',
              http_status=None)
    db.session.add(t)
    # помещает запись в таблицу, но не коммитит
    db.session.flush()

    r = Results(address=n_url,
                words_count=None,
                http_status_code=None)
    db.session.add(r)
    db.session.commit()
    # вместе с url передаем id нового url для его дальнейшего идентифицирования,
    # так как url не уникальны в БД
    data = {
        'address': n_url,
        'r.id': r.id,
        't.id': t.id
    }
    j_data = json.dumps(data)
    # низкий приоритет на задачи по передачи данных, высокий на запуск функции парсига
    # отправка данных в очередь NSQ топик send_task, который слушает обработчик
    send_data.apply_async(queue='low_priority', args=(j_data, 'send_task'))
    return True


def got_result(data):
    """
    записывает в БД финальный результат обработка URL
    """
    b_result_data = data.body
    j_result_data = b_result_data.decode()
    result_data = json.loads(j_result_data)

    r = Results.query.filter(Results.id == result_data['r.id']).all()
    r[0].words_count = result_data['words_count']
    r[0].http_status_code = result_data['http_status_code']

    t = Tasks.query.filter(Tasks.id == result_data['r.id']).all()
    t[0].task_status = result_data['task_status']
    t[0].http_status = result_data['http_status']

    db.session.commit()
    return True


# слушает топик send_url в ожидании данных от сервера по новому url
server = nsq.Reader(
    message_handler=new_url,
    nsqd_tcp_addresses=[f"{NSQD_LH}:{NSQD_P}"],
    topic='send_url',
    channel='db_url',
    lookupd_poll_interval=15
)


# слушает топик send_result в ожидании данных по результату обработки
parser = nsq.Reader(
    message_handler=got_result,
    nsqd_tcp_addresses=[f"{NSQD_LH}:{NSQD_P}"],
    topic='send_result',
    channel='db_result',
    lookupd_poll_interval=15
)


if __name__ == '__main__':
    db.create_all()
    nsq.run()
