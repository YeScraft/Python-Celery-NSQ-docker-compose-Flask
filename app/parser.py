import requests
import re
from requests import RequestException, HTTPError
import json
from my_celery import celery_app
from sender import send_data
from celery.exceptions import SoftTimeLimitExceeded
# для проверки time_limit
# import time


# time_limit - завершает обработку задания soft_time_limit - запускает обработку исключения при достижении времени
@celery_app.task(time_limit=12, soft_time_limit=10)
def parsing_sites(data, ok):
    """
    обрабатывает полученный url
    """
    try:
        # для проверки time_limit
        # time.sleep(12)
        data = json.loads(data)
        parsing_url = data['address']
        # проверка на наличие http или https
        if not (parsing_url.startswith('http') or parsing_url.startswith('https')):
            result = {
                'address': parsing_url,
                'r.id': data['r.id'],
                'words_count': None,
                'http_status_code': None,
                't.id': data['t.id'],
                'task_status': 'NOT_HTTP',
                'http_status': None
            }
            j_result = json.dumps(result)
            # низкий приоритет на задачи по передачи данных, высокий на запуск функции парсига
            # отправка данных в очередь NSQ топик send_result который слушает БД
            return send_data.apply_async(queue='low_priority', args=(j_result, 'send_result'))

        else:
            try:
                web_data = requests.get(parsing_url)
                web_data.raise_for_status()
            # исключение по несуществующему адресу
            except HTTPError as err:
                status_code = err.response.status_code
                result = {
                    'address': parsing_url,
                    'r.id': data['r.id'],
                    'words_count': None,
                    'http_status_code': status_code,
                    't.id': data['t.id'],
                    'task_status': 'NOT_ADDRESS',
                    'http_status': status_code
                }
                j_result = json.dumps(result)
                return send_data.apply_async(queue='low_priority', args=(j_result, 'send_result'))
            # исключение по несуществующему хосту
            except RequestException:
                result = {
                    'address': parsing_url,
                    'r.id': data['r.id'],
                    'words_count': None,
                    'http_status_code': None,
                    't.id': data['t.id'],
                    'task_status': 'NOT_HOST',
                    'http_status': None
                }
                j_result = json.dumps(result)
                return send_data.apply_async(queue='low_priority', args=(j_result, 'send_result'))

            else:
                number_of_python = 0
                if web_data.ok:
                    # регулярным выражением создаем список из слов
                    list_of_words = re.split('\W+', web_data.text)
                    number_of_python = list_of_words.count("Python")
                result = {
                    'address': parsing_url,
                    'r.id': data['r.id'],
                    'words_count': number_of_python,
                    'http_status_code': web_data.status_code,
                    't.id': data['t.id'],
                    'task_status': 'FINISHED',
                    'http_status': web_data.status_code
                }
                j_result = json.dumps(result)
                return send_data.apply_async(queue='low_priority', args=(j_result, 'send_result'))

    # обработка исключения если задача выполнялась дольше 10 секунд
    except SoftTimeLimitExceeded:
        data = json.loads(data)
        parsing_url = data['address']
        result = {
            'address': parsing_url,
            'r.id': data['r.id'],
            'words_count': None,
            'http_status_code': None,
            't.id': data['t.id'],
            'task_status': 'TIME_IS_OUT',
            'http_status': None
        }
        j_result = json.dumps(result)
        return send_data.apply_async(queue='low_priority', args=(j_result, 'send_result'))
