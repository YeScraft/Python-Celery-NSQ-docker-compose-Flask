# Файл создан чтобы можно было импортить классы БД в server из bd_engine
# иначе получался замкнутый импорт

from flask import Flask


app = Flask(__name__)
