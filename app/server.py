from flask import render_template, request, redirect
from sender import send_data
from bd_engine import Tasks, Results
from initialisation import app


@app.route("/get_url", methods=['GET', 'POST'])
def get_url():
    if request.method == 'GET':
        return render_template('get_url.html')

    if request.method == 'POST':
        parsing_url = str(request.form['url'])
        # низкий приоритет на задачи по передачи данных, высокий на запуск функции парсига
        # отправка данных в очередь NSQ топик send_url, который слушает БД
        send_data.apply_async(queue='low_priority', args=(parsing_url, 'send_url'))
        return redirect('/get_url')


@app.route("/", methods=['GET',])
def index():
    if request.method == 'GET':
        tasks = Tasks.query.order_by(Tasks.timestamp.desc()).all()
        results = Results.query.order_by(Results.id.desc()).all()
        return render_template('index.html', tasks=tasks, results=results)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
