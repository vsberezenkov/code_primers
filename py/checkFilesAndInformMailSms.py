import os
import pathlib
from pathlib import Path
from os.path import getctime 
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow import DAG
import smtplib
from datetime import datetime, timedelta, date
import requests
import json
from base64 import b64encode 
import time

# Данные для отправки email 
mail01 = 'defaulthuman01@gmail.com' 
mail02 = 'defaulthuman02@gmail.com'
mail03 = 'defaulthuman03@gmail.com'
to_addr_ok = mail01
to_addr_nok = mail01, mail02, mail03

# Данные для отправки СМС
username = 'username'
password = 'password'
userpass = f"{username}:{password}"
base64_userpass = b64encode(userpass.encode('ascii')).decode('ascii')
print (base64_userpass)
url = 'https://sms.ru/http-api/v1/messages'

#Номера на которые будет проведена рассылка
number01 = "79181111111" #Человек01 
number02 = "79181111112" #Человек02
number03 = "79181111113" #Человек03 
NumberLib = number01, number02, number03

is_pass = ['A01', 'A02', 'A03']

# Список ВСЕХ обрабатываемых папок
ch_send=['CH01','CH02','CH03','CH04','CH05']
# Список сверок, которые исключаются из рассылки по пустым файлам
ch_not_send = ['CH04','CH05']

# Timer start
abs_total = 0
start_time = datetime.now()
print(f"START_TIME = {start_time}")

# Основной процесс
def check_proc():
    # Циклом запускаем общие поиски, 2 и 3
    for pass_name in is_pass:
    # Путь корневого каталога, где лежат все поиски
    root_path = f"/ftp-output/{pass_name}"

    # Объявляем библиотеку, в которые потом положим найденные пути к отсутствующим файлам
    path_lib = []

    # Объявляем библиотеку, в которые потом положим найденные пути к пустым файлам 
    path_empty_lib =[]

    # Цикл проходится по всем подкаталогам root_path и в них ищет файлы
    for address, dirs, files in os.walk(root_path):
        for name in dirs:
            # Проходим только по каталогам заданным в cp_send
            if name in cp_send:
                this_dir = os.path.join(address, name)
                print(f"this_dir = {this_dir}")

                # эта конструкция находит имя последнего измененного файла с расширением .csv за исключением содержащих в имени "SMS", "DateTime", "DateCharge-Other" try:
                try:
                    last_file = max((e for e in os.scandir(this_dir) if (e.is_file(follow_symlinks =False) and e.name.endswith('.csv') and not e.name._contains___('SMS') and not e.name.__contains___('DateTime') and not e.name.__contains___('DateCharge-Other'))), key-lambda e: getattr(e.stat(), 'st_mtime', None) or e.stat().st_ctime)
                    print (f"last_file = {last_file.path}")

                    # Посчитаем количество строк в найденом файле
                    try:
                        file_line_count=open (last_file, 'r')
                        lines=0
                        for line in file_line_count:
                            lines
                            lines+=1
                        print(f"lines = {lines}")
                    except:
                        #По дефолту файл открывается как UTF8, но если он не такой, то вот этот кусок его откроет в Windows-1251 
                        try:
                            import codecs
                            file_line_count=codecs.open(last_file, 'r', 'Windows-1251')
                            lines=0
                            for line in file_line_count :
                                lines
                                lines+=1
                            print (f"lines = {lines}")
                        except Exception:
                            import traceback
                            print( f'ERROR: can not take lines count')
                            print(traceback.format_exc())

                        # определяем дату создания самого нового файла в директории 
                        last_file_date = datetime.fromtimestamp(getctime(last_file)).date()
                        print (f"last_file_date = {last_file_date}")

                        # Определяем в какой директории лежит этот файл 
                        last_file_dir = pathlib.Path(last_file).parents[0]
                        last_file_dir = str(last_file_dir)
                        # print(f"last_file_dir = {last_file_dir}")

                        #Определяем сегодняшнюю дату
                        current_date = datetime.now().date()
                        print (f"current_datetime = {current_date}")

                        # Кладём путь найденного файла в библиотеку если его дата создания не сегодня
                        if last_file_date != current_date :
                            path_lib.append(last_file_dir)
                            print (f"path_lib = {path_lib}")
                        else:    
                            # Кладём в библиотеку пути файлов у которых только заголовок (то есть по факту пустых) кроме списка исключений 
                            if name not in cp_not_send:
                                if lines in [0,1]:
                                    path_empty_lib.append(last_file_dir)
                                    print (f" path_empty_lib = {path_empty_lib}")
                except:
                    print (f"last_file не найден")
                    # Кладём путь к файлу в библиотеки 
                    path_lib.append(this_dir)
                    print (f"path_lib = {path_lib}")
                    # path_empty_lib.append(this_dir)
                    # print(f" path_empty_lib = {path_empty_lib}")
            print (f"\n")
        print (f"path_lib = {path_lib}")
        print (f"path_empty_lib = {path_empty_lib}")

        # Преобразовываем библиотеки в стрингу где каждый путь с новой строки для последующего использования в тексте письма 
        path_lib = '\n'.join(path_lib)
        print (f"path_lib = {path_lib}")
        path_empty_lib = '\n'.join(path_empty_lib)
        print (f"path_empty_lib = {path_empty_lib}")

        # Timer end
        end_time = datetime.now()
        print (f"END_TIME = {end_time}")
        total_time=int(end_time.timestamp ()-start_time.timestamp())
        print (f"TOTAL_TIME = {total_time} sec")
        
        # Отправка сообщения на почту
        print("\n Send messages...")
        def send_email(from_addr, to_addr, subject, text, encode='utf-8'):
    
            # Параметры подключения к почтовому серверу
            server "mail.inside.nothing.ru"
            port = 25
            charset= f'Content-Type: text/plain; charset={encode}'

            # формируем тело письма
            body = "\r\n".join((f"From: {from_addr}", f"To: {to_addr}", f"Subject: {subject}", charset, "", text))
            try:
                # подключаемся к почтовому сервису
                smtp = smtplib.SMTP (server, port)
                smtp.starttls()
                smtp.ehlo()
                # пробуем послать письмо
                smtp.sendmail (from_addr, to_addr, body.encode(encode))
            except smtplib.SMTPException as err: 
                print('что - то пошло не так...')
                raise err
            finally:
                smtp.quit()
        # print(f"to_addr = {to_addr}")
        from_addr = "smtp@mail.ru"
        subject = f"Kонтроль выгрузки {pass_name} OT {end_time.strftime("%d.%m.%Y %H:%M')}" 
        subject_empty f"Контроль заполненности {pass_name} OT {end_time.strftime('%d.%m.%Y %H:%M')}"

        # Отправляем сообщение если есть наличие отсутсвия файлов
        if path_lib != '':
            text = f"Внимание! \n\nОтсутствуют актуальные данные {pass_name} по пути: \n{path_lib}" 
            send_email(from_addr, to_addr_nok, subject, text)
            
            # Отправка SMS циклом для каждого номера с паузой в 1 секунду из за ограничений OMNI CHANNEL 
            for number in NumberLib:
                headers = {'Content-type': 'application/json', # Определение типа данныX
                            'Accept': 'text/plain',
                            'Content-Encoding': 'utf-8',
                            'Authorization': f"Basic (base64_userpass}"}
                data = {"messages":[{"content":{"short_text": f" {text}"},
                                    "to":[{"msisdn": f" {number}"}],
                                    "from":{"sms_address":"SMS"}
                                    }]
                        }
                    # print(f"SMS text: {data}")
                    answer = requests.post(url, data=json.dumps (data), headers-headers, verify=False) 
                    print(answer)
                    response = answer.json()
                    print(response)
                    time.sleep(1)
        else:
            text = f"Bcë ok"
            send_email(from_addr, to_addr_ok, subject, text)

        # Отправляем сообщение если есть наличие отсутсвия данных в файлах
        if path_empty_lib != "':
            text = f"Внимание! \n\nОтсутствунот данные в файлax {pass_name} по пути: \n{path_empty_lib}" 
            send_email(from_addr, to_addr_nok, subject_empty, text)
            
            # Отправка SMS циклом для каждого номера с паузой в 1 секунду из за ограничений OMNI CHANNEL 
            for number in NumberLib:
                headers = {'Content-type': 'application/json', # Определение типа данныX
                            'Accept': 'text/plain',
                            'Content-Encoding': 'utf-8',
                            'Authorization': f"Basic (base64_userpass}"}
                data = {"messages":[{"content":{"short_text": f" {text}"},
                                    "to":[{"msisdn": f" {number}"}],
                                    "from":{"sms_address":"SMS"}
                                    }]
                        }
                    # print(f"SMS text: {data}")
                    answer = requests.post(url, data=json.dumps (data), headers-headers, verify=False) 
                    print(answer)
                    response = answer.json()
                    print(response)
                    time.sleep(1)
        else:
            text = f"Bcë ok"
            send_email(from_addr, to_addr_ok, subject, text)
        print(f" \n Send messages DONE")

with DAG(
        'files_check',
        default_args={
            'depends_on_past': False,
            'email': ['defaulthuman01@gmail.com'],
            'email_on_failure': True,
            'email_on_retry': False
        },
        description='Проверка наличия выгруженных и заполненных файлов 2-3 проходов,
        # schedule_interval-timedelta (days=1),
        # Ежедневное выполение, время по UTC
        schedule='0 5 * * *'
        start_date=datetime (2023, 1, 1), catchup=False,
        tags=['file_check'],
) as dag:

    # Начальный таск
    start_task = BashOperator(
        task_id='start_task',
        bash_command='date',
    )

    # Конечный таск
    end_task= BashOperator(
        task_id='end_task',
        bash_command='date',
    )

    file_check_task= PythonOperator(
        task_id=f'file_check_task',
        python_callable=check_proc,
        pool='chdb1',
        dag=dag
    )
    # врубаем машину
    start_task >> file_check_task >> end_task