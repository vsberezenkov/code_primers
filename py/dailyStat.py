from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator from clickhouse_driver import Client
from airflow import DAG
import pysftp
import shutil
import csv
import os
from airflow.models import Variable
from airflow.operators.bash import BashOperator

settings={'socket_timeout': 30000000, 'connect_timeout': 260000, 'receive_timeout': 260000, 'send_timeout': 260000, 'max_bytes_before_external_group_by': 85899345920, 'send_receive_timeout': 30000}
servers ={'chdb1.db.nothing.ru': 'chdb1',}

def daily_stat(**kwargs):
    server = kwargs['server']
    CH_user = Variable.get("CH_server_login")
    CH_pass = Variable.get("CH_server_password")
    client = Client (host=server, port=9000, user=CH_user, password=CH_pass, send_receive_timeout = 30000, settings=settings)

def daily_stat (**kwargs):
    server = kwargs['server']
    CH_user = Variable.get("CH_server_login")
    CH_pass = Variable.get("CH_server_password")
    client = Client (host=server, port=9000, user=CH_user, password=CH_pass, send_receive_timeout = 30000, settings=settings)
    sql = f"SELECT * FROM db.stat order by chName asc, multiIf(macroRegion in ('Msk'), '0', macroRegion in ('Cnt'), '1', macroRegion in ('Nwe'), '2', macroRegion in ('Vlg'), '3', macroRegion in ('Url'), '4', macroRegion in ('Sth'), '5', macroRegion in ('Sib') ,'6', macroRegion in ('Fea'), '7','') asc, eventDate desc"
    sql='/*Airflow*/* +sql

    """Процедура выгрузки данных в csv файл"""
    print (kwargs)
    # путь для файла
    pass_path='/ftp-output/daily_statistics'
    pass_path_old = 'F:\\anything\\nothing\\statistics'

    # Получаем дату, за которую формируем сверку из справочника для дальнейшего использования в имени файла dt = datetime.now()
    current_date = dt.strftime('%Y-%m-%d')
    # Формируем имя файла и путь из дефолтного места развертки Airflow
    file_name = f"statistics-{current_date}.csv"
    full_file_name = f"/tmp/{file_name}"
    print (full_file_name)
    # Формируем имя архивного файла
    zip_name = f"{file_name}.zip"
    full_zip_name = f"/tmp/{zip_name}"
    # Обращаемся к библиотеке dataframe, чтобы воспользоваться функцией to_csv
    df = client.query_dataframe (query=sql, settings=settings)
    # выгружаем dataframe в csv
    df.to_csv(full_file_name, encoding='cp1251', index=False, header=True, sep=';', quoting-csv.QUOTE_ALL, date_format='%Y-%m-%d %H:%M:%S')
    # выгружаем dataframe в zip
    df.to_csv(full_zip_name, encoding='cp1251', index=False, header=True, sep=';', quoting-csv.QUOTE_ALL, date_format='%Y-%m-%d %H:%M:%S', compression='zip') 
    # Перемещаемся в директорию pass_path
    os.chdir(pass_path)
    # Загружаем файл
    shutil.copy(full_file_name, f'{pass_path}/{file_name}')
    # Загружаем архив с новым именем
    shutil.copy(full_zip_name, f'{pass_path}/statistics-{current_date}.zip')

    # Цикл удаления старых файлов
    i=1
    while i<6:
        # формируем имя файла для удаления
        dtd = datetime.now()-timedelta (days=i)
        del_date= dtd.strftime("%Y-%m-%d")
        file_name_del = f"statistics - {del_date}.csv"
        # удаляем старый файл
        try:
            print(f"removing file {pass_path}/{file_name_del}') os.remove(f' {pass_path}/{file_name_del}")
        except:
            i+=1
            print(f'WARNING: unable to remove file {pass_path}/{file_name_del}')
    
    # Выгрузка в FTP
    print("@conn..")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    # Имя сервера
    ftp_hostname = "111.111.111.111"
    # Логин/пароль из Variables Airflow
    ftp_user_name = Variable.get("ftp-user")
    ftp_password = Variable.get("ftp-password")
    print("conn..")
    # Подключаемся к FTP
    with pysftp.Connection (host-ftp_hostname, username=ftp_user_name, password=ftp_password, cnopts=cnopts) as sftp: 
        print("Connection successfully established ... ")
        sftp.chdir(pass_path_old)
        files = sftp.listdir()
        print(files)
        # Загружаем файл
        sftp.put(localpath-full_file_name, remotepath=f'{file_name}') 
        # Загружаем архив
        sftp.put(localpath-full_zip_name, remotepath=f'{zip_name}')
        # Переименовываем архив в нормальное имя
        try:
            if(sftp.exists(zip_name)):
                sftp.rename(zip_name, f"statistics-{current_date}.zip")
        except:
            print(f'unable to rename file {zip_name}')
    # Удаляем старый файл
    print (f' removing file {file_name_del}')
    try:
        if(sftp.exists(file_name_del)):
            sftp.remove(file_name_del)
        except:
            print (f'unable to remove file {file_name_del}')

    with DAG(
    'daily-statistics',
    default_args={    
        'depends_on_past': False,
        'email': ['human@mail.ru'],
        'email_on_failure': False,
        'email_on_retry': False
    },
    description='Ежедневная выгрузка статистики',
    schedule='30 5 * * *',
    start_date=datetime (2022, 1, 1),
    catchup=False,
    tags=['daily_stat'],
    ) as dag:

    # Начальный таск
    start_task= BashOperator(
        task_id='start_task',
        bash_command='date',
    )

    # Конечный таск
    end_task= BashOperator(
        task_id='end_task',
        bash_command='date',
    )

    for server in servers:
        print(servers [server])
        daily_stat_task = PythonOperator(task_id=f'daily_stat-{servers [server]}', python_callable-daily_stat, op_kwargs={'server': server})
        start_task >> daily_stat_task >> end_task