from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from airflow import DAG
import pandas as pd
import pysftp
import shutil
import csv
import os
from airflow.models import Variable

from airflow.operators.bash import BashOperator

settings = {'socket_timeout': 30000000, 'connect_timeout': 260000, 'receive_timeout': 260000, 'send_timeout': 260000, 'max_bytes_before_external_group_by': 85899345920, 'send_receive_timeout': 30000}

servers = {'qwerty.ru': 'asdfg',}
  
def daily_stat(**kwargs):
    server = kwargs['server']   
    CH_user = Variable.get("CH_server_login")
    CH_pass = Variable.get("CH_server_password")
    client = Client(host=server, port=9000, user=CH_user, password=CH_pass, send_receive_timeout = 30000, settings=settings)
    
    sql = f"select * from rlc.view"
    sql= '/*Airflow*/' +sql

    """Процедура выгрузки данные статистики в csv файл"""
    print(kwargs)
    # Путь для файла
    pass_path = '/ftp-output/daily_statistics'
    server = kwargs['server']

    # Получаем дату, за которую формируем сверку из справочника для дальнейшего использования в имени файла
    dt = datetime.now()
    current_date = dt.strftime('%Y-%m-%d')
    # Формируем имя файла и путь
    file_name = f"pivot-statistics-{current_date}.csv"
    full_file_name = f"/tmp/{file_name}"
    print(full_file_name)
    # Формируем имя архивного файла
    zip_name = f"{file_name}.zip"
    full_zip_name = f"/tmp/{zip_name}"
    # Обращаемся к библиотеке dataframe, чтобы воспользоваться функцией to_csv
    df = client.query_dataframe(query=sql, settings=settings)
    print(df)

    # Делаем сводную таблицу
    pt = pd.pivot_table(df, index=["eventDate"], 
                        columns=["macroRegion", "cpName"], 
                        values=["firstStream","secondStream","firstDlt","secondDlt","dsc"
                                ,"2pass","2passShoulder1","2passShoulder2","2passCp12Grp"
                                ,"2passCp12","2passCp08grp","2passCp08detalka"
                                ,"2passCp10DateCharge","2passCp10DateChargeOther"
                                ,"2passCp26GrpNaming","2passCp26GrpOrigToNumber","2passCp28GrpSystemId"
                                ,"2passCp29OriginalShoulder1","2passCp29OriginalShoulder2"
                                ,"2passCp29GrpOrig","2passCp33","2passCp33report"
                                ,"2passCp36","2passCp36ErrReportGroup"
                                ,"2passCp47","2passCp47dsc","2passCp47GrpNaming","2passCp47GrpSystemId"
                                ,"3pass","3passCp10SMS"],
                        aggfunc="sum", 
                        fill_value=0, 
                        margins=False)  
    # Сортируем даты в обратном порядке 
    pt = pt.sort_index(ascending=False)
    # Транспонируем то что получилось
    pt = pt.transpose()

    # Превращаем pivot_table обратно в dataframe
    print("Превращаем pivot_table обратно в dataframe")
    df2 = pt.reset_index()

    # Переименовываем столбец у которого раньше не было имени
    df2.rename(columns = {'level_0':'cpType'}, inplace = True )

    # Переносим столбцы региона и сверки вперёд
    print("пробуем перенести столбцы")
    cols_to_move = ['macroRegion', 'cpName']
    df2 = df2[cols_to_move + [x for x in df2.columns if x not in cols_to_move]]

    # Добавляем столбец для сортировки
    df2.insert(loc=2,column='cpType_copy',value=df2['cpType'])

    df2['cpType_copy']= df2['cpType_copy'].replace(["firstStream","secondStream","firstDlt","secondDlt","dsc"
                                                ,"2pass","2passShoulder1","2passShoulder2","2passCp12Grp"
                                                ,"2passCp12","2passCp08grp","2passCp08detalka"
                                                ,"2passCp10DateCharge","2passCp10DateChargeOther"
                                                ,"2passCp26GrpNaming","2passCp26GrpOrigToNumber","2passCp28GrpSystemId"
                                                ,"2passCp29OriginalShoulder1","2passCp29OriginalShoulder2"
                                                ,"2passCp29GrpOrig","2passCp33","2passCp33report"
                                                ,"2passCp36","2passCp36ErrReportGroup"
                                                ,"2passCp47","2passCp47dsc","2passCp47GrpNaming","2passCp47GrpSystemId"
                                                ,"3pass","3passCp10SMS"],
                                                ['01','02','03','04','05','06','07','08','09','10'
                                                  ,'11','12','13','14','15','16','17','18','19','20'
                                                  ,'21','22','23','24','25','26','27','28','29','30'])
    
    # Сортируем столбцы, чтобы красиво было
    df2 = df2.sort_values(by=['macroRegion','cpName','cpType_copy'])

    # Добавляем столбец с суммой всех строк, чтобы убрать его если там ноль
    df2['row_sum'] = df2.sum (axis=1)
    # Фильтруем строки с нулями
    df2 = df2[df2.row_sum != 0]

    # Удаляем технические столбцы
    df2.drop(['row_sum', 'cpType_copy'], axis= 1 , inplace= True )

     # Выгружаем dataframe в csv
    df2.to_csv(full_file_name, encoding='cp1251', index=False, header=True, sep=';', quoting=csv.QUOTE_ALL, date_format='%Y-%m-%d %H:%M:%S')
    # Выгружаем dataframe в zip
    df2.to_csv(full_zip_name, encoding='cp1251', index=False, header=True, sep=';', quoting=csv.QUOTE_ALL, date_format='%Y-%m-%d %H:%M:%S', compression='zip')
    # Перемещаемся в директорию pass_path
    os.chdir(pass_path)
    # Загружаем файл
    shutil.copy(full_file_name, f'{pass_path}/{file_name}')
    # Загружаем архив с новым именем
    shutil.copy(full_zip_name, f'{pass_path}/pivot-statistics-{current_date}.zip')
    # Цикл удаления старых файлов
    i=1
    while i<6:
        # Формируем имя файла для удаления
        dtd = datetime.now()-timedelta(days=i)
        del_date = dtd.strftime("%Y-%m-%d")
        file_name_del = f"pivot-statistics-{del_date}.csv"
        # Удаляем старый файл
        try:
            print(f'removing file {pass_path}/{file_name_del}')
            os.remove(f'{pass_path}/{file_name_del}')
        except:
            print(f'WARNING: unable to remove file {pass_path}/{file_name_del}')
        i+=1

with DAG(
    'daily-statistics-pivot',
    default_args={
        'depends_on_past': False,
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False
    },
    description='Тестовая выгрузка перевернутой статистики включая 2-3 проходы',
    schedule='40 5 * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['daily_stat', 'test'],
) as dag:
    # Начальный таск
    start_task = BashOperator(
        task_id='start_task',
        bash_command='date',
    )

    # Конечный таск
    end_task = BashOperator(
        task_id='end_task',
        bash_command='date',
    )

    for server in servers:
        print(servers[server])
        daily_stat_task = PythonOperator(task_id=f'pivot_daily_stat-{servers[server]}', python_callable=daily_stat, op_kwargs={'server': server})
        
        start_task >> daily_stat_task >> end_task