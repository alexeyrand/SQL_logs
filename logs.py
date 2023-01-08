import pandas as pd
import numpy as nm
import sqlite3 as sq

# Считываение данных из таблиц Excel
df1 = pd.read_excel('exfile.xlsx', sheet_name = "TASK_1.SALES_STAGE_DICT", usecols = 'A:B')  # Таблица 1
df2 = pd.read_excel('exfile.xlsx', sheet_name = "TASK_1.SALES_STAGE_LOG", usecols = 'A:D')   # Таблица 2

# Таблица 1 "SALES_STAGE_DICT"
with sq.connect('logs.db') as db:
    cur = db.cursor()

    # Запрос на создание таблицы 1
    cur.execute("DROP TABlE IF EXISTS SALES_STAGE_DICT")
    cur.execute("""CREATE TABLE IF NOT EXISTS SALES_STAGE_DICT(
                    HIGH_SALES_STAGE TEXT,
                    DETAIL_SALES_STAGE TEXT)
                    """)

    dict_df = df1.to_dict(orient='records')
    content = []        # Список для хранения содержимого из таблицы 1
    for i in dict_df:
        val = list(i.values())
        content.append(tuple(val))

    # Заполнение таблицы 1
    cur.executemany("INSERT INTO SALES_STAGE_DICT VALUES(?, ?)", content)



# Таблица 2 "SALES_STAGE_LOG"
with sq.connect('logs.db') as db:
    cur = db.cursor()

    # Запрос на создание таблицы 2
    cur.execute("DROP TABlE IF EXISTS SALES_STAGE_LOG")
    cur.execute(""" CREATE TABLE IF NOT EXISTS SALES_STAGE_LOG(
                    ID INTEGER,
                    DATE TEXT,
                    NEW_VALUE TEXT,
                    OLD_VALUE TEXT,
                    DURATION INTEGER,
                    START_DATE TEXT,
                    END_DATE TEXT)
                    """)

    dict_df = df2.to_dict(orient='records')
    content = []             # Список для хранения содержимого из таблицы 2
    duration_list = []       # Список для хранения значения длительности этапов
    start_date = []
    end_date = []
    for i in range(len(dict_df) - 1):   # Рассчет длительности этапов, дат начала и конца этапов
        if list(dict_df[i].values())[0] == list(dict_df[i+1].values())[0] and list(dict_df[i].values())[2] != 'Disqualification':
            start_date.append(list(dict_df[i].values())[1])
            end_date.append(list(dict_df[i+1].values())[1])
            duration = list(dict_df[i+1].values())[1] - list(dict_df[i].values())[1]
            duration = duration.days
            duration_list.append(duration)
        else:   # Условие, при котором завершающему этапу по каждой компании присваивается длительность t = 0
            start_date.append(list(dict_df[i].values())[1])
            end_date.append(list(dict_df[i].values())[1])
            duration = 'Null'
            duration_list.append(duration)

    duration_list.append('Null')
    start_date.append(list(dict_df[-1].values())[1])
    end_date.append(list(dict_df[-1].values())[1])

    for i, line in enumerate(dict_df):
        val = list(line.values())
        val.append(duration_list[i])              # Добавление к таблице 2 столбцы с длительностью этапов,
        val.append(start_date[i])                 #                                                датой начала,
        val.append(end_date[i])                   #                                                датой конца
        val[0] = int(val[0][2::])                 # Для перевод id в INTEGER
        val[1] = val[1].strftime("%Y-%m-%d")      # Формирование даты для таблицы
        val[5] = val[5].strftime("%Y-%m-%d")
        val[6] = val[6].strftime("%Y-%m-%d")
        content.append(tuple(val))
    # Заполнение таблицы 2
    cur.executemany("INSERT INTO SALES_STAGE_LOG VALUES(?, ?, ?, ?, ?, ?, ?)", content)

# Функция оформления вывода таблицы
def showtable():
    out = cur.fetchall()
    headers = list(map(lambda x: x[0], cur.description))
    columns_width = max(max(len(str(word)) for row in out for word in row), max(len(str(word)) for word in headers))
    columns_number = len(out[0])
    format_string = ("| {: <" + str(columns_width) + "} |") * columns_number
    print(('+' + ('-' * (columns_width + 2)) + '+') * columns_number)
    print(format_string.format(*headers))
    print(('+' + ('-' * (columns_width + 2)) + '+') * columns_number)
    for result in out:
        print(format_string.format(*result))
        print(('+' + ('-' * (columns_width + 2)) + '+') * columns_number)

# Формирование запросов для вывода таблицы по условию задания
with sq.connect('logs.db') as db:
    cur = db.cursor()

    #Таблица 1 SALES_STAGE_DICT
    print("'SALES_STAGE_DICT' – справочник названий действий и бизнес-процессов")

    cur.execute("""
                SELECT * FROM SALES_STAGE_DICT
                """)
    showtable()

    #Таблица 2 SALES_STAGE_LOG
    print("\n\n'SALES_STAGE_LOG' – таблица с логами активностей в CRM")
    cur.execute("""
                SELECT id, start_date, end_date, duration, new_value, old_value FROM SALES_STAGE_LOG
                """)
    showtable()

    #Таблица 3 результат выполнения технического задания
    print("\n\nТаблица с непрерывной длительностью этапов по каждой из компаний")
    cur.execute("""
                SELECT id, DATE(min(start_date)) as 'START_PERIOD', DATE(max(end_date)) as 'END_PERIOD', sum(duration) as 'DURATION', SALES_STAGE_DICT.high_sales_stage FROM SALES_STAGE_LOG
                JOIN SALES_STAGE_DICT WHERE SALES_STAGE_LOG.new_value = SALES_STAGE_DICT.detail_sales_stage
                GROUP BY SALES_STAGE_DICT.high_sales_stage, id
                ORDER BY id, start_date
                """)
    showtable()
    print("Где поля:\nSTART_PERIOD - дата начала периода, в котором был выполнен этап\nEND_PERIOD - дата, в которую завершился этап\nDURATION - длительность выполнения каждого этапа")
