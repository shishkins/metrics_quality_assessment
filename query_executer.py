import os
import dns_db_resources as db
import pandas as pd


def get_fspb_pg_conn() -> db.PostgreSQL:
    connection = db.PostgreSQL(host='adm-fspb-pgrepl.dns-shop.ru',
                               db='dep_spb',
                               login=os.environ['PG_FCS_LOGIN'],
                               password=os.environ['PG_FCS_PASSWORD'])
    return connection


def get_fspb_ch_conn() -> db.ClickHouse:
    connection = db.ClickHouse(host='adm-dv-ch.dns-shop.ru',
                               db='dns_log',
                               login=os.environ['CH_COM_LOGIN'],
                               password=os.environ['CH_COM_PASSWORD'])
    return connection


def csv_execute(period='month', how_long='3', rewrite=True):
    '''
    Функция извлечения "свежих" данных для записи в CSV
    :param period: какие периоды необходимы, значения 'year', 'month', 'week', 'day' STR
    :param how_long: сколько периодов необходимо INT
    :param rewrite: флаг перезаписи, по умолчанию TRUE (если FALSE, перезаписи нет)
    :return:
    '''

    try:
        os.environ['PG_FCS_LOGIN']
    except KeyError:
        print(
            'Отсутствует файл аутентификации (логин, пароль).\nСоздайте переменную окружения, указав имя файла с логином паролем подключения к базе:\nimport dns_db_resources as db\ndb.create_env("login_password.txt")\n\nФайл: get_env_auth.py')
        return None

    pg_conn = get_fspb_pg_conn()
    ch_conn = get_fspb_ch_conn()

    if not os.path.isdir("csv"):
        os.mkdir("csv") # создание папки с csv файлами, если таковой не имеется
    os.chdir('csv')  # изменяем текущую директорую на папку с csv

    # словарь dict_of_queries содержит в себе следующие пары ключ - значения:
    # 'название предполагаемого датафрейма': ''' запрос '''
    # в самих запросах можно настраивать период, за который необходимы данные (переменная period, how_long)
    dict_of_queries = {
        'reprices_log': '''
            SELECT
                pf.product_id::varchar as product_id,
                pf.price as price,
                DATE(pf.date) as date_reprice,
                pf.algorithm::int as algorithm
            FROM 
                pricing_api.pricing_final as pf
            ''',
        'reprices_errors_log': '''
            SELECT 
                DATE(pe.date) as date_error,
                pe.product_id::varchar as product_id,
                pe.algorithm::int as algorithm,
                pe.fc_orp as price,
                pe.rmin,
                pe.rmax,
                pe.type_error
            FROM 
                pricing_api.pricing_errors AS pe
            ''',
        'pricing_types': '''
            SELECT
                 pt.type_id::int as algorithm,
                 pt.type_name::varchar as type_name
            FROM 
                pricing_api.pricing_types as pt
            ''',
        'products_reference': '''
            WITH group_pricing AS (
                SELECT 
                    pf.product_id AS product_id
                FROM 
                    pricing_api.pricing_final AS pf
                UNION ALL 
                SELECT 
                    pe.product_id AS product_id
                FROM 
                    pricing_api.pricing_errors AS pe
            )
            SELECT 
                DISTINCT pr."Ссылка"::varchar AS product_id,
                pr."Наименование" AS product_name,
                pr."Код"::int AS product_code
            FROM data_lake.dim_products AS pr
            INNER JOIN group_pricing
                ON group_pricing.product_id = pr."Ссылка" 
            '''
    }
    if rewrite:
        for name_df, query in dict_of_queries.items():
            to_save = pg_conn.execute_to_df(query)
            to_save.to_csv(os.getcwd() + '\\{}.csv'.format(name_df), index_label=False)

        #датафрейм, содержащий дату последней записи данных
        write_date = pg_conn.execute_to_df(query='''SELECT NOW() as write_date''')
        write_date.to_csv(os.getcwd() + '\write_date.csv', index_label=False)

    os.chdir('..')  # возврат в предыдущую директорию

