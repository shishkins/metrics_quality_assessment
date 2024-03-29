import os
import dns_db_resources as db
import pandas as pd
import time
from calculate_t_test import calculate_t_test_with_mean, t_test_for_reprice
from calculate_rolling_metrics import calculate_mean, calculate_k_vars
from get_dataframes import get_data


# Флаг rewrite должен быть True в следующих случаях:
# - первый запуск дашборда;
# - обновление данных;
# - csv файлы пропали
rewrite = True

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
    # ch_conn = get_fspb_ch_conn()

    if not os.path.isdir("csv"):
        os.mkdir("csv") # создание папки с csv файлами, если такой не имеется
        os.chdir('csv')
    else:
        os.chdir('csv')  # изменяем текущую директорую на папку с csv
        for file in os.listdir():
            os.remove(file) # удаляем все файлы из директории csv


    # словарь dict_of_queries содержит в себе следующие пары ключ - значения:
    # 'название предполагаемого датафрейма': ''' запрос '''
    # в самих запросах можно настраивать период, за который необходимы данные (переменная period, how_long)
    dict_of_queries = {
        'sales_and_coeffs': '''
            WITH coeff_table AS (
                SELECT 
                    cosc.product AS product_id,
                    cosc.current_price_date,
                    cosc.last_price_date,
                    cosc.periodic,
                    cosc.koef_change_sale,
                    cosc.koef_change_revenue,
                    cosc.koef_change_profit,
                    cosc.koef_var_sale,
                    cosc.koef_var_revenue,
                    cosc.koef_var_profit,
                    cosc.current_price,
                    cosc.last_price,
                    cosc.delta_price
                FROM 
                    metrics_orp.coefficient_of_sales_change AS cosc
            ), count_90 AS (
                SELECT
                    sale."Номенклатура" AS product_id,
                    date(date_trunc('day', sale."Period")) AS sale_date,
                    coeff_table.current_price_date,
                    coeff_table.last_price_date,
                    coeff_table.periodic,
                    coeff_table.koef_change_sale,
                    coeff_table.koef_change_revenue,
                    coeff_table.koef_change_profit,
                    coeff_table.koef_var_sale,
                    coeff_table.koef_var_revenue,
                    coeff_table.koef_var_profit,
                    coeff_table.current_price,
                    coeff_table.last_price,
                    coeff_table.delta_price,
                    CASE 
                        WHEN korp."ВидКонтрагента" = 'Организация' OR korp."ВидКонтрагента" = 'ЧастноеЛицо'
                            THEN 1
                        ELSE sale."Количество"
                    END AS count_sale,
                    CASE 
                        WHEN korp."ВидКонтрагента" = 'Организация' OR korp."ВидКонтрагента" = 'ЧастноеЛицо'
                            THEN sale."Продажа" / sale."Количество"
                        ELSE sale."Продажа"
                    END AS sum_sale,
                    CASE  
                        WHEN korp."ВидКонтрагента" = 'Организация' OR korp."ВидКонтрагента" = 'ЧастноеЛицо'
                            THEN (sale."Продажа" - sale."СуммаНДС" + sale."Скидка"/1.2) / sale."Количество"
                        ELSE sale."Продажа" - sale."СуммаНДС" + sale."Скидка"/1.2
                    END AS sum_sale_clean,
                    CASE 
                        WHEN korp."ВидКонтрагента" = 'Организация' OR korp."ВидКонтрагента" = 'ЧастноеЛицо'
                            THEN sale."СебестоимостьБезНДС" / sale."Количество"
                        ELSE sale."СебестоимостьБезНДС"
                    END AS cost_price_clean
                FROM
                    "AccumulationRegistersManager"."Счет_90" AS sale
                LEFT JOIN "CatalogManager"."Контрагенты" AS korp
                    ON sale."Контрагент" = korp."Ref"
                INNER JOIN coeff_table
                    ON coeff_table.product_id = sale."Номенклатура"
                    AND sale."Period" BETWEEN coeff_table.current_price_date - coeff_table.periodic*INTERVAL '1 day' -- извлекаем только те данные, которые нужны для визуализации
                                        AND coeff_table.current_price_date + coeff_table.periodic*INTERVAL '1 day' + INTERVAL '1 day'
                WHERE
                    sale."Period" BETWEEN '2023-01-01' AND current_date
                    AND sale."Количество" > 0  
                    AND sale."Контрагент" != '9b983a73-59e3-11eb-a20f-00155df1b805'
                    AND sale."Recorder_type" = 'DocumentRef.РасходнаяНакладная'
            ), full_sale AS (
                SELECT
                    sale.product_id,
                    sale.sale_date, 
                    sale.current_price_date,
                    sale.last_price_date,
                    sale.periodic,
                    sale.koef_change_sale,
                    sale.koef_change_revenue,
                    sale.koef_change_profit,
                    sale.koef_var_sale,
                    sale.koef_var_revenue,
                    sale.koef_var_profit,
                    sum(sale.count_sale) AS count_sale,
                    sum(sale.sum_sale) AS sum_sale,
                    sum(sale.sum_sale_clean) AS sum_sale_clean,
                    sum(sale.cost_price_clean) AS cost_price_clean,
                    sale.current_price,
                    sale.last_price,
                    sale.delta_price
                FROM
                    count_90 AS sale
                GROUP BY 
                    sale.product_id,
                    sale.sale_date,
                    sale.current_price_date,
                    sale.last_price_date,
                    sale.periodic,
                    sale.koef_change_sale,
                    sale.koef_change_revenue,
                    sale.koef_change_profit,
                    sale.koef_var_sale,
                    sale.koef_var_revenue,
                    sale.koef_var_profit,
                    sale.current_price,
                    sale.last_price,
                    sale.delta_price
            ), seasonality as (
                SELECT
                    full_sale.*,
                    prod."Категория" as category_id,
                    prod."Код" as product_code,
                    prod."Наименование",
                    COALESCE(full_sale.count_sale, 0.01) / COALESCE(seasonality."WeekSeasonalFactor", 1) :: float AS clean_count_sale, --очищаем продажи от недельной сезонности--
                    COALESCE(full_sale.sum_sale_clean, 0.01) / COALESCE(seasonality."WeekSeasonalFactor", 1) :: float AS fact_revenue_clean,
                    COALESCE((full_sale.sum_sale_clean - full_sale.cost_price_clean) , 0.01) / COALESCE(seasonality."WeekSeasonalFactor", 1) :: float AS fact_profit_clean
                FROM 
                    full_sale
                LEFT JOIN data_lake.dim_products AS prod
                    ON prod."Ссылка" = full_sale.product_id
                LEFT JOIN orp.weekly_seasonality_of_categories AS seasonality 
                    ON prod."Категория" = seasonality."CategoryID" 
                    AND date_part('week', full_sale.sale_date) = seasonality."WeekNumber"
            )
            SELECT * FROM seasonality
            ''',
        'hierarchy': '''
            SELECT 
                guid AS category_id,
                name_1 AS department_name,
                name_2,
                name_3,
                name_4,
                COALESCE(name_4,name_3,name_2,name_1) AS category_name
            FROM 
                mart_metric.category_dim_hierarchy
            ''',
        'algorithms_reprices': '''
            SELECT 
                date(pricing."date" + INTERVAL '1 day') AS current_price_date,
                pricing.product_id AS product_id,
                pt.type_name AS algorithm_name,
                pt.type_id AS algorithm_id
            FROM 
                pricing_api.pricing_final AS pricing
            LEFT JOIN 
                pricing_api.pricing_types AS pt 
                ON pt.type_id = pricing.algorithm 
        ''',
        'assortment_status_products': '''
            SELECT
                prod."Код" AS product_code,
                prod."Ссылка" AS product_id,
                CASE prod."Ассортиментный_Статус" 
                    WHEN 'Базовый' THEN 'A'
                    WHEN 'ИнтернетФедеральный' THEN 'C'
                    WHEN 'ИнтернетРегиональный' THEN 'D'
                    WHEN 'Дополнительный' THEN 'B'
                END AS class_product
            FROM 
                data_lake.dim_products AS prod
            WHERE prod."Ассортиментный_Статус" IN ('Базовый','ИнтернетФедеральный','ИнтернетРегиональный','Дополнительный')
        ''',
    }

    if rewrite:
        for name_df, query in dict_of_queries.items():
            print(f'Выполняется загрузка данных для запроса: "{name_df}"')
            start_time_query = time.time()
            to_save = pg_conn.execute_to_df(query)
            start_time_csv = time.time()
            to_save.to_csv(os.getcwd() + '\\{}.csv'.format(name_df), index_label=False)
            end_time_csv = time.time()
            print(f'Загрузка данных для запроса "{name_df}" завершена\nДлительность выполнения запроса: {round(start_time_csv-start_time_query,2)}\nДлительность записи csv: {round(end_time_csv-start_time_csv,2)}')
            print('-'*50)


        #датафрейм, содержащий дату последней записи данных
        write_date = pg_conn.execute_to_df(query='''SELECT NOW() as write_date''')
        write_date.to_csv(os.getcwd() + '\write_date.csv', index_label=False)
        print(f'Выполняется расчёт Т-теста для данных')

        os.chdir('..') # возврат в предыдущую директорию
        start_time_t_test = time.time()
        t_test = calculate_t_test_with_mean(get_data()['sales_and_coeffs_df'])
        start_time_k_metrics = time.time()
        print(f'Т-тест посчитан, рассчитываются метрики со скользящим средним, время выполнения T-теста: {round(start_time_k_metrics-start_time_t_test,2)}')
        k_metrics = calculate_mean(get_data()['sales_and_coeffs_df'])
        end_time_k_metrics = time.time()
        print(f'Метрики со скользящим средним посчитаны, время выполнения: {round(end_time_k_metrics-start_time_k_metrics,2)}')
        os.chdir('csv')

        t_test.to_csv(os.getcwd() + '\\t_test_results.csv', index_label=False)
        end_time_t_test_save = time.time()
        print(f'Результаты Т-теста сохранены, время сохранения: {round(end_time_t_test_save-end_time_k_metrics,2)}')
        k_metrics.to_csv(os.getcwd() + '\\k_metrics_new.csv', index_label=False)
        end_time_k_metrics_save = time.time()
        print(f'Результаты метрик со скользящим средним сохранены, время сохранения: {round(end_time_k_metrics_save - end_time_t_test_save,2)}')
        os.chdir('..')

if __name__ == '__main__':
    csv_execute(rewrite = rewrite)