import pandas as pd
import os


def get_data():
    os.chdir('csv')  # заходим в папку csv
    if not 'pricing_types.csv' in os.listdir():  # проверка наличия файлов в папке csv
        os.chdir('..')  # выходим из папки cs
        print(
            'Ошибка: отсутствуют файлы в "./csv", попробуйте изменить параметр rewrite = True в обращении к функции csv_execute()')
        return False

    pricing_types_df = pd.read_csv('pricing_types.csv')
    products_reference_df = pd.read_csv('products_reference.csv')
    reprices_errors_log_df = pd.read_csv('reprices_errors_log.csv')
    reprices_log_df = pd.read_csv('reprices_log.csv')
    write_date_df = pd.read_csv('write_date.csv')
    os.chdir('..')  # выходим из папки csv

    # преобразуем типы данных
    reprices_log_df['date_reprice'] = pd.to_datetime(reprices_log_df['date_reprice'])
    reprices_errors_log_df['date_error'] = pd.to_datetime(reprices_errors_log_df['date_error'])

    reprices_log_df.rename(columns={'date_reprice': 'date'}, inplace=True)
    reprices_errors_log_df.rename(columns={'date_error': 'date'}, inplace=True)

    date_range_ser = pd.date_range(start=min(reprices_log_df['date'] - pd.DateOffset(months=1)),
                                   end=max(reprices_log_df['date'] + pd.DateOffset(months=1)))
    calendar_df = pd.DataFrame({'date': date_range_ser,
                                'week_day': date_range_ser.weekday})

    #                            )
    # for elem in reprices_log_df['date_reprice']:
    #     print(type(elem))
    return pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df, write_date_df
