import pandas as pd
import os
from datetime import date


def get_data():
    os.chdir('csv')  # заходим в папку csv
    if not os.listdir():  # проверка наличия файлов в папке csv
        print(
            'Ошибка: отсутствуют файлы в "./csv", попробуйте изменить параметр rewrite = True в обращении к функции csv_execute()')
        return False
    dict_of_dataframes = {}
    ''' Создание словаря датафреймов '''
    for dataframe in os.listdir():
        dict_of_dataframes[dataframe[0:-4] + '_df'] = pd.read_csv(dataframe)
    os.chdir('..')  # выходим из папки csv


    ''' Преобразование типов данных, настраивается пользователем '''
    reprices_log_df = dict_of_dataframes['reprices_log_df']
    reprices_errors_log_df = dict_of_dataframes['reprices_errors_log_df']

    # изменение типа данных
    reprices_log_df['date_reprice'] = pd.to_datetime(reprices_log_df['date_reprice'])
    reprices_errors_log_df['date_error'] = pd.to_datetime(reprices_errors_log_df['date_error'])

    reprices_log_df.rename(columns={'date_reprice': 'date'}, inplace=True)
    reprices_errors_log_df.rename(columns={'date_error': 'date'}, inplace=True)

    # загрузка измененного датафрейма в словарь
    dict_of_dataframes['reprices_log_df'] = reprices_log_df
    dict_of_dataframes['reprices_errors_log_df'] = reprices_errors_log_df

    ''' Создание календаря '''
    date_range_ser = pd.date_range(start=date(2023,1,1),
                                   end=date(2024,1,1))
    calendar_df = pd.DataFrame({'date': date_range_ser,
                                'week_day': date_range_ser.weekday})
    dict_of_dataframes['calendar_df'] = calendar_df

    #                            )
    # for elem in reprices_log_df['date_reprice']:
    #     print(type(elem))
    return dict_of_dataframes

data = get_data()