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
    sales_and_coeffs_df = dict_of_dataframes['sales_and_coeffs_df']
    hierarchy_df = dict_of_dataframes['hierarchy_df']
    write_date_df = dict_of_dataframes['write_date_df']

    # изменение типа данных
    sales_and_coeffs_df['current_price_date'] = pd.to_datetime(sales_and_coeffs_df['current_price_date'])
    sales_and_coeffs_df['last_price_date'] = pd.to_datetime(sales_and_coeffs_df['last_price_date'])
    sales_and_coeffs_df['sale_date'] = pd.to_datetime(sales_and_coeffs_df['sale_date'])
    sales_and_coeffs_df['product_code'] = sales_and_coeffs_df['product_code'].astype('str')
    write_date_df['write_date'] = pd.to_datetime(write_date_df['write_date'])

    sales_and_coeffs_df.rename(columns={'sale_date': 'date'}, inplace=True)

    # загрузка измененных датафреймов в словарь
    dict_of_dataframes['sales_and_coeffs_df'] = sales_and_coeffs_df
    dict_of_dataframes['hierarchy_df'] = hierarchy_df
    dict_of_dataframes['write_date_df'] = write_date_df

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
