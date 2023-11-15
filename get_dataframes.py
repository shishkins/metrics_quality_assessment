import pandas as pd
import os
from datetime import date
import numpy as np


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

    # расчет некоторых величин
    koeffs_df = sales_and_coeffs_df[['koef_change_sale','koef_change_revenue','koef_change_profit', 'current_price_date', 'product_code']].drop_duplicates()

    indicators_for_coeffs = pd.DataFrame(
        {
            'koef_change_sale_nulls': [
                koeffs_df.loc[
                    koeffs_df['koef_change_sale'] == 0,
                    'koef_change_sale'
                ].count()
            ],
            'koef_change_revenue_nulls': [
                koeffs_df.loc[
                    koeffs_df['koef_change_revenue'] == 0,
                    'koef_change_revenue'
                ].count()
            ],
            'koef_change_profit_nulls': [
                koeffs_df.loc[
                    koeffs_df['koef_change_profit'] == 0,
                    'koef_change_profit'
                ].count()
            ],
            'koef_change_sale_nulls_one': [
                koeffs_df.loc[
                    (koeffs_df['koef_change_sale'] == 0)
                    & (koeffs_df['koef_change_profit'] != 0)
                    & (koeffs_df['koef_change_revenue'] != 0),
                    'koef_change_sale'
                ].count()
            ],
            'koef_change_revenue_nulls_one': [
                koeffs_df.loc[
                    (koeffs_df['koef_change_revenue'] == 0)
                    & (koeffs_df['koef_change_profit'] != 0)
                    & (koeffs_df['koef_change_sale'] != 0),
                    'koef_change_revenue'
                ].count()
            ],
            'koef_change_profit_nulls_one': [
                koeffs_df.loc[
                    (koeffs_df['koef_change_profit'] == 0)
                    & (koeffs_df['koef_change_sale'] != 0)
                    & (koeffs_df['koef_change_revenue'] != 0),
                    'koef_change_profit'
                ].count()
            ],
            'all_reprices_count': [
                koeffs_df.shape[0]
            ]
        }
    )


    # Создаем pd.cut объект с интервалами
    params_of_interval = {
        'min': -80,
        'max': 128,
        'step': 100,
        'over_min':-np.inf,
        'over_max':np.inf
    }
    # создание интервалов
    array_of_intervals = np.linspace(start=params_of_interval['min'], stop=params_of_interval['max'], num=params_of_interval['step']) # ручное задание интервалов
    array_of_intervals = np.concatenate((np.array([params_of_interval['over_min']]), array_of_intervals, np.array([params_of_interval['over_max']])), axis=0)  # с обеих сторон прицепляем "куски" больших интервалов
    array_of_intervals = np.round(array_of_intervals, 2)
    array_up = array_of_intervals[0:-1]  #создание массива верхних значений интервалов
    array_down = array_of_intervals[1::]  # создание массива нижних значений интервалов
    bins = pd.IntervalIndex.from_arrays(array_up, array_down)  #фокус-покус, два массива превратились в IntervalIndex

    # считает количество вхождений каждого коэффициента в каждый бин, созданный выше
    sales = pd.cut(x=koeffs_df.loc[koeffs_df['koef_change_sale'] != 0, 'koef_change_sale'], bins=bins)
    revenue = pd.cut(x=koeffs_df.loc[koeffs_df['koef_change_revenue'] != 0, 'koef_change_revenue'], bins=bins)
    profit = pd.cut(x=koeffs_df.loc[koeffs_df['koef_change_profit'] != 0, 'koef_change_profit'], bins=bins)
    sales_counts = sales.value_counts()
    revenue_counts = revenue.value_counts()
    profit_counts = profit.value_counts()

    # собираем все в большого трансформера
    koeff_counts = pd.DataFrame(
        {
            'sales': sales_counts,
            'revenue': revenue_counts,
            'profit': profit_counts,
        },
        index=sales_counts.index
    )
    # сортируем значения по интервалам
    koeff_counts = koeff_counts.iloc[koeff_counts.index.codes.argsort()]
    koeff_counts['for_vizual'] = koeff_counts.index.astype('str')
    # передаем дальше

    sales_and_coeffs_df.rename(columns={'sale_date': 'date'}, inplace=True)

    # загрузка измененных датафреймов в словарь
    dict_of_dataframes['sales_and_coeffs_df'] = sales_and_coeffs_df
    dict_of_dataframes['hierarchy_df'] = hierarchy_df
    dict_of_dataframes['write_date_df'] = write_date_df
    dict_of_dataframes['koeff_counts'] = koeff_counts
    dict_of_dataframes['koeffs_df'] = koeffs_df
    dict_of_dataframes['indicators_for_coeffs'] = indicators_for_coeffs


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
