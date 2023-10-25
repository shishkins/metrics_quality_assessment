from get_dataframes import get_data
from datetime import date, datetime
import pandas as pd

dict_of_dataframes = get_data()


class data_lake():
    '''
    Класс объекта модели данных, принцип работы:
    __init__ инициализирует модель данных через словарь dict_of_dataframes, в котором содержатся все датафреймы через доступ по ключу
    Затем пользователем задаются аттрибуты фильтров
    После этого метод "filtered_df" выдает отфильтрованный датафрейм на работу
    '''

    def __init__(self, dict_of_dataframes):
        '''
        Инициализация объекта с данными
        :param kwargs:
        '''

        ''' Организация главного датафрейма, настраивается пользователем'''
        self.__dict__.update(dict_of_dataframes)
        self.main_df = self.sales_and_coeffs_df.merge(self.calendar_df, on='date', how='left')
        self.main_df = self.main_df.merge(self.hierarchy_df, on='category_id', how='left')
        self.main_df = self.main_df.merge(self.write_date_df, how='cross')

        ''' Фильтры для алгоритмов '''
        self.picked_data = pd.DataFrame(
            {
                'start_date': [self.main_df['date'].min()],
                'end_date': [self.main_df['date'].max()]
            })

    def filters_date(self, start_date=None, end_date=None):
        '''
        Метод, который обновляет выбранную пользователем дату
        :param start_date:
        :param end_date:
        :return:
        '''
        limits = pd.DataFrame({'start_date': [start_date],
                               'end_date': [end_date]})
        limits['start_date'] = pd.to_datetime(limits['start_date'], format='ISO8601')
        limits['end_date'] = pd.to_datetime(limits['end_date'], format='ISO8601')
        self.picked_data = limits


    def filtered_df(self, df):
        '''
        Метод, возвращающий отфильтрованный датафрейм в соответствием с тем, что выбрал пользователь
        :param df:
        :return:
        '''
        severed_df = df.loc[(df['date'] >= self.picked_data['start_date'].iloc[0]) &
                            (df['date'] <= self.picked_data['end_date'].iloc[0])]
        severed_df = severed_df.merge(self.picked_algorithms, on='type_name')
        return severed_df



sales = get_data()['sales_and_coeffs_df']

print(sales['product_code'])