from get_dataframes import get_data
from datetime import date, datetime
import pandas as pd

pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df, write_date_df = get_data()
dict_of_dfs = {'pricing_types_df': pricing_types_df,
               'product_reference_df': products_reference_df,
               'reprices_errors_log_df': reprices_errors_log_df,
               'calendar_df': calendar_df}


class data_lake(object):
    '''
    Класс объекта модели данных, принцип работы:
    __init__ инициализирует модель данных через словарь позиционных аргументов **kwargs
    Затем пользователем задаются аттрибуты фильтров
    После этого метод "filtered_df" выдает отфильтрованный датафрейм на работу
    '''

    def __init__(self, **kwargs):
        '''
        Инициализация объекта с данными
        :param kwargs:
        '''
        self.__dict__.update(kwargs)
        self.main_df = reprices_log_df.merge(calendar_df, left_on='date_reprice', right_on='date', how='left')
        self.main_df = self.main_df.merge(products_reference_df, on='product_id', how='left')
        self.main_df = self.main_df.merge(pricing_types_df, on='algorithm', how='left')
        self.main_df = self.main_df.merge(write_date_df, how='cross')
        self.errors_df = reprices_errors_log_df.merge(calendar_df, left_on='date_error', right_on='date', how='left')
        self.errors_df = self.errors_df.merge(products_reference_df, on='product_id', how='left')
        self.errors_df = self.errors_df.merge(pricing_types_df, on='algorithm', how='left')
        self.errors_df = self.errors_df.merge(write_date_df, how='cross')
        self.picked_data = pd.DataFrame(
            {
            'start_date' : [reprices_log_df['date_reprice'].min()],
            'end_date': [reprices_log_df['date_reprice'].max()]
        })
        self.picked_algorithms = pricing_types_df

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

    def filters_algorithms(self, algorithms=None):
        '''
        Метод, который обновляет список алгоритмов, выбранных пользователем
        :param algorithms:
        :return:
        '''
        algorithms_df = pd.DataFrame(
            {'type_name': algorithms}
        )

        self.picked_algorithms = algorithms_df
    def filtered_df(self, df):
        severed_df = df.loc[(df['date_reprice'] >= self.picked_data['start_date'].iloc[0]) &
                                      (df['date_reprice'] <= self.picked_data['end_date'].iloc[0])]
        severed_df = severed_df.merge(self.picked_algorithms, on = 'type_name')
        return severed_df

reprices_log_df['date_reprice'] = pd.to_datetime(reprices_log_df['date_reprice'])
reprices_errors_log_df['date_error'] = pd.to_datetime(reprices_errors_log_df['date_error'])

reprices_data = data_lake(pricing_types_df=pricing_types_df,
                          products_reference_df=products_reference_df,
                          reprices_errors_log_df=reprices_errors_log_df,
                          reprices_log_df=reprices_log_df,
                          calendar_df=calendar_df)


start_date = '2023-09-15'
end_date = '2023-09-18'
algorithms = ['Новинки', 'Третий трек']

reprices_data.filters_date(start_date=start_date, end_date=end_date)
reprices_data.filters_algorithms(algorithms=algorithms)

need_to_view_df = reprices_data.filtered_df(reprices_data.main_df)
need_errors_df = reprices_data.filtered_df(reprices_data.errors_df)

