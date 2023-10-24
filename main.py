# Импортирование библиотек
from dash import Dash, html, dash_table, dcc, callback, Output, Input
from query_executer import csv_execute
from get_dataframes import get_data
from datetime import date, datetime
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
import plotly.io as poi

''' GET DATA '''


class data_lake():
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
        self.main_df = reprices_log_df.merge(calendar_df, on='date', how='left')
        self.main_df = self.main_df.merge(products_reference_df, on='product_id', how='left')
        self.main_df = self.main_df.merge(pricing_types_df, on='algorithm', how='left')
        self.main_df = self.main_df.merge(write_date_df, how='cross')
        self.errors_df = reprices_errors_log_df.merge(calendar_df, on='date', how='left')
        self.errors_df = self.errors_df.merge(products_reference_df, on='product_id', how='left')
        self.errors_df = self.errors_df.merge(pricing_types_df, on='algorithm', how='left')
        self.errors_df = self.errors_df.merge(write_date_df, how='cross')
        self.picked_data = pd.DataFrame(
            {
                'start_date': [reprices_log_df['date'].min()],
                'end_date': [reprices_log_df['date'].max()]
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
            {'type_name': algorithms},
            dtype='str'
        )

        self.picked_algorithms = algorithms_df

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


pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df, write_date_df = get_data()

dict_of_dfs = {'pricing_types_df': pricing_types_df,
               'product_reference_df': products_reference_df,
               'reprices_errors_log_df': reprices_errors_log_df,
               'calendar_df': calendar_df}

reprices_data = data_lake(pricing_types_df=pricing_types_df,
                          products_reference_df=products_reference_df,
                          reprices_errors_log_df=reprices_errors_log_df,
                          reprices_log_df=reprices_log_df,
                          calendar_df=calendar_df)

''' LAYOUT '''

app = Dash(__name__,
           external_stylesheets=[dbc.themes.FLATLY],
           )

calendar_button = dcc.DatePickerRange(id='date-picker',
                                      min_date_allowed=min(calendar_df['date']),
                                      max_date_allowed=max(calendar_df['date']),
                                      initial_visible_month=reprices_log_df['date'].mean(),
                                      start_date=min(calendar_df['date']),
                                      end_date=max(calendar_df['date'])
                                      )

algorithm_filter = dcc.Checklist(
    id='check-list-algorithms',
    options=[{'label': option, 'value': option} for option in pricing_types_df['type_name']],
    value=pricing_types_df['type_name']
)

reprices_log_fig = dcc.Graph(id='hist-prices-log',
                             figure=go.Figure())
app.layout = html.Div([
    dbc.Row(html.H1('Hello Dash!'),
            style={'margin-bottom': 40}),
    dbc.Row([
        dbc.Col([
            html.Div(id='date-picker-info'),
            html.Div(calendar_button)],
            width={'size': 1, 'order': 'first', 'offset': 0}
        ),
        dbc.Col([
            html.Div('Выберите тип переоценки'),
            html.Div(algorithm_filter)],
            width={'size': 5, 'order': 'last', 'offset': 0})
    ], style={'margin-bottom': 40}),
    dbc.Row([
        dbc.Col([
            html.Div('Количество переоценок по датам:'),
            reprices_log_fig],
            width={'size': 4, 'order': 'first', 'offset': 0})
    ])
],
    style={'margin-left': '80px',
           'margin-right': '80px'})

''' CALLBACKS '''


@callback(
    Output('hist-prices-log', 'figure'),
    [Input('hist-prices-log', 'figure'),
     Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date'),
     Input('check-list-algorithms', 'value')]
)
def update_output(figure,start_date, end_date, algorithms):
    reprices_data.filters_date(start_date=start_date, end_date=end_date)
    reprices_data.filters_algorithms(algorithms=algorithms)

    need_to_view_df = reprices_data.filtered_df(reprices_data.main_df)
    need_errors_df = reprices_data.filtered_df(reprices_data.errors_df)
    reprices_log_fig = go.Figure()
    reprices_log_fig.add_trace(go.Histogram(x=need_to_view_df['date']))
    reprices_log_fig.add_trace(go.Histogram(x=need_errors_df['date']))

    return reprices_log_fig


if __name__ == '__main__':
    app.run_server(debug=True)
