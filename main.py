# Импортирование библиотек
import warnings

warnings.filterwarnings('ignore')
import re

from dash import Dash, html, dash_table, dcc, callback, Output, Input
from query_executer import csv_execute
from get_dataframes import get_data
from datetime import date, datetime
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import random
import plotly.express as px
import plotly.io as poi

''' GET DATA '''


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
        self.picked_product = [self.main_df['product_code'].iloc[0]]
        self.picked_koeffs = None

        filtered_products = self.main_df[['product_id','product_code']]
        filtered_products['reprice_flag'] = False
        self.filtered_products_koeffs = filtered_products[['product_code', 'reprice_flag']]

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

    def filter_product(self):

        to_filter = self.main_df[['product_code','koef_change_sale','koef_change_revenue','koef_change_profit', 'current_price_date']]  # то что будем фильтровать
        for koef, tuple_value in self.picked_koeffs.items():

            if koef is None or tuple_value[0] is None or (tuple_value[1] is None or tuple_value[1] == 4) :
                continue
            if tuple_value[1] == 1:
                to_filter = to_filter.loc[to_filter[koef] <= float(tuple_value[0])]
            elif tuple_value[1] == 2 :
                to_filter = to_filter.loc[abs(to_filter[koef] - float(tuple_value[0])) <= 0.05]
            elif tuple_value[1] == 3:
                to_filter = to_filter.loc[to_filter[koef] >= float(tuple_value[0])]

        to_filter['reprice_flag'] = True
        self.filtered_products_koeffs = to_filter[['product_code','current_price_date','reprice_flag']]  #записываем в переменную класса датафрейм с товарами, у которых появился reprice_flag = True


    def filter_hierarchy(self, selected_categories=None):
        pass

    def generate_hierarchy(self):
        # {
        #     'title': 'Parent',
        #     'key': '0',
        #     'children': [{
        #         'title': 'Child',
        #         'key': '0-0',
        #         'children': [
        #             {'title': 'Subchild1', 'key': '0-0-1'},
        #             {'title': 'Subchild2', 'key': '0-0-2'},
        #             {'title': 'Subchild3', 'key': '0-0-3'},
        #         ],
        #     }]}
        # list(sales_data.hierarchy_df['department_name'].unique())
        pass

    def filtered_df(self, df):
        '''
        Метод, возвращающий отфильтрованный датафрейм в соответствием с тем, что выбрал пользователь
        :param df:
        :return:
        '''
        severed_df = df.loc[(df['date'] >= self.picked_data['start_date'].iloc[0]) &
                            (df['date'] <= self.picked_data['end_date'].iloc[0])]
        self.picked_product = pd.DataFrame({'product_code': [self.picked_product]})
        severed_df = severed_df.merge(self.picked_product, on='product_code')
        severed_df = severed_df.merge(self.filtered_products_koeffs, on = ['product_code','current_price_date'], how = 'left')
        severed_df.sort_values(by='date', inplace=True)
        return severed_df



''' Получение данных, можно настроить вручную, какие датафреймы нужно получать '''

sales_data = data_lake(dict_of_dataframes=get_data())

''' LAYOUT '''
app = Dash(__name__,
           external_stylesheets=[dbc.themes.LUX],
           )

calendar_button = dcc.DatePickerRange(id='date-picker',
                                      min_date_allowed=min(sales_data.main_df['date']),
                                      # минимально-допустимая выбираемая дата, определена как минимум у объекта модели данных
                                      max_date_allowed=max(sales_data.main_df['date']),
                                      # максимально-допустимая выбираемая дата, определена как минимум у объекта модели данных
                                      initial_visible_month=sales_data.main_df['date'].mean(),
                                      # видимый месяц для выбора в календаре (среднее за все время)
                                      start_date=min(sales_data.main_df['date']),  # минимальная выбранная дата
                                      end_date=max(sales_data.main_df['date'])  # максимальная выбранная дата
                                      )

sale_graph = dcc.Graph(id='graph-sales-log',
                       figure=go.Figure(),
                       className='dbc')

product_filter_list = dcc.Dropdown(
    id='product-list',
    options=sales_data.filtered_products_koeffs['product_code'].unique(),
    placeholder='Введите код товара..'
)

value_koeff_filter = dbc.Col(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Input(placeholder='Введите значение КПРОД..',
                              type='float',
                              id='koeff-sales'),
                        ]
                ),
                dbc.Col(
                    [
                        dbc.RadioItems(
                            id="radios-koeff-sales",
                            class_name="btn-group",
                            inputClassName="btn-check",
                            labelClassName="btn btn-outline-primary",
                            labelCheckedClassName="active",
                            options=[
                                {"label": "Меньше <=", "value": 1},
                                {"label": "Равно =", "value": 2},
                                {"label": "Больше >=", "value": 3},
                                {"label": "Снять ограничение", "value":4}
                            ],
                            value=None
                        )
                    ]
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Input(placeholder='Введите значение КПРИБ..',
                                  type='float',
                                  id='koeff-profit'),
                    ],
                ),
                dbc.Col(
                    [
                        dbc.RadioItems(
                            id="radios-koeff-profit",
                            class_name="btn-group",
                            inputClassName="btn-check",
                            labelClassName="btn btn-outline-primary",
                            labelCheckedClassName="active",
                            options=[
                                {"label": "Меньше <=", "value": 1},
                                {"label": "Равно =", "value": 2},
                                {"label": "Больше >=", "value": 3},
                                {"label": "Снять ограничение", "value":4}
                            ],
                            value=None
                        )
                    ],
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Input(placeholder='Введите значение КОБ..',
                                  type='float',
                                  id='koeff-revenue'),
                    ]
                ),
                dbc.Col(
                    [
                        dbc.RadioItems(
                            id="radios-koeff-revenue",
                            class_name="btn-group",
                            inputClassName="btn-check",
                            labelClassName="btn btn-outline-primary",
                            labelCheckedClassName="active",
                            options=[
                                {"label": "Меньше <=", "value": 1},
                                {"label": "Равно =", "value": 2},
                                {"label": "Больше >=", "value": 3},
                                {"label": "Снять ограничение", "value":4}
                            ],
                            value=None,
                        )
                    ]
                )
            ]
        )
    ]
)


app.layout = dbc.Container(
    [
        html.Div(id='hidden-div', style={'display':'none'}),
        dbc.Row(
            [
                html.H1('Hello Dash!')
            ],
            style={'margin-bottom': 40}
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        calendar_button
                    ]
                ),
                dbc.Col(
                    [
                        product_filter_list
                    ],
                    width= {
                        'size': 2,
                        'offset': 0,
                        'order':'last'
                    }
                ),
                dbc.Col(
                    [
                        value_koeff_filter
                    ],
                    width = {
                        'size': 6
                    }
                )
            ]
        ),
        dbc.Row(
            [
                sale_graph
            ]
        )
    ],
    className='dbc'
)

''' CALLBACKS '''


@callback(
    Output('graph-sales-log', 'figure'),
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date'),
     Input('product-list', 'value')]
)
def update_fig(start_date, end_date, picked_code):
    print(sales_data.picked_product)
    sales_data.filters_date(start_date=start_date, end_date=end_date)
    sales_data.picked_product = picked_code
    need_to_view_df = sales_data.filtered_df(sales_data.main_df)
    sales_fig = go.Figure()
    sales_fig.update_layout(xaxis={'type': 'date'})


    # Код ниже отрисовывает график
    for reprice_date in need_to_view_df['current_price_date'].dt.date.unique():

        need_to_view_df_reprice = need_to_view_df.loc[need_to_view_df['current_price_date'].dt.date == reprice_date]  #находим конкретную переоценку

        if need_to_view_df_reprice['reprice_flag'].unique() == True:  #задание параметра прозрачности в зависимости от выбранных параметров коэффициентов
            opacity = '0.9'
        else:
            opacity = '0.3'

        count_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                   y=need_to_view_df_reprice['count_sale'],
                                   name='Продажи',
                                   mode='lines+markers',
                                   hovertemplate='<b>Количество: %{y}' +
                                                 '<br>Дата: %{x}' +
                                                 '<br>%{text}</b>',
                                   text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                   legendgroup=str(reprice_date),
                                   line={
                                       'color': 'rgba(255,99,71,'+opacity+')',
                                   })
        clean_count_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                         y=need_to_view_df_reprice['clean_count_sale'],
                                         name='Очищенные продажи',
                                         mode='lines+markers',
                                         hovertemplate='<b>Количество: %{y}' +
                                                       '<br>Дата: %{x}' +
                                                       '<br>%{text}</b>',
                                         text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                         legendgroup=str(reprice_date),
                                         showlegend=True,
                                         line={
                                             'color': 'rgba(30,144,255,'+opacity+')'
                                         })

        sales_fig.add_trace(count_scatter)
        sales_fig.add_trace(clean_count_scatter)
        sales_fig.update_layout(hoverlabel_font={'size': 16},
                                font_size=20)

        sales_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=0,
            x1=reprice_date,
            y1=need_to_view_df_reprice['count_sale'].max(),
            line=dict(
                color='rgba(255,69,0,'+opacity+')',
                dash="dash"
            ),
            legendgroup=str(reprice_date),
            name='Переоценка: ' + str(
                reprice_date) + f'\n<b>Код товара: {need_to_view_df_reprice["product_code"].unique()[0]}</b>',
            showlegend=True
        )

    return sales_fig


@callback(
    Output('product-list', 'options'),
    [Input('koeff-sales', 'value'),
     Input('radios-koeff-sales', 'value')],
    [Input('koeff-profit', 'value'),
     Input('radios-koeff-profit', 'value')],
    [Input('koeff-revenue', 'value'),
     Input('radios-koeff-revenue', 'value')]
)
def update_koefs(*args):
    dict_filters = dict()
    for name_koeff, slice in zip(['koef_change_sale','koef_change_profit','koef_change_revenue'],[args[0:2],args[2:4],args[4:6]]):
        dict_filters[name_koeff] = slice
    sales_data.picked_koeffs = dict_filters
    sales_data.filter_product()
    return sales_data.filtered_products_koeffs['product_code'].unique()



if __name__ == '__main__':
    app.run_server(debug=True)
