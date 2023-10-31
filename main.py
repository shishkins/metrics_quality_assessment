# Импортирование библиотек
import warnings

warnings.filterwarnings('ignore')
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
from dash_bootstrap_templates import load_figure_template

load_figure_template("LUMEN")
theme = dbc.themes.LUMEN

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

        filtered_products = self.main_df[['product_id', 'product_code']]
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

        to_filter = self.main_df[['product_code', 'koef_change_sale', 'koef_change_revenue', 'koef_change_profit',
                                  'current_price_date']]  # то что будем фильтровать
        for koef, tuple_value in self.picked_koeffs.items():

            if koef is None or tuple_value[0] is None or (tuple_value[1] is None or tuple_value[1] == 4):
                continue
            if tuple_value[1] == 1:
                to_filter = to_filter.loc[to_filter[koef] <= float(tuple_value[0])]
            elif tuple_value[1] == 2:
                to_filter = to_filter.loc[abs(to_filter[koef] - float(tuple_value[0])) <= 0.05]
            elif tuple_value[1] == 3:
                to_filter = to_filter.loc[to_filter[koef] >= float(tuple_value[0])]

        to_filter['reprice_flag'] = True
        self.filtered_products_koeffs = to_filter[['product_code', 'current_price_date',
                                                   'reprice_flag']]  # записываем в переменную класса датафрейм с товарами, у которых появился reprice_flag = True

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
        severed_df = severed_df.merge(self.filtered_products_koeffs, on=['product_code', 'current_price_date'],
                                      how='left')
        severed_df.sort_values(by='date', inplace=True)
        return severed_df


''' Получение данных, можно настроить вручную, какие датафреймы нужно получать '''

sales_data = data_lake(dict_of_dataframes=get_data())

''' LAYOUT '''
app = Dash(__name__,
           external_stylesheets=[theme],
           )

# рудимент
# calendar_button = dcc.DatePickerRange(id='date-picker',
#                                       min_date_allowed=min(sales_data.main_df['date']),
#                                       # минимально-допустимая выбираемая дата, определена как минимум у объекта модели данных
#                                       max_date_allowed=max(sales_data.main_df['date']),
#                                       # максимально-допустимая выбираемая дата, определена как минимум у объекта модели данных
#                                       initial_visible_month=sales_data.main_df['date'].mean(),
#                                       # видимый месяц для выбора в календаре (среднее за все время)
#                                       start_date=min(sales_data.main_df['date']),  # минимальная выбранная дата
#                                       end_date=max(sales_data.main_df['date'])  # максимальная выбранная дата
#                                       )
''' Задание независимых графиков'''

koeffs_fig_hist = go.Figure()  # создание фигуры с распределением коэффициентов

# цикл, в каждой итерации которого добавляется распределение каждого из коэффициентов
for name_koeff, koeff in zip(
        ['Коэффициент изменения продаж', 'Коэффициент изменения оборота', 'Коэффициент изменения прибыли'],
        sales_data.koeff_counts[['sales', 'revenue', 'profit']].columns):
    koeff_bar = go.Bar(x=sales_data.koeff_counts['for_vizual'],  # ось x, формат данных str
                       y=sales_data.koeff_counts[koeff],
                       # ось y - посчитанное количество вхождений для каждого бина, посчитана в get_dataframes , .value_counts()
                       name=name_koeff,
                       hovertemplate='<b>Количество вхождений: %{y}' +
                                     '<br>Интервал: %{x}' +
                                     '<br>%{text}</b>',
                       text=name_koeff)
    koeffs_fig_hist.add_trace(koeff_bar)



sale_graph = dcc.Graph(id='graph-sales-log',
                       figure=go.Figure(),
                       className='dbc')

# koeff_graph = dcc.Graph(id='koeff-sales-log',
#                         figure=koeffs_fig,
#                         className='dbc')

margin_graph = dcc.Graph(id='margin-sales-log',
                         figure=go.Figure(),
                         className='dbc')

revenue_graph = dcc.Graph(id='graph-revenue-log',
                          figure=go.Figure(),
                          className='dbc')
koeffs_hist_graph = dcc.Graph(id='koeffs_hist',
                              figure=koeffs_fig_hist,
                              className='dbc')

product_filter_list = dcc.Dropdown(
    id='product-list',
    options=sales_data.filtered_products_koeffs['product_code'].unique(),
    placeholder='Введите код товара..',
    className='dbc',
    style={
        'font-size': 20
    }
)

value_koeff_filter = dbc.Card(
    [
        dbc.CardHeader('В этой карточке вы можете настроить поиск товаров по значениям коэффициентов'),
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Input(placeholder='Введите значение КПРОД..',
                                          type='float',
                                          id='koeff-sales'),
                                dbc.Label(children='КПРОД - Коэффициент изменения продаж')
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
                                        {"label": "Снять ограничение", "value": 4}
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
                                dbc.Label(children='КПРИБ - Коэффициент изменения прибыли')
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
                                        {"label": "Снять ограничение", "value": 4}
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
                                dbc.Label(children='КОБ - Коэффициент изменения оборота')
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
                                        {"label": "Снять ограничение", "value": 4}
                                    ],
                                    value=None,
                                )
                            ]
                        )
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                product_filter_list,
                                dbc.Label(
                                    children='Код товара'
                                )
                            ],
                            width={
                                'size': 6,
                                'offset': 0,
                                'order': 1
                            },
                            align='start'
                        )
                    ]
                )
            ]
        )
    ]
)

app.layout = html.Div(
    [
        html.Div(id='hidden-div', style={'display': 'none'}),
        dbc.Row(
            [
                html.H1('Дашборд для оценки метрики качества переоценки')
            ],
            style={'margin-bottom': 40,
                   'margin-left': 40,
                   'margin-top:': 40}
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        value_koeff_filter
                        # dbc.Card(
                        #     dbc.Ca
                        # )
                    ],
                    width={
                        'size': 4,
                        'offset': 0,
                        'order': 1
                    }
                ),
                dbc.Col(
                    [
                        dbc.Card(koeffs_hist_graph)
                    ],
                    width={
                        'size': 6,
                        'offset': 0,
                        'order': 2
                    },
                )
            ],
            style={
                'margin-left': 40
            }
        ),
        dbc.Row(
            [
                html.H1(children='Выберете товар',
                        id='h1-chose',
                        style={'margin-bottom': 40,
                               'margin-left': 40,
                               'margin-top:': 40}
                        ),
                html.H3(children='',
                        id='h3-chosed-product',
                        style={'margin-bottom': 40,
                               'margin-left': 40,
                               'margin-top:': 40}
                        ),
                dbc.Col(
                    [
                        sale_graph,
                        revenue_graph,
                        margin_graph
                    ]
                ),
                dbc.Col(
                    [

                    ]
                )
            ],
            className='g-0'
        )
    ],
    className='dbc'
)


''' CALLBACKS '''


@callback(
    Output('h3-chosed-product', 'children'),
    Output('h1-chose', 'children'),
    Output('margin-sales-log', 'figure'),
    Output('graph-revenue-log', 'figure'),
    Output('graph-sales-log', 'figure'),
    # Input('date-picker', 'start_date'),
    #  Input('date-picker', 'end_date'),
    Input('product-list', 'value')
)
def update_fig(picked_code):
    # sales_data.filters_date(start_date=start_date, end_date=end_date)
    sales_data.picked_product = picked_code
    need_to_view_df = sales_data.filtered_df(sales_data.main_df)
    sales_fig = go.Figure()  # создание фигуры с продажами
    revenue_fig = go.Figure()  # создание фигуры с оборотом
    margin_fig = go.Figure()  # создание фигуры с маржой

    revenue_fig.update_layout(xaxis={'type': 'date'})
    sales_fig.update_layout(xaxis={'type': 'date'})

    # Код ниже отрисовывает график
    for reprice_date in need_to_view_df['current_price_date'].dt.date.unique():

        need_to_view_df_reprice = need_to_view_df.loc[
            need_to_view_df['current_price_date'].dt.date == reprice_date]  # находим конкретную переоценку

        if need_to_view_df_reprice[
            'reprice_flag'].unique() == True:  # задание параметра прозрачности в зависимости от выбранных параметров коэффициентов
            opacity = 0.9
        else:
            opacity = 0.3

        count_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                   y=need_to_view_df_reprice['count_sale'],
                                   name='Продажи',
                                   mode='lines+markers',
                                   hovertemplate='<b>Количество: %{y}' +
                                                 '<br>Дата: %{x}' +
                                                 '<br>%{text}</b>',
                                   text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                   legendgroup=str(reprice_date),
                                   opacity=opacity)
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
                                         opacity=opacity)
        sales_fig.add_trace(count_scatter)
        sales_fig.add_trace(clean_count_scatter)
        # sales_fig.update_layout(hoverlabel_font={'size': 16},
        #                         font_size=20)

        sales_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=0,
            x1=reprice_date,
            y1=need_to_view_df_reprice['count_sale'].max(),
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: ' + str(
                reprice_date) + '<br>' + f'<b>Код товара: {need_to_view_df_reprice["product_code"].unique()[0]}</b>',
            showlegend=True
        )

        revenue_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                     y=need_to_view_df_reprice['sum_sale_clean'],
                                     name='Оборот',
                                     mode='lines+markers',
                                     hovertemplate='<b>Количество: %{y}' +
                                                   '<br>Дата: %{x}' +
                                                   '<br>%{text}</b>',
                                     text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                     legendgroup=str(reprice_date),
                                     opacity=opacity)

        revenue_fig.add_trace(revenue_scatter)
        # revenue_fig.update_layout(hoverlabel_font={'size': 16},
        #                           font_size=20)

        revenue_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=0,
            x1=reprice_date,
            y1=need_to_view_df_reprice['sum_sale_clean'].max(),
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: ' + str(
                reprice_date) + '<br>' + f'\n<b>Код товара: {need_to_view_df_reprice["product_code"].unique()[0]}</b>',
            showlegend=True
        )

        margin_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                    y=(need_to_view_df_reprice['sum_sale_clean'] - need_to_view_df_reprice[
                                        'cost_price_clean']) / need_to_view_df_reprice['cost_price_clean'],
                                    name='Маржа',
                                    mode='lines+markers',
                                    hovertemplate='<b>%: %{y}' +
                                                  '<br>Дата: %{x}' +
                                                  '<br>%{text}</b>',
                                    text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                    legendgroup=str(reprice_date),
                                    opacity=opacity)
        margin_fig.add_trace(margin_scatter)
        # sales_fig.update_layout(hoverlabel_font={'size': 16},
        #                         font_size=20)

        margin_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=0,
            x1=reprice_date,
            y1=((need_to_view_df_reprice['sum_sale_clean'] - need_to_view_df_reprice['cost_price_clean']) /
                need_to_view_df_reprice['cost_price_clean']).max(),
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: ' + str(
                reprice_date) + '<br>' + f'<b>Код товара: {need_to_view_df_reprice["product_code"].unique()[0]}</b>',
            showlegend=True
        )

    if picked_code is None:
        h1_string = 'Выберете товар'
        h3_string = ''
    else:
        h1_string = 'Выбрано'
        h3_string = str(need_to_view_df["Наименование"].iloc[0])

    return h3_string, h1_string, margin_fig, revenue_fig, sales_fig


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
    for name_koeff, slice in zip(['koef_change_sale', 'koef_change_profit', 'koef_change_revenue'],
                                 [args[0:2], args[2:4], args[4:6]]):
        dict_filters[name_koeff] = slice
    sales_data.picked_koeffs = dict_filters
    sales_data.filter_product()
    return sales_data.filtered_products_koeffs['product_code'].unique()


if __name__ == '__main__':
    app.run_server(debug=True)
