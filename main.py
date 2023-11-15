# Импортирование библиотек

import warnings

warnings.filterwarnings('ignore')
from dash import Dash, html, dash_table, dcc, callback, Output, Input

from datetime import date, datetime
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import plotly.io as poi
from dash_bootstrap_templates import load_figure_template

from query_executer import csv_execute
from get_dataframes import get_data
from get_allocation_koeffs import calculate_allocation_koeffs


# # Вывод русских дат
# import locale
# locale.setlocale(locale.LC_ALL, 'ru_RU.utf8')

# для визуала
def russian_date(date):
    month_list = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    result_string = f'{date.day} ' + month_list[date.month - 1] + f' {date.year} г.'
    return result_string

# прикольные темы SLATE, LUX

load_figure_template("minty_dark")
theme = dbc.themes.MINTY

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
        self.main_df = self.sales_and_coeffs_df
        self.main_df = self.main_df.merge(self.write_date_df, how='cross')

        ''' Дополнительные датафреймы '''
        self.koeffs_df = self.main_df[['koef_change_sale', 'koef_change_revenue', 'koef_change_profit', 'current_price_date','product_code']].drop_duplicates(subset = ['product_code', 'current_price_date'])  # датафрейм коэффициентов

        ''' Фильтры для алгоритмов '''
        self.picked_product = None
        self.picked_koeffs = None
        self.picked_reprices_dates = None

        ''' Штука, которая возвращает список товаров, подходящие под фильтр, заданный пользователем под коэффициенты'''
        filtered_products = self.main_df[['product_id', 'product_code', 'current_price_date', 'category_id']]
        filtered_products['reprice_flag'] = True
        self.filtered_products_koeffs = filtered_products[
            ['product_code', 'reprice_flag', 'current_price_date', 'category_id']]

        self.filtered_categories = self.hierarchy_df  # инициализация иерархии (для дальнейшей фильтрации

        # инициализация словаря фильтрации
        self.filtered_hierarchy_df = self.hierarchy_df[:]

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

    def filter_reprice(self, reprices):
        df_reprices = pd.DataFrame(
            {
                'current_price_date': reprices
            }
        )
        df_reprices = pd.to_datetime(df_reprices['current_price_date'])
        self.picked_reprices_dates = df_reprices

    def filter_product(self):
        '''
        В основе работы функции лежит цикл, который перебирает все параметры, которые выбрал пользователь.
        Параметры self.picked_koeffs хранятся в объекте модели данных как словарь коэффициент - (значение, в какую сторону).
        Перебирая значения словаря методом .items(), формируется датафрейм to_filter, подходящий под условия фильтрации
        Выхлоп функции - создание датафрейма self.filtered_products_koeffs, который участвует в формировании доступного списка опций в DropDown меню выбора товара;
        А так же участвует в присвоении флага reprice_flag - помечающий те переоценки у товаров, где этот коэффициент подходит под условия фильтрации.

        Пример:
        Пользователь выбрал КПРОД = 200 >=, КПРИБ: 100 >=, КОБ: None
        Пройдя цикл формируется to_filter, содержащий все записи с вышеупомянутыми условиями.
        Создается filtered_products_koeffs с пометкой reprice_flag = True, и датой переоценки для соединения в дальнейшем
        '''
        to_filter = self.main_df[['product_code', 'koef_change_sale', 'koef_change_revenue', 'koef_change_profit',
                                  'current_price_date', 'category_id', 'delta_price']]  # то что будем фильтровать

        # достаточно неоднозначный для понимания цикл, принцип его работы:
        # Итера
        for koef, tuple_value in self.picked_koeffs.items():
            if \
                    (tuple_value[0] is None) \
                    or (tuple_value[1] is None or tuple_value[1] == 4):
                continue
            if tuple_value[0] == '':
                tuple_value = (0, tuple_value[1])
            if tuple_value[1] == 1:
                to_filter = to_filter.loc[to_filter[koef] <= float(tuple_value[0])]
            elif tuple_value[1] == 2:
                to_filter = to_filter.loc[abs(to_filter[koef] - float(tuple_value[0])) <= 0.05]
            elif tuple_value[1] == 3:
                to_filter = to_filter.loc[to_filter[koef] >= float(tuple_value[0])]

        to_filter['reprice_flag'] = True
        self.filtered_products_koeffs = to_filter[['product_code',
                                                   'current_price_date',
                                                   'reprice_flag',
                                                   'category_id']]  # записываем в переменную класса датафрейм с товарами, у которых появился reprice_flag = True

    def get_list_products(self):
        severed_product = self.filtered_products_koeffs[:]
        severed_product = severed_product.merge(self.filtered_hierarchy_df, on='category_id')[['product_code',
                                                                                               'current_price_date',
                                                                                               'reprice_flag']].drop_duplicates()
        return severed_product

    # def filter_hierarchy(self, filter_df):
    #     self.filtered_products_koeffs = self.filtered_products_koeffs.merge(filter_df, on = 'category_id')[['product_code', 'reprice_flag', 'current_price_date']]  #фильтруем список товара исходя из категории

    def filtered_df(self, df, who_called):
        '''
        Метод, возвращающий отфильтрованный датафрейм в соответствием с тем, что выбрал пользователь
        :param df:
        :return:
        '''

        severed_df = df
        severed_df = severed_df.loc[severed_df['product_code'] == self.picked_product]
        severed_df = severed_df.merge(self.filtered_products_koeffs, on=['product_code', 'current_price_date'],
                                      how='left')

        # проверяем, кто вызывает функцию, чтобы не отдать то, чего не нужно
        if who_called == 'update_of_reprices':
            if self.picked_reprices_dates is None:
                severed_df = severed_df
            else:
                severed_df = severed_df.merge(self.picked_reprices_dates, on='current_price_date')

        severed_df.sort_values(by='date', inplace=True)
        print(severed_df.columns)

        return severed_df


''' Получение данных, можно настроить вручную, какие датафреймы нужно получать '''

sales_data = data_lake(dict_of_dataframes=get_data())

''' Различные функции '''

# функция генерации ключей сортировки для списка категорий
# Принцип работы:
# функцию загоняется список list_of_category, например
# ['05. DIY и Климат', '10. Категории на вывод (Авто ЦО)', ... , '11. Нетоварные категории']
# Затем функция составляет новый список "ключей", который будет равен:
# [5, 10, ... 11], на основе расстановки значений в списке ключей и будет произведена сортировка исходного списка (см. ниже в callback -> hierarchy_choiser)
def sort_hierarchy(list_category):
    list_of_keys = []
    for i, elem in enumerate(list_category):
        if elem[0:2].isdigit():
            list_of_keys.append(int(elem[0:2]))
        else:
            list_of_keys.append(i+len(list_category))

    return list_of_keys


''' Front-end '''
app = Dash(__name__,
           external_stylesheets=[theme, dbc.icons.BOOTSTRAP],
           suppress_callback_exceptions=True
           # опция, выключающая ошибки, если в callback есть элементы, которые пока не отобразились в дашборде (например clipboard)
           )

''' Функции '''


def draw_allocation(koeff_counts, indicator_for_coeffs):
    koeffs_fig_hist = go.Figure()  # создание фигуры с распределением коэффициентов

    for name_koeff, koeff in zip(
            ['Коэффициент изменения продаж', 'Коэффициент изменения оборота', 'Коэффициент изменения прибыли'],
            koeff_counts[['sales', 'revenue', 'profit']].columns):
        koeff_bar = go.Bar(
            x=koeff_counts['for_vizual'],  # ось x, формат данных str
            y=koeff_counts[koeff],
            # ось y - посчитанное количество вхождений для каждого бина, посчитана в get_dataframes , .value_counts()
            name=name_koeff,
            text=(koeff_counts[koeff] /
                  indicator_for_coeffs['all_reprices_count'].iloc[0] * 100).round(4),
            hovertemplate='<b>Количество вхождений: %{y}' +
                          '<br>В процентах: %{text}%' +
                          '<br>Интервал: %{x}',
            legendgroup=name_koeff
        )
        koeff_scatter = go.Scatter(x=koeff_counts['for_vizual'],
                                   y=koeff_counts[koeff],
                                   text=(koeff_counts[koeff] /
                                         indicator_for_coeffs['all_reprices_count'].iloc[0] * 100).round(4),
                                   name=name_koeff,
                                   mode='lines',
                                   hovertemplate='<b>Количество вхождений: %{y}' +
                                                 '<br>В процентах: %{text}%' +
                                                 '<br>Интервал: %{x}',
                                   legendgroup=name_koeff
                                   )
        koeffs_fig_hist.add_trace(koeff_bar)
        koeffs_fig_hist.add_trace(koeff_scatter)

    return koeffs_fig_hist



''' Задание графических объектов '''

navbar = dbc.Navbar(
    dbc.Container(
        [
            dbc.Row([
                dbc.Col([
                    html.Img(src=r'assets/DNS-Logo.png', alt='image', height='60px'),
                    dbc.NavbarBrand("Дашборд для оценки метрики качества переоценки", className="ms-2")
                ],
                    width={"size": "auto"}),
                dbc.Col(
                    [
                        dbc.Nav(
                            [
                                dbc.NavItem(
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.H6(
                                                    children=['Актуальность данных на:',
                                                              html.H4(russian_date(sales_data.write_date_df['write_date'].iloc[0]))],
                                                    className='card-title'
                                                )
                                            ]
                                        )
                                    ),
                                )
                            ],
                            navbar=True
                        )
                    ],
                    width={"size": "auto"}
                )
            ],
                align="center",
                className="g-0"),

            dbc.Row([
                dbc.Col([
                    dbc.Nav([
                        # dbc.NavItem(dbc.NavLink("Fundamentals", href="/fundamentals")),
                        dbc.NavItem(
                            dbc.DropdownMenu(
                                children=[
                                    dbc.DropdownMenuItem("Расчет метрики",
                                                         href='https://confluence.dns-shop.ru/pages/viewpage.action?pageId=117180014')
                                ],
                                nav=True,
                                in_navbar=True,
                                label="Источники",
                            )
                        ),
                    ],
                        navbar=True
                    )
                ],
                    width={"size": "auto"},
                ),
                dbc.Col(
                        dbc.Nav([
                            dbc.NavItem(dbc.NavLink(html.I(className="bi bi-envelope"),
                                                    href="mailto:Tretyakov.AA@dns-shop.ru?subject=Обратная связь по отчету с метрикой качества переоценки",
                                                    external_link=True)),
                            dbc.NavItem(
                                dbc.NavLink(html.I(className="bi bi-telegram"), href="https://t.me/tretiakov_aal",
                                            external_link=True)),
                        ]
                    )
                )
            ],
                align="center")
        ],
        fluid=True
    ),
    color="dark",
    dark=True
)

koeffs_hist_graph = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H4(children='Распределение коэффициентов по интервалам, не включая нулевые значения', className='card-title'),
                html.P([
                    'При фильтрации товаров в карточке слева происходит и фильтрация данного визуального элемента.',
                    html.Br(),
                    'Для просмотра распределения интересующей группы товаров обновите график'
                ],
                    className='card-text'),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Button('Обновить график',
                                       id='refresh-allocation',
                                       color='primary'),
                            style={
                                'margin-bottom': 5
                            }
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dcc.Graph(id='koeffs_hist',
                                          className='dbc')
                            ]
                        )
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    [
                                    ],
                                    id = 'table-allocation'
                                )
                            ],
                            width={
                                'size': 12,
                                'order': 1
                            }
                        )
                    ]
                )
            ]
        )
    ],
)


product_filter_list = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H4(children='Поиск по коду товара', className='card-title'),
                html.P(
                    'Выберете код товара из выпадающего списка, или введите код вручную. Список формируется на основе фильтров выше.',
                    className='card-text'),
                dcc.Dropdown(
                    id='product-list',
                    options=sales_data.get_list_products()['product_code'].unique(),
                    placeholder='Введите код товара..'
                ),
            ]
        )
    ]

)

table_limits_df = pd.DataFrame(
    {
        ("Пределы", "Макс"): {
            'Коэффициент изменения продаж ("КПРОД")': sales_data.main_df['koef_change_sale'].max(),
            'Коэффициент изменения прибыли ("КПРИБ")': sales_data.main_df['koef_change_profit'].max(),
            'Коэффициент изменения оборота ("КОБ")': sales_data.main_df['koef_change_revenue'].max(),
        },
        ("Пределы", "Мин"): {
            'Коэффициент изменения продаж ("КПРОД")': sales_data.main_df['koef_change_sale'].min(),
            'Коэффициент изменения прибыли ("КПРИБ")': sales_data.main_df['koef_change_profit'].min(),
            'Коэффициент изменения оборота ("КОБ")': sales_data.main_df['koef_change_revenue'].min(),
        },
    }
)

table_limits_df.index.set_names("Коэффициент", inplace=True)

table_limits = dbc.Table.from_dataframe(
    table_limits_df, striped=True, bordered=True, hover=True, index=True
)

hierarchy_elem = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H4(children='Иерархия категорий', className='card-title'),
                html.P(
                    'Настройте поиск товаров по принадлежности к категории из иерархии',
                    className='card-text'
                ),
                dbc.Label('Департамент'),
                dcc.Dropdown(
                    id='department-choise',
                    options=sorted(sales_data.hierarchy_df['department_name'].dropna().unique(), key = sort_hierarchy),
                    placeholder='Введите название департамента..',
                    multi=True
                ),
                dbc.Label('Направление'),
                dcc.Dropdown(
                    id='direction-choise',
                    options=sorted(sales_data.hierarchy_df['name_2'].dropna().unique(), key = sort_hierarchy),
                    placeholder='Введите название направления..',
                    multi=True
                ),
                dbc.Label('Группа категорий'),
                dcc.Dropdown(
                    id='group-choise',
                    options=sorted(sales_data.hierarchy_df['name_3'].dropna().unique(), key = sort_hierarchy),
                    placeholder='Введите название группы категорий..',
                    multi=True
                ),
                dbc.Label('Категория'),
                dcc.Dropdown(
                    id='category-choise',
                    options=sorted(sales_data.hierarchy_df['category_name'].dropna().unique(), key = sort_hierarchy),
                    placeholder='Введите название категории..',
                    multi=True
                ),
            ]
        )
    ],
    style={
        'margin-bottom': 20
    }
)

value_koeff_filter = html.Div(
    [
        dbc.Card(
            [
                dbc.CardBody(
                    [
                        html.H4(children='Коэффициенты качества переоценок', className='card-title'),
                        html.P(
                            'Настройте поиск товаров по значениям коффэициентов',
                            className='card-text'
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(placeholder='Введите значение КПРОД..',
                                                          type='float',
                                                          id='koeff-sales'),
                                                html.Div(
                                                    [
                                                        dbc.RadioItems(
                                                            id="radios-koeff-sales",
                                                            className="btn-group",
                                                            inputClassName="btn-check",
                                                            labelClassName="btn btn-outline-primary",
                                                            labelCheckedClassName="active",
                                                            options=[
                                                                {"label": "<=", "value": 1},
                                                                {"label": "=", "value": 2},
                                                                {"label": ">=", "value": 3},
                                                                {"label": "Снять ограничение", "value": 4}
                                                            ],
                                                        )
                                                    ],
                                                    className='radio-group'
                                                )
                                            ],

                                        ),
                                        dbc.Label(children='КПРОД - Коэффициент изменения продаж')
                                    ],
                                ),
                                # dbc.Col(
                                #     [
                                #
                                #     ],
                                #     className='radio-group'
                                # )
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(placeholder='Введите значение КПРИБ..',
                                                          type='float',
                                                          id='koeff-profit'),
                                                html.Div(
                                                    [
                                                        dbc.RadioItems(
                                                            id="radios-koeff-profit",
                                                            className="btn-group",
                                                            inputClassName="btn-check",
                                                            labelClassName="btn btn-outline-primary",
                                                            labelCheckedClassName="active",
                                                            options=[
                                                                {"label": "<=", "value": 1},
                                                                {"label": "=", "value": 2},
                                                                {"label": ">=", "value": 3},
                                                                {"label": "Снять ограничение", "value": 4}
                                                            ],
                                                        )
                                                    ],
                                                    className='radio-group'
                                                )
                                            ],

                                        ),
                                        dbc.Label(children='КПРИБ - Коэффициент изменения прибыли')
                                    ],
                                ),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(placeholder='Введите значение КОБ..',
                                                          type='float',
                                                          id='koeff-revenue'),
                                                html.Div(
                                                    [
                                                        dbc.RadioItems(
                                                            id="radios-koeff-revenue",
                                                            className="btn-group",
                                                            inputClassName="btn-check",
                                                            labelClassName="btn btn-outline-primary",
                                                            labelCheckedClassName="active",
                                                            options=[
                                                                {"label": "<=", "value": 1},
                                                                {"label": "=", "value": 2},
                                                                {"label": ">=", "value": 3},
                                                                {"label": "Снять ограничение", "value": 4}
                                                            ],
                                                        )
                                                    ],
                                                    className='radio-group'
                                                )
                                            ],

                                        ),
                                        dbc.Label(children='КОБ - Коэффициент изменения оборота')
                                    ],
                                ),
                            ]
                        )
                    ]
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H4(children='Дельта переоценки', className='card-title'),
                                        html.P(
                                            'Выберете, с какой разницой в цене переоценки Вас интересуют: ',
                                            className='card-text'
                                        ),
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(placeholder='Введите дельту переоценки..',
                                                          type='float',
                                                          id='delta-reprice'),
                                                html.Div(
                                                    [
                                                        dbc.RadioItems(
                                                            id="radios-delta",
                                                            className="btn-group",
                                                            inputClassName="btn-check",
                                                            labelClassName="btn btn-outline-primary",
                                                            labelCheckedClassName="active",
                                                            options=[
                                                                {"label": "<=", "value": 1},
                                                                {"label": "=", "value": 2},
                                                                {"label": ">=", "value": 3},
                                                                {"label": "Снять ограничение", "value": 4}
                                                            ],
                                                        )
                                                    ],
                                                    className='radio-group'
                                                )
                                            ],

                                        ),

                                    ]
                                )
                            ],
                            style={
                                'margin-bottom': 10,
                                'margin-top': 10
                            }
                        )
                    ]
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Col(
                            [
                                hierarchy_elem,
                                product_filter_list,
                            ],
                            width={'size': 8}
                        ),
                        dbc.Label(
                            children='Код товара',
                            style={'margin-bottom': 20},
                            width={
                                'size': 6
                            }
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Card(
                                            [
                                                dbc.Label(
                                                    id='counter-product',
                                                    children='Количество товаров, подходящих под условие отбора: ' + str(
                                                        len(sales_data.get_list_products()[
                                                                'product_code'].unique())),
                                                    style={
                                                        'margin-left': 10,
                                                        'margin-top': 10,
                                                        'margin-right': 10,
                                                        'margin-bottom': 10
                                                    }
                                                )
                                            ],
                                            style={
                                                'margin-bottom': 40
                                            }
                                        )
                                    ],
                                    width={
                                        'size': 8,
                                        'offset': 0,
                                        'order': 1
                                    },
                                    align='start'
                                ),
                                dbc.Col(
                                    [
                                        dbc.Card(
                                            [
                                                dbc.Label(
                                                    children='Всего товаров: ' + str(
                                                        len(sales_data.main_df[
                                                                'product_code'].unique())),
                                                    style={
                                                        'margin-left': 10,
                                                        'margin-top': 10,
                                                        'margin-right': 10,
                                                        'margin-bottom': 10
                                                    }
                                                )
                                            ],
                                            style={
                                                'margin-bottom': 20
                                            }
                                        )
                                    ],
                                    width={
                                        'size': 4,
                                        'offset': 0,
                                        'order': 2
                                    },
                                )
                            ]
                        )
                    ]
                ),
            ]
        ),
        dbc.Row(
            [
                table_limits
            ],
            style={
                'margin-left': 2,
                'margin-right': 2
            }
        )
    ]
)

''' Размещение всех созданных элементов на дашборде '''

app.layout = html.Div(
    [
        navbar,
        dbc.Row(
            [
                dbc.Col(
                    [
                        value_koeff_filter  # карточка фильтрации для выбора товара
                    ],
                    width={
                        'size': 4,
                        'offset': 0,
                    },
                    style = {
                        'margin-top': 20
                    }
                ),
                dbc.Col(
                    [
                        koeffs_hist_graph  # График распределения коэффициентов
                    ],
                    width={
                        'size': 7,
                        'offset': 0
                    },
                    style={
                        'margin-top': 20
                    }
                )
            ],
            style={
                'margin-left': 40
            },
        ),
        dbc.Row(
            [
                html.H1(children='Выберете товар',
                        id='h1-chose',
                        style={'margin-bottom': 10,
                               'margin-left': 40,
                               'margin-top:': 40}
                        ),
                html.H3(children='',
                        id='h3-chosed-product',
                        style={'margin-bottom': 5,
                               'margin-left': 40,
                               'margin-top:': 5}
                        ),
                dbc.Col(
                    [
                        html.Div(
                            children=[

                            ],
                            id='h5-chosed-product',  # Код товара с кнопкой буффера обмена
                            style={'margin-bottom': 10,
                                   'margin-left': 40,
                                   'margin-top:': 5}
                        ),
                        html.Div(
                            id='main-table',
                        ),
                        html.Div(
                            id='graphs',  # интерактивно-генерирующиеся графики
                            children=[]
                        )
                    ],
                    width={
                        'size': 7
                    }
                ),
                dbc.Col(
                    children=[

                    ],
                    id='place-for-reprices-choiser',
                    className="radio-group",
                    width={
                        'size': 2
                    }
                )
            ],
            style={
                'margin-left': 40
            }
        )
    ],
    className='dbc'
)

''' FUNCTIONS '''


def draw_figures(df):
    need_to_view_df = df
    sales_fig = go.Figure()  # создание фигуры с продажами
    revenue_fig = go.Figure()  # создание фигуры с оборотом
    profit_fig = go.Figure()  # создание фигуры с маржой

    revenue_fig.update_layout(xaxis={'type': 'date'})
    sales_fig.update_layout(xaxis={'type': 'date'})
    # задание параметров для рисунков
    count_reprices = len(need_to_view_df['current_price_date'].dt.date.unique())
    font_size_annotations = 16
    font_style_annotations = 'Arial,sans-serif'
    color_text_annotations = '#b8b4b4'
    bgcolor = '#1d2024'
    border_color = '#c7c7c7'
    offset_top = 1.18
    list_of_fontsizes = [16,14,13,12,10,8]  # шрифты при различном количестве переоценок
    if count_reprices> len(list_of_fontsizes):
        list_of_fontsizes += [list_of_fontsizes[-1]]*(count_reprices-len(list_of_fontsizes))  # добавляет в список последний элемент n раз, где n - разница между количеством переоценок и длиной списка размера шрифтов list_of_fontsizes

    # Код ниже отрисовывает графики
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
                                                 '<br>Дата: %{x}',
                                   legendgroup=str(reprice_date),
                                   opacity=opacity)
        clean_count_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                         y=need_to_view_df_reprice['clean_count_sale'],
                                         name='Очищенные продажи',
                                         mode='lines+markers',
                                         hovertemplate='<b>Количество: %{y}' +
                                                       '<br>Дата: %{x}',
                                         legendgroup=str(reprice_date),
                                         showlegend=True,
                                         opacity=opacity)
        sales_fig.add_trace(count_scatter)
        sales_fig.add_trace(clean_count_scatter)
        # sales_fig.update_layout(hoverlabel_font={'size': 16},
        #                         font_size=20)

        # Добавляем примочки в виде пояснительных штук:
        # Вертикальная линия в дату переоценки
        # максимальные ограничения для подсказок на графике
        max_value_sale_reprice = max(list(need_to_view_df_reprice['count_sale']),
                                     key=lambda x: abs(
                                         x))  # максимальное значение Y на графике для конкретной переоценки
        min_value_sale_reprice = min(list(need_to_view_df_reprice['count_sale']),
                                     key=lambda x: abs(
                                         x))  # минимальное значение Y на графике для конкретной переоценки
        max_value_sale = max(list(need_to_view_df['count_sale']),
                             key=lambda x: abs(x))  # максимальное значение Y на графике для товара
        min_value_sale = min(list(need_to_view_df['count_sale']),
                             key=lambda x: abs(x))  # минимальное значение Y на графике для товара

        # Сама вертикальная линия
        sales_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=min_value_sale,
            x1=reprice_date,
            y1=max_value_sale_reprice,
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: '
                 + '<br>'
                 + str(reprice_date),
            showlegend=True
        )

        # Подсказка (текст над вертикальной линией  - "переоценка: .."
        sales_fig.add_annotation(
            x=reprice_date,
            y=max_value_sale_reprice,
            text='Переоценка: ' +
                 '<br>' +
                 f'{reprice_date}',
            opacity=opacity
        )

        # Подсказка, какая цена была до (слева от вертикальной линии на 1 день, выравнивание по правому краю)
        sales_fig.add_annotation(
            x=reprice_date - pd.to_timedelta(1, unit='D'),
            xanchor='right',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена до: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["last_price"].unique()[0])}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=list_of_fontsizes[count_reprices-1],
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какая цена была после (справа от вертикальной линии на 1 день, выравнивание по левому краю)
        sales_fig.add_annotation(
            x=reprice_date + pd.to_timedelta(1, unit='D'),
            xanchor='left',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена после: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["current_price"].unique()[0])}',
            showarrow=False,
            arrowcolor='#FFFFFF',
            opacity=opacity,
            bordercolor=border_color,
            font=dict(
                family=font_style_annotations,
                size=list_of_fontsizes[count_reprices-1],
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какой КПРОД у переоценки
        sales_fig.add_annotation(
            x=reprice_date,
            y=offset_top,
            yref='paper',
            text='КПРОД = ' +
                 f'{round(need_to_view_df_reprice["koef_change_sale"].unique()[0], 2)}',
            showarrow=False,
            arrowcolor='#FFFFFF',
            opacity=opacity,
            bordercolor=border_color,
            font=dict(
                family=font_style_annotations,
                size=list_of_fontsizes[count_reprices-1],
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=2,
        )

        revenue_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                     y=need_to_view_df_reprice['sum_sale_clean'],
                                     name='Оборот',
                                     mode='lines+markers',
                                     hovertemplate='<b>Сумма: %{y}' +
                                                   '<br>Дата: %{x}',
                                     text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                     legendgroup=str(reprice_date),
                                     opacity=opacity)

        revenue_fig.add_trace(revenue_scatter)
        # revenue_fig.update_layout(hoverlabel_font={'size': 16},
        #                           font_size=20)
        max_value_revenue_reprice = max(list(need_to_view_df_reprice['sum_sale_clean']),
                                        key=lambda x: abs(
                                            x))  # максимальное значение Y на графике для конкретной переоценки
        min_value_revenue_reprice = min(list(need_to_view_df_reprice['sum_sale_clean']),
                                        key=lambda x: abs(
                                            x))  # минимальное значение Y на графике для конкретной переоценки
        max_value_revenue = max(list(need_to_view_df['sum_sale_clean']),
                                key=lambda x: abs(x))  # максимальное значение Y на графике для товара
        min_value_revenue = min(list(need_to_view_df['sum_sale_clean']),
                                key=lambda x: abs(x))  # минимальное значение Y на графике для товара

        revenue_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=min_value_revenue,
            x1=reprice_date,
            y1=max_value_revenue_reprice,
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: ' +
                 str(reprice_date),
            showlegend=True
        )

        # Подсказка, вертикальная линия
        revenue_fig.add_annotation(
            x=reprice_date,
            y=max_value_revenue_reprice,
            text='Переоценка: ' +
                 '<br>' +
                 f'{reprice_date}',
            opacity=opacity
        )

        # Подсказка, какая цена была до (слева от вертикальной линии на 1 день, выравнивание по правому краю)
        revenue_fig.add_annotation(
            x=reprice_date - pd.to_timedelta(1, unit='D'),
            xanchor='right',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена до: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["last_price"].unique()[0])}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какая цена была после (справа от вертикальной линии на 1 день, выравнивание по левому краю)
        revenue_fig.add_annotation(
            x=reprice_date + pd.to_timedelta(1, unit='D'),
            xanchor='left',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена после: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["current_price"].unique()[0])}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какой КОБ у переоценки
        revenue_fig.add_annotation(
            x=reprice_date,
            y=offset_top,
            yref='paper',
            text='КОБ = ' +
                 f'{round(need_to_view_df_reprice["koef_change_revenue"].unique()[0], 2)}',
            showarrow=False,
            arrowcolor='#FFFFFF',
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        max_value_profit_reprice = max(
            list(need_to_view_df_reprice['sum_sale_clean'] - need_to_view_df_reprice['cost_price_clean']),
            key=lambda x: abs(x))  # максимальное значение Y на графике для конкретной переоценки
        min_value_profit_reprice = min(
            list(need_to_view_df_reprice['sum_sale_clean'] - need_to_view_df_reprice['cost_price_clean']),
            key=lambda x: abs(x))  # минимальное значение Y на графике для конкретной переоценки
        max_value_profit = max(list(need_to_view_df['sum_sale_clean'] - need_to_view_df['cost_price_clean']),
                               key=lambda x: abs(x))  # максимальное значение Y на графике для товара
        min_value_profit = min(list(need_to_view_df['sum_sale_clean'] - need_to_view_df['cost_price_clean']),
                               key=lambda x: x + abs(x) * 10)  # минимальное значение Y на графике для товара

        profit_scatter = go.Scatter(x=need_to_view_df_reprice['date'].dt.date,
                                    y=need_to_view_df_reprice['sum_sale_clean'] - need_to_view_df_reprice[
                                        'cost_price_clean'],
                                    # Оборот без НДС - Сумма без НДС
                                    name='Прибыль',
                                    mode='lines+markers',
                                    hovertemplate='<b>Сумма: %{y}' +
                                                  '<br>Дата: %{x}',
                                    text='Код товара: ' + need_to_view_df_reprice['product_code'],
                                    legendgroup=str(reprice_date),
                                    opacity=opacity)

        profit_fig.add_trace(profit_scatter)

        profit_fig.add_shape(
            type="line",
            x0=reprice_date,
            y0=min_value_profit,
            x1=reprice_date,
            y1=max_value_profit_reprice,
            line=dict(
                dash="dash"
            ),
            opacity=opacity,
            legendgroup=str(reprice_date),
            name='Переоценка: ' +
                 '<br>' +
                 str(reprice_date),
            showlegend=True
        )

        profit_fig.add_annotation(
            x=reprice_date,
            y=max_value_profit_reprice,
            ay=max_value_profit_reprice * 1.25,
            ayref='y',
            text='Переоценка: '
                 + '<br>'
                 + f'{reprice_date}',
            opacity=opacity
        )

        # Подсказка, какая цена была до (слева от вертикальной линии на 1 день, выравнивание по правому краю)
        profit_fig.add_annotation(
            x=reprice_date - pd.to_timedelta(1, unit='D'),
            xanchor='right',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена до: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["last_price"].unique()[0])}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какая цена была после (справа от вертикальной линии на 1 день, выравнивание по левому краю)
        profit_fig.add_annotation(
            x=reprice_date + pd.to_timedelta(1, unit='D'),
            xanchor='left',
            y=-0.09,
            yref='paper',
            yanchor='top',
            text='Цена после: ' +
                 '<br>' +
                 f'{int(need_to_view_df_reprice["current_price"].unique()[0])}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

        # Подсказка, какой КПРИБ у переоценки
        profit_fig.add_annotation(
            x=reprice_date,
            y=offset_top,
            yref='paper',
            text='КПРИБ = ' +
                 f'{round(need_to_view_df_reprice["koef_change_profit"].unique()[0], 2)}',
            showarrow=False,
            bordercolor=border_color,
            opacity=opacity,
            font=dict(
                family=font_style_annotations,
                size=font_size_annotations,
                color=color_text_annotations
            ),
            bgcolor=bgcolor,
            borderwidth=2,
            borderpad=4,
        )

    # формируем список из графиков, который затем преобразуется в список dbc.Элементов. Этот список "воткнется" в html.Div(children = "сюда")
    list_of_figs = [('График изменения продаж', sales_fig),
                    ('График изменения прибыли', profit_fig),
                    ('График изменения оборота', revenue_fig)]
    list_of_graphs = []

    for name, fig in list_of_figs:
        list_of_graphs.append(
            dbc.Card(
                [
                    dbc.CardHeader(
                        children=name
                    ),
                    dcc.Graph(figure=fig,
                              className='dbc')
                ]
            )
        )

    return list_of_graphs


''' CALLBACKS '''


@callback(
    Output('place-for-reprices-choiser', 'children'),
    Output('main-table', 'children'),
    Output('h5-chosed-product', 'children'),
    Output('h3-chosed-product', 'children'),
    Output('h1-chose', 'children'),
    Output('graphs', 'children', allow_duplicate=True),
    Input('product-list', 'value'),
    prevent_initial_call=True
)
def update_of_pick_product(picked_code):
    sales_data.picked_product = picked_code
    need_to_view_df = sales_data.filtered_df(sales_data.main_df, who_called=update_of_pick_product.__name__)

    if picked_code is None:
        h1_string = 'Выберете товар'
        h3_string = ''
        h5_string = ''
        need_table = html.Div(
            [
                html.H2('Для отображения таблицы выберете товар..', className='display-3'),
                html.Hr(className='my-2')
            ],
            className='h-100 p-5 bg-light border rounded-3'
        )
        returned_object_clipboard = []
        list_of_graphs = []
        reprices_choiser = [
        ]
    else:
        h1_string = 'Выбрано:'
        h3_string = str(need_to_view_df["Наименование"].iloc[0])
        h5_string = 'Код товара: ' + str(need_to_view_df["product_code"].iloc[0])

        table_df_from_need = need_to_view_df[['current_price_date',
                                              'last_price_date',
                                              'koef_change_sale',
                                              'koef_change_revenue',
                                              'koef_change_profit',
                                              'current_price',
                                              'last_price',
                                              'delta_price']]
        table_df_from_need.drop_duplicates(inplace=True)
        table_df = pd.DataFrame(
            {
                'Переоценка ФЦ ОРП': table_df_from_need['current_price_date'].dt.date,
                'Выставленная цена': table_df_from_need['current_price'],
                'Предыдущая переоценка ФЦ ОРП': table_df_from_need['last_price_date'].dt.date,
                'Предыдущая выставленная цена': table_df_from_need['last_price'],
                'Изменение цены': table_df_from_need['delta_price'],
                'КПРОД': table_df_from_need['koef_change_sale'],
                'КПРИБ': table_df_from_need['koef_change_profit'],
                'КОБ': table_df_from_need['koef_change_revenue']
            }
        )
        # возвращаем объект dbc, который "воткнется" в html.div, с указанным id
        returned_object_clipboard = [
            dbc.Row(
                [
                    dbc.Col(
                        children=[
                            html.H4(h5_string)
                        ],
                        width={
                            'size': 3,
                            'order': 1
                        }
                    ),
                    dbc.Col(
                        [
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        [
                                            html.I(className="bi bi-clipboard"),
                                            ' Скопировать код товара',
                                            dcc.Clipboard(
                                                id='clipboard-code',
                                                className="position-absolute start-0 top-0 h-100 w-100 opacity-0"
                                            )
                                        ],
                                        className="position-relative",
                                        color="success",
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="bi bi-clipboard"),
                                            ' Скопировать UUID товара',
                                            dcc.Clipboard(
                                                id='clipboard-uuid',
                                                className="position-absolute start-0 top-0 h-100 w-100 opacity-0"
                                            )
                                        ],
                                        className="position-relative",
                                        color="success",
                                    )
                                ]
                            )
                        ],
                        width={
                            'size': 6,
                            'order': 2
                        }
                    )
                ]
            )
        ]
        need_table = dbc.Table.from_dataframe(table_df, striped=True, bordered=True, hover=True)
        list_of_graphs = draw_figures(need_to_view_df)

        reprices_choiser = [
            dbc.Card(
                [
                    html.H2(children='Выбранные переоценки: ', style={'margin-left': 20, 'margin-top': 20}),
                    dbc.Col(
                        [
                            dbc.Checklist(
                                options=[
                                    {
                                        'label': f'Переоценка {i}',
                                        'value': i
                                    } for i in need_to_view_df['current_price_date'].dt.date.unique()
                                ],
                                value=need_to_view_df['current_price_date'].dt.date.unique(),
                                id='reprices-choiser',
                                className="btn-inline-group",
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-primary",
                                inline=False
                            )
                        ],
                        width={
                            'size': 1,
                            'order': 1
                        },
                        style={
                            'margin-left': 20,
                            'margin-bottom':20

                        }
                    )
                ]
            )
        ]
        #     )

    return reprices_choiser, need_table, returned_object_clipboard, h3_string, h1_string, list_of_graphs


@callback(
    Output('graphs', 'children', allow_duplicate=True),
    Output('counter-product', 'children'),
    Output('product-list', 'options'),
    [Input('koeff-sales', 'value'),
     Input('radios-koeff-sales', 'value')],
    [Input('koeff-profit', 'value'),
     Input('radios-koeff-profit', 'value')],
    [Input('koeff-revenue', 'value'),
     Input('radios-koeff-revenue', 'value')],
    [Input('delta-reprice', 'value'),
     Input('radios-delta', 'value')],
    prevent_initial_call=True
)
def update_koefs(*args):
    dict_filters = dict()
    for name_koeff, slice in zip(['koef_change_sale', 'koef_change_profit', 'koef_change_revenue', 'delta_price'],
                                 [args[0:2], args[2:4], args[4:6], args[6:8]]):
        dict_filters[name_koeff] = slice
    sales_data.picked_koeffs = dict_filters
    sales_data.filter_product()
    counter = str(len(sales_data.get_list_products()['product_code'].unique()))
    return_string = 'Количество товаров, подходящих под условие отбора: ' + counter

    need_to_view_df = sales_data.filtered_df(sales_data.main_df, who_called=update_koefs.__name__)
    # print(need_to_view_df.columns)
    return draw_figures(need_to_view_df), return_string, sales_data.get_list_products()['product_code'].unique()


# кнопка копии кода товара
@callback(
    Output('clipboard-code', 'content'),
    Input('clipboard-code', 'n_clicks')
)
def copy_code(_):
    code = sales_data.picked_product  # вытаскиваем код товара, который выбрал пользователь из модели данных
    return code


# кнопка копии уида товара
@callback(
    Output('clipboard-uuid', 'content'),
    Input('clipboard-uuid', 'n_clicks')
)
def copy_code(_):
    product_code = sales_data.picked_product
    product_id = str(sales_data.main_df.loc[sales_data.main_df['product_code'] == product_code, 'product_id'].unique()[
                         0])  # находим product_id по коду, который выбран в модели данных
    return product_id


@callback(
    Output('graphs', 'children', allow_duplicate=True),
    Input('reprices-choiser', 'value'),
    prevent_initial_call=True
)
def update_of_reprices(reprices):
    sales_data.filter_reprice(reprices)
    need_to_view_df = sales_data.filtered_df(sales_data.main_df, who_called=update_of_reprices.__name__)

    return draw_figures(need_to_view_df)


@callback(
    Output('table-allocation', 'children'),
    Output('koeffs_hist', 'figure'),
    Input('refresh-allocation', 'n_clicks')
)
def update_allocation(*args):

    # обращаемся к модулю calculate_allocation_koeffs за получением таблицы с рапределением, таблицы с рассчитанными нулями, и информативной таблицей
    koeff_counts, indicator_for_coeffs, table_reprices_koefs_df = calculate_allocation_koeffs(sales_data.koeffs_df,
                                                                                              filters={('product_code', 'current_price_date'): sales_data.get_list_products()})  #тут важно указать поля, по которым соединяются данные

    table_koeffs = dbc.Table.from_dataframe(
        table_reprices_koefs_df, striped=True, bordered=True, hover=True, index=True,
    )

    figure_to_return = draw_allocation(koeff_counts, indicator_for_coeffs)

    return table_koeffs, figure_to_return

# CALLBACK для иерархии
@callback(
    Output('product-list', 'options', allow_duplicate=True),  # изменяем список возможного товара
    Output('counter-product', 'children', allow_duplicate=True),
    Output('department-choise', 'options'),
    Output('direction-choise', 'options'),
    Output('group-choise', 'options'),
    Output('category-choise', 'options'),
    Input('department-choise', 'value'),
    Input('direction-choise', 'value'),
    Input('group-choise', 'value'),
    Input('category-choise', 'value'),
    prevent_initial_call=True
)
def hierarchy_choiser(*args):
    # Следующий кусок кода формирует датафрейм to_filter, который содержит в себе отфильтрованную иерархию в соответствии с выбором пользователя
    # Так же он содержит колонку с ГУИД-ами категорий, подходящих под фильтр
    dict_of_hierarchy = {}
    for hierarchy_name, hierarchy_elem in zip(['department_name', 'name_2', 'name_3', 'name_4'], args):
        if hierarchy_elem is None:  # не учитываем в фильтре не выбранные значения ( Пандас ругается на None )
            continue
        dict_of_hierarchy[hierarchy_name] = hierarchy_elem
    to_filter = sales_data.hierarchy_df[:]  # копирование датафрейма не изменяя исходный объект методом [:]

    for name, item in dict_of_hierarchy.items():
        if item:
            to_filter = to_filter.merge(pd.DataFrame({name: item}), on=name)
        else:
            continue
    sales_data.filtered_hierarchy_df = to_filter  # сохраняем результат, для работы дашборда в дальнейшем


    # Да через одно место, но работает. Визуализируем возможные варианты выбора
    to_return_dep = sales_data.hierarchy_df['department_name'].dropna().unique()
    to_return_dir = sales_data.hierarchy_df.merge(sales_data.filtered_hierarchy_df, on='department_name')[
        'name_2_x'].dropna().unique()
    to_return_group = sales_data.hierarchy_df.merge(sales_data.filtered_hierarchy_df, on='name_2')[
        'name_3_x'].dropna().unique()
    to_return_cat = sales_data.hierarchy_df.merge(sales_data.filtered_hierarchy_df, on='name_3')[
        'name_4_x'].dropna().unique()

    counter = str(len(sales_data.get_list_products()['product_code'].unique()))
    return_string = 'Количество товаров, подходящих под условие отбора: ' + counter

    return sales_data.get_list_products()['product_code'].unique(), \
        return_string, \
        sorted(list(to_return_dep), key = sort_hierarchy), \
        sorted(list(to_return_dir), key = sort_hierarchy), \
        sorted(list(to_return_group), key = sort_hierarchy),\
        sorted(list(to_return_cat), key = sort_hierarchy)


if __name__ == '__main__':
    app.run_server(debug=True)
