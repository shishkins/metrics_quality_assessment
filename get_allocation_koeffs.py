import numpy as np
import pandas as pd


def calculate_allocation_koeffs(df, filters, params_of_interval):
    # Создаем pd.cut объект с интервалами
    koeffs_df = df[
        ['koef_change_sale',
         'koef_change_revenue',
         'koef_change_profit',
         'current_price_date',
         'product_code'
        ]
    ].drop_duplicates(subset=['product_code', 'current_price_date'])

    for column, filter in filters.items():
        koeffs_df = koeffs_df.merge(filter,
                                    on=column) # фильтруем датафрейм с коэффициентами по колонке, указанной при обращении к функции

    koeffs_df.drop(columns = ['reprice_flag', 'product_id', 'class_product', 'current_price'], inplace=True)
    koeffs_df.drop_duplicates(inplace= True)

    indicator_for_coeffs = pd.DataFrame(
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
            ],
            'all_koeffs_nulls': [
                koeffs_df.loc[
                    (koeffs_df['koef_change_profit'] == 0)
                    & (koeffs_df['koef_change_sale'] == 0)
                    & (koeffs_df['koef_change_revenue'] == 0),
                    'koef_change_profit'
                ].count()
            ]
        }
    )

    # создание интервалов
    array_of_intervals = np.linspace(start=params_of_interval['min'], stop=params_of_interval['max'],
                                     num=params_of_interval['step'])  # ручное задание интервалов
    array_of_intervals = np.concatenate((np.array([params_of_interval['over_min']]), array_of_intervals,
                                         np.array([params_of_interval['over_max']])),
                                        axis=0)  # с обеих сторон прицепляем "куски" больших интервалов
    array_of_intervals = np.round(array_of_intervals, 2)
    array_up = array_of_intervals[0:-1]  # создание массива верхних значений интервалов
    array_down = array_of_intervals[1::]  # создание массива нижних значений интервалов
    bins = pd.IntervalIndex.from_arrays(array_up,
                                        array_down)  # фокус-покус, два массива превратились в IntervalIndex

    # koeffs_df = koeffs_df.merge(self.filtered_products_koeffs, on = 'product_code')

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

    table_reprices_koefs_df = pd.DataFrame(
        {
            ("Количество переоценок, у которых хотя бы один из коэффициентов равен нулю", "В кол."): {
                'Коэффициент изменения продаж ("КПРОД")': indicator_for_coeffs['koef_change_sale_nulls'],
                'Коэффициент изменения прибыли ("КПРИБ")': indicator_for_coeffs['koef_change_profit_nulls'],
                'Коэффициент изменения оборота ("КОБ")': indicator_for_coeffs['koef_change_revenue_nulls'],
            },
            ("Количество переоценок, у которых хотя бы один из коэффициентов равен нулю", "в %"): {
                'Коэффициент изменения продаж ("КПРОД")': round(
                    (indicator_for_coeffs['koef_change_sale_nulls'] /
                     indicator_for_coeffs['all_reprices_count']) * 100, 3),
                'Коэффициент изменения прибыли ("КПРИБ")': round(
                    (indicator_for_coeffs['koef_change_profit_nulls'] /
                     indicator_for_coeffs['all_reprices_count']) * 100
                    , 3),
                'Коэффициент изменения оборота ("КОБ")': round(
                    (indicator_for_coeffs['koef_change_revenue_nulls'] /
                     indicator_for_coeffs['all_reprices_count']) * 100, 3),
            },
            ("Количество переоценок, у которых только один коэффициент равен нулю", "В кол."): {
                'Коэффициент изменения продаж ("КПРОД")': indicator_for_coeffs[
                    'koef_change_sale_nulls_one'],
                'Коэффициент изменения прибыли ("КПРИБ")': indicator_for_coeffs[
                    'koef_change_profit_nulls_one'],
                'Коэффициент изменения оборота ("КОБ")': indicator_for_coeffs[
                    'koef_change_revenue_nulls_one'],
            },
            ("Количество переоценок, у которых только один коэффициент равен нулю", "в %"): {
                'Коэффициент изменения продаж ("КПРОД")': round(
                    (indicator_for_coeffs['koef_change_sale_nulls_one'] /
                     indicator_for_coeffs['all_reprices_count']) * 100, 3),
                'Коэффициент изменения прибыли ("КПРИБ")': round(
                    (indicator_for_coeffs['koef_change_profit_nulls_one'] /
                     indicator_for_coeffs['all_reprices_count']) * 100, 3),
                'Коэффициент изменения оборота ("КОБ")': round(
                    (indicator_for_coeffs['koef_change_revenue_nulls_one'] /
                     indicator_for_coeffs['all_reprices_count']) * 100, 3),
            }
        }
    )

    table_reprices_koefs_df.index.set_names(
        f"Всего переоценок: {indicator_for_coeffs['all_reprices_count'].iloc[0]}"
            ,
        inplace=True)



    return koeff_counts,indicator_for_coeffs,table_reprices_koefs_df
