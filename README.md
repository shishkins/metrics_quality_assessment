# metrics_quality_assesment



## Что это такое?

Данный репозиторий содержит:

- Файл извлечения данных <b>get_dataframes.py</b> , содержащий функцию <b>get_data()</b>
- Файл сохранения запроса в csv файлы <b>query_executer.py</b>, содержащий функцию <b>csv_execute()</b>
- Файл аутентификации в базах данных ClickHouse и PostgreSQL <b>get_env_auth.py</b>
- Точка входа для работы дашбдорда <b>main.py</b>


## Как использовать?

1. Клонируете репозиторий;<br>
2. ``` pip install -r requirements.txt```
3. Запустите файл <b>get_env_auth.py</b>, предварительно расположив в репозитории файл с логином паролем для подключения к базе
4. В файле <b>query_executer.py</b> измените переменную rewrite = True и запустите скрипт;
5. После отработки скрипта, rewrite = False. (в комментарии указано, когда менять на True);
6. Запустите скрипт <b>main.py</b>
7. Переходите сюда в [локальный хост](http://127.0.0.1:8050/)



## Как это работает?
    Данный раздел рекомендуется к прочтению, если вам интересно устройство работы с дашбордами на PlotlyDash,
    которое я посчитал удобным. Если есть предложения по усовершенствованию работы кода - буду рад обратной связи)


1. При запуске <b>main.py</b> происходит инициализация объекта модели данных, 
<br>конструирование разметки для визуализации дашборда, инициализация функции обратной связи (обратного вызова "callback");
2. Создание объекта модели данных, в данном случае <b>sales_data</b>, происходит благодаря функции <b>get_data()</b>
3. <b>get_data()</b> в свою очередь генерирует <b>словарь</b> из датафреймов <i>"dict_of_dataframes"</i>, имеющий следующую структуру:
<br>{'ключ как название датафрейма' : объект датафрейма ...}
<br> Данный словарь формируется на основании ".csv" файлов, полученных из папки "csv"
4. ".csv" файлы формируются на основании запросов из функции <b>csv_execute()</b>
<br><b>csv_execute()</b> в свою очередь содержит словарь запросов <i>"dict_of_queries"</i>, имеющий следующую структуру:<br>
{'ключ как название запроса' : строка запроса ...}
<br><b>Здесь важное замечание:</b>
<br>Между словарем запросов <i>"dict_of_queries"</i> и словарем датафреймов <i>"dict_of_dataframes"</i> есть связь:
<br>Название запроса соответствует названию ".csv" файла, куда он будет сохранен, и соответственно
<br>В модели данных датафрейм, прочитанный с помощью ".csv" файла, будет иметь такое же название, только с суффиксом "_df"

Пример:

    
    Вы создали словарь запросов dict_of_queries = {
        'sales' : ''' ... '''
    }
    Данный запрос выполняется и сохраняется в файл "sales.csv"
    Затем при чтении .csv файла формируется датафрейм dict_of_dataframes = {
    'sales_df': sales_df
    }
    и возвращается функцией get_data()

5. После чтения файлов, словарь датафреймов отправляется в конструктор класса <b>data_lake()</b>, для создания модели данных.<br>
Внутри неё пользователь может определить собственные переменные объекта класса <b>data_lake()</b>, фильтры и т.д.
6. Формируется <b>app.layout</b>, содержащий в себе объекты библиотек :<br><b>dash_core_components</b> - визуальные объекты, см. офф. документацию
;<br><b>dash_bootstrap_components</b> - то же самое, но содержащий в себе интеграцию с фреймворком bootstrap (CSS, HTML);
<br><b>dash</b> - тут много всего, объекты <b>Dash, html, dash_table, dcc, callback, Output, Input</b>
7. Определяется функция обратного вызова с декоратором <b>@callback</b>, таких функций можно сделать сколь - угодно


@Callback достаточно неочевидная штука, как я понял:<BR>
Декоратор <B>@callback</B> содержит в себе параметры <B>Input</B> и <B>Output</B>, которые были импортированы из dash;
В <B>Input</B>, <B>Output</B> передается 2 параметра, <b>'component-id'</b> и <b>'component-property'</b>
первое - id графического элемента, второе - тип возвращаемых значений (value,children и т.д., см. офф. док.)

Работает это так:
```
@callback(
Output('fig-id','figure')
Input('list-id','value')
)
def update_fig(check_value):
    ...
    return go.Figure(go.Scatter(x = ...))
```
Принцип работы: ежесекундно подаем в функцию <B>update_fig()</B> значение <B>check_value</B>,
которое формируется благодаря <B>Input</B> (он берет состояние кнопки визуального элемента,
ссылаясь на 'list-id')<br>
В ходе функции мы что-то делаем, а потом возвращаем фигуру,
потому что указали <b>Output</b> тип возвращаемого значения 'figure'

В ходе работы программы, функции <b>update_fig</b> и <b>update_koeffs</b> постоянно обновляют модель данных,<br>
вернее её собственные переменные, содержащие выбранные пользователем значения.<br>
После обновления всех фильтрующих переменных, формируются итоговый датафрейм, который нужно отобразить на графиках
