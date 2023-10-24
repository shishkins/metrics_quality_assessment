def table(func):
    def inner(*args, **kwargs):
        # что-то
        print('start decorator..')
        func(*args, **kwargs)
        print('end decorator')

    return inner


def say(value):
    print(value)


def sqrt(x):
    '''
    Функция возвращает значения х в квадрате
    :param x:
    :return:
    '''
    return x ** 2


say = table(say)

say('hello')
