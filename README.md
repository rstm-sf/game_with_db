# Вольный отчет по заданию курса «Нереляционные базы данных»


## Введение

По заданию необходимо было реализовать следующие варианты моделей БД:

1. Реляционная модель данных.
2. Объектно-реляционная модель данных.
3. Документная модель данных.
4. Модель «семейство столбцов».
5. Модель «ключ-значение».
6. Графовая модель данных.

Данные модели были реализованы в следующих БД, соответственно:

1. [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) _(да-да-да, это
СУБД)_.
2. PostgreSQL _(произносится «Пост-Грэс-Кью-Эл»)_.
3. [MongoDB](https://en.wikipedia.org/wiki/MongoDB).
4. [MonetDB](https://en.wikipedia.org/wiki/MonetDB) _(тоже СУБД, пионерская в
мире столбцов, по заверениям)_.
5. [Riak](https://en.wikipedia.org/wiki/Riak) и
[Redis](https://en.wikipedia.org/wiki/Redis) _(бессмысленно и беспощадно)_.
6. [ArangoDB](https://en.wikipedia.org/wiki/ArangoDB) _(этакий «комбайн»)_.


## Основная часть

В качестве формирования базы использовались открытые данные и были взяты
[отсюда](https://opengovdata.ru/dataset/crimestatsocial/resource/bed46ea9-28f5-4c4f-b30f-bd96251e2012).

Серверная часть поднята на Ubuntu 16.04, клиентская часть написана на языке
Python 3 _(формирование базы расположено в `src/<db>/`)_, для наглядности
примеры запросов написаны в
[jupyter-тетрадках](https://en.wikipedia.org/wiki/Project_Jupyter)
_(расположены в `notebooks/`)_. Поэтому базы данных выбирались из следующих
предпочтений: Ubuntu, официальный или рекомендуемый клиент для языка Python,
без JVM _(не хотелось устанавливать еще что-то)_.


### Реляционная модель данных

Наглядно посмотреть тетрадку можно по данной
[ссылке](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/postgres.ipynb).


### Объектно-реляционная модель данных

Наглядно посмотреть тетрадку можно по данной
[ссылке](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/postgres_obj.ipynb).


### Документная модель данных

Наглядно посмотреть тетрадку можно по данной
[ссылке](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/mongo.ipynb).


### Модель «семейство столбцов»

Наглядно посмотреть тетрадку можно по данной
[ссылке](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/monet.ipynb).


### Модель «ключ-значение»

Наглядно посмотреть тетрадку можно по данным ссылкам:
[раз](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/riak.ipynb),
[два](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/redis.ipynb).


### Графовая модель данных

Наглядно посмотреть тетрадку можно по данной
[ссылке](https://nbviewer.jupyter.org/urls/bitbucket.org/rstm-sf/game_with_db/raw/59c094f920bf04dde6fa2408d25432d067325f95/notebooks/arango.ipynb).
