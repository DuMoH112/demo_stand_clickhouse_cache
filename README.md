# Исходники для <a href="https://ewdim.notion.site/Postgre-ClickHouse-c759b75bd5f546d3a79df8e1dcc6caaa">статьи</a>

Чтобы развернуть данный стенд у себя на машине нужно:

1. Запустить контейнеры postgredb и clickhousedb-server в Docker
2. Провести нужные миграции в Postgre. (миграции расположены по пути ./db_scripts/create_db.sql)
3. Запустить контейнер app
