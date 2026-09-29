# Исходники для <a href="https://ewdim.notion.site/Postgre-ClickHouse-c759b75bd5f546d3a79df8e1dcc6caaa">статьи</a> о кэшировании запросов PostgreSQL в ClickHouse

Чтобы развернуть стенд локально, нужно:

1. Запустить контейнеры postgredb и clickhousedb-server в Docker
2. Применить миграции Postgres из `./db_scripts/create_db.sql`.
3. Запустить контейнер app
