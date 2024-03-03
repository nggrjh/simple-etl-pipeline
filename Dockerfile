FROM python:3.8

WORKDIR /app

COPY alembic /app/alembic
COPY alembic.ini /app/alembic.ini

RUN pip install alembic psycopg2-binary

CMD ["alembic", "upgrade", "head"]
