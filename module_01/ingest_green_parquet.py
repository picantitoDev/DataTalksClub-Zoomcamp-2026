#!/usr/bin/env python
import click
import pandas as pd
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default=5432)
@click.option('--pg-db', default='ny_taxi')
@click.option('--target-table', default='green_taxi_trips')
@click.option('--url', required=True, help='Parquet file URL')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, url):

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    df = pd.read_parquet(url)
    df.columns = df.columns.str.lower()

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )

if __name__ == '__main__':
    run()
