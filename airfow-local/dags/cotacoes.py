from pendulum import datetime

import requests
import pandas as pd
import logging

from io import StringIO
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.decorators import (
    dag,
    task
)

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["bcb"],
)
def fin_cotacoes_bcb_task_flow():
    """
    Essa DAG faz o download de cotações do BCB e salva os valores em um banco POSTGRES.
    """

    @task()
    def extract(**kwargs):
        """
        Extrair dados do BCB
        https://www.bcb.gov.br/estabilidadefinanceira/cotacoestodas
        """
        ds_nodash = kwargs["ds_nodash"]

        base_url = "https://www4.bcb.gov.br/Download/fechamento/"
        full_url = base_url + ds_nodash + ".csv"
        logging.warning(full_url)

        full_url = "https://www4.bcb.gov.br/Download/fechamento/20231208.csv"
        logging.warning(full_url)

        try:
            response = requests.get(full_url)
            if response.status_code == 200:
                csv_data = response.content.decode("utf-8")
                return { "data": csv_data }

            raise Exception("Error getting data from BCB")
        except Exception as e:
            logging.error(e)


    @task(multiple_outputs=True)
    def transform(cotacoes):
        """
        Transformar CSV em DataFrame
        """
        csvStringIO = StringIO(cotacoes.get("data"))

        column_names = [ 
            "DT_FECHAMENTO",
            "COD_MOEDA",
            "TIPO_MOEDA",
            "DESC_MOEDA",
            "TAXA_COMPRA",
            "TAXA_VENDA",
            "PARIDADE_COMPRA",
            "PARIDADE_VENDA"
        ] 
        
        data_types = {
            "DT_FECHAMENTO": str,
            "COD_MOEDA": str,
            "TIPO_MOEDA": str,
            "DESC_MOEDA": str,
            "TAXA_COMPRA": float,
            "TAXA_VENDA": float,
            "PARIDADE_COMPRA": float,
            "PARIDADE_VENDA": float
        }

        parse_dates = ["DT_FECHAMENTO"]

        df = pd.read_csv(
            csvStringIO,
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            header=None,
            names=column_names, 
            dtype=data_types, 
            parse_dates=parse_dates
        )

        df['data_processamento'] = datetime.now()

        return { "cotacoes_df": df }

    @task()
    def load(cotacoes_df):
        """
        Salvar dados em POSTGRES
        """
        table_name = "cotacoes"

        # Criar Tabela
        create_table_ddl = """
            CREATE TABLE if not exists cotacoes (
                dt_fechamento DATE,
                cod_moeda TEXT,
                tipo_moeda TEXT,
                desc_moeda TEXT,
                taxa_compra REAL,
                taxa_venda REAL,
                paridade_compra REAL,
                paridade_venda real,
                data_processamento TIMESTAMP,
                CONSTRAINT table_pk PRIMARY KEY ( dt_fechamento, cod_moeda)
            )
        """

        load_postgres = PostgresOperator(
            task_id="test_conn_postgres",
            postgres_conn_id="postgres_astro",
            sql=create_table_ddl
        )
        load_postgres.execute(context=None)


        postgres_hook= PostgresHook(postgres_conn_id="postgres_astro", schema="astro") 

        rows = list(cotacoes_df.itertuples(index=False))

        postgres_hook.insert_rows(
            table_name,
            rows,
            replace=True,
            replace_index=["DT_FECHAMENTO", "COD_MOEDA"],
            target_fields=["DT_FECHAMENTO", "COD_MOEDA", "TIPO_MOEDA", "DESC_MOEDA", "TAXA_COMPRA", "TAXA_VENDA", "PARIDADE_COMPRA", "PARIDADE_VENDA", "DATA_PROCESSAMENTO"]
        )


    cotacoes_csv = extract()
    cotacoes_df = transform(cotacoes_csv)
    load(cotacoes_df["cotacoes_df"])


fin_cotacoes_bcb_task_flow()