# 🔧 ETL Flow

Laboratório de **engenharia de dados** com dois ambientes locais: orquestração com **Apache Airflow (Astronomer)** e processamento com **Apache Spark / Delta Lake** em notebooks.

## Conteúdo

| Pasta | O que tem |
| --- | --- |
| [`airfow-local/`](airfow-local) | Projeto Astro CLI com a DAG `fin_cotacoes_bcb_task_flow`: baixa diariamente as cotações de fechamento do **Banco Central (BCB)**, trata com pandas e grava em **PostgreSQL** |
| [`spark-local/DeltaSpark/`](spark-local/DeltaSpark) | Notebook que cria uma tabela **Delta Lake** (`RAW/CLIENTES`), mostrando escrita, versões e `_delta_log` |
| [`spark-local/PrecoGasolina/`](spark-local/PrecoGasolina) | Análise com PySpark (DataFrame API e SQL) de preços de gasolina, etanol e GLP da ANP |

## Tecnologias

Apache Airflow · Astro CLI · Docker · PostgreSQL · PySpark · Delta Lake · pandas · Jupyter

## Como rodar

**Airflow**

```bash
cd airfow-local
astro dev start        # UI em http://localhost:8080
```

Crie no Airflow (Admin → Connections) a conexão `postgres_astro` apontando para o seu PostgreSQL (schema `astro`).

**Spark**

```bash
cd spark-local/DeltaSpark      # ou PrecoGasolina
pip install pyspark delta-spark jupyter
jupyter notebook
```

## Autor

Feito por [Amós Magalhães](https://github.com/amslimaa).
