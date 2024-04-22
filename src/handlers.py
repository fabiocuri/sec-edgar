import pandas as pd
from google.cloud import bigquery


def upload_table_to_bigquery(bigquery_client, dataset_ref, df, table_name):
    df = df.copy()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r"[^A-Za-z0-9_\s]", "", regex=True)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_")
    df.fillna("", inplace=True)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    table_ref = dataset_ref.table(table_name)

    job = bigquery_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )

    job.result()


def read_table_as_dataframe(bigquery_client, dataset_ref, table_name):
    table_ref = dataset_ref.table(table_name)
    table = bigquery_client.get_table(table_ref)
    rows = bigquery_client.list_rows(table)

    data = [row.values() for row in rows]
    column_names = [field.name for field in rows.schema]

    # Creating DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df
