import os

from prefect import Parameter, task, Flow
from prefect.tasks.snowflake.snowflake import SnowflakeQuery

TABLE = Parameter("TABLE", default="my_table")
BLOB_URL = Parameter("BLOB_URL", default="azure://hvacingest.blob.core.windows.net/data/")
SAS_TOKEN = Parameter("SAS_TOKEN", default="sp=rl&st=2022-04-17T10:39:14Z&se=2022-05-01T18:39:14Z&spr=https&sv=2020-08-04&sr=c&sig=9I3nhWW%2FVIkholZAt8l5ZIiLmFq15pfxhTHAfYtQBWs%3D")
SNOWFLAKE_ACCOUNT = Parameter("SNOWFLAKE_ACCOUNT", default="")
SNOWFLAKE_USER = Parameter("SNOWFLAKE_USER", default="")
SNOWFLAKE_PASSWORD = Parameter("SNOWFLAKE_PASSWORD", default="")


@task(log_stdout=True)
def load_from_blob(table, blob_url, sas_token, snowflake_account, snowflake_user, snowflake_password) -> None:
    query = f"""
    copy into {table}
    from '{blob_url}'
    credentials=(azure_sas_token='{sas_token}')
    file_format=(format_name=my_csv_format)
    """
    copy_cmd_result = SnowflakeQuery(account=snowflake_account, user=snowflake_user, password=snowflake_password,
                              database="TEST", schema="PUBLIC", query=query).run()
    for line in copy_cmd_result:
        print(line)


with Flow("Snowflake load") as flow:
    load_from_blob(TABLE, BLOB_URL, SAS_TOKEN,
                   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)

flow.run(parameters=dict(SNOWFLAKE_ACCOUNT=os.environ['SNOWFLAKE_ACCOUNT'],
                         SNOWFLAKE_USER=os.environ['SNOWFLAKE_USER'],
                         SNOWFLAKE_PASSWORD=os.environ['SNOWFLAKE_PASSWORD']))
#flow.visualize()
