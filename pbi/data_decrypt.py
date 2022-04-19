import os 
import base64
import pandas as pd
import hvac
import snowflake.connector

hvac_client = hvac.Client(url='http://10.211.55.2:8200',
                          token=os.environ['VAULT_TOKEN'],
                          verify=False)
mount_point = os.environ['VAULT_TRANSIT_MOUNT_POINT']

connection = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    database='TEST',
    schema='PUBLIC'
)


def vault_decrypt(data, client, mount_point):
    decrypt_data_response = client.secrets.transit.decrypt_data(
        mount_point=mount_point,
        name='hvac-key',
        ciphertext=data
    )
    return base64.b64decode(decrypt_data_response['data']['plaintext'].encode('ascii')).decode()


cursor = connection.cursor()
cursor.execute('select CUSTOMER_ID, CUSTOMER_NAME, PRODUCT_ID, AMOUNT from my_table')

df = cursor.fetch_pandas_all()

for c in ['CUSTOMER_ID', 'CUSTOMER_NAME']:
    if c in df.columns:
        df[c] = df[c].apply(vault_decrypt, client=hvac_client, mount_point=mount_point)

print(df)
