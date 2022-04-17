import os
from base64 import urlsafe_b64encode
import hvac
import pandas as pd
import dask.dataframe as dd
from azure.storage.blob import BlobServiceClient
from prefect import task, Flow


class CSV:
    def __init__(self, filename, encrypt_cols):
        self._filename = filename
        self._dataframe = None
        self._encrypt_cols = encrypt_cols
        self._hvac_client = hvac.Client(url='https://localhost:8200',
                                        token=os.environ['VAULT_TOKEN'],
                                        verify=False)
        self._mount_point = os.environ['VAULT_TRANSIT_MOUNT_POINT']
        unseal_response1 = self._hvac_client.sys.submit_unseal_key(os.environ['VAULT_UNSEAL_KEY1'])
        unseal_response2 = self._hvac_client.sys.submit_unseal_key(os.environ['VAULT_UNSEAL_KEY2'])
        unseal_response3 = self._hvac_client.sys.submit_unseal_key(os.environ['VAULT_UNSEAL_KEY3'])
        try:
            read_key_response = self._hvac_client.secrets.transit.read_key(mount_point="transit-richard_kooijman", name='hvac-key')
        except hvac.exceptions.InvalidPath as e:
            self._hvac_client.secrets.transit.create_key(mount_point="transit-richard_kooijman", name='hvac-key')

    def read(self):
        self._dataframe = pd.read_csv(self._filename, header=0, sep=';', index_col=False)

    def encrypt(self):
        #df = dd.from_pandas(self._dataframe, npartitions=10)
        df = self._dataframe
        for c in self._encrypt_cols:
            if c in df.columns:
                df[c] = df[c].apply(vault_encrypt, client=self._hvac_client, mount_point=self._mount_point)
        #self._dataframe = df.compute()

    def upload(self, account, container, name, sas_token):
        blob_service_client = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net",
                                                credential=sas_token)
        container_client = blob_service_client.get_container_client(container)
        data = self._dataframe.to_csv(encoding='utf-8', sep=';', index=None)
        blob_client = container_client.upload_blob(name, data, overwrite=True)
        properties = blob_client.get_blob_properties()
        return properties.last_modified


def vault_encrypt(data, client, mount_point):
    encrypt_data_response = client.secrets.transit.encrypt_data(
        mount_point=mount_point,
        name='hvac-key',
        plaintext=urlsafe_b64encode(str(data).encode()).decode('ascii')
    )
    return encrypt_data_response['data']['ciphertext']


@task
def csv_read(name, encrypt_cols):
    csv = CSV(name, encrypt_cols)
    csv.read()
    return csv

@task
def csv_encrypt(csv):
    csv.encrypt()
    return csv

@task(log_stdout=True)
def csv_upload(csv, name):
    result = csv.upload("hvacingest",
           "data",
           name,
           os.environ['AZURE_SAS_TOKEN']
    )
    print(f"last modified datetime: {result}")
    return csv


with Flow("encrypted CSV ingest") as flow:
    csv = csv_read("./data.csv", ["customer_name", "customer_id"])
    csv = csv_encrypt(csv=csv)
    csv_upload(csv=csv, name="blob2.csv")

flow.run()
