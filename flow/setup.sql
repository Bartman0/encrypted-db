--create or replace storage integration azure_integration
--    type = external_stage
--    storage_provider = AZURE
--    enabled = true
--    azure_tenant_id = '<tenant_id>'
--    storage_allowed_locations = ('azure://<account>.blob.core.windows.net/<container>')
--;

--create or replace stage azure_blob
--  url='azure://<account>.blob.core.windows.net/<container>'
--;

--create or replace external table azure_table
--(
--    customer_id as number,
--    customer_name as text,
--    product_id as number,
--    amount as number(18,5)
--)
--integration = 'azure_integration'
--location=@azure_blob
--auto_refresh = true
--file_format = (type = csv)
--;

create or replace table my_table
(
    customer_id number,
    customer_name text,
    product_id number,
    amount number(18,5)
)
;

create or replace file format my_csv_format
    type = csv
    field_delimiter = ';'
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = none
;

copy into my_table
    from 'azure://hvacingest.blob.core.windows.net/data/'
    credentials=(azure_sas_token='?sv=2020-08-04&ss=b&srt=sco&sp=rwdlacitfx&se=2022-04-15T04:31:48Z&st=2022-04-10T20:31:48Z&spr=https&sig=ZwFOBtpWpteFsutrXqUDOEUxoe1%2FhC3U9JyfwZj2RZQ%3D')
    file_format=(format_name=my_csv_format)
;

select *
from da.my_table
;
