from pathlib import Path as pt
import boto3
import time

def crear_cliente(aws_access_key, aws_secret_key, aws_region, *args):
    aws_clients = dict()
    for arg in args:
        client = boto3.client(
        arg,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
        )
        aws_clients[arg] = client
    return aws_clients

def ejecutar_query(cliente, query, database, output):
    
    query_response = cliente.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": database, "Catalog":"AwsDataCatalog"},
    ResultConfiguration={
        "OutputLocation": output,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
        },
    )
    
    while True:
        try:
            # This function only loads the first 1000 rows
            cliente.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    
    return query_response

def bajar_archivo(s3_client, s3_bucket_name, query_response, temp_file_location):
    
    s3_client.download_file(
        s3_bucket_name,
        f"{query_response['QueryExecutionId']}.csv",
        temp_file_location
        )

aws_access_key = ""
aws_secret_key = ""
schema_name = "db_fake_analytics"
s3_staging_dir = "s3://athena-fake-prod"
aws_region = "us-east-1"
s3_bucket_name = "athena-fake-prod"
s3_output_directory = "s3://athena-fake-prod"
temp_file_location = "athena_query_results.csv"

path_consultas = pt.home() / "directory/consultas"
path_data = pt.home() / "directory/data"

clientes = crear_cliente(aws_access_key, aws_secret_key, aws_region, "athena", "s3")

if path_consultas.exists() and path_data.exists():
    consultas = path_consultas.glob("*.sql")
    filename_max = max([len(qfile.stem) for qfile in consultas])
    
    for query_file in consultas:
        try:
            query_response = ejecutar_query(clientes['athena'], query_file.read_text(), schema_name, s3_staging_dir)
            bajar_archivo(clientes['s3'], s3_bucket_name, query_response, path_data / f"{query_file.stem}.csv")
        except clientes['athena'].exceptions.InvalidRequestException as err:
            print(f"{query_file.stem:<{filename_max}} no ejecutada: {err}.")
        else:
            print(f"{query_file.stem:<{filename_max}} ejecutada.")
    input("Se han descargado todas las consultas. Presione enter para salir.")
else:
    input("Directorios de consulta y/o data no encontrados. Presione enter para salir.")