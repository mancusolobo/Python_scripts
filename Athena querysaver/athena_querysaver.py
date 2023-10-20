from pathlib import Path as pt
import boto3
import time
import json

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

path_consultas = pt.home() / r"directory/consultas"
path_data = pt.home() / r"directory/data"
pass_file = path_consultas.parent / r"./aws.conf"

try:
    aws_cred = json.loads(pass_file.read_text())
except FileNotFoundError:
    input(f"No se encontro el archivo de configuracion. Revise el mismo.")
    exit()
except json.JSONDecodeError:
    input(f"Las credenciales estan en un formato erroneo. Revise las mismas.")
    exit()
except Exception as err:
    input(f"Error desconocido. Favor revisar archivo de configuracion.\nDetalles del error:\n{err}")
    exit()

clientes = crear_cliente(aws_cred['aws_access_key'], aws_cred['aws_secret_key'], aws_cred['aws_region'], "athena", "s3")

if path_consultas.exists() and path_data.exists():
    
    consultas = list(path_consultas.glob("*.sql"))
    print(f"Bienvenido al botcito de AWS!")
    
    if consultas:
        filename_max = max([len(qfile.stem) for qfile in consultas])
        print(f"Se encontraron {len(consultas)} consultas para descargar.")
    else:
        input(f"No se encontraron consultas para descargar. Presione enter para salir.")
        exit()
    
    for query_file in consultas:
        try:
            start_time = time.time()
            query_response = ejecutar_query(clientes['athena'], query_file.read_text(), aws_cred['schema_name'], aws_cred['s3_staging_dir'])
            bajar_archivo(clientes['s3'], aws_cred['s3_bucket_name'], query_response, path_data / f"{query_file.stem}.csv")
            segundos = time.time() - start_time
        except clientes['athena'].exceptions.InvalidRequestException as err:
            print(f"{query_file.stem:<{filename_max}} no ejecutada, se procede a la proxima. Error:\n{err}.")
        except clientes['athena'].exceptions.ClientError as err:
            input(f"No se pudo generar el cliente. Verifique credenciales en la configuracion.\nEnter para salir.")
            break
        except Exception as err:
            input(f'Se ha producido un error inesperado. Presione para salir.\nDetalles del error:\n{err}')
            exit()
        else:
            print(f"{query_file.stem:<{filename_max}} ejecutada ({round(segundos, 2)}s).")
    else:
        input("Se han descargado todas las consultas. Presione enter para salir.")
else:
    input("Directorios de consulta y/o data no encontrados. Presione enter para salir.")