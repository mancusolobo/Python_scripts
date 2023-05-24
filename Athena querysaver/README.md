# Athena query saver
Un script con la función de buscar los archivos de consultas _.sql_ en determinado directorio, los ejecuta en Amazon Athena y luego descarga los resultados _.csv_ en otra carpeta.  
Utiliza _boto3_ para el manejo de los servicios de AWS, _pathlib_ para la adminitración de archivos y directorios y _time_ para los tiempos de espera.  
Nos sirve de mucho en el proceso de descarga y actualizacion de consultas mas el plus de performance en cuanto a descarga e importación de informacion respecto a otros metodos u librerias como _pyAthena_.
