# encoding=utf8
import sys
import os
#import parametros as param
import boto3
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import dateutil.relativedelta
import pytz
########################
datos_mcp = {'proceso' : '1632251091|cdr_bt_voz_volte_etl_kpi_llamadas_no_clientes|lambda-glue',
            'id_tarea' : '1632251397'}

datos_entrada = {
    'nombre_proceso' : 'ETL KPI LLAMADAS',
    'particion_salida':'anio_mes'
}

#dev
buckets_bd = {
    's3_bucket_landingzone' : 's3://entel-datalake-cl-landingzone-dev/',
    's3_bucket_analytics' : 's3://entel-datalake-cl-analytics-dev/',
    's3_bucket_output' : 's3://entel-datalake-cl-output-dev/',
    'base_datos_landing' : 'db_entel_landingzone_dev',
    'base_datos_analytics' : 'db_entel_analytics_dev'
}

directorios = {
    'directorio_salida_entrantes' : 'cdr_bt_kpi_llamadas_entrantes_no_clientes',
    'directorio_salida_salientes' : 'cdr_bt_kpi_llamadas_salientes_no_clientes',
    'directorio_volte' : 'cdr_bt_volte',
    'directorio_voz' : 'cdr_bt_voz',
    'directorio_mc' : 'lk_movil_compania',
    'directorio_mv' : 'bt_moviles_vigentes',
    'directorio_redes_celdas' : 'bt_redes_celdas'
}

columnas = {
    'cols_volte' : ['movil', 'movil_origen','movil_destino', 'first_cell_id','tipo_llamada', 'orig_cant_duracion', 'termin_cant_duracion', 'dia'],
    'cols_voz' : ['movil', 'movil_origen','movil_destino', 'first_cell_id','tipo_llamada', 'duracion', 'dia'],
    'cols_movil_cia' : ['movil', 'nombre_compania'],
    'cols_movil_vig' : ['descr_movil', 'desc_mercadocliente', 'id_dia_corte'],
    'cols_redes_celdas' : ['cell_id','comuna'],
    'string_cols_entrantes' : ['nombre_compania','desc_mercadocliente','comuna_cliente'],
    'string_cols_salientes' : ['nombre_compania','desc_mercadocliente','comuna_cliente']
}
#########################
def leer_parametros():
    proceso = datos_mcp['proceso']
    id_tarea = datos_mcp['id_tarea']
    id_proceso = proceso+'|'+id_tarea
    nombre_proceso = datos_entrada['nombre_proceso']
    particion_salida = datos_entrada['particion_salida']
    s3_bucket_landingzone = buckets_bd['s3_bucket_landingzone']
    s3_bucket_analytics = buckets_bd['s3_bucket_analytics']
    s3_bucket_output = buckets_bd['s3_bucket_output']
    base_datos_landing = buckets_bd['base_datos_landing']
    base_datos_analytics = buckets_bd['base_datos_analytics']
    directorio_salida_entrantes = directorios['directorio_salida_entrantes']
    directorio_salida_salientes = directorios['directorio_salida_salientes']
    directorio_volte = directorios['directorio_volte']
    directorio_voz = directorios['directorio_voz']
    directorio_mc = directorios['directorio_mc']
    directorio_mv = directorios['directorio_mv']
    directorio_redes_celdas = directorios['directorio_redes_celdas']
    cols_volte = columnas['cols_volte']
    cols_voz = columnas['cols_voz']
    cols_movil_cia = columnas['cols_movil_cia']
    cols_movil_vig = columnas['cols_movil_vig']
    cols_redes_celdas = columnas['cols_redes_celdas']
    string_cols_entrantes = columnas['string_cols_entrantes']
    string_cols_salientes = columnas['string_cols_salientes']
    return proceso,id_tarea,id_proceso,nombre_proceso,particion_salida,s3_bucket_landingzone,s3_bucket_analytics,s3_bucket_output,base_datos_landing,base_datos_analytics,directorio_salida_entrantes,directorio_salida_salientes,\
    directorio_volte,directorio_voz,directorio_mc,directorio_mv,directorio_redes_celdas,cols_volte,cols_voz,cols_movil_cia,cols_movil_vig,cols_redes_celdas,string_cols_entrantes,string_cols_salientes

#Función para actualizar estado e insertar errores en mcp.
def updateMCP(estado, error, id_tarea):
    from datetime import datetime, timedelta
    import dateutil.tz
    region = 'us-east-1'
    delimitador_llave = '|'
    tabla_error_proceso = 'mcp_control_error'
    tabla_exito_proceso = 'mcp_control_proceso'
    time_zone = dateutil.tz.gettz('Chile/Continental')
    formato_fecha_proceso = '%Y%m%d%H%M%S'
    formato_fecha_inicio = '%Y-%m-%d %H:%M:%S'
    formato_fecha_actualizacion = '%Y-%m-%d %H:%M:%S'
    fecha_proceso = datetime.now(time_zone).strftime(formato_fecha_proceso)
    fecha_inicio = datetime.now(time_zone).strftime(formato_fecha_inicio)
    fecha_proceso_form_exito = datetime.now(time_zone).strftime(formato_fecha_actualizacion)
    #Donde-> estado: Estado del job, error: Error capturado.
    fecha_actualizacion = datetime.now(time_zone).strftime(formato_fecha_actualizacion)
    #Se define un recurso del tipo dynamodb en la región us-east-1 (N.Virginia)
    dynamodb = boto3.resource('dynamodb', region_name=region)
    #En caso de existir error se inserta un nuevo item en la tabla mcp_control_error.
    id_proceso_tarea = id_proceso + delimitador_llave + str(id_tarea)
    table = dynamodb.Table(tabla_error_proceso)
    table_exito = dynamodb.Table(tabla_exito_proceso)
    if (error=='1'):
        response = table.update_item(
            Key={'id_proceso_tarea': id_proceso_tarea,'fecha_proceso': fecha_proceso},
            UpdateExpression="SET fecha_error= :var1, desc_error= :var2",
            ExpressionAttributeValues={':var1': fecha_actualizacion, ':var2': estado},
            ReturnValues="UPDATED_NEW")
    else:
        response = table_exito.update_item(
            Key={'id_proceso': id_proceso_tarea,'fecha_proceso': fecha_proceso},
            UpdateExpression="SET id_tarea_actual= :var1, fecha_actualizacion= :var2, fecha_termino= :var3, detalle= :var4, fecha_inicio= :var5, estado= :var6",
            ExpressionAttributeValues={':var1': id_tarea,':var2': fecha_actualizacion,':var3': fecha_actualizacion,':var4': estado,':var5': fecha_proceso_form_exito,':var6': 'OK'},
            ReturnValues="UPDATED_NEW")
    return True

def obtener_bucket(s3_bucket):
    nombre_bucket = s3_bucket.replace('s3://','').replace('/','')
    return nombre_bucket

def obtener_prefijo_particion(particion_salida, valor_particion):
    prefijo = ('{0}={1}/'.format(particion_salida, valor_particion))
    return prefijo

def eliminar_archivos_particion(s3_bucket, directorio, particion_salida, valor_particion):
    nombre_bucket = obtener_bucket(s3_bucket)
    prefijo = obtener_prefijo_particion(particion_salida, valor_particion)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(nombre_bucket)
    prefijo = '{}/{}'.format(directorio,prefijo)
    print('prefijo final: {}'.format(prefijo))
    bucket.objects.filter(Prefix=prefijo).delete()
    return True

def llenar_nulls(df, list_string_cols, list_numbers_cols):
    print('Aplicando regla fill NI y 0')
    for column in list_string_cols:
        df = df.withColumn(column, when(((col(column).isNull()) | (trim(col(column))=='')), lit('NI')).otherwise(col(column)))
    for column in list_numbers_cols:
        df = df.withColumn(column, when(((col(column).isNull()) | (trim(col(column))=='')), lit(0)).otherwise(col(column)))
    return df

#anio_mes_duc = str(datetime.datetime.strptime(anio_mes, '%Y-%m-%d') - dateutil.relativedelta.relativedelta(days=1))[:7]
def obtener_fecha_cercana(rutaS3tabla, particion_fecha, fecha, formato_fecha_salida):
    fecha_orig = fecha
    i = 0
    intentos = 150
    if ((len(fecha)==7) and (formato_fecha_salida=='anio_mes_dia')):#tipo mensual
        fecha = fecha+'-01'
        fecha = str(datetime.datetime.strptime(fecha, '%Y-%m-%d') + dateutil.relativedelta.relativedelta(months=1))[:10]
    for i in range(intentos):
        try:
            #print('Intentando leer la ruta {} con la fecha {}'.format(rutaS3+tabla,fecha))
            df = spark.read.parquet(rutaS3tabla + "/" + particion_fecha + "=" + fecha + "/")
            return fecha
        except Exception as e:
            if (formato_fecha_salida=='anio_mes_dia'):
                fecha = str(datetime.datetime.strptime(fecha, '%Y-%m-%d') - dateutil.relativedelta.relativedelta(days=1))[:10]
            elif (formato_fecha_salida=='anio_mes'):
                fecha = str(datetime.datetime.strptime(fecha, '%Y-%m') - dateutil.relativedelta.relativedelta(days=1))[:7]
            #print('fecha no encontrada, probando una anterior...')
    print('no encontada la fecha {} ni hasta el {} para la ruta {}'.format(fecha_orig, fecha, rutaS3+tabla))
    return fecha_orig

def crear_spark(nombre_proceso):
    spark = SparkSession \
            .builder \
            .appName(nombre_proceso)\
            .config("spark.memory.offHeap.size","6g") \
            .config("spark.memory.offHeap.enabled", True)\
            .config("spark.dynamicAllocation.maxExecutors=10")\
            .config("spark.yarn.executor.memoryOverhead=3096")\
            .enableHiveSupport() \
            .getOrCreate()
    return spark

def leer_fuentes(s3_bucket_landingzone,directorio_redes_celdas,directorio_mv,directorio_volte,directorio_voz,directorio_mc,fecha_proceso):
    fecha_redes_celdas = obtener_fecha_cercana(s3_bucket_landingzone+directorio_redes_celdas, 'dia', fecha_proceso, 'anio_mes_dia')
    df_redes_celdas = spark.read.parquet('{}{}'.format(s3_bucket_landingzone, directorio_redes_celdas)).filter(col('dia')==fecha_redes_celdas).\
    select(*cols_redes_celdas).withColumnRenamed('comuna', 'comuna_cliente')
    fecha_mov_vig = obtener_fecha_cercana(s3_bucket_landingzone+directorio_mv, 'id_dia_corte', fecha_proceso,'anio_mes_dia')
    df_mov_vig = spark.read.parquet('{}{}'.format(s3_bucket_landingzone, directorio_mv)).filter(col('id_dia_corte')==fecha_mov_vig).\
    select(*cols_movil_vig).withColumnRenamed('descr_movil', 'movil')
    df_volte = spark.read.parquet('{}{}'.format(s3_bucket_landingzone, directorio_volte)).filter(col('dia').like('{}%'.format(fecha_proceso))).filter((upper(col('tipo_llamada'))=='MSORIGINATING') | (upper(col('tipo_llamada'))=='MSTERMINATING')).\
    select(*cols_volte).withColumn('duracion', when((col('orig_cant_duracion')>0), col('orig_cant_duracion')).otherwise(col('termin_cant_duracion')))
    df_voz = spark.read.parquet('{}{}'.format(s3_bucket_landingzone, directorio_voz)).filter(col('dia').like('{}%'.format(fecha_proceso))).filter((upper(col('tipo_llamada'))=='MSORIGINATING') | (upper(col('tipo_llamada'))=='MSTERMINATING')).select(*cols_voz)
    df_llamadas = df_volte.select(*cols_voz).union(df_voz).withColumn('duracion', col('duracion')/60)
    df_llamadas = df_llamadas.withColumn('minutos_salida', when((col('movil'))==(col('movil_origen')), col('duracion')).otherwise(0)).\
    withColumn('minutos_entrada', when((col('movil'))!=(col('movil_origen')), col('duracion')).otherwise(0)).\
    withColumn('tipo_llamada', when((col('movil')==col('movil_origen')), lit('saliente')).otherwise(lit('entrante'))).\
    withColumnRenamed('first_cell_id','cell_id')
    df_cliente_comuna = df_llamadas.join(df_redes_celdas,['cell_id'],'left').select('movil','comuna_cliente','duracion')
    df_cliente_comuna = df_cliente_comuna.groupBy('movil','comuna_cliente').agg(round(sum(col('duracion')),2).alias('duracion')).orderBy(col('duracion').desc(),col('movil').desc())
    df_cliente_comuna = df_cliente_comuna.groupBy('movil').agg(first(col('comuna_cliente')).alias('comuna_cliente'))
    df_llamadas_agg = df_llamadas.groupBy('movil','movil_origen','movil_destino','tipo_llamada').\
    agg(sum(col('minutos_salida')).alias('total_minutos_salida'), sum(col('minutos_entrada')).alias('total_minutos_entrada'))
    df_llamadas_agg2 = df_llamadas_agg.withColumn('total_minutos_salida', round(col('total_minutos_salida'),2)).\
    withColumn('total_minutos_entrada', round(col('total_minutos_entrada'),2))
    #fecha_lk_cia='2021-09-09'
    fecha_lk_cia = obtener_fecha_cercana(s3_bucket_landingzone+directorio_mc, 'ingest_date', fecha_proceso,'anio_mes_dia')
    df_movil_cia = spark.read.parquet('{}{}'.format(s3_bucket_landingzone, directorio_mc)).\
    filter(col('ingest_date')==fecha_lk_cia).select(*cols_movil_cia).withColumnRenamed('movil', 'movil_cia')
    df_llamadas_cia_entrantes = df_llamadas_agg2.filter(col('tipo_llamada')=='entrante').join(df_movil_cia, [df_llamadas_agg2.movil_origen==df_movil_cia.movil_cia], 'left').\
    filter(((~(col('nombre_compania').like('ENTEL%'))) ))
    df_llamadas_cia_salida = df_llamadas_agg2.filter(col('tipo_llamada')=='saliente').join(df_movil_cia, [df_llamadas_agg2.movil_destino==df_movil_cia.movil_cia], 'left').\
    filter(((~(col('nombre_compania').like('ENTEL%'))) ))
    df_llamadas_cia = df_llamadas_cia_entrantes.union(df_llamadas_cia_salida)
    ##agrupacion de datos
    return df_llamadas_cia,df_mov_vig,df_cliente_comuna

#llamadas salientes
def llamadas_salientes(df_llamadas_agg2,df_mov_vig,df_redes_celdas):
    df_llamadas_agg3 = df_llamadas_agg2.filter(~(col('total_minutos_entrada')>0.0))
    df_llamadas_agg_key = df_llamadas_agg3.withColumn('key', concat(col('movil'),col('movil_destino'))).\
    orderBy(col('movil'),col('total_minutos_salida').desc())
    df_top1 = df_llamadas_agg_key.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_destino')).alias('movil_destino'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_salida')).alias('total_minutos_salida'))
    df_llamadas_agg_key1 = df_llamadas_agg_key.join(df_top1.select('key'),['key'],'leftanti').orderBy(col('total_minutos_salida').desc())
    df_top2 = df_llamadas_agg_key1.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_destino')).alias('movil_destino'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_salida')).alias('total_minutos_salida'))
    df_llamadas_agg_key2 = df_llamadas_agg_key1.join(df_top2.select('key'),['key'],'leftanti').orderBy(col('total_minutos_salida').desc())
    df_top3 = df_llamadas_agg_key2.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_destino')).alias('movil_destino'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_salida')).alias('total_minutos_salida'))
    df_llamadas_agg_key3 = df_llamadas_agg_key2.join(df_top3.select('key'),['key'],'leftanti').orderBy(col('total_minutos_salida').desc())
    df_top4 = df_llamadas_agg_key3.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_destino')).alias('movil_destino'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_salida')).alias('total_minutos_salida'))
    df_llamadas_agg_key4 = df_llamadas_agg_key3.join(df_top4.select('key'),['key'],'leftanti').orderBy(col('total_minutos_salida').desc())
    df_top5 = df_llamadas_agg_key4.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_destino')).alias('movil_destino'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_salida')).alias('total_minutos_salida'))
    df_top = df_top1.union(df_top2).union(df_top3).union(df_top4).union(df_top5).drop('key')
    #unir para saber si son personas o empresas y la comuna
    #mercado -> persona o empresa
    df_top_mer = df_top.join(df_mov_vig.select('movil','desc_mercadocliente'), ['movil'], 'left')
    df_llamadas_salientes = df_top_mer.join(df_redes_celdas, ['movil'], 'left')
    df_llamadas_salientes2 = df_llamadas_salientes.distinct().orderBy(col('movil').desc(),col('total_minutos_salida').desc()).\
    withColumnRenamed('movil_destino','movil_no_cliente')
    return df_llamadas_salientes2

#llamadas entrantes
def llamadas_entrantes(df_llamadas_agg2,df_mov_vig,df_redes_celdas):
    df_llamadas_agg3 = df_llamadas_agg2.filter(~(col('total_minutos_salida')>0.0))
    df_llamadas_agg_key = df_llamadas_agg3.withColumn('key', concat(col('movil'),col('movil_origen'))).\
    orderBy(col('movil'),col('total_minutos_entrada').desc())
    df_top1 = df_llamadas_agg_key.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_origen')).alias('movil_origen'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_entrada')).alias('total_minutos_entrada'))
    df_llamadas_agg_key1 = df_llamadas_agg_key.join(df_top1.select('key'),['key'],'leftanti').orderBy(col('total_minutos_entrada').desc())
    df_top2 = df_llamadas_agg_key1.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_origen')).alias('movil_origen'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_entrada')).alias('total_minutos_entrada'))
    df_llamadas_agg_key2 = df_llamadas_agg_key1.join(df_top2.select('key'),['key'],'leftanti').orderBy(col('total_minutos_entrada').desc())
    df_top3 = df_llamadas_agg_key2.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_origen')).alias('movil_origen'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_entrada')).alias('total_minutos_entrada'))
    df_llamadas_agg_key3 = df_llamadas_agg_key2.join(df_top3.select('key'),['key'],'leftanti').orderBy(col('total_minutos_entrada').desc())
    df_top4 = df_llamadas_agg_key3.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_origen')).alias('movil_origen'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_entrada')).alias('total_minutos_entrada'))
    df_llamadas_agg_key4 = df_llamadas_agg_key3.join(df_top4.select('key'),['key'],'leftanti').orderBy(col('total_minutos_entrada').desc())
    df_top5 = df_llamadas_agg_key4.groupBy('movil').agg(first(col('key')).alias('key'),first(col('movil_origen')).alias('movil_origen'),\
    first(col('nombre_compania')).alias('nombre_compania'),first(col('total_minutos_entrada')).alias('total_minutos_entrada'))
    df_top = df_top1.union(df_top2).union(df_top3).union(df_top4).union(df_top5).drop('key')
    df_top_mer = df_top.join(df_mov_vig.select('movil','desc_mercadocliente'), ['movil'], 'left')
    df_llamadas_entrantes = df_top_mer.join(df_redes_celdas, ['movil'], 'left')
    df_llamadas_entrantes2 = df_llamadas_entrantes.distinct().orderBy(col('movil').desc(),col('total_minutos_entrada').desc()).\
    withColumnRenamed('movil_origen','movil_no_cliente')
    return df_llamadas_entrantes2

def escribir_datos(df_final, bucket_s3, bucket_output, tabla_destino, valor_particion):
#def escribir_datos(df_final, bucket_s3, tabla_destino):
    try:
        ruta_destino = bucket_s3+tabla_destino
        print('Procesando y escribiendo los datos...')
        df_final.repartition(10).write.partitionBy(particion_salida).mode("overwrite").parquet(ruta_destino)
        df_final.repartition(1).write.format('csv').mode('overwrite').option('header', 'true').save('{}{}/{}.csv'.format(bucket_output,tabla_destino,valor_particion))
        print('Datos escritos exitosamente')
        print('Creando particiones dinámicas...')
        add_partition_query = ('alter table {0}.{1} add if not exists partition ({2} = "{3}")'.format(base_datos_landing, tabla_destino,'anio_mes',valor_particion))
        spark.sql(add_partition_query)
        add_partition_query = ('alter table {0}.{1} add if not exists partition ({2} = "{3}")'.format(base_datos_analytics, tabla_destino,'anio_mes',valor_particion))
        spark.sql(add_partition_query)
        print('Se agregaron las particiones dinámicas')
    except Exception as e:
        print("Problemas al escribir los datos")
        print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(str(e))

try:
    proceso,id_tarea,id_proceso,nombre_proceso,particion_salida,s3_bucket_landingzone,s3_bucket_analytics,s3_bucket_output,base_datos_landing,base_datos_analytics,directorio_salida_entrantes,directorio_salida_salientes,\
    directorio_volte,directorio_voz,directorio_mc,directorio_mv,directorio_redes_celdas,cols_volte,cols_voz,cols_movil_cia,cols_movil_vig,cols_redes_celdas,string_cols_entrantes,string_cols_salientes = leer_parametros()
    spark = crear_spark(nombre_proceso)
    argumentos = sys.argv[1:]
    tipo_proceso = argumentos[0]
    print('Tipo de ejecución: {}'.format(tipo_proceso))
    if (tipo_proceso == 'R'):
        fecha_proceso = argumentos[1]
    elif (tipo_proceso == 'N'):
        ct = datetime.datetime.now(pytz.timezone('America/Santiago'))
        fecha_proceso = str(ct - dateutil.relativedelta.relativedelta(months=1))[:7]
    else:
        print('Error! el tipo de proceso solo puede ser "N" o "R"')
        updateMCP('Error! el tipo de proceso solo puede ser "N" o "R", ESTADO: Error ' , '1', id_tarea)
        quit()
    print('Preparando los datos para el periodo: {}'.format(fecha_proceso))
    df_llamadas,df_mov_vig,df_cliente_comuna = leer_fuentes(s3_bucket_landingzone,directorio_redes_celdas,directorio_mv,directorio_volte,directorio_voz,directorio_mc,fecha_proceso)
    #sincronizar los dos directorios de salida
    print('entrantes')
    os.system('emrfs delete {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_entrantes,fecha_proceso))
    os.system('emrfs sync {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_entrantes,fecha_proceso))
    print('salientes')
    os.system('emrfs delete {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_salientes,fecha_proceso))
    os.system('emrfs sync {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_salientes,fecha_proceso))
    print('Termino de eliminar')
    print('Llenando nulls')
    print('Llamadas salientes')
    df_salientes = llamadas_salientes(df_llamadas,df_mov_vig,df_cliente_comuna)
    print('Llamadas entrantes')
    df_entrantes = llamadas_entrantes(df_llamadas,df_mov_vig,df_cliente_comuna)
    hc = HiveContext(spark)
    hc.setConf("spark.sql.sources.partitionOverwriteMode","dynamic")
    fecha_proceso = fecha_proceso.replace('-','')
    df_entrantes = df_entrantes.withColumn('anio_mes', lit(fecha_proceso))
    df_salientes = df_salientes.withColumn('anio_mes', lit(fecha_proceso))
    print('Eliminando particiones')
    eliminar_archivos_particion(s3_bucket_landingzone, directorio_salida_entrantes, particion_salida, fecha_proceso)
    eliminar_archivos_particion(s3_bucket_landingzone, directorio_salida_salientes, particion_salida, fecha_proceso)
    eliminar_archivos_particion(s3_bucket_analytics, directorio_salida_entrantes, particion_salida, fecha_proceso)
    eliminar_archivos_particion(s3_bucket_analytics, directorio_salida_salientes, particion_salida, fecha_proceso)
    print('Sincronizando s3...')
    #sincronizar los dos directorios de salida
    print('entrantes')
    os.system('emrfs delete {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_entrantes,fecha_proceso))
    os.system('emrfs sync {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_entrantes,fecha_proceso))
    print('salientes')
    os.system('emrfs delete {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_salientes,fecha_proceso))
    os.system('emrfs sync {}{}/anio_mes={}'.format(s3_bucket_landingzone,directorio_salida_salientes,fecha_proceso))
    print('Termino de eliminar')
    print('Llenando nulls')
    df_entrantes = llenar_nulls(df_entrantes, string_cols_entrantes, [])
    df_salientes = llenar_nulls(df_salientes, string_cols_salientes, [])
    df_entrantes.persist()
    df_salientes.persist()
    print('Entrantes')
    escribir_datos(df_entrantes,s3_bucket_landingzone,s3_bucket_output,directorio_salida_entrantes,fecha_proceso)
    #(df_final, bucket_s3, s3_bucket_analytics, tabla_destino):
    print('Salientes')
    escribir_datos(df_salientes,s3_bucket_landingzone,s3_bucket_output,directorio_salida_salientes,fecha_proceso)
    #(df_final, bucket_s3, s3_bucket_analytics, tabla_destino):
    ct = str(datetime.datetime.now(pytz.timezone('America/Santiago')))
    print("Hora fin: ", ct)
    print("Fin del programa")
    updateMCP('Escritura output ejecutado correctamente, ESTADO: Exitoso', '0', id_tarea)
except Exception as e:
    print("Problemas en el main")
    print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    print(str(e))
    updateMCP('Proceso terminado incorrectamente, ESTADO: Error '+str(e)+ 'Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), '1', id_tarea)
