# encoding=utf8
import sys
import os
import boto3
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import dateutil.relativedelta
import pytz
from parametros import leer_parametros
from mcp import updateMCP

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
        df = df.withColumn(column, when(((col(column).isNull()) | (trim(col(column))=='') | (trim(col(column))=='NI')), lit('-1')).otherwise(col(column)))
    for column in list_numbers_cols:
        df = df.withColumn(column, when(((col(column).isNull()) | (trim(col(column))=='') | (trim(col(column))=='')), lit('NI')).otherwise(col(column)))
    return df


def crear_spark(nombre_proceso):
    spark = SparkSession \
            .builder \
            .appName(nombre_proceso)\
            .config("spark.memory.offHeap.size","2g") \
            .config("spark.memory.offHeap.enabled", True)\
            .config("spark.dynamicAllocation.maxExecutors=15")\
            .config("spark.yarn.executor.memoryOverhead=2096")\
            .enableHiveSupport() \
            .getOrCreate()
    return spark

def read_datos(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente, particion_dia, fechas):
    control_inicio = True
    for fecha in fechas:
        if control_inicio:
            df = spark.read.parquet('{0}{1}/{2}={3}/{4}={5}/'.format(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente, particion_dia, fecha)).select(*cols_base).distinct()
            control_inicio=False
        else:
            df = df.union(spark.read.parquet('{0}{1}/{2}={3}/{4}={5}/'.format(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente, particion_dia, fecha)).select(*cols_base).distinct())
    return df

def read_voz(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente2, particion_dia, fechas):
    control_inicio = True
    for fecha in fechas:
        if control_inicio:
            df = spark.read.parquet('{0}{1}/{2}={3}/{4}={5}/'.format(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente2, particion_dia, fecha)).select(*cols_base).distinct()
            control_inicio=False
        else:
            df = df.union(spark.read.parquet('{0}{1}/{2}={3}/{4}={5}/'.format(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente2, particion_dia, fecha)).select(*cols_base).distinct())
    return df

def read_fuentes_bt(s3_bucket_landingzone, directorio, particion, fechas):
    for fecha in reversed(fechas):
        try:
            df=spark.read.parquet('{0}{1}/{2}={3}/'.format(s3_bucket_landingzone,directorio,particion,fecha))
            return df
        except Exception as e:
            print('Fecha {3} no encontrada en {0}{1}/{2}={3}/ intentando leer un dia antes...'.\
            format(s3_bucket_landingzone,directorio,particion,fecha))
    print('Error! No se procesarán los datos.')
    print('Ninguna fecha encontrada en el rango especificado.')
    exit()


def leer_semanas_mensual(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semanas,anio):
    control_inicio=True
    for num_semana in semanas:
        if control_inicio:
            df=spark.read.parquet('{0}{1}/{2}=W{3}_{4}/'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,num_semana,anio))
            control_inicio=False
        else:
            df=df.union(spark.read.parquet('{0}{1}/{2}=W{3}_{4}/'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,num_semana,anio)))
    return df

"""
Si el proceso es N: A partir de una fecha dada (dia jueves) le resta 10 dias para obtener
el lunes de la semana pasada hasta el día domingo de la semana pasada.
Si el proceso es R: A partir del dia lunes de la semana dado, obtiene el resto de la semana.
"""
def obtener_fechas_a_procesar(tipo_proceso,fecha):
    fechas = []
    if (tipo_proceso=='N'):
        for i in range(7):
            fechas.append(str(datetime.datetime.strptime(fecha, '%Y-%m-%d') + dateutil.relativedelta.relativedelta(days=i-10))[:10])
    elif (tipo_proceso=='R'):
        for i in range(7):
            fechas.append(str(datetime.datetime.strptime(fecha, '%Y-%m-%d') + dateutil.relativedelta.relativedelta(days=i))[:10])
    else:
        print('Error: el tipo de proceso debe ser N o R')
        updateMCP('Error: el tipo de proceso debe ser "N" o "R", ESTADO: Error ' , '1', id_tarea, id_proceso)
        exit()
    return fechas


def escribir_datos(df_final, bucket_s3, directorio_salida, particion_salida,valor_particion, bd_landing, bd_analytics):
    try:
        ruta_destino = bucket_s3+directorio_salida
        print('Procesando y escribiendo los datos...')
        df_final.repartition(10).write.partitionBy(particion_salida).mode("overwrite").parquet(ruta_destino)
        print('Datos escritos exitosamente')
        print('Creando particiones dinámicas...')
        add_partition_query = ('alter table {0}.{1} add if not exists partition ({2} = "{3}")'.format(bd_landing, directorio_salida,particion_salida,valor_particion))
        spark.sql(add_partition_query)
        add_partition_query = ('alter table {0}.{1} add if not exists partition ({2} = "{3}")'.format(bd_analytics, directorio_salida,particion_salida,valor_particion))
        spark.sql(add_partition_query)
        print('Se agregaron las particiones dinámicas')
    except Exception as e:
        print("Problemas al escribir los datos")
        print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        updateMCP('Error! el tipo de proceso solo puede ser "N" o "R", ESTADO: Error ' , '1', id_tarea, id_proceso)
        print(str(e))

try:
    proceso,id_tarea,id_proceso,nombre_proceso,particion_salida_semanal,particion_salida_mensual,s3_bucket_landingzone,\
    s3_bucket_analytics,base_datos_landing,base_datos_analytics,directorio_base,directorio_mov_vigentes,\
    directorio_mobiprof,directorio_gtpv2,directorio_salida,directorio_salida_mensual,particion_fuente,particion_dia,valor_fuente,\
    valor_fuente2,cols_base,cols_mobiprof,cols_mov_vigentes,cols_salida = leer_parametros()
    spark = crear_spark(nombre_proceso)
    argumentos = sys.argv[1:]
    temporalidad = argumentos[0]
    tipo_proceso = argumentos[1]
    print('Temporalidad: {}'.format(temporalidad))
    print('Tipo de proceso: {}'.format(tipo_proceso))
    if (temporalidad=='semanal'):
        if (tipo_proceso=='N'):
            fecha = str(datetime.datetime.now(pytz.timezone('America/Santiago')))[:10]
        elif (tipo_proceso=='R'):
            fecha=argumentos[2]
        else:
            print('Error! el tipo de proceso puede ser solo R o N')
            updateMCP('Error: el tipo de proceso debe ser "N" o "R", ESTADO: Error ' , '1', id_tarea, id_proceso)
            exit()
        fechas=obtener_fechas_a_procesar(tipo_proceso,fecha)
        semana_proceso='W{0}_{1}'.format(str(datetime.datetime.strptime(fechas[0], '%Y-%m-%d').isocalendar()[1]),fechas[0][:4])
        print('Semana a procesar: {} ... Dias {}'.format(semana_proceso,fechas))
        hc = HiveContext(spark)
        hc.setConf('spark.sql.sources.partitionOverwriteMode','dynamic')
        os.system('emrfs delete {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semana_proceso))
        os.system('emrfs sync {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semana_proceso))
        eliminar_archivos_particion(s3_bucket_landingzone, directorio_salida, particion_salida_semanal, semana_proceso)
        eliminar_archivos_particion(s3_bucket_analytics, directorio_salida, particion_salida_semanal, semana_proceso)
        os.system('emrfs delete {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semana_proceso))
        os.system('emrfs sync {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semana_proceso))
        print('Sincronizado s3...')
        print('Leyendo las fuentes de datos...')
        df = read_datos(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente, particion_dia, fechas).\
        union(read_voz(s3_bucket_landingzone, directorio_base, particion_fuente, valor_fuente2, particion_dia, fechas))
        df_vig = read_fuentes_bt(s3_bucket_landingzone, directorio_mov_vigentes, 'id_dia_corte', fechas).\
        select(*cols_mov_vigentes).withColumnRenamed('descr_movil','movil')
        df_mobiprof = read_fuentes_bt(s3_bucket_landingzone, directorio_mobiprof, 'id_dia', fechas).\
        select(*cols_mobiprof).withColumnRenamed('descr_movil','movil')
        print('Fuentes leídas, procesando datos...')
        df2 = df.groupBy('movil','id_imsi','imei_cdr').agg(lit(1).alias('to_sum')).drop('to_sum').persist()
        df3 = df2.join(df_vig,['movil'],'left').join(df_mobiprof, ['movil'], 'left')
        df4 = df3.withColumnRenamed('movil','msisdn').withColumn('plmn_id', substring(col('id_imsi'),1,5)).withColumn('tacfac_vigente', substring(col('imei_cdr'),1,8)).\
        withColumn('tacfac_mobiprof', substring(col('id_imei'),1,8)).withColumnRenamed('id_mercadocliente','id_mercado').\
        withColumnRenamed('id_mercadocliente','mercado').withColumnRenamed('desc_negocio','negocio').withColumnRenamed('desc_grupocliente','grupo').\
        withColumnRenamed('desc_estadoactualmovil','estado').withColumnRenamed('desc_mercadocliente','mercado').persist()
        df_imei=df4.withColumn('imei_temp', when(((col('imei_cdr')=='NI') & (col('id_imei').isNotNull())), col('id_imei')).otherwise(col('imei_cdr')))
        df_imei2=df_imei.withColumn('imei_temp', when((col('imei_temp')=='NI'), lit('-1')).otherwise(col('imei_temp')))
        repetido = df_imei2.filter(col('imei_temp')!='-1').select('imei_temp').groupBy('imei_temp').agg(count(col('imei_temp')).alias('repetido')).\
        orderBy(col('repetido').desc())
        repetido2 = repetido.withColumn('repetidos', when((col('repetido')==1), lit('0')).otherwise(lit('1'))).withColumnRenamed('imei_temp','imei_cdr')
        df5 = df4.join(repetido2.select('imei_cdr','repetidos'),['imei_cdr'],'left').withColumn('semana', lit(semana_proceso))
        cols_nums_str = ['id_imsi','id_mercado','id_negocio','id_equipomobiprof']
        df_gtpv = read_fuentes_bt(s3_bucket_landingzone, directorio_gtpv2, 'ingest_date', fechas).select('msisdn','imei','sv').\
        withColumn('largo', length(col('msisdn'))).withColumn('imei', substring(col('imei'),1,14))
        df_gtpv2 = df_gtpv.withColumn('msisdn', when((col('largo')==9), concat(lit('56'),col('msisdn'))).otherwise(col('msisdn'))).drop('largo').\
        groupBy('msisdn','imei','sv').agg(count(col('msisdn')).alias('counted')).drop('counted').withColumnRenamed('imei','imei_cdr')
        df5 = df5.join(df_gtpv2, ['msisdn','imei_cdr'],'left')
        df5 = llenar_nulls(df5, cols_nums_str,['mercado','negocio','grupo','estado','imei_cdr','id_imei','tacfac_vigente','tacfac_mobiprof','repetidos','sv'])
        df5.persist()
        print('Antes de escribir los datos')
        escribir_datos(df5, s3_bucket_landingzone, directorio_salida, particion_salida_semanal, semana_proceso,base_datos_landing, base_datos_analytics)
        print('Fin del programa, se escribieron exitosamente los datos')
        updateMCP('Escritura output ejecutado correctamente, ESTADO: Exitoso', '0', id_tarea, id_proceso)
    ##proceso mensual
    elif(temporalidad=='mensual'):
        if (tipo_proceso=='N'):
            mes_salida=str(datetime.datetime.now(pytz.timezone('America/Santiago')) - dateutil.relativedelta.relativedelta(months=1))[5:7]
            numero_semana=(datetime.datetime.now(pytz.timezone('America/Santiago')) - dateutil.relativedelta.relativedelta(days=7)).isocalendar()[1]
            anio=str(datetime.datetime.now(pytz.timezone('America/Santiago')) - dateutil.relativedelta.relativedelta(months=1))[:4]
            semanas=[]
            for i in range(4):
                semanas.append(numero_semana-i)
        elif (tipo_proceso=='R'):
            fecha=argumentos[2]
            mes_salida=fecha[5:7]
            numero_semana='{0}{1}'.format(str(datetime.datetime.strptime(fecha, '%Y-%m-%d') + \
            dateutil.relativedelta.relativedelta(months=1))[0:8],'04')
            numero_semana = (datetime.datetime.strptime(numero_semana, '%Y-%m-%d') - dateutil.relativedelta.relativedelta(days=7)).isocalendar()[1]
            anio=str(fecha)[:4]
            semanas=[]
            for i in range(4):
                semanas.append(numero_semana-i)
        else:
            print('Error. Tipo de proceso solo puede ser N o R')
            updateMCP('Proceso terminado incorrectamente, ESTADO: Error '+str(e)+ 'Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), '1', id_tarea,id_proceso)
            exit()
        mes_proceso='M{0}_{1}'.format(mes_salida,anio)
        print('Mes a procesar {} ... Numero de semanas {}'.format(mes_salida,semanas))
        os.system('emrfs delete {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida_mensual,particion_salida_mensual,mes_proceso))
        os.system('emrfs sync {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida_mensual,particion_salida_mensual,mes_proceso))
        eliminar_archivos_particion(s3_bucket_landingzone, directorio_salida_mensual, particion_salida_mensual, mes_proceso)
        eliminar_archivos_particion(s3_bucket_analytics, directorio_salida_mensual, particion_salida_mensual, mes_proceso)
        os.system('emrfs delete {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida_mensual,particion_salida_mensual,mes_proceso))
        os.system('emrfs sync {0}{1}/{2}={3}'.format(s3_bucket_landingzone,directorio_salida_mensual,particion_salida_mensual,mes_proceso))
        print('Sincronizado s3...')
        print('Leyendo las fuentes de datos...')
        df=leer_semanas_mensual(s3_bucket_landingzone,directorio_salida,particion_salida_semanal,semanas,anio)
        df=df.groupBy('msisdn','imei_cdr','id_imsi','id_mercado','mercado','id_negocio','negocio','grupo','estado','id_equipomobiprof',\
        'id_imei','plmn_id','tacfac_vigente','tacfac_mobiprof','sv').agg(max(col('repetidos')).alias('repetidos')).\
        withColumn('mes', lit(mes_proceso))
        df=df.withColumn('repetidos', when(col('repetidos').isNull(), lit('0')).otherwise(col('repetidos')))
        print('Antes de escribir los datos')
        escribir_datos(df, s3_bucket_landingzone, directorio_salida_mensual, particion_salida_mensual, mes_proceso, base_datos_landing, base_datos_analytics)
        print('Fin del programa, se escribieron exitosamente los datos')
        updateMCP('Escritura output ejecutado correctamente, ESTADO: Exitoso', '0', id_tarea,id_proceso)

except Exception as e:
    print("Problemas en el main")
    print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    updateMCP('Proceso terminado incorrectamente, ESTADO: Error '+str(e)+ 'Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), '1', id_tarea, id_proceso)
    print(str(e))
