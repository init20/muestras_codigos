#coding=utf8
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from cryptography.fernet import Fernet
import sys
import time
import os
import shutil
import sys
import glob
import parametros as param
from datetime import datetime, timedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

#ejecucion en windows o linux
#var_sys = '\\' #windows
var_sys = '/' #linux
tiempo_entre_click = 1


def llamar_parametros():
    file_key = param.llaves['key']
    file_user = param.llaves['username']
    file_pass = param.llaves['password']
    rep_descargas = param.rutas['rep_descargas']
    PATH = param.rutas['path_chrome_driver']
    home_sernac = param.rutas['home_sernac']
    return file_key, file_user, file_pass, rep_descargas, PATH, home_sernac

def load_key():
    return open(file_key, 'rb').read()

def abre_key_password():
    return open(file_user, 'rb').read()

def abre_key_username():
    return open(file_pass, 'rb').read()

def cambiar_nombre_archivo(user,medio,fecha_ayer):
    filename_output = 'SERNAC_{}_{}_{}.csv'.format(user, medio,fecha_ayer.replace('/','')[4:]+fecha_ayer.replace('/','')[2:4]+fecha_ayer.replace('/','')[:2])
    filename = max([rep_descargas + var_sys + f for f in os.listdir(rep_descargas)],key=os.path.getctime)
    shutil.move(filename,os.path.join(rep_descargas, filename_output))

def limpiar_repositorio_descargas(rep_descargas, tipo_limpieza):
    if (tipo_limpieza!='limpieza_inicial'):
        archivos_usuarios_no_descargados = glob.glob(rep_descargas + var_sys + '*_no_descargados*.csv')
        for f in archivos_usuarios_no_descargados:
            os.remove(f)
    else:
        archivos_total = glob.glob(rep_descargas + var_sys + '*.csv')
        for f in archivos_total:
            os.remove(f)


#datos e-mails
def descargar_datos_email(user):
    fecha_ayer = obtener_fecha_ayer()
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/ul/li[2]/a').click()
    time.sleep(3.3)
    driver.find_element_by_id('intervalo_ini_mail').send_keys(fecha_ayer)
    driver.find_element_by_id('intervalo_ter_mail').send_keys(fecha_ayer)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[2]/div/div/div/div/ul/li[4]/button').click()
    time.sleep(tiempo_entre_click)
    #marcar datos como descargados haciendo una descarga
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[1]/div/div/button').click()
    time.sleep(tiempo_entre_click)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[1]/div/div/div/button[1]').click()
    time.sleep(3.3)
    cambiar_nombre_archivo(user,'email_no_descargados',obtener_fecha_ayer())
    #descargar los datos marcados como descargados
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[2]/div/div/div/div/ul/li[1]/div/div[2]/select/option[2]').click()
    time.sleep(3.3)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[1]/div/div/button').click()
    time.sleep(tiempo_entre_click)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[2]/div[1]/div/div/div/button[1]').click()
    time.sleep(3.3)
    cambiar_nombre_archivo(user,'email',obtener_fecha_ayer())


def descargar_datos_telefono(user):
    #driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[2]/div/div/div/div/ul/li[3]/div/div[2]/select/option[2]').click()
    fecha_ayer = obtener_fecha_ayer()
    driver.find_element_by_id('intervalo_ini_telefono').send_keys(fecha_ayer)
    driver.find_element_by_id('intervalo_ter_telefono').send_keys(fecha_ayer)
    driver.find_element_by_id('realizar-filtro-telefono').click()
    time.sleep(tiempo_entre_click)
    #marcar datos como descargados haciendo una descarga
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[1]/div/div/button').click()
    time.sleep(tiempo_entre_click)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[1]/div/div/div/button[1]').click()
    time.sleep(3.3)
    cambiar_nombre_archivo(user,'telefono_no_descargados',obtener_fecha_ayer())
    #descargar los datos marcados como descargados
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[2]/div/div/div/div/ul/li[1]/div/div[2]/select/option[2]').click()
    time.sleep(3.3)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[1]/div/div/button').click()
    time.sleep(tiempo_entre_click)
    driver.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div/div[1]/div[1]/div/div/div/button[1]').click()
    time.sleep(3.3)
    cambiar_nombre_archivo(user,'telefono',obtener_fecha_ayer())

def navegar_no_molestar():
    driver.find_element_by_xpath('/html/body/div[3]/div/aside/ul/li[3]/span').click()
    time.sleep(tiempo_entre_click)
    driver.find_element_by_xpath('/html/body/div[3]/div/aside/ul/li[3]/ul/li[2]/span').click()
    time.sleep(3.3)
    driver.switch_to.frame('frameNoMolestar')

def obtener_fecha_ayer():
    fecha_ayer = (datetime.now() - timedelta(1)).strftime('%d/%m/%Y')
    return fecha_ayer

def login_sernac(user, password):
    inputElement = driver.find_element_by_name('nombreUsuario')
    inputElement.send_keys(user)
    inputElement = driver.find_element_by_name('passwordUsuario')
    inputElement.send_keys(password)
    inicio_sesion = driver.find_element_by_name('formation_button').click()
    time.sleep(tiempo_entre_click)

if __name__ == '__main__':
    print('########################################################################################')
    print('########################################################################################')
    print('#####################  INICIO RPA SERNAC DESCARGA NO MOLESTAR    #######################')
    print('########################################################################################')
    print('########################################################################################')
    try:
        file_key, file_user, file_pass, rep_descargas, PATH, home_sernac = llamar_parametros()
        key = load_key()
        f = Fernet(key)
        #sys.exit('my error from robot')
        #Desencripta Password
        password = abre_key_password()
        decrypted_pass = f.decrypt(password).decode()
        pass_list = decrypted_pass.split(';')
        #Desencripta Username
        username = abre_key_username()
        decrypted_user = f.decrypt(username).decode()
        users_list = decrypted_user.split(';')
        limpiar_repositorio_descargas(rep_descargas, 'limpieza_inicial')

    except Exception as e:
        print('Error al leer los parametros')
        print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(str(e))
    try:
        for iter in range(len(users_list)):
        #for iter in range(1):
            user = users_list[iter]
            password = pass_list[iter]
            print('Iniciando proceso de descarga con el usuario: {}'.format(user))
            ##carga de driver para navegador
            options = Options()
            options.add_argument('--headless')
            options.add_argument('window-size=1400,1500')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('start-maximized')
            options.add_argument('enable-automation')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-dev-shm-usage')
            prefs = {
            'download.default_directory': rep_descargas,
            'download.prompt_for_download': False,
            #'download.extensions_to_open': 'csv',
            'safebrowsing.enabled': True
            }
            options.add_experimental_option('prefs', prefs)
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            driver = webdriver.Chrome(PATH,options=options)
            driver.get(home_sernac)
            print('Logging')
            login_sernac(user,password)
            time.sleep(3.3)
            print('Navegando...')
            navegar_no_molestar()
            print('Descargando los datos de medio telefono...')
            descargar_datos_telefono(user)
            print('Descargando los datos de medio mail...')
            descargar_datos_email(user)
            time.sleep(3.3)
    except Exception as e:
        print('Error en la iteracion de los usuarios')
        print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(str(e))
    try:
        print('Eliminando archivos con sufijo no_descargados')
        limpiar_repositorio_descargas(rep_descargas,'')
    except Exception as e:
        print('Error en la eliminacion de los archivos')
        print('Error en la línea {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        print(str(e))
    print('########################################################################################')
    print('########################################################################################')
    print('#######################  FIN RPA SERNAC DESCARGA NO MOLESTAR    ########################')
    print('########################################################################################')
    print('########################################################################################')
