import os
import csv
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common import exceptions as selexceptions
from webdriver_manager.chrome import ChromeDriverManager

def login_whatsapp(driver):
    """Inicia sesión en WhatsApp Web."""
    try:
        print("Iniciando sesión en WhatsApp Web...")
        driver.get('https://web.whatsapp.com/')
        # Esperar hasta que la página cargue completamente
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((
            By.XPATH, '//*[@id="app"]/div/div[3]/div/div[3]/header'
        )))
        print("Sesión iniciada correctamente.")
    except selexceptions.TimeoutException:
        print("Error: Tiempo de espera agotado al iniciar sesión en WhatsApp Web.")
        driver.quit()
        raise

def validate_whatsapp(driver, phone_number):
    """Valida si un número tiene WhatsApp."""
    try:
        # Hacer clic en el botón de nuevo chat
        new_chat_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((
            By.XPATH, '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button'
        )))
        driver.execute_script("arguments[0].scrollIntoView(true);", new_chat_btn)  # Asegurarse de que el botón sea visible
        new_chat_btn.click()

        # Buscar el número en el cuadro de búsqueda
        search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((
            By.XPATH, '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div[1]/p'
        )))
        search_box.clear()
        search_box.send_keys(phone_number)
        time.sleep(1)

        # Verificar si el número tiene WhatsApp
        chat_exist = WebDriverWait(driver, 5).until(EC.presence_of_element_located((
            By.CLASS_NAME,
            '_ak8l'
            # '#app > div > div.x78zum5.xdt5ytf.x5yr21d > div > div.x10l6tqk.x13vifvy.x17qophe.x78zum5.xh8yej3.x5yr21d.x6ikm8r.x10wlt62.x47corl > div._aigw.false.xxpasqj.x9f619.x1n2onr6.x5yr21d.x17dzmu4.x1i1dayz.x2ipvbc.x1w8yi2h.x78zum5.xdt5ytf.xd32934.x6ikm8r.x10wlt62.x1ks9yow.xy80clv.x26u7qi.x1ux35ld > span > div > span > div > div.x1n2onr6.x1n2onr6.xyw6214.x78zum5.x1r8uery.x1iyjqo2.xdt5ytf.x6ikm8r.x1odjw0f.x1hc1fzr.x150wa6m > div:nth-child(2) > div > div > div:nth-child(2) > div > div > div._ak8l'
        )))
        if chat_exist:
            chat_exist.click()  # Open the chat
            print(f"Número {phone_number} tiene WhatsApp.")
            driver.refresh()
            return True
        else:
            print(f"Número {phone_number} no tiene WhatsApp.")
            driver.refresh()
            return False
    except selexceptions.ElementClickInterceptedException:
        print(f"Error: El botón de nuevo chat está bloqueado...")
        time.sleep(2)  # Esperar un momento antes de reintentar
        driver.refresh()
    except selexceptions.TimeoutException:
        print(f"Error: No se pudo validar el número {phone_number}.")
        return False

def process_numbers(input_path, output_path):
    """Procesa los números desde un archivo CSV y valida si tienen WhatsApp."""
    # Configurar Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(
        ChromeDriverManager().install()), options=chrome_options)

    # Iniciar sesión en WhatsApp Web
    login_whatsapp(driver)

    # Leer el archivo CSV de entrada
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')

        # Verificar si el archivo de salida ya existe
        if not os.path.exists(output_path):
            # Si no existe, escribir los encabezados
            with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow(["Numero", "Tiene WhatsApp", "Fecha de Entrada", "Hora con Minutos", "Hora"])

        # Iterar sobre los números y validar
        for row in reader:
            phone_number = row[0]  # Asume que el número está en la primera columna
            has_whatsapp = validate_whatsapp(driver, phone_number)

            # Obtener la fecha y hora actuales
            now = datetime.now()
            fecha_entrada = now.strftime("%Y-%m-%d")
            hora_con_minutos = now.strftime("%H:%M")
            solo_hora = now.strftime("%H")

            # Escribir los resultados en el archivo de salida inmediatamente
            with open(output_path, 'a', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow([phone_number, "Si" if has_whatsapp else "No", fecha_entrada, hora_con_minutos, solo_hora])

    # Cerrar el navegador
    driver.quit()
    print("Proceso completado. Resultados guardados en:", output_path)

# Variables del usuario
user = "c.operativo"
date = "2025-04-21"
input_path = f"C:/Users/{user}/Downloads/Demograficos Consolidados 20250421 1.csv"
output_path = f"C:/Users/{user}/Downloads/Detalle Validacion RPA WhatsApp {date}.csv"

# Ejecutar el proceso
process_numbers(input_path, output_path)