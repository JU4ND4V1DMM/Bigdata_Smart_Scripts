import os
import time
from datetime import datetime
from random import randint
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  # Add this import
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, regexp_replace
from selenium.common import exceptions as selexceptions
from webdriver_manager.chrome import ChromeDriverManager  # Add this import

# Create the Spark session
spark = SparkSession.builder \
    .appName("Excel to DataFrame") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate()
    
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")

sqlContext = SQLContext(spark)

# Function to log in to WhatsApp Web


def login_whatsapp(driver: WebDriver):
    try:
        print("Starting login process to WhatsApp Web")
        driver.get('https://web.whatsapp.com/')
        print("Waiting for login...")
        # Wait until the new chat button is visible (indicating the session is logged in)
        new_chat_btn = WebDriverWait(driver, 90).until(EC.element_to_be_clickable((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button'
        )))
        time.sleep(2)  # Wait a few seconds after loading WhatsApp Web

    except selexceptions.TimeoutException:
        print("Timeout waiting to log in")
        driver.quit()

# Function to send messages


def send_message(driver: WebDriver, phone_number: str, message: str, sleep: int = 2):
    try:
        # Find and click the new chat button
        new_chat_btn = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button'
        )))
        new_chat_btn.click()
        time.sleep(randint(1, 5))

        # Find the search box and enter the phone number
        text_box = WebDriverWait(driver, 15).until(EC.presence_of_element_located((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div[1]/p'
        )))
        time.sleep(0.5)
        text_box.send_keys(phone_number)
        print("Phone Writed")
        time.sleep(1)

        # Wait and check if there is a chat with that number
        chat_exist = WebDriverWait(driver, 15).until(EC.presence_of_element_located((
            By.CLASS_NAME,
            '_ak8l'
            # '#app > div > div.x78zum5.xdt5ytf.x5yr21d > div > div.x10l6tqk.x13vifvy.x17qophe.x78zum5.xh8yej3.x5yr21d.x6ikm8r.x10wlt62.x47corl > div._aigw.false.xxpasqj.x9f619.x1n2onr6.x5yr21d.x17dzmu4.x1i1dayz.x2ipvbc.x1w8yi2h.x78zum5.xdt5ytf.xd32934.x6ikm8r.x10wlt62.x1ks9yow.xy80clv.x26u7qi.x1ux35ld > span > div > span > div > div.x1n2onr6.x1n2onr6.xyw6214.x78zum5.x1r8uery.x1iyjqo2.xdt5ytf.x6ikm8r.x1odjw0f.x1hc1fzr.x150wa6m > div:nth-child(2) > div > div > div:nth-child(2) > div > div > div._ak8l'
        )))

        if chat_exist:
            chat_exist.click()  # Open the chat
            print("Open the Chat")
            time.sleep(3)
            # Send the message

            try:
                message_box = WebDriverWait(driver, 15).until(EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    '#main > footer > div.x1n2onr6.xhtitgo.x9f619.x78zum5.x1q0g3np.xuk3077.x193iq5w.x122xwht.x1bmpntp.xs9asl8.x1swvt13.x1pi30zi.xnpuxes.copyable-area > div > span > div > div._ak1r > div.x9f619.x12lumcd.x1qrby5j.xeuugli.xisnujt.x6prxxf.x1fcty0u.x1fc57z9.xe7vic5.x1716072.xgde2yp.x89wmna.xbjl0o0.x13fuv20.xu3j5b3.x1q0q8m5.x26u7qi.x178xt8z.xm81vs4.xso031l.xy80clv.x1lq5wgf.xgqcy7u.x30kzoy.x9jhf4c.x1a2a7pz.x13w7htt.x78zum5.x96k8nx.xdvlbce.x1ye3gou.xn6708d.x1ok221b.xu06os2.x1i64zmx.x1emribx > div.x1n2onr6.xh8yej3.lexical-rich-text-input > div.x1hx0egp.x6ikm8r.x1odjw0f.x1k6rcq7.x6prxxf > p'
                )))
            except selexceptions.TimeoutException:
                try:
                    message_box = WebDriverWait(driver, 15).until(EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/div[1]/div/div/div[3]/div/div[4]/div/footer/div[1]/div/span/div/div[2]/div[1]/div[2]/div[1]/p'
                    )))
                except Exception as e:
                    print(
                        'No se ha podido encontrar la caja de texto por favor verifica el CSS_SELECTOR o el XPATH'
                    )

            message_box.click()
            message_box.clear()
            message_box.send_keys(message)
            time.sleep(randint(2, 5))
            message_box.send_keys(Keys.ENTER)
            return True
        else:
            print("Chat without Open")
            return False
    except Exception as e:
        print(f"Error sending message {e}")
        # Clear the phone number input
        try:
            # Attempt to clear the text box
            text_box.clear()  # Assuming text_box is defined elsewhere
            time.sleep(3)  # Wait for a moment before refreshing

            # Refresh the WhatsApp Web page
            driver.refresh()
            time.sleep(3)  # Wait for the page to load completely

        except Exception as inner_e:
            print(f"Error while clearing number or refreshing")
        return False

# Function to send summary report


def send_summary_report(driver: WebDriver, phone_number: str, sent_count: int, not_sent_count: int, total_count: int, Phone: str):
    pending_count = total_count - (sent_count + not_sent_count)
    report_message = (f"{Phone}\n"
                      f"Enviados: {sent_count} - No enviados: {not_sent_count}\n"
                      f"Procesados: {sent_count + not_sent_count} - Pendientes: {pending_count}\n")

    success = send_message(driver, phone_number, report_message)
    if success:
        print("Resumen enviado correctamente.")
    else:
        print("Error al enviar el resumen.")

# Main function to process the CSV and send the messages


def process_and_send(Path, Outpath, Number, Root_Number, Phone, Operator, start_time_process, end_time_process):
    # Read the CSV file
    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(Path)
        
    # Select necessary columns
    df = df.select("Cuenta", "SMS", "Dato_Contacto", "Edad_Mora", "CRM") # Filter necessary columns
    #df = df.withColumn("Dato_Contacto", regexp_replace(col("Dato_Contacto").cast("double"), "\.0", ""))
    #df = df.withColumn("Dato_Contacto", col("Dato_Contacto").cast("int"))
    
    # Check if DataFrame is empty
    if df.count() == 0:
        print("DataFrame is empty. No data to process.")
        return
    # Initialize WebDriver
    chrome_options = Options()
    # Added to ensure proper functioning in headless mode
    chrome_options.add_argument("--disable-gpu")
    # Added to avoid issues in some environments
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(
        ChromeDriverManager().install()), options=chrome_options)
    # Log in to WhatsApp Web
    login_whatsapp(driver)

    # Prepare the output list
    output_data = []
    sent_count = 0
    not_sent_count = 0

    # Check if the output CSV file already exists
    file_exists = os.path.isfile(Outpath)

    # Write headers if the file does not exist
    if not file_exists:
        with open(Outpath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Edad_Mora", "CRM", "Cuenta", "Dato_Contacto", "Mensaje",
                            "Estado", "MIN Remitente", "Operador", "Dispositivo", "Hora_Envio", "Hora"])

    # Iterate over the rows of the DataFrame
    for row in df.collect():

        now = datetime.now()

        if start_time_process <= (now.hour, now.minute) <= end_time_process:
            # Extract data from the row
            #############################################

            cuenta = row["Cuenta"]
            mensaje = row["SMS"]
            phone_number = row["Dato_Contacto"]
            estado = "Not sent"

            print(f'Este es el numero de celular {phone_number}')
            
            cuenta = row["Cuenta"]

            mensaje_final = (
                ", te ofrecemos una oportunidad para comenzar una nueva etapa en tu historial crediticio.\n"
                "Puedes elegir la forma que mejor se adapte a tus necesidades y te beneficiarás en:\n"
                "* _Reduciendo tu deuda pagando menos_\n"
                "* _Generando reportes positivos en centrales de riesgo_ \n"
                "* _Mejorando tu historial crediticio mes a mes_ \n"
                "* _Incrementando tu *puntaje*_ \n"
                "Con QNT, desde el primer pago tu compromiso estará al día en centrales de riesgo como Datacrédito o TransUnion. *Comunícate con nosotros al 313 273 6944 y recibe atención personalizada*"
            )
            mensaje = f"{mensaje}{mensaje_final}\n"
            # Try to send the message
            success = send_message(driver, phone_number, mensaje)
            
            if success:
                estado = "Sent"
                sent_count += 1
            else:
                not_sent_count += 1

            # Get the current time
            hora_envio = time.strftime("%Y-%m-%d %H:%M:%S")
            hora = time.strftime("%H")

            # Append the result to the output data list
            mensaje_correct ="Mensaje Personalizado"
            output_data.append([row["Edad_Mora"], row["CRM"], cuenta, phone_number,
                                mensaje_correct, estado, Root_Number, Operator, Phone, hora_envio, hora])
            print(
                f"Message to {phone_number} at {hora_envio} ({estado}) - {mensaje}")

            # Write to CSV file after each iteration
            with open(Outpath, 'a', newline='', encoding='utf-8') as f:  # Open in append mode
                writer = csv.writer(f)
                writer.writerow(output_data[-1])  # Write the last added row

            time.sleep(randint(1, 5))  # Wait a little between messages
            send_summary_report(driver, Number, sent_count,
                                not_sent_count, df.count(), Phone)
    else:
        print("No se ha podido enviar el mensaje, debido al horario establecido")
        # Close the browser
        driver.quit()

## User variables
user = "c.operativo"

# Call the main function
Date = "2025-04-21"
Path = f"C:/Users/{user}/Downloads/Base WP {Date} 2.xlsx"
Outpath = f"C:/Users/{user}/Downloads/Detalle Ejecución RPA WhatsApp {Date}.csv"
Number = 3118185075  # Número al cuál envía un detalle de cómo va
Root_Number = 3181202609  # NÚMERO DESDE EL CUÁL SE ENVÍA
Operator = "Movistar"  # Operador de número desde el cuál se envía
Phone = f"IMEI 86033406868{user} - Color Negro"  # Telkef+ono desde el cál se envía
start_time_process = (8, 0)
end_time_process = (18, 50)

# Call the function to process and send messages
process_and_send(Path, Outpath, Number, Root_Number, Phone,
                 Operator, start_time_process, end_time_process)