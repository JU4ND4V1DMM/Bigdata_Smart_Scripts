import os
import time
from random import randint
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait  # Add this import
from selenium.webdriver.support import expected_conditions as EC  # Add this import
from pyspark.sql import SparkSession, SQLContext
from selenium.common import exceptions as selexceptions
from webdriver_manager.chrome import ChromeDriverManager  # Add this import

# Create the Spark session
spark = SparkSession.builder.appName("WhatsApp Sender").getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")

sqlContext = SQLContext(spark)

# Function to log in to WhatsApp Web
def login_whatsapp(driver: WebDriver):
    try:
        print("Starting login process to WhatsApp Web")
        driver.get('https://web.whatsapp.com/')
        print("Waiting for login...")
        # Wait until the new chat button is visible (indicating the session is logged in)
        new_chat_btn = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((
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
            '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div[2]/div/div/p'
        )))
        time.sleep(0.5)
        text_box.send_keys(phone_number)
        time.sleep(1)
        
        # Wait and check if there is a chat with that number
        chat_exist = WebDriverWait(driver, 15).until(EC.presence_of_element_located(( 
            By.CLASS_NAME,
            '_ak8q'  # This is an example, you may need to adjust the class name
        )))
        
        if chat_exist:
            chat_exist.click()  # Open the chat
            time.sleep(1)
            # Send the message
            message_box = WebDriverWait(driver, 15).until(EC.presence_of_element_located(( 
                By.XPATH,
                '//*[@id="main"]/footer/div[1]/div/span/div/div[2]/div[1]/div[2]/div[1]/p'
            )))
            message_box.click()
            message_box.clear()
            message_box.send_keys(message)
            time.sleep(randint(1, 5))
            message_box.send_keys(Keys.ENTER)
            return True
        else:
            return False
    except Exception as e:
        print(f"Error sending message")
        # Clear the phone number input
        try:
            # Attempt to clear the text box
            text_box.clear()  # Assuming text_box is defined elsewhere
            time.sleep(0.5)  # Wait for a moment before refreshing

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
def process_and_send(Path, Outpath, Number, Root_Number, Phone, Operator):
    # Read the CSV file
    df = spark.read.option("header", "true").option("sep", ";").csv(Path)
    df = df.select("Cuenta", "SMS", "Dato_Contacto", "Edad_Mora", "CRM")  # Filter necessary columns
    # Check if DataFrame is empty
    if df.count() == 0:
        print("DataFrame is empty. No data to process.")
        return
    # Initialize WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Added to ensure proper functioning in headless mode
    chrome_options.add_argument("--no-sandbox")  # Added to avoid issues in some environments
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
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
            writer.writerow(["Edad_Mora", "CRM", "Cuenta", "Dato_Contacto", "Mensaje", "Estado", "MIN Remitente", "Operador", "Dispositivo", "Hora_Envio", "Hora"])
    
    # Iterate over the rows of the DataFrame
    for row in df.collect():
        cuenta = row["Cuenta"]
        mensaje = row["SMS"]
        phone_number = row["Dato_Contacto"]
        estado = "Not sent"
        
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
        output_data.append([row["Edad_Mora"], row["CRM"], cuenta, phone_number, mensaje, estado, Root_Number, Operator, Phone, hora_envio, hora])
        print(f"Sent message to {phone_number} at {hora_envio} ({estado})")
        
        # Write to CSV file after each iteration
        with open(Outpath, 'a', newline='', encoding='utf-8') as f:  # Open in append mode
            writer = csv.writer(f)
            writer.writerow(output_data[-1])  # Write the last added row
        
        time.sleep(randint(1, 5))  # Wait a little between messages
        send_summary_report(driver, Number, sent_count, not_sent_count, df.count(), Phone)
    
    # Close the browser
    driver.quit()
    
# Call the main function
Path = "D:/BASES CLARO/Resultados//Base WP 2.csv"
Outpath = "D:/BASES CLARO/Resultados/Detalle_WhatsApp 2.csv"
Number = 3118185075
Root_Number = 3159766109
Operator = "Movistar"
Phone = "IMEI 860334068681123 - Color Negro"

process_and_send(Path, Outpath, Number, Root_Number, Phone, Operator)