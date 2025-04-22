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
from selenium.webdriver.common.keys import Keys

def login_whatsapp(driver):
    """Log in to WhatsApp Web."""
    try:
        print("Logging into WhatsApp Web...")
        driver.get('https://web.whatsapp.com/')
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/div[3]/div/div[3]/header')))
        print("Logged in successfully.")
    except selexceptions.TimeoutException:
        print("Error: Timeout while logging in.")
        driver.quit()
        raise

def validate_whatsapp(driver, phone_number):
    """Check if the phone number has WhatsApp."""
    try:
        new_chat_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button')))
        driver.execute_script("arguments[0].scrollIntoView(true);", new_chat_btn)
        new_chat_btn.click()

        # Search box element
        search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div[1]/p')))
        
        # Clear the search box before entering the new phone number
        search_box.clear()
        search_box.send_keys(phone_number)
        time.sleep(0.2)

        # Wait for the element indicating a chat exists
        chat_exist = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, '_ak8l')))
        if chat_exist:
            chat_exist.click()
            print(f"Number {phone_number} has WhatsApp. ✅")
            return True
        else:
            print(f"Number {phone_number} does not have WhatsApp. ❌")
            return False
    except (selexceptions.TimeoutException, selexceptions.ElementClickInterceptedException):
        # Ensure search box is cleared if an error occurs
        search_box.send_keys(Keys.ESCAPE)
        print(f"Number {phone_number} does not have WhatsApp. ❌")
        return False

def process_numbers(input_path, output_path):
    """Process phone numbers from a CSV file and validate if they have WhatsApp."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    login_whatsapp(driver)

    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')

        # Check if the output file exists
        if not os.path.exists(output_path):
            with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow(["Number", "WhatsApp", "Date", "Time", "Hour"])

        # Iterate over the numbers and validate
        for row in reader:
            phone_number = row[0]  # Assuming the number is in the first column
            has_whatsapp = validate_whatsapp(driver, phone_number)

            # Get the current date and time
            now = datetime.now()
            entry_date = now.strftime("%Y-%m-%d")
            time_with_minutes = now.strftime("%H:%M:%S")
            only_hour = now.strftime("%H")

            # Write results to the output file immediately
            with open(output_path, 'a', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow([phone_number, "True" if has_whatsapp else "❌ False", entry_date, time_with_minutes, only_hour])

    driver.quit()
    print(f"Process completed. Results saved to: {output_path}")

# Get the user and date for dynamic paths
date = datetime.now().strftime("%Y-%m-%d")
path = r"C:\Users\juan_\Downloads\demos\Demograficos Consolidados 20250421 1.csv" 
output_path = "C:/Users/juan_/Downloads/Whatsapp_Check_Results.csv"

# Run the process
process_numbers(path, output_path)