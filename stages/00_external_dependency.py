from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import os

def scrape_s3_links(url, output_file):
    # Set up Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
 
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    # Navigate to the website
    driver.get(url)
    
    # Find all <a> tags in the page
    links = driver.find_elements(By.TAG_NAME, 'a')
    
    # Filter out the links that contain 's3' in their href attribute
    s3_links = [link.get_attribute('href') for link in links if 's3' in link.get_attribute('href')]
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write the links to the output file
    with open(output_file, 'w') as f:
        for link in s3_links:
            f.write(f"{link}\n")
    
    # Clean up: close the browser
    driver.quit()

# URL of the website to scrape
url = "https://moleculenet.org/datasets-1"

# Output file path
output_file = "download/data-links.txt"

# Run the scraping function
scrape_s3_links(url, output_file)

print(f"Finished scraping S3 links to {output_file}")




