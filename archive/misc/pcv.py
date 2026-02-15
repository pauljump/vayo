from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# Set up the driver (ensure you have ChromeDriver installed)
driver = webdriver.Chrome()

# Load the website
driver.get("https://www.stuytown.com/nyc-apartments-for-rent/?Bedrooms=3&Order=low-price&Bathrooms=2&AmenityCodes=WasherDryer")

# Wait for JavaScript to load content
time.sleep(5)

# Find listings
listings = driver.find_elements(By.CLASS_NAME, 'listing')  # Update based on the inspected HTML

# Extract and print data
for listing in listings:
    print(listing.text)

# Close the driver
driver.quit()
