#!/usr/bin/env python3
"""
Urban Dictionary Scraper

This script scrapes Urban Dictionary pages using Selenium in headless Chrome mode
and extracts slang words with their definitions, examples, and metadata.
"""

import os
import csv
import time
import random
import logging
import argparse
import re
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("urban_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_driver():
    """
    Configure and return a headless Chrome WebDriver instance optimized for scraping.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--window-size=1920,1080")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Use CDP to block images properly
        driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp"]})
        driver.execute_cdp_cmd('Network.enable', {})
        
        return driver
    except WebDriverException as e:
        logger.error(f"Failed to initialize Chrome driver: {e}")
        raise

def extract_text_safely(element, selector, default=""):
    """
    Extract text from an element using the provided selector, returning default if not found.
    Try multiple selector strategies for robustness.
    """
    try:
        # Try CSS selector first
        found_element = element.find_element(By.CSS_SELECTOR, selector)
        return found_element.text.strip()
    except (NoSuchElementException, StaleElementReferenceException):
        try:
            # Try XPath as fallback for more complex selections
            xpath_selector = f".//*[contains(@class, '{selector.strip('.')}')]"
            found_element = element.find_element(By.XPATH, xpath_selector)
            return found_element.text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            return default

def extract_date(text):
    """
    Extract and standardize the date from contributor text using regex.
    """
    if not text:
        return ""
        
    # Common date formats in Urban Dictionary
    date_patterns = [
        r'(?:by\s+[\w\s]+\s+)(\w+\s+\d{1,2},\s+\d{4})',  # Month DD, YYYY
        r'(?:by\s+[\w\s]+\s+)(\d{1,2}\s+\w+\s+\d{4})',   # DD Month YYYY
        r'(?:by\s+[\w\s]+\s+)(\w+\s+\d{4})'              # Month YYYY
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    
    # Default fallback to old method if regex fails
    try:
        date_part = text.split("by ")[1].split(" ")[2:]
        return " ".join(date_part)
    except (IndexError, AttributeError):
        return ""

def extract_votes(element, is_upvote=True):
    """
    Extract vote count as integer using multiple selector strategies.
    """
    vote_text = ""
    selectors = [
        # Try data attribute selectors (Alpine.js)
        f"button[data-x-bind='thumb{'Up' if is_upvote else 'Down'}'] span",
        # More general selectors
        f".flex.items-center button:nth-{'child(1)' if is_upvote else 'child(2)'} span",
        # Direct XPath
        f"//button[contains(@class, 'rounded-{'tl' if is_upvote else 'tr'}-3xl')]//span"
    ]
    
    for selector in selectors:
        try:
            if selector.startswith('//'):
                vote_element = element.find_element(By.XPATH, selector)
            else:
                vote_element = element.find_element(By.CSS_SELECTOR, selector)
            vote_text = vote_element.text.strip()
            if vote_text and vote_text.isdigit():
                break
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    
    try:
        return int(vote_text)
    except (ValueError, AttributeError):
        return 0

def scrape_page_with_retry(page_num, max_retries=3):
    """
    Scrape a single Urban Dictionary page with retry logic.
    """
    for attempt in range(max_retries):
        try:
            return scrape_page(page_num)
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Retry {attempt+1}/{max_retries} for page {page_num} after error: {str(e)}")
                time.sleep(random.uniform(2, 5))  # Increasing backoff
            else:
                logger.error(f"Failed all {max_retries} attempts for page {page_num}: {str(e)}")
                return []

def scrape_page(page_num):
    """
    Scrape a single Urban Dictionary page and extract all entries.
    
    Args:
        page_num: Page number to scrape
        
    Returns:
        List of dictionaries containing scraped data
    """
    url = f"https://www.urbandictionary.com/?page={page_num}"
    driver = None
    entries = []
    
    try:
        driver = setup_driver()
        logger.info(f"Process {os.getpid()}: Scraping page {page_num}")
        
        # Add random delay for polite scraping
        time.sleep(random.uniform(1, 3))
        
        driver.get(url)
        
        # Wait for definitions to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".definition"))
        )
        
        # Find all definition containers
        definition_elements = driver.find_elements(By.CSS_SELECTOR, ".definition")
        
        seen_words = set()  # Track duplicates within the same page
        
        for element in definition_elements:
            try:
                # Extract slang word
                word = extract_text_safely(element, ".word")
                if not word:
                    continue
                
                # Skip duplicate words within the page
                word_key = word.lower().strip()
                if word_key in seen_words:
                    logger.debug(f"Skipping duplicate word: {word}")
                    continue
                seen_words.add(word_key)
                
                # Extract definition text
                definition = extract_text_safely(element, ".meaning")
                
                # Extract example usage
                example = extract_text_safely(element, ".example")
                
                # Extract contributor and date
                contributor_text = extract_text_safely(element, ".contributor")
                
                # Try to extract the date first, as it's more structured
                date = extract_date(contributor_text)
                
                # Then extract contributor by removing the date and "by" prefix
                contributor = contributor_text
                if date:
                    contributor = contributor_text.replace(date, "")
                contributor = contributor.replace("by", "").strip()
                
                # Extract votes using more robust method
                upvotes = extract_votes(element, is_upvote=True)
                downvotes = extract_votes(element, is_upvote=False)
                
                entry = {
                    "word": word,
                    "definition": definition,
                    "example": example,
                    "contributor": contributor,
                    "date": date,
                    "upvotes": upvotes,
                    "downvotes": downvotes,
                    "page": page_num,
                    "scraped_date": datetime.now().strftime('%Y-%m-%d')
                }
                
                entries.append(entry)
                logger.debug(f"Extracted entry: {word}")
                
            except Exception as e:
                logger.warning(f"Failed to extract entry on page {page_num}: {str(e)}")
                continue
        
        logger.info(f"Successfully scraped page {page_num}: {len(entries)} entries")
        return entries
            
    except Exception as e:
        logger.error(f"Error scraping page {page_num}: {str(e)}")
        return []
        
    finally:
        if driver:
            driver.quit()

def save_to_csv(entries, filename):
    """
    Save entries to CSV file.
    """
    if not entries:
        logger.warning("No entries to save")
        return
    
    fieldnames = ["word", "definition", "example", "contributor", "date", 
                 "upvotes", "downvotes", "page", "scraped_date"]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for entry in entries:
                writer.writerow(entry)
        
        logger.info(f"Successfully saved {len(entries)} entries to {filename}")
    
    except Exception as e:
        logger.error(f"Error saving to CSV: {str(e)}")

def main():
    """
    Main function to run the scraper with command line arguments.
    """
    parser = argparse.ArgumentParser(description="Scrape Urban Dictionary for slang terms")
    parser.add_argument("--start", type=int, default=1, help="Starting page number (default: 1)")
    parser.add_argument("--end", type=int, default=985, help="Ending page number (default: 985)")
    parser.add_argument("--workers", type=int, default=2, help="Number of parallel workers (default: 2)")
    parser.add_argument("--output", type=str, default=f"data/urban_dict_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
                        help="Output CSV filename")
    parser.add_argument("--chunk-size", type=int, default=50, 
                        help="Save data in chunks of this many pages (default: 50)")
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    
    all_entries = []
    global_seen_words = set()  # Track duplicates across all pages
    start_time = time.time()
    
    logger.info(f"Starting Urban Dictionary scraper from page {args.start} to {args.end} with {args.workers} workers")
    
    # Create list of page ranges for each worker
    page_numbers = list(range(args.start, args.end + 1))
    
    try:
        # Process pages in chunks to allow for intermediate saving
        for chunk_start in range(0, len(page_numbers), args.chunk_size):
            chunk_end = min(chunk_start + args.chunk_size, len(page_numbers))
            chunk_pages = page_numbers[chunk_start:chunk_end]
            
            chunk_entries = []
            
            # Use ProcessPoolExecutor for parallel scraping
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                # Submit jobs with retry logic
                future_to_page = {executor.submit(scrape_page_with_retry, page): page for page in chunk_pages}
                
                # Process results as they complete
                for future in as_completed(future_to_page):
                    page = future_to_page[future]
                    try:
                        page_entries = future.result()
                        
                        # Filter out duplicates across all pages
                        new_entries = []
                        for entry in page_entries:
                            word_key = entry["word"].lower().strip()
                            if word_key not in global_seen_words:
                                global_seen_words.add(word_key)
                                new_entries.append(entry)
                            else:
                                logger.debug(f"Skipping global duplicate: {entry['word']}")
                        
                        chunk_entries.extend(new_entries)
                        all_entries.extend(new_entries)
                        logger.info(f"Completed page {page}: {len(new_entries)} unique entries")
                        
                    except Exception as e:
                        logger.error(f"Page {page} generated an exception: {str(e)}")
            
            # Save chunk results to avoid losing data if the script crashes
            if chunk_entries:
                chunk_filename = f"{os.path.splitext(args.output)[0]}_chunk{chunk_start}-{chunk_end}.csv"
                save_to_csv(chunk_entries, chunk_filename)
                logger.info(f"Saved chunk {chunk_start}-{chunk_end} with {len(chunk_entries)} entries to {chunk_filename}")
    
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user. Saving current results...")
    
    # Save all collected entries to final CSV
    save_to_csv(all_entries, args.output)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Scraping completed in {elapsed_time:.2f} seconds")
    logger.info(f"Total entries collected: {len(all_entries)}")
    logger.info(f"Total unique words: {len(global_seen_words)}")

if __name__ == "__main__":
    main()
