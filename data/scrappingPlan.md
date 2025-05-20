robust, efficient Python script that scrapes Urban Dictionary pages using Selenium in headless Chrome mode on Linux.

Requirements:

1. Scrape multiple pages from https://www.urbandictionary.com/?page=1 up to the user-specified max page (e.g., 985).
2. For each page, extract all slang word entries with:
   - The slang word
   - Definition text
   - Example usage text
   - Contributor name and contribution date
   - Upvote count and downvote count
3. Handle dynamic content loading and ensure all data is properly extracted even if some elements are missing.
4. Use parallel scraping with Python's multiprocessing or concurrent.futures ProcessPoolExecutor to run 3 or 4 headless Selenium browsers in parallel, to speed up the scraping without overloading the CPU or memory.
5. Implement polite scraping by adding a small randomized delay between requests within each process.
6. Save all scraped entries into a CSV file with appropriate headers for further analysis in pandas.
7. Implement error handling to skip pages with issues and log errors gracefully.
8. Use Selenium best practices: headless mode, disable images and unnecessary content for speed, and proper driver management.
9. The script should be runnable on a Linux machine without GPU, with Python 3.
10. Include comments for clarity, and modular functions for scalability.