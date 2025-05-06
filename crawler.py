# crawler.py

import time
import logging
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_driver():
    logger.info("Starting get_driver() function")
    try:
        # Try to install chromedriver automatically
        try:
            logger.debug("Attempting to install chromedriver automatically")
            chromedriver_path = chromedriver_autoinstaller.install()
            logger.info(f"Installed chromedriver at: {chromedriver_path}")
        except Exception as e:
            logger.warning(f"Failed to auto-install chromedriver: {str(e)}")
            logger.warning("Will try to use system path instead")
            chromedriver_path = "/usr/bin/chromedriver"

        logger.debug("Configuring Chrome options")
        options = Options()
        options.add_argument("--headless=new")  # 최신 헤드리스 모드
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        
        try:
            logger.debug("Setting Chrome binary location")
            options.binary_location = "/usr/bin/google-chrome" 
            logger.debug(f"Chrome binary location set to: {options.binary_location}")
        except Exception as e:
            logger.warning(f"Error setting binary location: {str(e)}")

        # Try to create Chrome driver
        logger.info("Initializing Chrome driver")
        try:
            driver = webdriver.Chrome(
                service=Service(chromedriver_path),
                options=options
            )
            logger.info("Chrome driver initialized successfully")
            return driver
        except Exception as e:
            logger.error(f"Failed to initialize driver with service: {str(e)}")
            # Fallback - try without service parameter
            logger.info("Trying fallback driver initialization")
            driver = webdriver.Chrome(options=options)
            logger.info("Fallback Chrome driver initialized successfully")
            return driver
            
    except Exception as e:
        logger.critical(f"Failed to initialize Chrome driver: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to initialize Chrome driver: {str(e)}")

def search_character(server_name: str, character_name: str, wait_sec: float = 10):
    logger.info(f"Starting search for character '{character_name}' on server '{server_name}'")
    URL = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    driver = None
    
    try:
        logger.debug("Getting webdriver")
        driver = get_driver()
        logger.debug(f"Creating WebDriverWait with timeout: {wait_sec} seconds")
        wait = WebDriverWait(driver, wait_sec)
        
        logger.info(f"Navigating to URL: {URL}")
        driver.get(URL)
        logger.debug("Page loaded, current URL: " + driver.current_url)
        
        # 1) 서버 선택
        logger.info("Step 1: Selecting server")
        try:
            logger.debug("Waiting for server selection box to be clickable")
            box = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    ".select_area .select_server .select_box"
                ))
            )
            logger.debug("Server selection box found, clicking with JavaScript")
            driver.execute_script("arguments[0].scrollIntoView(true);", box)
            driver.execute_script("arguments[0].click();", box)
            print("✅ 서버 선택 박스 클릭 성공")
            # driver.execute_script("arguments[0].click();", box)
            
            logger.debug("Waiting for server options to be displayed")
            wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select_option li").is_displayed())
            
            logger.debug("Finding all server options")
            server_options = driver.find_elements(By.CSS_SELECTOR, ".select_option li")
            logger.debug(f"Found {len(server_options)} server options")
            
            server_found = False
            for i, opt in enumerate(server_options):
                server_text = opt.text.strip()
                logger.debug(f"Server option {i+1}: '{server_text}'")
                if server_text == server_name:
                    logger.info(f"Found matching server: '{server_text}'")
                    logger.debug("Clicking on server option")
                    driver.execute_script("arguments[0].click();", opt)
                    server_found = True
                    break
            
            if not server_found:
                available_servers = [opt.text.strip() for opt in server_options]
                logger.error(f"Server '{server_name}' not found. Available servers: {available_servers}")
                raise ValueError(f"서버 '{server_name}' 없음. 사용 가능한 서버: {', '.join(available_servers)}")
                
            logger.debug("Waiting 1 second for server selection to take effect")
            time.sleep(1)
            
        except TimeoutException as e:
            logger.error(f"Timeout waiting for server selection: {str(e)}")
            raise TimeoutException(f"서버 선택 시간 초과: {str(e)}")
        
        # 2) 캐릭터 검색
        logger.info("Step 2: Searching for character")
        try:
            logger.debug("Waiting for search input field to be clickable")
            inp = wait.until(EC.element_to_be_clickable((By.NAME, "search")))
            
            logger.debug("Clearing search input field")
            inp.clear()
            
            logger.debug(f"Entering character name: '{character_name}' and pressing Enter")
            inp.send_keys(character_name, Keys.ENTER)
            
            logger.debug(f"Waiting for character element to be present in the DOM")
            selector = f'dd[data-charactername="{character_name}"]'
            logger.debug(f"Using selector: {selector}")
            
            try:
                dd = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                logger.info(f"Character '{character_name}' found in the search results")
            except TimeoutException:
                logger.error(f"Character '{character_name}' not found within {wait_sec} seconds")
                # Try to capture any error message on the page
                try:
                    error_msg = driver.find_element(By.CSS_SELECTOR, ".error_msg, .alert, .message").text
                    logger.error(f"Error message on page: {error_msg}")
                except:
                    logger.debug("No specific error message found on page")
                
                # Save screenshot for debugging
                try:
                    screenshot_path = f"search_error_{character_name}.png"
                    driver.save_screenshot(screenshot_path)
                    logger.info(f"Screenshot saved to {screenshot_path}")
                except Exception as ss_err:
                    logger.error(f"Failed to save screenshot: {str(ss_err)}")
                
                # Save page source for debugging
                try:
                    html_path = f"search_error_{character_name}.html"
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    logger.info(f"Page source saved to {html_path}")
                except Exception as html_err:
                    logger.error(f"Failed to save page source: {str(html_err)}")
                    
                raise ValueError(f"캐릭터 '{character_name}'를 찾을 수 없습니다.")
            
            # 결과 li 요소
            logger.debug("Finding parent li element")
            li = dd.find_element(By.XPATH, "./ancestor::li")
            
            logger.debug("Extracting rank information")
            rank = li.find_element(By.CSS_SELECTOR, "dt").text.strip()
            logger.debug(f"Rank: {rank}")
            
            logger.debug("Extracting server information")
            server = li.find_element(By.XPATH, ".//dt[text()='서버명']/following-sibling::dd").text.strip()
            logger.debug(f"Server: {server}")
            
            logger.debug("Extracting class information")
            cls = li.find_element(By.XPATH, ".//div[4]//dd").text.strip()
            logger.debug(f"Class: {cls}")
            
            logger.debug("Extracting power information")
            power = li.find_element(By.CSS_SELECTOR, "div:nth-of-type(5) dd").text.strip()
            logger.debug(f"Power: {power}")
            
            result = {
                "server": server, 
                "rank": rank, 
                "class": cls, 
                "power": power,
                "name": character_name
            }
            logger.info(f"Search completed successfully: {result}")
            
            return result
            
        except NoSuchElementException as e:
            logger.error(f"Element not found: {str(e)}", exc_info=True)
            raise ValueError(f"필요한 요소를 찾을 수 없습니다: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error searching for character: {str(e)}", exc_info=True)
        raise
    finally:
        if driver:
            logger.debug("Quitting webdriver")
            try:
                driver.quit()
                logger.debug("Webdriver closed successfully")
            except Exception as e:
                logger.warning(f"Error closing webdriver: {str(e)}")

# Add a testing section if run directly
if __name__ == "__main__":
    try:
        logger.info("=== TESTING CHARACTER SEARCH ===")
        # Default test values - change these as needed
        test_server = "던컨"
        test_character = "힝트"
        
        logger.info(f"Searching for character '{test_character}' on server '{test_server}'")
        result = search_character(test_server, test_character)
        logger.info(f"Search result: {result}")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
