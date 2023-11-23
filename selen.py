import re
import time
import zipfile

from fake_useragent import UserAgent
from selenium.common import NoSuchElementException

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium import webdriver

from webdriver_manager.chrome import ChromeDriverManager

import secure
from db_sql import insert_url_table


countries = [
    'India', 'China', 'Armenia', 'Kazakhstan', 'Kyrgyzstan', 'Azerbaijan', 'Tajikistan', 'Uzbekistan'
]


def set_driver_options(options):
    # безголовый режим браузера
    # options.add_argument('--headless=new')
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument("--disable-blink-features=AutomationControlled")
    prefs = {
        'profile.managed_default_content_settings.images': 2,
    }
    options.add_experimental_option("prefs", prefs)


def get_selenium_driver(use_proxy, num_proxy):
    ua = UserAgent()
    options = webdriver.ChromeOptions()
    set_driver_options(options)

    if use_proxy:
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        plugin_file = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(plugin_file, 'w') as zp:
            zp.writestr('manifest.json', secure.get_proxy_pref(num_proxy, 0))
            zp.writestr('background.js', secure.get_proxy_pref(num_proxy, 1))

        options.add_extension(plugin_file)

    options.add_argument(f'--user-agent={ua.chrome}')

    caps = DesiredCapabilities().CHROME
    caps['pageLoadStrategy'] = 'normal'

    service = Service(ChromeDriverManager().install(), desired_capabilities=caps)
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def get_country(connection):
    # for country in countries:
    #     print(country)
    link = 'https://www.uniagents.com/search-agents-new.php'
    driver = get_selenium_driver(True, 0)
    driver.get(link)
    time.sleep(2)
    select = driver.find_element(By.XPATH, "//select[@name='country']")
    all_options = select.find_elements(By.TAG_NAME, "option")
    for option in all_options:
        sel = option.text
        if sel.lower() == 'Azerbaijan'.lower():
            print(f"Value is: {sel}")
            option.click()
            time.sleep(1)
            break
    search = driver.find_element(By.XPATH, "//input[@value='Search']")
    search.click()
    time.sleep(2)
    paginate = driver.find_element(By.CLASS_NAME, "paginate")
    max_num = 1
    if paginate:
        hrefs = paginate.find_elements(By.TAG_NAME, "a")
        for href in hrefs:
            number = href.text
            count = re.findall(r'\d+', number)
            if count:
                number = int(count[0])
                if number > max_num:
                    max_num = number
    link_page = 'https://www.uniagents.com/search-agents-new.php?page='
    get_links(connection, driver, link_page, max_num)


def get_links(connection, driver: webdriver.Chrome, link_page, max_num):
    try:
        for i in range(1, max_num + 1):
            driver.get(link_page + str(i))
            time.sleep(2)
            cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'col-3 gold1')]")
            for card in cards:
                span = card.find_element(By.TAG_NAME, 'span').text
                span_split = span.split(',')
                country = ''
                if span_split:
                    country = span_split[-1]
                    country = country.replace(' ', '')

                div = card.find_element(By.CLASS_NAME, "content")
                # for div in divs:
                a = div.find_element(By.TAG_NAME, "a")
                href = a.get_attribute("href")
                href = href.replace("'", "''")
                span = a.find_element(By.TAG_NAME, "span").text
                # span = span.replace('\\', '')
                span = span.replace("'", "''")
                find = False
                for el in countries:
                    if country.lower() == el.lower():
                        find = True
                        break
                if find is False:
                    continue
                # # print(href)
                insert_url_table(connection, href, country, i, span)
    except IndexError as ierr:
        print(f'get_links_index_error {ierr}')
    except NoSuchElementException as ex:
        reason = "get_links_NoSuchElementException"
        secure.log.write_log(reason, ex)
