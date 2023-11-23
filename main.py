import aiohttp
import asyncio
import os

import secure

from beautiful_soup import get_soup

from db_sql import (connect_db, add_path_page, get_id_from_table, add_main_data, check_exist_table,
                    create_table, get_links_from_table)
from secure import log
from selen import get_country

GLOB_ID = 0

headers = {
    'authority': 'www.uniagents.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'PHPSESSID=65jfa414jf8i1era70nqifhti7',
    'dnt': '1',
    'pragma': 'no-cache',
    'referer': 'https://www.uniagents.com/search-agents-new.php?page=40',
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.1077 YaBrowser/23.9.1.1077 Yowser/2.5 Safari/537.36',
}


def get_main_data():
    connection = None
    try:
        connection = connect_db()
        connection.autocommit = True

        links = get_id_from_table(connection)

        for link in links:
            scrap_data(connection, link)

    except IndexError as ierr:
        print(f"YAAAAAAAA {ierr}")
    except Exception as _ex:
        print("tk_clicked_get_phone_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] Сбор основных данных завершен")


def scrap_data(connection, link):
    id_db = link[0]
    path_page = link[1]
    print(f'id: {id_db}; path: {path_page}')
    soup = get_soup(path_page)
    if soup is not None:
        organization = 'Sorry we have no any information about this agent'
        h1 = soup.find('h1', {'class': 'main-head primary'})
        if h1:
            organization = h1.text.split(',')[0].rstrip().replace("'", "''")
        div = soup.find('div', {'class': 'service-offered2'})
        li_val = 'No information'
        if div:
            strong_val = div.find('strong').text
            if strong_val.lower() == 'country deals in':
                # Получить все значения тега li и объединить их в строку через запятую
                li_val = ', '.join([li.text.strip() for li in div.find_all('li')])
        print(f'id: {id_db}; Организация: {organization}; Страны: {li_val}')

        add_main_data(connection, id_db, organization, li_val)


def start():
    connection = None
    try:
        log.create_log()
        connection = connect_db()
        connection.autocommit = True

        if check_exist_table(connection) is False:
            create_table(connection)

        get_country(connection)

    except Exception as _ex:
        print("start_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] Сбор основных данных завершен")


def get_links_source():
    connection = None
    try:
        connection = connect_db()
        connection.autocommit = True

        data = get_links_from_table(connection)
        data_len = len(data)

        step = 0
        if data_len > 9:
            for j in range(10, 20):
                if data_len % j == 0:
                    step = j
                    break
            for j in range(9, 1, -1):
                if data_len % j == 0:
                    step = j
                    break
        elif data_len < 10:
            for j in range(10, 1, -1):
                if data_len % j == 0:
                    step = j
                    break

        get_async_data(connection, data, data_len, step)

    except IndexError as ierr:
        print(f"YAAAAAAAA {ierr}")
    except Exception as _ex:
        print("get_links_source_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] Сохранение url-ов на диск завершено")


def get_async_data(connection, data, data_len, step):

    for row in range(0, data_len, step):
        ids = []
        urls = []
        batch = data[row:row + step]
        for i in batch:
            ids.append(i[0])
            urls.append(i[1])
        asyncio.run(save_pages(connection, ids, urls))


async def fetch_url(connection, session, id_db, url):
    print(f'id = {id_db}')
    url_split = url.split('/')
    file_name = url_split[-1]
    if len(file_name) > 3:
        async with session.get(url, headers=headers, proxy=secure.prox) as response:
            html_content = await response.text()

            if not os.path.exists("data"):
                os.mkdir("data")

            file_name = str(file_name).replace("'", '')

            with open(f"data/{file_name}", "w", encoding="utf-8") as file:
                file.write(html_content)

            # Assuming `add_path_page` is a synchronous function
            add_path_page(connection, id_db, f'data/{file_name}')


async def save_pages(connection, ids, urls):
    step = len(ids)  # Assuming ids and urls are defined somewhere before this code
    tasks = []
    async with aiohttp.ClientSession() as session:
        for j in range(0, step):
            id_db = ids[j]
            url = urls[j]

            task = fetch_url(connection, session, id_db, url)
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():

    start()
    get_links_source()
    get_main_data()


if __name__ == '__main__':
    main()
