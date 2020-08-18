from oauth2client.service_account import ServiceAccountCredentials
from pyppeteer.errors import ElementHandleError, TimeoutError, NetworkError
from googleapiclient import discovery
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
from pyppeteer import launch
import requests
import httplib2
import asyncio
import sys
import os
import re

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path): load_dotenv(dotenv_path)
else: sys.exit('Отсутствует файл с переменными окружения. Парсинг невозможен.')

CREDENTIALS_FILE = os.getenv('TOKENS')  # api-токены
SPREADSHEET_ID = os.getenv('ID')  # id google-таблицы

creditentials = ServiceAccountCredentials.from_json_keyfile_name(
                    CREDENTIALS_FILE,
                    ['https://www.googleapis.com/auth/spreadsheets',
                     'https://www.googleapis.com/auth/drive'])  # подключаемые google-сервисы
httpAuth = creditentials.authorize(httplib2.Http())  # аутентификация в документе
service = discovery.build('sheets', 'v4', http=httpAuth)  # экземпляр api

ya_links, ya_reviews = [], []  # список ссылок и список полученных данных для яндекс.карт
ggl_links, ggl_reviews = [], []  # список ссылок и список полученных данных для google.maps
gis_links, gis_reviews = [], []  # список ссылок и список полученных данных для 2gis


def get_from_table(id=SPREADSHEET_ID,
                   service=service):
    '''
    Функция для взятия ссылок на карты из google-таблицы.
    Через API по ID google-таблицы берёт ссылки на карты из указанных столбцов и помещает в соостветсвующие списки.
    '''
    ya_values = service.spreadsheets().values().get(
        spreadsheetId=id,
        range='Лист1!D3:D112',
        majorDimension='COLUMNS'
    ).execute()['values'][0]
    for i in ya_values: ya_links.append(i)

    ggl_values = service.spreadsheets().values().get(
        spreadsheetId=id,
        range='Лист1!G3:G112',
        majorDimension='COLUMNS'
    ).execute()['values'][0]
    for i in ggl_values: ggl_links.append(i)

    gis_values = service.spreadsheets().values().get(
        spreadsheetId=id,
        range='Лист1!J3:J112',
        majorDimension='COLUMNS'
    ).execute()['values'][0]
    for i in gis_values: gis_links.append(i)


def yandex_parser():
    '''
    Парсер яндекс.карты.
    По каждой ссылке из списка ya_links из html кода извлекает кол-во отзывов и рейтинг организации.
    Помещает кортеж из этой пары в список ya_rewiews.
    Если ссылка некорректна, то в список ya_rewiews возвращается пара ('-', '-').
    '''
    for link in ya_links:
        if link.find('http') != -1:
            page = requests.get(link)
            soup = BeautifulSoup(page.text, 'lxml')
            count = soup.find('span',
                              class_='business-header-rating-view__text _clickable')
            rating = soup.find('span',
                               class_='business-rating-badge-view__rating-text _size_m')
            if (count and rating) is not None and not count.text[0].isalpha():
                review = (re.findall(r'\d+', count.text)[0], rating.text.replace('.', ','))
                ya_reviews.append(review)
            else:
                ya_reviews.append(('0', '0'))
        else: ya_reviews.append(('-', '-'))


async def google_parser():
    '''
    Парсер google maps.
    В цикле по каждой ссылке из списка ggl_links открывает headless-браузер и извлекает кол-во отзывов
    и рейтинг организации из html кода.
    Помещает кортеж из этой пары в список ggl_rewiews.
    В конце каждой итерации закрывает headless-браузер.
    Если ссылка некорректна, то в список ggl_rewiews возвращается пара ('-', '-').
    '''
    for link in ggl_links:
        if link.find('http') != -1:
            browser = await launch()
            page = await browser.newPage()

            try:
                await page.goto(link)
                await page.waitForSelector('.widget-pane-link')
                rating_count = await page.evaluate('(element) => element.textContent',
                                                   await page.querySelector('.widget-pane-link'))
                rating = await page.evaluate('(element) => element.textContent',
                                             await page.querySelector('.section-star-display'))
            except (ElementHandleError, TimeoutError, NetworkError):
                ggl_reviews.append(('0', '0'))
                await browser.close()
                continue

            review = (rating_count[1:-1], rating)
            ggl_reviews.append(review)
            await browser.close()
        else: ggl_reviews.append(('-', '-'))
    await browser.close()


async def gis_parser():
    '''
    Парсер 2gis
    В цикле по каждой ссылке из списка gis_links открывает headless-браузер и извлекает кол-во отзывов
    и рейтинг организации из html кода.
    Помещает кортеж из этой пары в список gis_rewiews.
    В конце каждой итерации закрывает headless-браузер.
    Если ссылка некорректна, то в список gis_rewiews возвращается пара ('-', '-').
    '''
    for link in gis_links:
        if link.find('http') != -1:
            browser = await launch()
            page = await browser.newPage()

            try:
                await page.goto(link)
                await page.waitForSelector('._gg5kmr')
                rating_count = await page.evaluate('(element) => element.textContent',
                                                   await page.querySelector('._gg5kmr'))
                rating = await page.evaluate('(element) => element.textContent',
                                             await page.querySelector('._36rspy'))
            except (ElementHandleError, TimeoutError, NetworkError):
                gis_reviews.append(('0', '0'))
                print(gis_reviews[-1])
                await browser.close()
                continue

            review = (rating_count, str(float(rating)).replace('.', ','))
            print(review)
            gis_reviews.append(review)
            await browser.close()
        else:
            gis_reviews.append(('-', '-'))
    await browser.close()


def add_to_table(id=SPREADSHEET_ID,
                 ya_data=ya_reviews,
                 ggl_data=ggl_reviews,
                 gis_data=gis_reviews,
                 service=service):
    '''
    Функция для добавления количества отзывов и среднего рейтинга в google-таблицу.
    Из соответствующих списков берёт кортежи ('кол-во отзывов', 'средний рейтинг') и заносит эти данные
    в соответствующие столбцы google-таблицы.
    '''
    ya_values = service.spreadsheets().values().batchUpdate(
        spreadsheetId=id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "Лист1!E3:F112",
                 "majorDimension": "ROWS",
                 "values": [ya_data[i] for i in range(len(ya_data))]},
            ]
        }
    ).execute()

    ggl_values = service.spreadsheets().values().batchUpdate(
        spreadsheetId=id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "Лист1!H3:I112",
                 "majorDimension": "ROWS",
                 "values": [ggl_data[i] for i in range(len(ggl_data))]},
            ]
        }
    ).execute()

    gis_values = service.spreadsheets().values().batchUpdate(
        spreadsheetId=id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "Лист1!K3:L112",
                 "majorDimension": "ROWS",
                 "values": [gis_data[i] for i in range(len(gis_data))]},
            ]
        }
    ).execute()


async def run_parser():
    """ Функция для выполнения парсинга в event loop"""
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Начало парсинга.')
    get_from_table()
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Все ссылки взяты из таблицы.'
          f'\n[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Выполняется парсинг яндекс.карт.')
    #yandex_parser()
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Парсинг яндекс.карт выполнен.'
          f'\n[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Выполняется парсинг гугл.карт.')
    #await google_parser()
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Парсинг гугл.карт выполнен.'
          f'\n[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Выполняется парсинг 2gis.')
    await gis_parser()
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Парсинг 2gis выполнен.')
    add_to_table()
    print(f'[{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}] Все данные занесены в таблицу, парсинг успешно завершён.')


if __name__ == '__main__':
    asyncio.run(run_parser())
