from oauth2client.service_account import ServiceAccountCredentials
from pyppeteer.errors import ElementHandleError, TimeoutError
from googleapiclient import discovery
from bs4 import BeautifulSoup
from datetime import datetime
from pyppeteer import launch
import requests
import httplib2
import asyncio
import re

CREDENTIALS_FILE = ''  # api-токены
SPREADSHEET_ID = ''  # id google-таблицы

creditentials = ServiceAccountCredentials.from_json_keyfile_name(
                    CREDENTIALS_FILE,
                    ['https://www.googleapis.com/auth/spreadsheets',
                     'https://www.googleapis.com/auth/drive'])  # подключаемые google-сервисы
httpAuth = creditentials.authorize(httplib2.Http())  # аутентификация в документе
service = discovery.build('sheets', 'v4', http=httpAuth)  # экземпляр api

ya_links, ya_rewiews = [], []  # список ссылок и список полученных данных для яндекс.карт
ggl_links, ggl_rewiews = [], []  # список ссылок и список полученных данных для google.maps
gis_links, gis_rewiews = [], []  # список ссылок и список полученных данных для 2gis


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
    for i in ya_values:
        ya_links.append(i)

    ggl_values = service.spreadsheets().values().get(
        spreadsheetId=id,
        range='Лист1!G3:G112',
        majorDimension='COLUMNS'
    ).execute()['values'][0]
    ggl_links.clear()
    for i in ggl_values:
        ggl_links.append(i)

    gis_values = service.spreadsheets().values().get(
        spreadsheetId=id,
        range='Лист1!J3:J112',
        majorDimension='COLUMNS'
    ).execute()['values'][0]
    gis_links.clear()
    for i in gis_values:
        gis_links.append(i)


def yandex_parser():
    '''
    Парсер яндекс.карты.
    По каждой ссылке из списка ya_links из html кода извлекает кол-во отзывов и рейтинг организации.
    Помещает кортеж из этой пары в список ya_rewiews.
    Если ссылка некорректна, то в список ya_rewiews возвращается пара ('-', '-').
    '''
    ya_rewiews.clear()
    for link in ya_links:
        if link.find('http') != -1:
            page = requests.get(link)
            soup = BeautifulSoup(page.text, 'lxml')
            count = soup.find('span',
                              class_='business-header-rating-view__text _clickable')
            rating = soup.find('span',
                               class_='business-rating-badge-view__rating-text _size_m')
            if (count and rating) is not None and not count.text[0].isalpha():
                rewiew = (re.findall(r'\d+', count.text)[0], rating.text.replace('.', ','))
                ya_rewiews.append(rewiew)
            else:
                ya_rewiews.append(('0', '0'))
        else:
            ya_rewiews.append(('-', '-'))


async def google_parser():
    '''
    Парсер google maps.
    В цикле по каждой ссылке из списка ggl_links открывает headless-браузер и извлекает кол-во отзывов
    и рейтинг организации из html кода.
    Помещает кортеж из этой пары в список ggl_rewiews.
    В конце каждой итерации закрывает headless-браузер.
    Если ссылка некорректна, то в список ggl_rewiews возвращается пара ('-', '-').
    '''
    ggl_rewiews.clear()
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
            except (ElementHandleError, TimeoutError):
                ggl_rewiews.append(('0', '0'))
                await browser.close()
                continue

            review = (rating_count[1:-1], rating)
            ggl_rewiews.append(review)
            await browser.close()
        else:
            ggl_rewiews.append(('-', '-'))
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
    gis_rewiews.clear()
    for link in gis_links:
        if link.find('http') != -1:
            browser = await launch()
            page = await browser.newPage()

            try:
                await page.goto(link)
                await page.waitForSelector('._65o8tv')
                rating_count = await page.evaluate('(element) => element.textContent',
                                                   await page.querySelector('._65o8tv'))
                rating = await page.evaluate('(element) => element.textContent',
                                             await page.querySelector('._1n8h0vx'))
            except (ElementHandleError, TimeoutError):
                gis_rewiews.append(('0', '0'))
                await browser.close()
                continue

            review = (rating_count, str(float(rating)).replace('.', ','))
            gis_rewiews.append(review)
            await browser.close()
        else:
            gis_rewiews.append(('-', '-'))
    await browser.close()


def add_to_table(id=SPREADSHEET_ID,
                 ya_data=ya_rewiews,
                 ggl_data=ggl_rewiews,
                 gis_data=gis_rewiews,
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
    print('Начало парсинга.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    get_from_table()
    print('Все ссылки взяты из таблицы.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
          '\nВыполняется парсинг яндекс.карт.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    yandex_parser()
    print('Парсинг яндекс.карт выполнен.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
          '\nВыполняется парсинг гугл.карт.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    await google_parser()
    print('Парсинг гугл.карт выполнен.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
          '\nВыполняется парсинг 2gis.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    await gis_parser()
    print('Парсинг 2gis выполнен.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    add_to_table()
    print('Все данные занесены в таблицу, парсинг успешно завершён.', datetime.now().strftime("%d-%m-%Y %H:%M:%S"))


if __name__ == '__main__':
    asyncio.run(run_parser())