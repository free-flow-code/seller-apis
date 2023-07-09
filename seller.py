import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Аргументы:
        last_id (str): Идентификатор последнего товара на странице.
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        dict: словарь с параметрами товаров.

    Пример:
        >>> get_product_list(last_id, client_id, seller_token)
        {"items": [{"product_id": 223681945,"offer_id": "136748"}],"total": 1,"last_id": "bnVсbA=="}

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Получает список товаров в магазине и добавляет их артикулы в итоговый список.

    Аргументы:
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        list: список строк с артикулами.

    Пример:
        >>> get_offer_ids(client_id, seller_token)
        ['4534534']

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Установить цены на товары в магазине.

    Аргументы:
        prices (list): Список словарей с деталями товаров.
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        json: объект с параметрами ответа.

    Примеры:
        >>> update_price(prices: list, client_id, seller_token)
        {"result": [{"product_id": 1386,"offer_id": "PH8865","updated": true,"errors": [ ]}]}
        >>> update_price(prices: list, client_id, seller_token)
        {"code": 0,"details": [{"typeUrl": "string","value": "string"}],"message": "string"}

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Аргументы:
        stocks (list): Список словарей с артикулами товаров и количеством остатков.
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        json: объект с параметрами ответа.

    Примеры:
        >>> update_stocks(stocks: list, client_id, seller_token)
        {"result": [{"product_id": 55946,"offer_id": "PG-2404С1","updated": true,"errors": [ ]}]}
        >>> update_stocks(stocks: list, client_id, seller_token)
        {"code": 0,"details": [{"typeUrl": "string","value": "string"}],"message": "string"}

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

    Возвращает:
        dict: словарь с таблицей товаров (часов).

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    print(watch_remnants)
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков.

    Аргументы:
        watch_remnants (dict): Словарь с таблицей товаров (часов).
        offer_ids (list): Артикулы товаров магазина озон.

    Возвращает:
        list: список словарей с артикулами и количеством товаров.

    Пример:
        >>> create_stocks(watch_remnants, offer_ids)
        [{"offer_id": "4534534", "stock": 10}]

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список с характеристиками цен.

    Аргументы:
        watch_remnants (dict): Словарь с таблицей товаров (часов).
        offer_ids (list): Артикулы товаров магазина озон.

    Возвращает:
        list: список словарей с характеристиками цены.

    Пример:
        >>> create_prices(watch_remnants, offer_ids)
        [{
        "auto_action_enabled": "UNKNOWN",
        "currency_code": "RUB",
        "offer_id": "4345345",
        "old_price": "0",
        "price": "5990"),
        }]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену исключив из нее лишние символы.

    Аргументы:
        price (str): Цена товара.

    Возвращает:
        str: строка содержащая только цифры.

    Примеры:
        >>> price_conversion(r'5'990.00 руб.')
        '5990'

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Аргументы:
        lst (list): Список, который нужно разделить.
        n (int): Количество элементов, которое должно быть в каждой части.

    Возвращает:
        Списоки с n элементами.

    Пример:
        >>> divide([1, 2, 3, 4], 2)
        [1, 2][3, 4]

    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновить цены товаров.

    Аргументы:
        watch_remnants (dict): Словарь с таблицей товаров (часов).
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        Список словарей с характеристиками цен, для которых были обновлены значения в магазине.

    Пример:
        >>> create_prices(watch_remnants, offer_ids)
        [{
        "auto_action_enabled": "UNKNOWN",
        "currency_code": "RUB",
        "offer_id": "4345345",
        "old_price": "0",
        "price": "5990"),
        }]

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновить остатки.

    Аргументы:
        watch_remnants (dict): Словарь с таблицей товаров (часов).
        client_id (str): Идентификатор пользователя Ozon.
        seller_token (str): Токен продавца Ozon.

    Возвращает:
        list: список словарей с артикулами и количеством товаров, количество которых больше нуля.
        list: список словарей с артикулами и количеством всех товаров.

    Примеры:
        [{"offer_id": "4534534", "stock": 10}],
        [{"offer_id": "4534534", "stock": 0}]

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Обновить количество остатков и цены товаров в магазине."""
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
