import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров магазина Yandex Market.

        Аргументы:
            page (str): Номер страницы.
            campaign_id (str): Идентификатор кампании.
            access_token (str): OAuth-токен.

        Возвращает:
            dict: словарь с параметрами товаров.

        Пример:
            >>> get_product_list(page, campaign_id, access_token)
             {"paging": {"nextPageToken": "string", "prevPageToken": "string"}, "offerMappingEntries": [ ... ]}

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки.

        Аргументы:
            stocks (list): Список словарей с артикулами товаров и количеством остатков.
            campaign_id (str): Идентификатор кампании.
            access_token (str): OAuth-токен.

        Возвращает:
            json: объект с параметрами ответа.

        Примеры:
            >>> update_stocks(stocks, campaign_id, access_token)
            {"skus": [{"sku": "string", "warehouseId": 0, "items": [{
                "count": 0, "type": "FIT", "updatedAt": "2022-12-29T18:02:01Z"}]}]}
            >>> update_stocks(stocks, campaign_id, access_token)
            {"status": "OK", "errors": [{"code": "string", "message": "string"}]}

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Установить цены на товары в магазине.

        Аргументы:
            prices (list): Список словарей с деталями товаров.
            campaign_id (str): Идентификатор кампании.
            access_token (str): OAuth-токен.

        Возвращает:
            json: объект с параметрами ответа.

        Примеры:
            >>> update_price(prices, campaign_id, access_token)
            {"offers": [{"offerId": "string", "id": "string", "feed": {"id": 0},
            "price": {"value": 0, "discountBase": 0, "currencyId": "RUR", "vat": 0},
            "marketSku": 0,
            "shopSku": "string"}]}
            >>> update_price(prices, campaign_id, access_token)
            {"status": "OK", "errors": [{"code": "string", "message": "string"}]}

        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.

    Получает список товаров в магазине и добавляет их артикулы в итоговый список.

    Аргументы:
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth-токен.

    Возвращает:
        list: список строк с артикулами.

    Пример:
        >>> get_offer_ids(campaign_id, market_token)
        ['4534534']

    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список остатков.

        Аргументы:
            watch_remnants (dict): Словарь с таблицей товаров (часов).
            offer_ids (list): Артикулы товаров магазина Yandex Market.
            warehouse_id (str): Идентификатор склада на Маркете.

        Возвращает:
            list: список словарей с артикулами и количеством товаров.

        Пример:
            >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
            [{"sku": "4534534", "warehouseId": 104564, "items": [
                "count": 10, "type": "FIT", updatedAT": "2023-07-09T17:37:34Z"]}]

        """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список с характеристиками цен.

        Аргументы:
            watch_remnants (dict): Словарь с таблицей товаров (часов).
            offer_ids (list): Артикулы товаров магазина.

        Возвращает:
            list: список словарей с характеристиками цены.

        Пример:
            >>> create_prices(watch_remnants, offer_ids)
            [{"id": "3463463", "price": {"value": 5990, "currencyId": "RUR"}}]

        """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Обновить цены товаров.

        Аргументы:
            watch_remnants (dict): Словарь с таблицей товаров (часов).
            campaign_id (str): Идентификатор кампании.
            market_token (str): OAuth-токен.

        Возвращает:
            Список словарей с характеристиками цен, для которых были обновлены значения в магазине.

        Пример:
            >>> create_prices(watch_remnants, campaign_id, market_token)
            [{"id": "3463463", "price": {"value": 5990, "currencyId": "RUR"}}]

        """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновить остатки.

        Аргументы:
            watch_remnants (dict): Словарь с таблицей товаров (часов).
            campaign_id (str): Идентификатор кампании.
            market_token (str): OAuth-токен.
            warehouse_id (str): Идентификатор склада на Маркете.

        Возвращает:
            list: список словарей с артикулами и количеством товаров, количество которых больше нуля.
            list: список словарей с артикулами и количеством всех товаров.

        Примеры:
            [{"sku": "4534534", "warehouseId": 104564, "items": [
                "count": 10, "type": "FIT", updatedAT": "2023-07-09T17:37:34Z"]}],
            [{"sku": "4534534", "warehouseId": 104564, "items": [
                "count": 0, "type": "FIT", updatedAT": "2023-07-09T17:37:34Z"]}]

        """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
