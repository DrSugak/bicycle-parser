import json
import logging
from os import getenv
from threading import Thread
from typing import List

import lxml
import pika
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class Request:
    @staticmethod
    def request(links: list, user_agent: dict) -> list:
        responses = []
        with requests.Session() as session:
            for link in links:
                try:
                    response = session.get(link, headers=user_agent, timeout=10)
                    responses.append(response)
                except requests.exceptions.RequestException:
                    continue
        return responses


class RabbitMQPublisher:
    def __init__(self) -> None:
        self.validate_env_vars(["RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_DEFAULT_USER", "RABBITMQ_DEFAULT_PASS"])
        self.host = getenv("RABBITMQ_HOST")
        self.port = getenv("RABBITMQ_PORT")
        self.user = getenv("RABBITMQ_DEFAULT_USER")
        self.password = getenv("RABBITMQ_DEFAULT_PASS")
        self.queue = "parsed_bicycle_ads"
        self.connection = None
        self.channel = None
        self.connect()

    def validate_env_vars(self, required_vars: List[str]) -> None:
        if missing_vars := [var for var in required_vars if not getenv(var)]:
            logging.critical(f"RabbitMQ: missing required environment variables: {', '.join(missing_vars)}")
            raise SystemExit(1)

    def connect(self) -> None:
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue, durable=True)
            logging.info("RabbitMQ: connected")
        except Exception as e:
            logging.critical("RabbitMQ: connection failed")
            raise SystemExit(1)

    def send(self, notifications: List[dict]) -> None:
        if not self.connection or self.connection.is_closed:
            self.connect()
        try:
            for notification in notifications:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=self.queue,
                    body=json.dumps(notification),
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
                )
        except Exception as exc:
            logging.warning(f"RabbitMQ: message not sent: {exc}")


class Olx:
    def __init__(self, rabbitmq: RabbitMQPublisher, html_parser: str, user_agent: str) -> None:
        self.rabbitmq = rabbitmq
        self.html_parser = html_parser
        self.user_agent = {"User-Agent": user_agent}
        self.base_url = "https://www.olx.ua"
        self.category = "/hobbi-otdyh-i-sport/velo"
        self.subcategories = []
        self.last_part_url = "?currency=UAH&search[order]=created_at:desc&view=list"
        self.responses = []

    def main(self) -> None:
        self.subcategories = self.find_subcategories()
        for subcategory in self.subcategories:
            link = f"{self.base_url}{self.category}{subcategory}{self.last_part_url}"
            response = Request.request([link], self.user_agent)
            self.responses += response
            if response:
                links = self.find_pagination(response[0], subcategory)
                responses = Request.request(links, self.user_agent)
                self.responses += responses
        if self.responses:
            self.parse_all_pages()

    def find_subcategories(self) -> list:
        subcategories = []
        link = f"{self.base_url}{self.category}"
        response = Request.request([link], self.user_agent)
        if response:
            soup = BeautifulSoup(response[0].text, self.html_parser)
            try:
                tags = soup.find("ul", {"data-testid": "category-count-links"}).find_all("li")
                for tag in tags:
                    subcategories.append(f"/{tag.find("a").get("href").split("/velo/")[-1]}")
            except AttributeError:
                pass
        return subcategories

    def find_pagination(self, response, subcategory: str) -> list:
        links = []
        soup = BeautifulSoup(response.text, self.html_parser)
        tags = soup.find_all("li", {"data-testid": "pagination-list-item"})
        if tags:
            last_page = int(tags[-1].get_text())
            for number in range(2, last_page + 1):
                link = f"{self.base_url}{self.category}{subcategory}{self.last_part_url}&page={number}"
                links.append(link)
        return links

    def parse_all_pages(self) -> None:
        for response in self.responses:
            soup = BeautifulSoup(response.text, self.html_parser)
            notifications = self.find_all_ads_on_page(soup)
            if notifications:
                self.rabbitmq.send(notifications)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> list:
        notifications = []
        ads = soup.find_all("div", {"data-cy": "l-card", "data-testid": "l-card"})
        for ad in ads:
            notice = {"site": "olx"}
            try:
                advert = {}
                advert["title"] = ad.find("h4").get_text().lower()
                advert["price"] = ad.find("p", {"data-testid": "ad-price"}).get_text().split("грн")[0].strip()
                advert["link"] = f"{self.base_url}{ad.find("a").get("href")}"
            except AttributeError:
                continue
            notice["id"] = ad["id"]
            notice[notice["id"]] = advert
            notifications.append(notice)
        return notifications


class XT:
    def __init__(self, rabbitmq: RabbitMQPublisher, html_parser: str, user_agent: str) -> None:
        self.rabbitmq = rabbitmq
        self.html_parser = html_parser
        self.user_agent = {"User-Agent": user_agent}
        self.base_url = "http://xt.ht/phpbb"
        self.categories = [
            "/viewforum.php?f=44&price_type_sel=0&sk=t&sd=d&page=all",
            "/viewforum.php?f=83&price_type_sel=0&sk=m&sd=d&page=all",
        ]
        self.responses = []

    def main(self) -> None:
        for category in self.categories:
            link = f"{self.base_url}{category}"
            self.responses += Request.request([link], self.user_agent)
        if self.responses:
            self.parse_all_pages()

    def parse_all_pages(self) -> None:
        for response in self.responses:
            soup = BeautifulSoup(response.text, self.html_parser)
            notifications = self.find_all_ads_on_page(soup)
            if notifications:
                self.rabbitmq.send(notifications)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> list:
        notifications = []
        ads = soup.find_all("tr")
        for ad in ads:
            notice = {"site": "xt"}
            try:
                advert = {}
                advert["title"] = ad.find("a", {"class": "topictitle"}).get_text().lower()
                advert["price"] = ad.find("span", {"name": "uah_cur"}).get_text().split("грн")[0].strip()
                topictitle = ad.find("a", {"class": "topictitle"})
                href = topictitle.get("href").split("&sid=")[0][1:]
                advert["link"] = f"{self.base_url}{href}"
            except AttributeError:
                continue
            notice["id"] = advert["link"].split(".php?")[1]
            notice[notice["id"]] = advert
            notifications.append(notice)
        return notifications


class XBikers:
    def __init__(self, rabbitmq: RabbitMQPublisher, html_parser: str, user_agent: str) -> None:
        self.rabbitmq = rabbitmq
        self.html_parser = html_parser
        self.user_agent = {"User-Agent": user_agent}
        self.base_url = "https://x-bikers.com/board/"
        self.responses = []

    def main(self) -> None:
        self.responses += Request.request([self.base_url], self.user_agent)
        if self.responses:
            links = self.find_pagination(self.responses[0])
            self.responses += Request.request(links, self.user_agent)
            self.parse_all_pages()

    def find_pagination(self, response) -> list:
        links = []
        soup = BeautifulSoup(response.text, self.html_parser)
        try:
            pagination = soup.find("li", {"class": "last"}).find("a").get("href")
            last_page = int(pagination.split("page=")[1])
            for number in range(1, last_page + 1):
                link = f"{self.base_url}index.php?&page={number}"
                links.append(link)
        except AttributeError:
            pass
        return links

    def parse_all_pages(self) -> None:
        for response in self.responses:
            soup = BeautifulSoup(response.text, self.html_parser)
            notifications = self.find_all_ads_on_page(soup)
            if notifications:
                self.rabbitmq.send(notifications)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> list:
        notifications = []
        ads = soup.find_all("tr", {"valign": "middle"})
        for ad in ads:
            notice = {"site": "xbikers"}
            try:
                advert = {}
                advert["title"] = ad.find("a", {"class": "gb"}).get_text().lower()
                advert["price"] = ad.find_all("td", {"class": "bfb"})[2].get_text().split("грн")[0].strip()
                advert["link"] = ad.find("a", {"class": "gb"}).get("href")
            except AttributeError:
                continue
            notice["id"] = advert["link"].split("id=")[-1]
            notice[notice["id"]] = advert
            notifications.append(notice)
        return notifications


def main() -> None:
    rabbitmq = RabbitMQPublisher()
    html_parser = lxml.__name__
    user_agent = UserAgent(platforms="pc").random
    olx = Olx(rabbitmq, html_parser, user_agent)
    xt = XT(rabbitmq, html_parser, user_agent)
    x_bikers = XBikers(rabbitmq, html_parser, user_agent)
    thread_olx = Thread(target=olx.main)
    thread_xt = Thread(target=xt.main)
    thread_x_bikers = Thread(target=x_bikers.main)
    thread_olx.start()
    thread_xt.start()
    thread_x_bikers.start()
    thread_olx.join()
    thread_xt.join()
    thread_x_bikers.join()
    if rabbitmq.connection and rabbitmq.connection.is_open:
        rabbitmq.connection.close()
        logging.info("RabbitMQ: connection closed")


if __name__ == "__main__":
    main()
