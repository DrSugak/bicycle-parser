from threading import Thread

import lxml
import redis
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


class Olx:
    def __init__(self, db_size: int, html_parser: str, user_agent: str) -> None:
        self.send_notice = db_size > 0
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
            soup = BeautifulSoup(response[0].content, self.html_parser)
            try:
                tags = soup.find("ul", {"data-testid": "category-count-links"}).find_all("li")
                for tag in tags:
                    subcategories.append(f"/{tag.find("a").get("href").split("/velo/")[-1]}")
            except AttributeError:
                pass
        return subcategories

    def find_pagination(self, response, subcategory: str) -> list:
        links = []
        soup = BeautifulSoup(response.content, self.html_parser)
        tags = soup.find_all("li", {"data-testid": "pagination-list-item"})
        if tags:
            last_page = int(tags[-1].get_text())
            for number in range(2, last_page + 1):
                link = f"{self.base_url}{self.category}{subcategory}{self.last_part_url}&page={number}"
                links.append(link)
        return links

    def parse_all_pages(self) -> None:
        for response in self.responses:
            soup = BeautifulSoup(response.content, self.html_parser)
            self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find_all("div", {"data-cy": "l-card", "data-testid": "l-card"})
        for ad in ads:
            try:
                advert = {}
                advert["title"] = ad.find("h4").get_text().lower()
                advert["price"] = ad.find("p", {"data-testid": "ad-price"}).get_text().split("грн")[0].strip()
                advert["link"] = f"{self.base_url}{ad.find("a").get("href")}"
            except AttributeError:
                continue
            if self.send_notice:
                self.send_advert(ad["id"], advert)

    def send_advert(self, _id: str, advert: dict) -> None:
        _id = f"olx:{_id}"
        with redis.Redis(decode_responses=True) as redis_client:
            exists = redis_client.exists(_id)
            if not exists:
                redis_client.hset(_id, mapping=advert)
                redis_client.publish("bicycle", _id)


class XT:
    def __init__(self, db_size: int, html_parser: str, user_agent: str) -> None:
        self.base_url = "http://xt.ht/phpbb"
        self.category_url = [
            "http://xt.ht/phpbb/viewforum.php?f=44&price_type_sel=0&sk=t&sd=d&page=all",
            "http://xt.ht/phpbb/viewforum.php?f=83&price_type_sel=0&sk=m&sd=d&page=all",
        ]
        self.html_parser = html_parser
        self.user_agent = {"User-Agent": user_agent}
        self.send_notice = db_size > 0

    def main(self) -> None:
        self.parse_all_pages()

    def parse_all_pages(self) -> None:
        for category in self.category_url:
            try:
                page = requests.get(category, headers=self.user_agent, timeout=10)
            except requests.exceptions.RequestException:
                continue
            soup = BeautifulSoup(page.text, self.html_parser)
            self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find("div", {"id": "pagecontent"}).find_all("tr")
        for ad in ads:
            try:
                advert = {}
                advert["title"] = ad.find("a", {"class": "topictitle"}).get_text().lower()
                advert["price"] = ad.find("span", {"name": "uah_cur"}).get_text().split("грн")[0].strip()
                advert["link"] = self.base_url + ad.find("a", {"class": "topictitle"}).get("href").split("&sid=")[0][1:]
            except AttributeError:
                continue
            _id = advert["link"].split(".php?")[1]
            self.check_ad(_id, advert)

    def check_ad(self, _id: str, advert: dict) -> None:
        _id = f"xt:{_id}"
        with redis.Redis(decode_responses=True) as redis_client:
            exists = redis_client.exists(_id)
            if not exists:
                redis_client.hset(_id, mapping=advert)
                if self.send_notice:
                    redis_client.publish("bicycle", _id)


class XBikers:
    def __init__(self, db_size: int, html_parser: str, user_agent: str) -> None:
        self.base_url = "https://x-bikers.com/board/"
        self.html_parser = html_parser
        self.user_agent = {"User-Agent": user_agent}
        self.send_notice = db_size > 0

    def main(self) -> None:
        self.parse_all_pages()

    def parse_all_pages(self) -> None:
        try:
            page = requests.get(self.base_url, headers=self.user_agent, timeout=10)
        except requests.exceptions.RequestException:
            return
        soup = BeautifulSoup(page.content, self.html_parser)
        self.find_all_ads_on_page(soup)
        last_page = soup.find("li", {"class": "last"}).find("a").get("href")
        pagination = int(last_page.split("page=")[1])
        for number in range(1, pagination + 1):
            link = f"https://x-bikers.com/board/index.php?&page={number}"
            try:
                page = requests.get(link, headers=self.user_agent, timeout=10)
            except requests.exceptions.RequestException:
                continue
            soup = BeautifulSoup(page.content, self.html_parser)
            self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find_all("tr", {"valign": "middle"})
        for ad in ads:
            try:
                advert = {}
                advert["title"] = ad.find("a", {"class": "gb"}).get_text().lower()
                advert["price"] = ad.find_all("td", {"class": "bfb"})[2].get_text().split("грн")[0].strip()
                advert["link"] = ad.find("a", {"class": "gb"}).get("href")
            except AttributeError:
                continue
            _id = advert["link"].split("id=")[-1]
            self.check_ad(_id, advert)

    def check_ad(self, _id: str, advert: dict) -> None:
        _id = f"xbikers:{_id}"
        with redis.Redis(decode_responses=True) as redis_client:
            exists = redis_client.exists(_id)
            if not exists:
                redis_client.hset(_id, mapping=advert)
                if self.send_notice:
                    redis_client.publish("bicycle", _id)


def main() -> None:
    try:
        with redis.Redis(decode_responses=True) as redis_client:
            db_size = redis_client.dbsize()
    except redis.exceptions.ConnectionError:
        return
    html_parser = lxml.__name__
    user_agent = UserAgent(platforms="pc").random
    olx = Olx(db_size, html_parser, user_agent)
    xt = XT(db_size, html_parser, user_agent)
    x_bikers = XBikers(db_size, html_parser, user_agent)
    thread_olx = Thread(target=olx.main)
    thread_xt = Thread(target=xt.main)
    thread_x_bikers = Thread(target=x_bikers.main)
    thread_olx.start()
    thread_xt.start()
    thread_x_bikers.start()
    thread_olx.join()
    thread_xt.join()
    thread_x_bikers.join()


if __name__ == "__main__":
    main()
