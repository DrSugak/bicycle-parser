import json
from datetime import datetime
from pathlib import Path
from threading import Thread

import lxml
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

Path("data").resolve().mkdir(exist_ok=True)


class Olx:
    def __init__(self, parser: str, user_agent: str) -> None:
        self.base_url = "https://www.olx.ua"
        self.category_url = [
            "/hobbi-otdyh-i-sport/velo/velozapchasti/",
            "/hobbi-otdyh-i-sport/velo/veloaksessuary/",
        ]
        self.last_part_url = "?currency=UAH&search[order]=created_at:desc&view=list"
        self.new_data = {}
        self.json_data = {}
        self.parser = parser
        self.user_agent = {"User-Agent": user_agent}

    def main(self) -> None:
        self.new_data = {}
        path = Path("data", "olx.json").resolve()
        if not path.exists():
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump({"olx": None}, json_file, indent=4, ensure_ascii=False)
        with path.open(mode="r", encoding="utf-8") as json_file:
            self.json_data = json.load(json_file)
        self.parse_all_pages()
        if len(self.new_data) > 1:
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump(self.new_data, json_file, indent=4, ensure_ascii=False)
        self.json_data = {}

    def parse_all_pages(self) -> None:
        for category in self.category_url:
            link = self.base_url + category + self.last_part_url
            try:
                page = requests.get(link, headers=self.user_agent, timeout=10)
            except requests.exceptions.RequestException:
                return
            soup = BeautifulSoup(page.content, self.parser)
            self.find_all_ads_on_page(soup)
            pagination = soup.find_all("li", {"data-testid": "pagination-list-item"})
            last_page = int(pagination[-1].get_text())
            for number in range(2, last_page + 1):
                next_page_url = f"?currency=UAH&page={number}&search[order]=created_at:desc&view=list"
                link = self.base_url + category + next_page_url
                try:
                    page = requests.get(link, headers=self.user_agent, timeout=10)
                except requests.exceptions.RequestException:
                    return
                soup = BeautifulSoup(page.content, self.parser)
                self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find_all("div", {"data-cy": "l-card", "data-testid": "l-card"})
        for ad in ads:
            try:
                advert = {}
                advert["name"] = ad.find("h6").get_text().lower()
                advert["price"] = ad.find("p", {"data-testid": "ad-price"}).get_text().split("грн")[0].strip()
                advert["link"] = self.base_url + ad.find("a").get("href")
                date = ad.find("p", {"data-testid": "location-date"}).get_text().split("-")[-1].strip()
            except AttributeError:
                continue
            if "/uk/" in advert["link"]:
                href = advert["link"].split("/uk/")
                advert["link"] = href[0] + "/" + href[1]
            if "/obyavlenie/" in advert["link"]:
                if ad["id"] not in self.new_data:
                    self.new_data[ad["id"]] = advert
                    if ad["id"] not in self.json_data:
                        if len(date.split()[0]) != 2:
                            self.check_ad(advert)

    def check_ad(self, advert: dict) -> None:
        if len(self.json_data) <= 1:
            return
        try:
            page = requests.get(advert["link"], headers=self.user_agent, timeout=10)
        except requests.exceptions.RequestException:
            return
        soup = BeautifulSoup(page.content, self.parser)
        try:
            category = soup.find("ol", {"data-cy": "categories-breadcrumbs"}).find_all("li")[3].find("a").get("href")
        except AttributeError:
            return
        if (self.category_url[0] or self.category_url[1]) in category:
            print(advert["link"])


class XT:
    def __init__(self, parser: str, user_agent: str) -> None:
        self.base_url = "http://xt.ht/phpbb"
        self.category_url = [
            "http://xt.ht/phpbb/viewforum.php?f=44&price_type_sel=0&sk=t&sd=d&page=all",
            "http://xt.ht/phpbb/viewforum.php?f=83&price_type_sel=0&sk=m&sd=d&page=all",
        ]
        self.new_data = {}
        self.json_data = {}
        self.parser = parser
        self.user_agent = {"User-Agent": user_agent}

    def main(self) -> None:
        self.new_data = {}
        path = Path("data", "xt.json").resolve()
        if not path.exists():
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump({"xt": None}, json_file, indent=4, ensure_ascii=False)
        with path.open(mode="r", encoding="utf-8") as json_file:
            self.json_data = json.load(json_file)
        self.parse_all_pages()
        if len(self.new_data) > 1:
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump(self.new_data, json_file, indent=4, ensure_ascii=False)
        self.json_data = {}

    def parse_all_pages(self) -> None:
        for category in self.category_url:
            try:
                page = requests.get(category, headers=self.user_agent, timeout=10)
            except requests.exceptions.RequestException:
                continue
            soup = BeautifulSoup(page.text, self.parser)
            self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find("div", {"id": "pagecontent"}).find_all("tr")
        for ad in ads:
            try:
                advert = {}
                advert["name"] = ad.find("a", {"class": "topictitle"}).get_text().lower()
                advert["price"] = ad.find("span", {"name": "uah_cur"}).get_text().split("грн")[0].strip()
                advert["link"] = self.base_url + ad.find("a", {"class": "topictitle"}).get("href").split("&sid=")[0][1:]
            except AttributeError:
                continue
            self.new_data[advert["link"].split(".php?")[1]] = advert
            if advert["link"].split(".php?")[1] not in self.json_data:
                self.check_ad(advert)

    def check_ad(self, advert: dict) -> None:
        if len(self.json_data) > 1:
            print(advert["link"])


class XBikers:
    def __init__(self, parser: str, user_agent: str) -> None:
        self.base_url = "https://x-bikers.com/board/"
        self.new_data = {}
        self.json_data = {}
        self.parser = parser
        self.user_agent = {"User-Agent": user_agent}

    def main(self) -> None:
        self.new_data = {}
        path = Path("data", "x-bikers.json").resolve()
        if not path.exists():
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump({"x-bikers": None}, json_file, indent=4, ensure_ascii=False)
        with path.open(mode="r", encoding="utf-8") as json_file:
            self.json_data = json.load(json_file)
        self.parse_all_pages()
        if len(self.new_data) > 1:
            with path.open(mode="w", encoding="utf-8") as json_file:
                json.dump(self.new_data, json_file, indent=4, ensure_ascii=False)
        self.json_data = {}

    def parse_all_pages(self) -> None:
        try:
            page = requests.get(self.base_url, headers=self.user_agent, timeout=10)
        except requests.exceptions.RequestException:
            return
        soup = BeautifulSoup(page.content, self.parser)
        self.find_all_ads_on_page(soup)
        last_page = soup.find("li", {"class": "last"}).find("a").get("href")
        pagination = int(last_page.split("page=")[1])
        for number in range(1, pagination + 1):
            link = f"https://x-bikers.com/board/index.php?&page={number}"
            try:
                page = requests.get(link, headers=self.user_agent, timeout=10)
            except requests.exceptions.RequestException:
                continue
            soup = BeautifulSoup(page.content, self.parser)
            self.find_all_ads_on_page(soup)

    def find_all_ads_on_page(self, soup: BeautifulSoup) -> None:
        ads = soup.find_all("tr", {"valign": "middle"})
        for ad in ads:
            try:
                advert = {}
                advert["name"] = ad.find("a", {"class": "gb"}).get_text().lower()
                advert["price"] = ad.find_all("td", {"class": "bfb"})[2].get_text().split("грн")[0].strip()
                advert["link"] = ad.find("a", {"class": "gb"}).get("href")
            except AttributeError:
                continue
            self.new_data[advert["link"].split("id=")[-1]] = advert
            if advert["link"].split("id=")[-1] not in self.json_data:
                self.check_ad(advert)

    def check_ad(self, advert: dict) -> None:
        if len(self.json_data) > 1:
            print(advert["link"])


def main() -> None:
    parser = lxml.__name__
    user_agent = UserAgent()
    while True:
        print("Start", datetime.now())
        olx = Olx(parser, user_agent.random)
        xt = XT(parser, user_agent.random)
        x_bikers = XBikers(parser, user_agent.random)
        thread_olx = Thread(target=olx.main)
        thread_xt = Thread(target=xt.main)
        thread_x_bikers = Thread(target=x_bikers.main)
        thread_olx.start()
        thread_xt.start()
        thread_x_bikers.start()
        thread_olx.join()
        thread_xt.join()
        thread_x_bikers.join()
        print("Finish", datetime.now())


if __name__ == "__main__":
    main()
