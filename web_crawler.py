""" Basic web crawl implementation """

import sys
import os
import time
import requests
import urllib.request
import multiprocessing

from queue import Empty
from concurrent.futures import ProcessPoolExecutor, Future, ALL_COMPLETED, wait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


import urllib.robotparser
from urllib.parse import urlparse
import urlcanon
import validators

from bs4 import BeautifulSoup

from utils import db_connect


frontier = multiprocessing.Queue()
manager = multiprocessing.Manager()
shared_dict = manager.dict()

class Worker:

    """ Base class for web crawler.

    TODO: - HTTP downloader and renderer: To retrieve and render a web page.
          - Data extractor: Links extracting done 70% done. Need images.
          - Duplicate detector: Basic done. Need advanced based on content.
          - Datastore: To store the data and additional metadata used by the crawler.

    """

    def __init__(self):
        self.driver = None
        self.db_connection = None
        self.robots_parser = urllib.robotparser.RobotFileParser()

    def __get_chrome_driver(self):
        # TODO - Pretend to be a browser
        chrome_options = Options()
        chrome_options.add_argument('--headless')

        if sys.platform == 'win32':
            driver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
        else:
            driver_path = os.path.join(os.getcwd(), 'chromedriver')

        self.driver = webdriver.Chrome(driver_path, options=chrome_options)

    def get_root_domain(self, url: str):
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}/'.format(uri=parsed_uri)
        return domain

    def parse_robots(self, url: str):
        # Standard robot parser
        path = urlcanon.semantic(urlcanon.parse_url(url)) + "robots.txt"
        self.robots_parser.set_url(path)
        self.robots_parser.read()

        # Sitemap parsing
        contents = urllib.request.urlopen(path).read().decode("utf-8")
        sitemaps = [line for line in contents.split("\n") if "Sitemap" in line]
        links = [link.split(" ")[1] for link in sitemaps]

        for link in links:
            contents = urllib.request.urlopen(link).read()
            soup = BeautifulSoup(contents, 'html.parser')
            for loc in soup.find_all("loc"):
                if loc.contents not in shared_dict.keys():
                    frontier.put(loc.contents)

        return self.robots_parser

    def fetch_url(self, url: str):

        # TODO: here we must check status codes
        #       also how to handle timeouts?
        # response = requests.get(url, timeout=5)

        # Is there a way to fetch page with URL and
        # then render it with selenium? Would prefer to use
        # requests module for retrieving page content.

        # TODO Try catch and error detection here

        try:
            rootd = self.get_root_domain(url)

            if rootd in shared_dict.keys():
                self.robots_parser = shared_dict[rootd]
            else:
                shared_dict[rootd] = self.parse_robots(rootd)
                self.robots_parser = shared_dict[rootd]

            curl = urlcanon.semantic(urlcanon.parse_url(url))

            if curl not in shared_dict.keys() and self.robots_parser.can_fetch("*", curl):
                # TODO More advanced already visited detection, ex. reverse hash functions
                shared_dict[curl] = None
                cd = self.robots_parser.crawl_delay("*")

                if cd is not None:
                    time.sleep(int(cd))
                else:
                    time.sleep(0.5)

                self.driver.get(url)
                self.parse_page_content()
            else:
                pass
        except Exception as ex:
            print(ex)
            pass

    def parse_page_content(self,):
        # Parse all links and put them in the frontier after checking they're '.gov.si'
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        hrefs = [a.get("href") for a in soup.find_all('a', href=True) if validators.url(a.get("href"))]

        [frontier.put(href) for href in hrefs if urlcanon.semantic(urlcanon.parse_url(href))
         not in shared_dict.keys() and ".gov.si" in href]

        images = [a.get for a in soup.find_all('img')]

        # TODO Implement JS onclick in beautiful soup
        # TODO Filter bad images
        # TODO how to handle JS - depends on wether we want to always run Selenium or not
        # scripts = [a for a in soup.find_all('script')]
        # pprint(scripts)
        # print(self.driver.page_source)

    def dequeue_url(self):
        # Fetch URLs from Frontier.
        while True:
            try:
                url = frontier.get(True, timeout=10)
            except Empty:
                return 'Process {} stopped. No new URLs in Frontier\n'.format(os.getpid())

            # print(os.getpid(), "got", url, 'is empty:', frontier.empty())
            self.fetch_url(url)
            time.sleep(1)  # simulate a "long" operation

    def __call__(self):
        # connect to PostgreSQL database
        self.db_connection = db_connect()
        print(self.db_connection)

        self.__get_chrome_driver()

        # TODO: gracefully close connection,
        #       when process is finished.
        self.db_connection.close()
        print(self.db_connection)

        return self.dequeue_url()


def _future_callback(future: Future):
    print(future.result())


if __name__ == "__main__":
    worker = Worker()

    sites = [
        'http://evem.gov.si/',
        'https://e-uprava.gov.si/',
        'https://podatki.gov.si/',
        'http://www.e-prostor.gov.si/'
    ]
    for site in sites:
        frontier.put(site)

    workers = 4
    with ProcessPoolExecutor(max_workers=workers) as executor:
        def submit_worker(_f):
            _future = executor.submit(_f)
            _future.add_done_callback(_future_callback)
            return _future

        futures = [submit_worker(worker) for _ in range(workers)]

        # while True:
        #     in_num = input()
        #     if in_num == 42:
        #         break
        #     else:
        #         frontier.put(in_num)

        # This will stop our crawler when none of the running
        # processes cant fetch URL from Frontier
        wait(futures, return_when=ALL_COMPLETED)

    sys.exit(0)
