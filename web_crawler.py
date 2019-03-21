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
from hashing import *

frontier = multiprocessing.Queue()
manager = multiprocessing.Manager()
visited_dict = manager.dict()
roots_dict = manager.dict()
documents_dict = manager.dict()

supported_files = [".pdf", ".doc", ".docx", ".ppt", ".pptx"]

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
        self.root_name = ""
        self.current_page = ""

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

    def to_canonical(self, url: str):
        return urlcanon.semantic(urlcanon.parse_url(url))

    def is_valid_url(self,url: str):
        return validators.url(url)

    def parse_robots(self, url: str):
        # Standard robot parser
        try:
            path = str(self.to_canonical(url)) + "robots.txt"
            response = requests.get(path, timeout=5)
            self.robots_parser.set_url(path)
            lines = response.text.split("\n")
            self.robots_parser.parse(lines)

            # Sitemap parsing
            sitemaps = [line for line in lines if "Sitemap" in line]
            links = [link.split(" ")[1] for link in sitemaps]

            for link in links:
                if not self.is_government_url(link) or self.is_already_visited(link):
                    continue
                frontier.put(link)
        except Exception as e:
            print("Http error while fetching " + path)

    def fetch_url(self, url: str):

        # TODO: here we must check status codes
        #       also how to handle timeouts?
        # response = requests.get(url, timeout=5)

        # Is there a way to fetch page with URL and
        # then render it with selenium? Would prefer to use
        # requests module for retrieving page content.

        # TODO Try catch and error detection here

        try:
            # Check if we already saw this root domain
            rootd = self.get_root_domain(url)
            self.root_name = rootd

            if rootd in roots_dict.keys():
                self.robots_parser = roots_dict[rootd]
            else:
                self.parse_robots(rootd)
                roots_dict[rootd] = self.robots_parser

            curl = str(self.to_canonical(url))

            def should_fetch_url(url):
                return not self.is_already_visited(url) and self.is_allowed_by_robots(url) and self.is_government_url(curl)

            if not should_fetch_url(curl):
                print("Url " + curl + " not allowed! Skipping ...")
                return

            visited_dict[curl] = None
            self.current_page = curl

            crawl_delay = self.robots_parser.crawl_delay("*")
            if crawl_delay:
                time.sleep(int(crawl_delay))
            else:
                time.sleep(0.5)

            if self.should_download_and_save_file(curl):
                print("Downloading file " + str(url))
                self.download_file(url, "output/")
            else:
                print("Requesting " + str(url))
                self.driver.get(curl)
                # TODO check if page is similar to some other one with the hash crap
                self.parse_page_content()

        except Exception as ex:
            print(ex)
            pass

    def parse_page_content(self,):
        # Parse all links and put them in the frontier after checking they're '.gov.si'
        print("Did receive " + str(self.current_page))

        document = self.driver.page_source
        hashed = hash_document(document)
        if hashed in documents_dict:
            print("Already visited! Skipping ...")
            return
        else:
            documents_dict[hashed] = True # TODO: - What value here??

        soup = BeautifulSoup(document, 'html.parser')
        hrefs = [a.get("href") for a in soup.find_all(href=True) if self.is_valid_url(a.get("href"))]

        print("Received " + str(len(hrefs)) + " potential new urls")

        added = 0
        for href in hrefs:
            canonical = str(self.to_canonical(href))
            if not self.is_government_url(canonical) or self.is_already_visited(canonical):
                continue
            frontier.put(canonical)
            added += 1
        print("Added " + str(added) + " new urls from hrefs")

        # Handling JS onclick
        # TODO Needs field testing
        all_tags = soup.find_all()
        clickables = [str(a.get("onclick")).split("=")[-1].replace('"', '').replace("'", "") for a in all_tags if
                      "onclick" in str(a)]
        added = 0
        for c in clickables:
            nurl = self.to_canonical(self.current_page) + c
            if self.is_valid_url(nurl) and self.is_allowed_by_robots(nurl):
                frontier.put(self.to_canonical(nurl))
                added+=1
        print("Added " + str(added) + " new urls from js click")

        # Image collection
        # TODO Needs field testing
        images = [a.get("src") for a in soup.find_all('img')]
        image_sources = []
        added = 0
        for imgs in images:
            if self.is_valid_url(imgs):
                image_sources.append(imgs)
                added += 1
            elif self.is_valid_url(self.to_canonical(self.current_page)+imgs):
                    image_sources.append(imgs)
                    added += 1
            else:
                print("Could not parse image link "+imgs)

        print("Added " + str(added) + " new images to list")

    def download_file(self, url, path="output/"):
        request = requests.get(url, allow_redirects=True)
        file_name = url.split("/")[-1:][0]
        with open(file_name, 'wb') as handle:
            # TODO: - not sure why appending `output/` to the beginning of the line
            # doesnt work ??
            handle.write(request.content)

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
        # self.db_connection = db_connect()
        # print(self.db_connection)
        #
        self.__get_chrome_driver()
        #
        # # TODO: gracefully close connection,
        # #       when process is finished.
        # self.db_connection.close()
        # print(self.db_connection)

        return self.dequeue_url()

    def is_government_url(self, url):
        return ".gov.si" in url

    def is_already_visited(self, url):
        return url in visited_dict.keys()

    def is_allowed_by_robots(self, url):
        return self.robots_parser.can_fetch("*", url)

    def should_download_and_save_file(self, url):
        for f in supported_files: # TODO: - refactor this using python magic
            if f in url:
                return True
        return False

def _future_callback(future: Future):
    print(future.result())


if __name__ == "__main__":
    sites = [
        'http://evem.gov.si/',
        'https://e-uprava.gov.si/',
        'https://podatki.gov.si/',
        'http://www.e-prostor.gov.si/',
        # additional
        'http://www.gov.si/',
        'http://prostor3.gov.si/preg/',
        'https://egp.gu.gov.si/egp/',
        'http://www.gu.gov.si/',
        'https://gis.gov.si/ezkn/'
    ]
    for site in sites:
        frontier.put(site)

    workers = int(sys.argv[1]) if len(sys.argv) >= 2 else 4
    with ProcessPoolExecutor(max_workers=workers) as executor:
        def submit_worker(_f):
            _future = executor.submit(_f)
            _future.add_done_callback(_future_callback)
            return _future

        futures = [submit_worker(Worker()) for _ in range(workers)]

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
