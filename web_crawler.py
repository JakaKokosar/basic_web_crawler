""" Basic web crawl implementation """

import sys
import os
import time
import datetime
from urllib.parse import urlparse

import requests
import multiprocessing
import urllib3
import urlcanon
import validators

from queue import Empty

from psycopg2._psycopg import ProgrammingError
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ProcessPoolExecutor, Future, ALL_COMPLETED, wait
from urllib import robotparser, request, parse

import sitemap
from utils import DBConn, DBApi
from hashing import *

manager = multiprocessing.Manager()
frontier = multiprocessing.Queue()
frontier_dict = manager.dict()
visited_dict = manager.dict()
site_domains = manager.dict()
documents_dict = manager.dict()


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


download_dir = "data/"
supported_files = ["pdf", "doc", "docx", "ppt", "pptx"]

try:
    os.makedirs(download_dir)
except OSError as e:
    pass

connections = None

class Worker:
    """ Base class for web crawler.

    TODO: - HTTP downloader and renderer: To retrieve and render a web page.
          - Data extractor: Links extracting done 70% done. Need images.
          - Duplicate detector: Basic done. Need advanced based on content.
          - Datastore: To store the data and additional metadata used by the crawler.

    """

    def __init__(self, id):
        self.id = id
        self.driver = None
        self.root_name = ""
        self.current_page = ""

    def __get_chrome_driver(self):
        # TODO - Pretend to be a browser
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        # chrome_options.accept_untrusted_certs = True

        if sys.platform == "win32":
            driver_path = os.path.join(os.getcwd(), "chromedriver.exe")
        else:
            driver_path = os.path.join(os.getcwd(), "chromedriver")

        self.driver = webdriver.Chrome(driver_path, options=chrome_options)
        self.driver.set_page_load_timeout(10)

    def get_root_domain(self, url: str):
        parsed_uri = urlparse(url)
        domain = '{uri.netloc}/'.format(uri=parsed_uri)
        return domain

    def to_canonical(self, url: str):
        return urlcanon.semantic(urlcanon.parse_url(url))

    def is_valid_url(self, url: str):
        return validators.url(url)

    def add_to_frontier(self, url, site_id):
        frontier.put(url)
        frontier_dict[url] = True
        page_id = self.conn.insert_page(site_id, "FRONTIER", url, None, None, None)
        return page_id

    @property
    def conn(self) -> DBApi:
        return connections[self.id]

    def parse_robots(self, url: str):
        """  Standard robot parser
        """
        site_domain = self.get_domain_from_url(url)
        if site_domain in site_domains:
            # we have already saw this site
            site_id = self.conn.site_id_for_domain(site_domain)
            return site_id, site_domains.get(site_domain, None)
        else:
            # first time we are on this domain, check for robots.txt, parse it, save it!
            robots_location = "http://" + site_domain + "/robots.txt"
            robot_file_parser = robotparser.RobotFileParser()
            robot_file_parser.set_url(robots_location)
            try:
                response = requests.get(robots_location, timeout=10)
                response.raise_for_status()
                robots_content = response.text.split("\n")
            except requests.exceptions.RequestException as err:
                # This is a general request exception. Should we care about if something went wrong?
                # For now we assume that robots.txt in unavailable or is not present at all.
                # TODO: this should be logs not prints.
                print(
                    "Unexpected error when requesting robots.txt for {}".format(url),
                    err,
                )
                # need to store some value so we know that we already examined this domain
                site_domains[site_domain] = "not present" # need to store something
                site_id = self.conn.insert_site(site_domain, "/", "/")
                return site_id, None

            robot_file_parser.parse(robots_content)
            site_domains[site_domain] = robot_file_parser

            # Sitemap parsing
            sitemaps = [line for line in robots_content if "Sitemap" in line]
            links = [link.split(" ")[1] for link in sitemaps]

            sitemaps_content = ""

            urls_to_add = []

            for link in links:
                req = requests.get(link)
                sitemap_urls = sitemap.parse_xml(req.text)
                sitemaps_content += req.text + "\n"
                for url in sitemap_urls:
                    if not self.is_government_url(url) or self.is_already_visited(url) or url in frontier_dict.keys():
                        continue
                    urls_to_add.append(url)

            site_id = self.conn.insert_site(site_domain, response.text, sitemaps_content)
            for url in urls_to_add:
                page_id = self.add_to_frontier(url, site_id)
                print("Added " + url + " to `FRONTIER` page with id " + str(page_id))

            print("Added %d urls from sitemap!" % len(urls_to_add))

            return site_id, robot_file_parser

    def parse_url(self, url: str):
        # unify url representation
        url = str(self.to_canonical_form(url))

        if url in frontier_dict.keys() or url in visited_dict.keys():
            # TODO: this should not happen, but it does :S
            return

        # get robot parser object for current site domain.
        site_id, robot_parser = self.parse_robots(url)

        # URL passed all checks. We can store it as visited.
        visited_dict[url] = True

        # fetch url
        self.fetch_url(url, site_id, robot_parser)

    def fetch_url(self, url: str, site_id: int, robots: robotparser.RobotFileParser):
        try:
            response = self.get_response(url)  # this can raise exception
            status_code = response.status_code

            if self.should_download_and_save_file(url):
                self.save_file(url, response)
            elif "image" in response.headers["Content-Type"]:
                self.save_image(url, response)
            elif "text/html" in response.headers["Content-Type"]:
                # open with selenium to render all the javascript
                try:
                    self.driver.get(url)
                    self.parse_page_content(site_id, url, status_code, datetime.datetime.now(), robots)
                except Exception as e:
                    print("An error occured while parsing page content: " + str(e))
                    self.conn.insert_page(site_id, "HTML", url, None, 500, datetime.datetime.now())
                    # TODO: - should update

        except Exception as err:
            print("Error at {}".format(url), err)
            page_id = self.conn.insert_page(site_id, "HTML", url, None, 404, datetime.datetime.now())
            # TODO: - should update

    def parse_page_content(self, site_id: int, url: str, status_code, accessed_time, robots: robotparser.RobotFileParser):
        document = self.driver.page_source
        hashed = hash_document(document)

        existing_page_id = self.conn.page_id_for_page_in_frontier(site_id, url)

        try:
          if hashed in documents_dict:
              if existing_page_id:
                  self.conn.update_page(existing_page_id, "DUPLICATE", None, status_code, accessed_time)
                  print("Updated page to `DUPLICATE` with id " + str(existing_page_id) + " at url: " + url)
              else:
                  page_id = self.conn.insert_page(site_id, "DUPLICATE", url, None, status_code, accessed_time)
                  print("Added `DUPLICATE` page with id " + str(page_id) + " at url: " + url)
              return
          else:
              documents_dict[hashed] = True

              if existing_page_id:
                  self.conn.update_page(existing_page_id, "HTML", document, status_code, accessed_time)
                  print("Updated page to `HTML` with id " + str(existing_page_id) + " at url: " + url)
              else:
                  existing_page_id = self.conn.insert_page(site_id, "HTML", url, document, status_code, accessed_time)
                  if not existing_page_id:
                      return
                  print("Added `HTML` page with id " + str(existing_page_id) + " at url: " + url)
        except Exception as e:
          print(e)
          return

        if not existing_page_id:
            print()

        soup = BeautifulSoup(document, 'html.parser')
        hrefs = [
            str(self.to_canonical_form(a.get("href")))
            for a in soup.find_all(href=True)
            if self.is_valid_url(a.get("href"))
        ]
        print("Found " + str(len(hrefs)) + " potential new urls")

        added = 0
        for href in hrefs:
            if not self.is_government_url(href) or self.is_already_visited(href) or href in frontier_dict.keys():
                continue
            # print("Added " + href + " to `FRONTIER` page with id " + str(page_id))
            page_id = self.add_to_frontier(href, site_id)
            self.conn.insert_link(existing_page_id, page_id)
            added += 1

        print("Added " + str(added) + " new urls from hrefs")

        # # Handling JS onclick
        # # TODO Needs field testing
        # all_tags = soup.find_all()
        # clickables = [
        #     str(a.get("onclick")).split("=")[-1].replace('"', '').replace("'", "")
        #     for a in all_tags
        #     if "onclick" in str(a)
        # ]
        # added = 0
        # for c in clickables:
        #     nurl = url + c
        #     if self.is_valid_url(nurl) and self.is_allowed_by_robots(nurl, robots):
        #         frontier.put(self.to_canonical(nurl))
        #         added += 1
        # print("Added " + str(added) + " new urls from js click")

        # Image collection
        # TODO Needs field testing
        # images = [a.get("src") for a in soup.find_all('img')]
        # image_sources = []
        # added = 0
        # for img in images:
        #     if self.is_valid_url(img):
        #         image_sources.append(img)
        #         added += 1
        #         if self.is_already_visited(img) or img in frontier_dict.keys():
        #             continue
        #         self.add_to_frontier(img, site_id)
        #     elif self.is_valid_url(url + img):
        #         image_sources.append(img)
        #         added += 1
        #         if self.is_already_visited(img) or img in frontier_dict.keys():
        #             continue
        #         self.add_to_frontier(img, site_id)

        print("Added " + str(added) + " new images to list")

    def save_file(self, url: str, response):
        data_type_code = [
            extension
            for extension in supported_files
            if extension in response.headers["Content-Type"]
        ]
        if not data_type_code:
            # something went wrong! abort ..
            print("Error storing file from %s! Invalid Content-Type: %s" % (url, response.headers["Content-Type"]))
            return

        page_id = self.conn.page_for_url(url)
        self.conn.insert_page_data(page_id, data_type_code[0].upper(), response.content)

        # TODO: - update `FRONTIER` to `BINARY`

    def save_image(self, url: str, response):
        page_id = self.conn.page_for_url(url)
        file_name = url.split("/")[-1:]
        self.conn.insert_image(page_id, file_name, response.headers["Content-Type"], response.content, datetime.datetime.now())

        # TODO: - update `FRONTIER` to `BINARY`

    def dequeue_url(self):
        # Fetch URLs from Frontier.
        while True:
            try:
                url = frontier.get(True, timeout=60)
                del frontier_dict[url]
            except Empty:
                return "Process {} stopped. No new URLs in Frontier\n".format(os.getpid())

            # print(os.getpid(), "got", url, 'is empty:', frontier.empty())
            self.parse_url(url)
            print('Dequed: ', url)

            # This is default delay
            time.sleep(4)

    @staticmethod
    def get_response(url: str):
        """ This is where we fetch url content using request. We need to do that if we want to download files
            and we need this for storing status codes.

            TODO: can someone check if we must store visited links with bad status codes?
                  if i'm not mistaken that is the case. please investigate.
        """
        response = requests.get(url, timeout=10, allow_redirects=False, verify=False)
        response.raise_for_status()
        return response

    @staticmethod
    def is_government_url(url: str):
        return ".gov.si" in url

    @staticmethod
    def is_already_visited(url: str):
        return url in visited_dict.keys()

    @staticmethod
    def is_allowed_by_robots(url: str, robot: robotparser.RobotFileParser):
        if not robot or not isinstance(robot, robotparser.RobotFileParser):
            return True
        return robot.can_fetch("*", url)

    @staticmethod
    def should_download_and_save_file(url):
        for f in supported_files:  # TODO: - refactor this using python magic
            if f in url:
                return True
        return False

    @staticmethod
    def get_domain_from_url(url: str):
        return "{uri.netloc}/".format(uri=parse.urlparse(url)).replace('www.', '')[:-1]  # remove trailing slash

    @staticmethod
    def to_canonical_form(url: str):
        return str(urlcanon.semantic(urlcanon.parse_url(url)))

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


def _future_callback(future: Future):
    print(future.result())


DEFAULT_CONCURRENT_WORKERS = 4

if __name__ == "__main__":
    sites = [
        "http://evem.gov.si/",
        # "https://e-uprava.gov.si/",
        # "https://podatki.gov.si/",
        # "http://www.e-prostor.gov.si/",
        # additional
        # 'http://www.gov.si/',
        # 'http://prostor3.gov.si/preg/',
        # 'https://egp.gu.gov.si/egp/',
        # 'http://www.gu.gov.si/',
        # 'https://gis.gov.si/ezkn/'
    ]
    for site in sites:
        frontier.put(site)
        frontier_dict[site] = True

    workers = int(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_CONCURRENT_WORKERS
    connections = {id: DBApi(DBConn()) for id in range(workers)}

    # conn: DBApi = connections[0]
    # pages = conn.select_all_pages()
    # for page in pages:
    #     if page[2] == "FRONTIER":
    #         frontier.put(page[3])
    #         frontier_dict[page[3]] = True
    #     else:
    #         visited_dict[page[3]] = True

    with ProcessPoolExecutor(max_workers=workers) as executor:

        def submit_worker(_f):
            _future = executor.submit(_f)
            _future.add_done_callback(_future_callback)
            return _future

        futures = [submit_worker(Worker(id)) for id in range(workers)]

        # This will stop our crawler when none of the running
        # processes cant fetch URL from Frontier
        wait(futures, return_when=ALL_COMPLETED)

    sys.exit(0)
