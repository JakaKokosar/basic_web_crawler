""" Basic web crawl implementation """

import sys
import os
import time
import datetime


import requests
import urllib3
import urlcanon
import validators

from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, Future, ALL_COMPLETED, wait
from urllib import robotparser, parse

from crawler.utils import DBConn, DBApi
from crawler.sitemap import *
from crawler.hashing import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
supported_files = ["pdf", "doc", "docx", "ppt", "pptx"]
connections = None


class Worker:
    """ Base class for web crawler.
    """

    def __init__(self, id):
        self.id = id
        self.driver = None
        self.root_name = ""
        self.current_page = ""

    def get_chrome_driver(self):
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

    def add_to_frontier(self, url, site_id, is_binary=False):
        page_id = self.conn.insert_page(site_id, "FRONTIER", url, None, None, None, is_binary=is_binary)
        return page_id

    @property
    def conn(self) -> DBApi:
        return connections[self.id]

    def parse_robots(self, url: str):
        """  Standard robot parser
        """
        site_domain = self.get_domain_from_url(url)
        robots_content = self.conn.select_robots_by_domain(site_domain)

        robots_location = "http://" + site_domain + "/robots.txt"
        robots_parser = robotparser.RobotFileParser()
        robots_parser.set_url(robots_location)

        # site_id = self.conn.site_id_for_domain(site_domain)
        if robots_content:
            # we have already saw this site
            site_id = self.conn.site_id_for_domain(site_domain)
            robots_parser.parse(robots_content)
            return site_id, robots_parser
        else:
            try:
                response = requests.get(robots_location, timeout=10)
                response.raise_for_status()
                robots_content = response.text.split("\n")
            except requests.exceptions.RequestException as err:
                # This is a general request exception. Should we care about if something went wrong?
                # For now we assume that robots.txt in unavailable or is not present at all.
                print("Unexpected error when requesting robots.txt for {}".format(url), err)
                site_id = self.conn.insert_site(site_domain, "/", "/")
                return site_id, None

            robots_parser.parse(robots_content)

            # Sitemap parsing
            sitemaps = [line for line in robots_content if "Sitemap" in line]
            links = [link.split(" ")[1] for link in sitemaps]

            sitemaps_content = ""

            urls_to_add = []

            for link in links:
                req = requests.get(link)
                sitemap_urls = parse_xml(req.text)
                sitemaps_content += req.text + "\n"
                for url in sitemap_urls:
                    if not self.is_government_url(url) or self.is_already_visited(url):
                        continue
                    urls_to_add.append(url)

            site_id = self.conn.insert_site(site_domain, response.text, sitemaps_content)
            for url in urls_to_add:
                page_id = self.add_to_frontier(url, site_id)
                print("Added " + url + " to `FRONTIER` page with id " + str(page_id))

            print("Added %d urls from sitemap!" % len(urls_to_add))

            return site_id, robots_parser

    def parse_url(self, url: str, is_binary: bool):
        # unify url representation
        url = str(self.to_canonical_form(url))

        if is_binary:
            site_id = self.conn.site_id_for_domain(self.get_domain_from_url(url))
            robot_parser = None
        else:
            # get robot parser object for current site domain.
            site_id, robot_parser = self.parse_robots(url)

        default_crawl_delay = 4
        try:
            if not robot_parser is None:
                crawl_delay = robot_parser.crawl_delay('*')
                if not crawl_delay is None:
                    default_crawl_delay = int(crawl_delay)
        except AttributeError:
            pass
        time.sleep(default_crawl_delay)

        # fetch url
        self.fetch_url(url, site_id, is_binary)

    def fetch_url(self, url: str, site_id: int, is_binary):
        try:
            response = self.get_response(url)  # this can raise exception
            status_code = response.status_code
            time.sleep(0.2)

            if self.should_download_and_save_file(url) or \
                "msword" in response.headers["Content-Type"] or \
                "powerpoint" in response.headers["Content-Type"] or \
                "/vnd.openxmlformats-officedocument.wordprocessingml.document" in response.headers["Content-Type"] or \
                "/vnd.openxmlformats-officedocument.presentationml.presentation" in response.headers["Content-Type"]:

                self.save_file(url, response)
            elif is_binary or "image" in response.headers["Content-Type"]:
                self.save_image(url, response)
            elif "text/html" in response.headers["Content-Type"]:
                # open with selenium to render all the javascript
                try:
                    self.driver.get(url)
                    print("Did receive HTML content from: " + str(url))
                    self.parse_page_content(site_id, url, status_code, datetime.datetime.now())
                except Exception as e:
                    print("An error occured while parsing page content: " + str(e) + " from url " + str(url))
                    page_id = self.conn.page_id_for_page_in_frontier(site_id, url)
                    if page_id:
                        self.conn.update_page(page_id, "HTML", None, 500, datetime.datetime.now())
                    else:
                        self.conn.insert_page(site_id, "HTML", url, None, 500, datetime.datetime.now())
            else:
                print("Content at " + str(url) + " is of unknown content-type. Removing from frontier ...")
                self.conn.remove_page(url, datetime.datetime.now())
        except Exception as err:
            print("Error at {}".format(url), err)
            page_id = self.conn.page_id_for_page_in_frontier(site_id, url)
            if page_id:
                self.conn.update_page(page_id, "HTML", None, 404, datetime.datetime.now())
            else:
                self.conn.insert_page(site_id, "HTML", url, None, 404, datetime.datetime.now())

    def parse_page_content(self, site_id: int, url: str, status_code, accessed_time):
        document = self.driver.page_source
        hashed = hash_document(document)

        existing_page_id = self.conn.page_for_url(url)

        try:
            duplicate_id = self.conn.page_for_hash(hashed)
            if duplicate_id:
                if existing_page_id:
                    self.conn.update_page(existing_page_id, "DUPLICATE", None, status_code, accessed_time, duplicate_page_id=duplicate_id)
                    print("Updated page to `DUPLICATE` with id " + str(existing_page_id) + " at url: " + url)
                else:
                    page_id = self.conn.insert_page(site_id, "DUPLICATE", url, None, status_code, accessed_time, duplicate_page_id=duplicate_id)
                    print("Added `DUPLICATE` page with id " + str(page_id) + " at url: " + url)
                return
            else:
                if existing_page_id:
                    self.conn.update_page(existing_page_id, "HTML", document, status_code, accessed_time, hash=hashed)
                    print("Updated page to `HTML` with id " + str(existing_page_id) + " at url: " + url)
                else:
                    existing_page_id = self.conn.insert_page(site_id, "HTML", url, document, status_code, accessed_time, hash=hashed)
                    if not existing_page_id:
                        return
                    print("Added `HTML` page with id " + str(existing_page_id) + " at url: " + url)
        except Exception as e:
            print(e)
            return

        if not existing_page_id:
            print()

        soup = BeautifulSoup(document, 'html.parser')

        base_url_for_image = None
        base_tags_urls = [base_url.get('href') for base_url in soup.findAll('base')]
        if len(base_tags_urls) > 0:
            base_url_for_image = base_tags_urls[0]

        hrefs = [
            str(self.to_canonical_form(a.get("href")))
            for a in soup.find_all(href=True)
            if self.is_valid_url(a.get("href"))
        ]
        print("Found " + str(len(hrefs)) + " potential new urls")

        added = 0
        for href in hrefs:
            if not self.is_government_url(href) or self.is_already_visited(href):
                continue
            # print("Added " + href + " to `FRONTIER` page with id " + str(page_id))
            page_id = self.add_to_frontier(href, site_id)
            self.conn.insert_link(existing_page_id, page_id)
            added += 1

        print("Added " + str(added) + " new urls from hrefs")

        # # Handling JS onclick
        # # TODO  -- NOT Working :(
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
        images = [a.get("src") for a in soup.find_all('img')]
        image_sources = []
        added = 0
        for img in images:
            if self.is_valid_url(img):
                image_sources.append(img)
                added += 1
                if self.is_already_visited(img):
                    continue
                self.add_to_frontier(img, site_id, True)
            else:
                if not base_url_for_image is None:
                    img = self.to_canonical_form(os.path.join(base_url_for_image, img))

                if self.is_valid_url(img) and 'base64' not in img:
                    image_sources.append(img)
                    added += 1
                    if self.is_already_visited(img):
                        continue
                    self.add_to_frontier(img, site_id, True)

        print("Added " + str(added) + " new images to list")

    def save_file(self, url: str, response):
        page_id = self.conn.page_for_url(url)

        data_type_code = [
            extension
            for extension in supported_files
            if extension in url
        ]
        if not data_type_code:
            # something went wrong! abort ..
            print("Error storing file from %s! Invalid Content-Type: %s" % (url, response.headers["Content-Type"]))
            self.conn.update_page(page_id, "BINARY", None, 500, datetime.datetime.now(), is_binary=True)
            return

        self.conn.insert_page_data(page_id, data_type_code[0].upper(), response.content)
        self.conn.update_page(page_id, "BINARY", None, 200, datetime.datetime.now())

    def save_image(self, url: str, response):
        page_id = self.conn.page_for_url(url)
        file_name = url.split("/")[-1:]
        self.conn.insert_image(page_id,
                               file_name,
                               response.headers["Content-Type"],
                               response.content,
                               datetime.datetime.now())
        self.conn.update_page(page_id, "BINARY", None, 200, datetime.datetime.now(), is_binary=True)

    def dequeue_url(self):
        retry_count = 50
        while True:
            try:
                # Fetch URLs from Frontier.
                if retry_count == 0:
                    print("Finished crawling")
                    return

                result = self.conn.select_from_frontier()
                if not result:
                    time.sleep(10)
                    retry_count -= 1
                    continue
                retry_count = 50
                id, url, is_binary = result
                self.conn.update_page(id, "IN PROGRESS", None, None, None)

                # print(os.getpid(), "got", url, 'is empty:', frontier.empty())
                self.parse_url(url, is_binary)
                print('Dequed: ', url)
            except:
                continue

    @staticmethod
    def get_response(url: str):
        """ This is where we fetch url content using request. We need to do that if we want to download files
            and we need this for storing status codes.
        """
        response = requests.get(url, timeout=10, allow_redirects=False, verify=False)
        response.raise_for_status()
        return response

    @staticmethod
    def is_government_url(url: str):
        return ".gov.si" in url

    def is_already_visited(self, url: str):
        page_id = self.conn.page_for_url(url)
        return page_id

    @staticmethod
    def is_allowed_by_robots(url: str, robot: robotparser.RobotFileParser):
        if not robot or not isinstance(robot, robotparser.RobotFileParser):
            return True
        return robot.can_fetch("*", url)

    @staticmethod
    def should_download_and_save_file(url):
        for f in supported_files:
            if f in url:
                return True
        return False

    @staticmethod
    def get_domain_from_url(url: str):
        return "{uri.netloc}/".format(uri=parse.urlparse(url)).replace('www.', '')[:-1]  # remove trailing slash

    @staticmethod
    def to_canonical_form(url: str):
        return str(urlcanon.semantic(urlcanon.parse_url(url)))

    @staticmethod
    def to_canonical(url: str):
        return urlcanon.semantic(urlcanon.parse_url(url))

    @staticmethod
    def is_valid_url(url: str):
        return validators.url(url)

    def __call__(self):
        self.get_chrome_driver()
        return self.dequeue_url()


def _future_callback(future: Future):
    print(future.result())


DEFAULT_CONCURRENT_WORKERS = 4

if __name__ == "__main__":
    sites = [
        "http://evem.gov.si/",
        "https://e-uprava.gov.si/",
        "https://podatki.gov.si/",
        "http://www.e-prostor.gov.si/",
        # additional
        'http://www.gov.si/',
        'http://prostor3.gov.si/preg/',
        'https://egp.gu.gov.si/egp/',
        'http://www.gu.gov.si/',
        'https://gis.gov.si/ezkn/'
    ]

    workers = int(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_CONCURRENT_WORKERS
    connections = {id: DBApi(DBConn()) for id in range(workers)}

    api = DBApi(DBConn())
    api.in_progress_to_frontier()
    if not api.select_from_frontier():
        worker = Worker(0)
        for url in sites:
            worker.get_chrome_driver()
            worker.parse_url(url, False)

    with ThreadPoolExecutor(max_workers=workers) as executor:

        def submit_worker(_f):
            _future = executor.submit(_f)
            _future.add_done_callback(_future_callback)
            return _future

        futures = [submit_worker(Worker(id)) for id in range(workers)]

        # This will stop our crawler when none of the running
        # processes cant fetch URL from Frontier
        wait(futures, return_when=ALL_COMPLETED)

    sys.exit(0)
