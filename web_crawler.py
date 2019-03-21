""" Basic web crawl implementation """

import sys
import os
import time
import requests
import multiprocessing
import urllib3
import urlcanon
import validators

from queue import Empty
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ProcessPoolExecutor, Future, ALL_COMPLETED, wait
from urllib import robotparser, request, parse


from utils import DBConn
from hashing import *

frontier = multiprocessing.Queue()
manager = multiprocessing.Manager()
visited_dict = manager.dict()
site_domains = manager.dict()
documents_dict = manager.dict()


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


download_dir = "data/"
supported_files = [".pdf", ".doc", ".docx", ".ppt", ".pptx", "mp4", "mp3"]

try:
    os.makedirs(download_dir)
except OSError as e:
    pass


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
        self.root_name = ""
        self.current_page = ""

    def __get_chrome_driver(self):
        # TODO - Pretend to be a browser
        chrome_options = Options()
        chrome_options.add_argument("--headless")

        if sys.platform == "win32":
            driver_path = os.path.join(os.getcwd(), "chromedriver.exe")
        else:
            driver_path = os.path.join(os.getcwd(), "chromedriver")

        self.driver = webdriver.Chrome(driver_path, options=chrome_options)

    def parse_robots(self, url: str):
        """  Standard robot parser
        """
        site_domain = self.get_domain_from_url(url)
        if site_domain in site_domains:
            # we have already saw this site
            return site_domains.get(site_domain, None)
        else:
            # first time we are on this domain, check for robots.txt, parse it, save it!
            robots_location = url + "robots.txt"
            robots_content = []
            try:
                response = requests.get(robots_location, timeout=5)
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

            robot_file_parser = robotparser.RobotFileParser()
            robot_file_parser.set_url(robots_location)
            robot_file_parser.parse(robots_content)

            # Sitemap parsing
            sitemaps = [line for line in robots_content if "Sitemap" in line]
            links = [link.split(" ")[1] for link in sitemaps]

            for link in links:
                # Todo: Make sure our content parser knows how to handle xml. Sitemaps are
                #       usually in xml format. This is unhandled at this point!!!!!!!!!!!!!!!!!!!!!
                #       Also i skipped is_goverment_url and already_visited check. No need to check this here, i guess?
                #       Should we parse sitemap xml at this point? And not just putting it in frontier?
                #       Find solutions guys :D
                frontier.put(link)

            site_domains[site_domain] = robot_file_parser
            return robot_file_parser

    def parse_url(self, url: str):
        # unify url representation
        url = str(self.to_canonical_form(url))

        # get robot parser object for current site domain.
        robot_parser = self.parse_robots(url)

        # Note: this was changed just for readability issues.
        #       Now we can debug why was url skipped.
        if self.is_already_visited(url):
            print("URL: {} already visited! Skipping ...".format(url))
            return
        elif robot_parser is not None and not self.is_allowed_by_robots(
            url, robot_parser
        ):
            print("URL: {} Not allowed by robots.txt! Skipping ...".format(url))
            return
        elif not self.is_government_url(url):
            print("URL: {} Not from gov.si domain! Skipping ...".format(url))
            return

        # URL passed all checks. We can store it as visited.
        visited_dict[url] = True

        self.fetch_url(url)

    def fetch_url(self, url: str):

        try:
            response = self.get_response(url)  # this can raise exception
            status_code = response.status_code

            if self.should_download_and_save_file(url):
                # TODO: this should be done with temprary files until put in database
                #       https://docs.python.org/2/library/tempfile.html
                print("Downloading file from: " + str(url))
                file_path = os.path.join(download_dir, url.split("/")[-1:][0])
                with open(file_path, "wb") as fp:
                    fp.write(response.content)

            elif "text/xml" in response.headers["Content-Type"]:
                # TODO: this is probably a sitemap xml file. Parse links and add to frontier
                #       Check how to properly extract links from sitemaps.
                # Deni, i'm mad at you!
                pass

            else:
                # if its not a file we need to download or xml then presume its some html/javascript payload.
                # open with selenium to render all the javascript
                self.driver.get(url)
                # TODO check if page is similar to some other one with the hash crap
                self.parse_page_content()

        except requests.exceptions.RequestException as err:
            # TODO: HANDLE THIS PROPERLY
            #       ivse seen timeouts and this: HTTPSConnectionPool(host='sicas-x509si.gov.si', port=443):
            #       Max retries exceeded with url: /idpX509/login?policy=KDP-SI&service=https%3A%2F%2Fsicas.gov.si%2Fbl%2FhandleIdpResponse&lang=si
            #       (Caused by SSLError(SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1051)')))
            print("Error at {}".format(url), err)

    def parse_page_content(self):

        # TODO: I did not touch this function at all. Sorry :( its 00:15
        # Parse all links and put them in the frontier after checking they're '.gov.si'
        # print("Did receive " + str(self.current_page))

        document = self.driver.page_source
        hashed = hash_document(document)
        if hashed in documents_dict:
            print("Already visited! Skipping ...")
            return
        else:
            documents_dict[hashed] = True  # TODO: - What value here??

        soup = BeautifulSoup(document, "html.parser")
        hrefs = [
            a.get("href")
            for a in soup.find_all("a", href=True)
            if validators.url(a.get("href"))
        ]

        print("Received " + str(len(hrefs)) + " potential new urls")

        added = 0
        for href in hrefs:
            canonical = str(self.to_canonical_form(href))
            if not self.is_government_url(canonical) or self.is_already_visited(
                canonical
            ):
                continue
            frontier.put(canonical)
            added += 1
        print("Added " + str(added) + " new urls")

        images = [a.get for a in soup.find_all("img")]

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
                return "Process {} stopped. No new URLs in Frontier\n".format(
                    os.getpid()
                )

            # print(os.getpid(), "got", url, 'is empty:', frontier.empty())
            self.parse_url(url)

            # This is default delay
            time.sleep(2)

    @staticmethod
    def get_response(url: str):
        """ This is where we fetch url content using request. We need to do that if we want to download files
            and we need this for storing status codes.

            TODO: can someone check if we must store visited links with bad status codes?
                  if i'm not mistaken that is the case. please investigate.
        """
        try:
            response = requests.get(url, timeout=5, allow_redirects=True, verify=False)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise err

        return response

    @staticmethod
    def is_government_url(url: str):
        return ".gov.si" in url

    @staticmethod
    def is_already_visited(url: str):
        return url in visited_dict.keys()

    @staticmethod
    def is_allowed_by_robots(url: str, robot: robotparser.RobotFileParser):
        return robot.can_fetch("*", url)

    @staticmethod
    def should_download_and_save_file(url):
        for f in supported_files:  # TODO: - refactor this using python magic
            if f in url:
                return True
        return False

    @staticmethod
    def get_domain_from_url(url: str):
        return "{uri.netloc}/".format(uri=parse.urlparse(url))

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


if __name__ == "__main__":
    sites = [
        "http://evem.gov.si/",
        "https://e-uprava.gov.si/",
        "https://podatki.gov.si/",
        "http://www.e-prostor.gov.si/",
        # additional
        # 'http://www.gov.si/',
        # 'http://prostor3.gov.si/preg/',
        # 'https://egp.gu.gov.si/egp/',
        # 'http://www.gu.gov.si/',
        # 'https://gis.gov.si/ezkn/'
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

        # This will stop our crawler when none of the running
        # processes cant fetch URL from Frontier
        wait(futures, return_when=ALL_COMPLETED)

    sys.exit(0)
