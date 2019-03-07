""" Basic web crawl implementation """

import sys
import os
import time
import requests
import multiprocessing

from queue import Empty
from concurrent.futures import ProcessPoolExecutor, Future, ALL_COMPLETED, wait

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


frontier = multiprocessing.Queue()


class Worker:
    """ Base class for web crawler.

    TODO: - HTTP downloader and renderer: To retrieve and render a web page. (Downloader is done, need renderer)
          - Data extractor: Minimal functionalities to extract images and hyperlinks.
          - Duplicate detector: To detect already parsed pages.
          - URL frontier: A list of URLs waiting to be parsed. ------> Done, implemented as FIFO queue
          - Datastore: To store the data and additional metadata used by the crawler.

    """

    def __init__(self):
        self.driver = None

    def __get_chrome_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')

        if sys.platform == 'win32':
            driver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
        else:
            driver_path = os.path.join(os.getcwd(), 'chromedriver')

        self.driver = webdriver.Chrome(driver_path, options=chrome_options)

    def fetch_url(self, url: str):

        # TODO: here we must check status codes
        #       also how to handle timeouts?
        # response = requests.get(url, timeout=5)

        # Is there a way to fetch page with URL and
        # then render it with selenium? Would prefer to use
        # requests module for retrieving page content.

        self.driver.get(url)
        self.parse_page_content()

    def parse_page_content(self,):
        print(self.driver.page_source)

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
        self.__get_chrome_driver()
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
