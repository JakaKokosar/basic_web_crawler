""" Basic web crawl implementation """
import requests


from collections import deque
from concurrent.futures import ThreadPoolExecutor

# This is a frontier implemented as
# FIFO using collections.deque
frontier = deque([])


def fetch_url(url):
    # TODO: here we must check status codes
    #       and also terminate on timeout
    return requests.get(url)


def parse_content(future):
    response = future.result()
    print(response)


class WebCrawler:
    """ Base class for web crawler.

    TODO: - HTTP downloader and renderer: To retrieve and render a web page. (Downloader is done, need renderer)
          - Data extractor: Minimal functionalities to extract images and hyperlinks.
          - Duplicate detector: To detect already parsed pages.
          - URL frontier: A list of URLs waiting to be parsed. ------> Done, implemented as FIFO queue
          - Datastore: To store the data and additional metadata used by the crawler.

    """

    def __init__(self, max_threads=4):
        self.max_threads = max_threads

        self.sites = [
            'http://evem.gov.si/',
            'https://e-uprava.gov.si/',
            'https://podatki.gov.si/',
            'http://www.e-prostor.gov.si/',
        ]

        [frontier.append(site) for site in self.sites]

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            while frontier:
                future = executor.submit(fetch_url, frontier.popleft())
                future.add_done_callback(parse_content)


if __name__ == "__main__":
    WebCrawler()

