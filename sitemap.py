# https://github.com/stchris/untangle
import untangle

def parse_xml(xml: str):
    tree = untangle.parse(xml)
    urlset = tree.urlset
    if not urlset:
        return []
    urls = urlset.url
    if not urls:
        return []
    sitemap_urls = [url.loc.cdata for url in urls]
    return sitemap_urls