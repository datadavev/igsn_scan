from datetime import datetime
import pyld
from pprint import pprint
import ldscanner.spiders.ldsitemapspider
import ldscanner.items

class JsonldSpider(ldscanner.spiders.ldsitemapspider.LDSitemapSpider):

    name = "JsonldSpider"
    #sitemap_urls = ['https://www.bco-dmo.org/sitemap.xml',]

    def __init__(self, sitemap_urls="", *args, **kwargs):
        super(JsonldSpider, self).__init__(*args, **kwargs)
        self.sitemap_urls = sitemap_urls.split(" ")

    def sitemap_filter(self, entries):
        for entry in entries:
            pprint(entry)
            #print(f"ENTRY DT = {entry.get('lastmod')}")
            yield entry

    def parse(self, response, **kwargs):
        #print(f"URL = {response.url}")
        #print(f"TS = {response.request.meta['loc_timestamp']}")
        try:
            jsonld = pyld.jsonld.load_html(
                response.body,
                response.url,
                None,
                {
                    'extractAllScripts':True
                }
            )
            if len(jsonld) > 0:
                item = ldscanner.items.LdscannerItem()
                item['url'] = response.url
                item['jsonld'] = jsonld
                yield item
        except Exception as e:
            print(f'ERROR! : {e}')
        yield None
