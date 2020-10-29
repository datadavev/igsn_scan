import datetime
import logging
import sickle
import sickle.models
import sickle.response
import sickle.iterator
import sickle.oaiexceptions
import scrapy
import igsn_lib.oai
from lxml import etree

XMLParser = etree.XMLParser(
    remove_blank_text=True, recover=True, resolve_entities=False
)


class OAIPMHSpider(scrapy.spiders.Spider):
    """
    Implements a spider for the OAI-PMH protocol by using the Python sickle library.

    In case of successful harvest (OAI-PMH crawling) the spider will remember the initial starting
    date and will use it as `from_date` argument on the next harvest.

    """

    name = "OAI-PMH"
    state = {}

    def __init__(
        self,
        url,
        ignore_deleted=False,
        metadata_prefix="oai_dc",
        set_spec=None,
        alias=None,
        from_date=None,
        until_date=None,
        granularity="YYYY-MM-DDThh:mm:ssZ",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.ignore_deleted = ignore_deleted
        self.metadata_prefix = metadata_prefix
        self.set_spec = set_spec
        self.granularity = granularity
        self.alias = alias or self._make_alias()
        self.from_date = from_date or self.state.get(self.alias)
        self.until_date = until_date

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(OAIPMHSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        crawler.signals.connect(spider.spider_idle, signal=scrapy.signals.spider_idle)
        crawler.signals.connect(
            spider.engine_stopped, signal=scrapy.signals.engine_stopped
        )
        return spider

    def spider_closed(self, spider):
        spider.logger.debug("Spider close requested: %s", spider.name)

    def spider_idle(self, spider):
        self.logger.debug("Spider Idle: %s", spider.name)

    def engine_stopped(self):
        self.logger.debug("Engine is stopped.")

    def start_requests(self):
        self.logger.info(
            "Starting harvesting of {url} with set={set} and metadataPrefix={metadata_prefix}, from={from_date}, until={until_date}".format(
                url=self.url,
                set=self.set_spec,
                metadata_prefix=self.metadata_prefix,
                from_date=self.from_date,
                until_date=self.until_date,
            )
        )
        params = {
            "verb": "ListRecords",
            "set": self.set_spec,
            "metadataPrefix": self.metadata_prefix,
        }
        if self.from_date is not None:
            params["from"] = self._format_date(self.from_date)
        if self.until_date is not None:
            params["until"] = self._format_date(self.until_date)
        request = scrapy.http.FormRequest(
            self.url,
            formdata=params,
            method="GET",
            callback=self.parse,
            cb_kwargs=params,
        )
        yield request
        self.logger.debug("Harvesting start_requests complete.")

    def parse_record(self, record):
        """
        This method need to be reimplemented in order to provide special parsing.
        """
        raise NotImplementedError()

    def parse_metadata(self, response, **kwargs):
        self.logger.debug("RESOLVED TO: %s", response.headers.get('Location', "ERR"))
        return None

    def parse(self, response, **kwargs):
        errors = response.xpath(
            ".//oai:error", namespaces=igsn_lib.oai.IGSN_OAI_NAMESPACES_INV
        )
        for error in errors:
            code = error.attrib.get("code", "UNKNOWN")
            description = error.text or ""
            try:
                raise getattr(sickle.oaiexceptions, code[0].upper() + code[1:])(
                    description
                )
            except AttributeError:
                raise sickle.oaiexceptions.OAIError(description)
        tok = response.xpath(
            ".//oai:resumptionToken//text()",
            namespaces=igsn_lib.oai.IGSN_OAI_NAMESPACES_INV,
        )
        if len(tok) > 0:
            params = {
                "resumptionToken": tok[0].get(),
                "verb": kwargs["verb"],
            }
            yield scrapy.http.FormRequest(
                self.url,
                formdata=params,
                method="GET",
                callback=self.parse,
                cb_kwargs=params,
            )
        records = response.xpath(
            ".//oai:record", namespaces=igsn_lib.oai.IGSN_OAI_NAMESPACES_INV
        )
        for arecord in records:
            record = sickle.models.Record(arecord.root)
            self.logger.debug("RECORD DATE = %s", record.header.datestamp)
            self.state[self.alias] = record.header.datestamp
            data = self.parse_record(record)
            yield data
            #TODO: support resolve
            #resolve_request = scrapy.http.Request(
            #    f"https://hdl.handle.net/10273/{data['igsn_id']}", callback=self.parse_metadata
            #)
            #resolve_request.meta['handle_httpstatus_list'] = [301,302, ]
            #yield resolve_request

    def _format_date(self, datetime_object):
        if datetime_object is None:
            return None
        if self.granularity == "YYYY-MM-DD":
            return datetime_object.strftime("%Y-%m-%d")
        elif self.granularity == "YYYY-MM-DDThh:mm:ssZ":
            return datetime_object.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            raise RuntimeError("Invalid granularity: %s" % self.granularity)

    def _make_alias(self):
        return "{url}-{metadata_prefix}-{set}".format(
            url=self.url, metadata_prefix=self.metadata_prefix, set=self.set_spec
        )
