import scrapy
import igsn_lib.oai
from . import oaipmh


class DoidbSpider(oaipmh.OAIPMHSpider):
    name = "doidb"

    def __init__(self, service_url=None, set_spec=None, *args, **kwargs):
        if service_url is None:
            service_url = igsn_lib.oai.DEFAULT_IGSN_OAIPMH_PROVIDER
        super(DoidbSpider, self).__init__(
            service_url,
            metadata_prefix="igsn",
            set_spec=set_spec,
            ignore_deleted=True,
            *args,
            **kwargs
        )

    def parse_record(self, record):
        if record.deleted and self.ignore_deleted:
            self.logger.debug("Record %s deleted.", record.header.identifier)
            return
        data = igsn_lib.oai.oaiRecordToDict(record.raw)
        return data
