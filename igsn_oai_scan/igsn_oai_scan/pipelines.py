# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import logging
import igsn_lib.oai
import igsn_lib.time
import igsn_lib.models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc


def getEngine(db_connection):
    engine = create_engine(db_connection)
    igsn_lib.models.createAll(engine)
    return engine


def getSession(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


class IgsnOaiScanPipeline:
    def __init__(self, db_url, service_id):
        self.db_url = db_url
        self.logger = logging.getLogger("OAIscan")
        self._engine = getEngine(self.db_url)
        self._service_id = service_id
        self._session = None

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get("DATABASE_URL", None)
        service_id = crawler.settings.get("SERVICE_ID", 1)
        return cls(db_url, service_id)

    def open_spider(self, spider):
        self.logger.debug("open_spider, DB = %s", self.db_url)
        self._session = getSession(self._engine)
        self.logger.debug("Spider startup setspec = %s", spider.set_spec)
        self.logger.debug("Spider startup from date = %s", spider.from_date)
        if spider.from_date is None:
            # Set from date to the most recent data for the spider set_spec
            service = (
                self._session.query(igsn_lib.models.Service)
                .filter(igsn_lib.models.Service.id == self._service_id)
                .one()
            )
            # Set the set_spec if one provided with spider
            set_spec = None
            if spider.set_spec is not None:
                set_spec = spider.set_spec
            # Get the most recent record to set the retrieval FROM date
            last_record = service.mostRecentIdentifierRetrieved(
                self._session, set_spec=set_spec
            )
            if not last_record is None:
                self.logger.info(
                    "Setting spider start time to: %s", last_record.id_time
                )
                spider.from_date = last_record.id_time

    def close_spider(self, spider):
        self.logger.debug("close_spider")
        if not self._session is None:
            self._session.close()
        pass

    def process_item(self, item, spider):
        try:
            igsn = igsn_lib.models.Identifier(
                service_id=self._service_id, harvest_time=igsn_lib.time.dtnow()
            )
            igsn.fromOAIRecord(item)
            exists = self._session.query(igsn_lib.models.Identifier).get(igsn.id)
            if not exists:
                self.logger.debug("NEW id: %s", igsn.id)
                try:
                    self.logger.info("add: %s", igsn.id)
                    self._session.add(igsn)
                    self._session.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    self.logger.warning("Entry exists: %s", igsn.id)
            else:
                self.logger.debug("EXISTING id: %s", igsn.id)
        except Exception as e:
            self.logger.error(e)
        return item
