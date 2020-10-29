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
    def __init__(self, db_url):
        self.db_url = db_url
        self.logger = logging.getLogger("OAIscan")
        self._engine = getEngine(self.db_url)
        self._session = None
        # TODO: need to set this properly
        self.service_id = 1

    @classmethod
    def from_crawler(cls, crawler):
        return cls(db_url=crawler.settings.get("DATABASE_URL", None))

    def open_spider(self, spider):
        self.logger.debug("open_spider, DB = %s", self.db_url)
        self._session = getSession(self._engine)
        pass

    def close_spider(self, spider):
        self.logger.debug("close_spider")
        if not self._session is None:
            self._session.close()
        pass

    def process_item(self, item, spider):
        # self.logger.debug("process_item")
        try:
            #data = igsn_lib.oai.oaiDictRecordToJson(
            #    item, indent=2, include_source=False
            #)
            igsn = igsn_lib.models.Identifier(
                service_id=self.service_id, harvest_time=igsn_lib.time.dtnow()
            )
            igsn.fromOAIRecord(item)
            self.logger.debug(igsn)
            exists = self._session.query(igsn_lib.models.Identifier).get(igsn.id)
            if not exists:
                self.logger.debug("NEW")
                try:
                    self.logger.info("Add identifier: %s", igsn.id)
                    self._session.add(igsn)
                    self._session.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    self.logger.warning("IGSN entry already exists: %s", str(igsn))
            else:
                self.logger.debug("EXISTING")
        except Exception as e:
            self.logger.error(e)
        return item
