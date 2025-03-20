from typing import Any

import chompjs
from scrapy import Spider
from scrapy.http import Response

from locations.categories import Categories, Extras, apply_category, apply_yes_no
from locations.dict_parser import DictParser
from locations.items import Feature
from locations.pipelines.address_clean_up import clean_address


class MitsubishiCZSpider(Spider):
    name = "mitsubishi_cz"
    item_attributes = {"brand": "Mitsubishi", "brand_wikidata": "Q36033"}
    start_urls = ["https://www.mitsubishi-motors.cz/prodejci/"]

    def parse(self, response: Response, **kwargs: Any) -> Any:
        for location in chompjs.parse_js_object(
            response.xpath('//script[contains(text(), "var locations")]/text()').get()
        ):
            item = DictParser.parse(location)
            if "prodej" in location["type"]:
                apply_category(Categories.SHOP_CAR, item)
                apply_yes_no(Extras.CAR_REPAIR, item, "servis" in location["type"])
            elif "servis" in location["type"]:
                apply_category(Categories.SHOP_CAR_REPAIR, item)

            yield response.follow(url=location["url"], callback=self.parse_location_details, cb_kwargs=dict(item=item))

    def parse_location_details(self, response: Response, item: Feature) -> Any:
        item["website"] = response.url
        location = response.xpath(f'//*[@class="dealer-info dealer_info_{item["ref"]}"]//*[@class="dealer-detail"]')
        address = location.xpath(".//p[1]/text()").getall()
        # clean address
        address_end_index = -1
        for index, text in enumerate(address):
            if "IČ:" in text:
                address_end_index = index
                break
        item["addr_full"] = clean_address(address[:address_end_index])
        yield item
