import json

import scrapy

from locations.dict_parser import DictParser
from locations.hours import DAYS, OpeningHours


class GelsonsSpider(scrapy.spiders.SitemapSpider):
    name = "gelsons"
    item_attributes = {
        "brand": "Gelson's",
        "brand_wikidata": "Q16993993",
        "country": "US",
    }

    sitemap_urls = ["https://www.gelsons.com/sitemap.xml"]
    sitemap_rules = [("/stores/", "parse_store")]

    def sitemap_filter(self, entries):
        for entry in entries:
            if "virtual" in entry["loc"]:
                continue
            yield entry

    def parse_store(self, response):
        content = json.loads(response.xpath('//script[@type="application/json"]/text()').extract_first())["props"][
            "pageProps"
        ]
        store_json = content["store"]

        item = DictParser.parse(store_json)
        item["branch"] = item.pop("name")
        item["street_address"] = item.pop("addr_full", None)
        item["phone"] = store_json["storePhone"]
        item["website"] = response.url
        if hours := content["pageComponents"][0]["headline"]:
            item["opening_hours"] = self.parse_hours(hours)

        yield item

    def parse_hours(self, hour_string):
        """Hours look like one of the following:

        Hours: 7am - 9pm, 7 days a week
        Hours: 7am - 9:30pm, 7 days a week

        """
        hours = OpeningHours()
        hour_string = (
            hour_string.replace("Hours:", "")
            .replace("7 days a week", "")
            .replace(",", "")
            .replace(".", "")
            .replace(" ", "")
        )
        open_time, close_time = hour_string.split("-")

        # Add minutes, if necessary
        if ":" not in open_time:
            open_time = f"{open_time[:-2]}:00am"

        if ":" not in close_time:
            close_time = f"{close_time[:-2]}:00pm"

        for day in DAYS:
            hours.add_range(day, open_time, close_time, time_format="%I:%M%p")

        return hours
