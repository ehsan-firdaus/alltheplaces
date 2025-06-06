import re
from typing import Iterable

from scrapy.http import Response

from locations.categories import Categories, Extras, Fuel, apply_category, apply_yes_no
from locations.items import Feature
from locations.storefinders.agile_store_locator import AgileStoreLocatorSpider


class PetrolBGSpider(AgileStoreLocatorSpider):
    name = "petrol_bg"
    item_attributes = {"brand_wikidata": "Q24315"}
    allowed_domains = ["www.petrol.bg"]

    def post_process_item(self, item: Feature, response: Response, feature: dict) -> Iterable[Feature]:
        if m := re.match(r"^(\d+) (.+)$", item.pop("name")):
            item["ref"] = m.group(1)
            item["branch"] = m.group(2)

        categories = (feature["categories"] or "").split(",")

        apply_yes_no(Fuel.ELECTRIC, item, "31" in categories)
        apply_yes_no(Fuel.DIESEL, item, ("19" in categories or "20" in categories))
        apply_yes_no(Fuel.OCTANE_100, item, "21" in categories)
        apply_yes_no(Fuel.OCTANE_95, item, "22" in categories)
        apply_yes_no(Fuel.LPG, item, "23" in categories)
        apply_yes_no(Fuel.CNG, item, "24" in categories)
        apply_yes_no(Extras.CAR_WASH, item, "25" in categories)
        apply_yes_no(Extras.ATM, item, "26" in categories)
        apply_yes_no("cafe", item, "27" in categories)
        apply_yes_no("self_service", item, "28" in categories)
        apply_yes_no(Fuel.ADBLUE, item, "29" in categories)
        apply_category(Categories.FUEL_STATION, item)

        yield item
