from typing import Iterable

from scrapy.http import Response

from locations.categories import Categories, apply_category
from locations.items import Feature
from locations.storefinders.opendatasoft_explore import OpendatasoftExploreSpider


class GreaterGeelongCityCouncilCctvAUSpider(OpendatasoftExploreSpider):
    name = "greater_geelong_city_council_cctv_au"
    item_attributes = {"operator": "Greater Geelong City Council", "operator_wikidata": "Q112919122", "nsi_id": "N/A"}
    api_endpoint = "https://www.geelongdataexchange.com.au/api/explore/v2.1/"
    dataset_id = "cctv-locations"
    no_refs = True

    def post_process_item(self, item: Feature, response: Response, feature: dict) -> Iterable[Feature]:
        item["name"] = feature.get("sitename")
        apply_category(Categories.SURVEILLANCE_CAMERA, item)
        item["extras"]["surveillance"] = "public"
        item["extras"]["camera:type"] = "fixed"
        yield item
