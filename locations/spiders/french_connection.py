from locations.storefinders.storemapper import StoremapperSpider


class FrenchConnectionSpider(StoremapperSpider):
    name = "french_connection"
    item_attributes = {
        "brand_wikidata": "Q306457",
        "brand": "French Connection",
    }
    company_id = "11232"
