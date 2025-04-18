import base64
import datetime
import hashlib
import io
import json
import logging
import uuid
from typing import Any, Generator, Type

from scrapy import Item, Spider
from scrapy.exporters import JsonItemExporter
from scrapy.utils.misc import walk_modules
from scrapy.utils.python import to_bytes
from scrapy.utils.spider import iter_spider_classes

from locations.extensions.add_lineage import spider_class_to_lineage
from locations.settings import SPIDER_MODULES

mapping = (
    ("addr_full", "addr:full"),
    ("housenumber", "addr:housenumber"),
    ("street", "addr:street"),
    ("street_address", "addr:street_address"),
    ("city", "addr:city"),
    ("state", "addr:state"),
    ("postcode", "addr:postcode"),
    ("country", "addr:country"),
    ("name", "name"),
    ("branch", "branch"),
    ("phone", "phone"),
    ("website", "website"),
    ("twitter", "contact:twitter"),
    ("facebook", "contact:facebook"),
    ("email", "email"),
    ("opening_hours", "opening_hours"),
    ("image", "image"),
    ("brand", "brand"),
    ("brand_wikidata", "brand:wikidata"),
    ("operator", "operator"),
    ("operator_wikidata", "operator:wikidata"),
    ("located_in", "located_in"),
    ("located_in_wikidata", "located_in:wikidata"),
    ("nsi_id", "nsi_id"),
)


def item_to_properties(item: Item) -> dict[str, Any]:
    props = {}

    # Ref is required, unless `no_refs = True` is set in spider
    if ref := item.get("ref"):
        props["ref"] = str(ref)

    # Add in the extra bits
    if extras := item.get("extras"):
        for key, value in extras.items():
            if value is not None and value != "":
                # Only export populated values
                props[key] = value

    # Bring in the optional stuff
    for map_from, map_to in mapping:
        if item_value := item.get(map_from):
            if item_value is not None and item_value != "":
                props[map_to] = item_value

    return props


def item_to_geometry(item: Item) -> dict | None:
    """
    Convert the item to a GeoJSON geometry object. If the item has lat and lon fields,
    but no geometry field, then a Point geometry will be created. Otherwise the
    geometry field will be returned as is.

    :param item: The scraped item.
    :return: The GeoJSON geometry object.
    """

    lat = item.get("lat")
    lon = item.get("lon")
    geometry = item.get("geometry")

    if lat and lon and not geometry:
        try:
            geometry = {
                "type": "Point",
                "coordinates": [float(item["lon"]), float(item["lat"])],
            }
        except ValueError:
            logging.warning("Couldn't convert lat (%s) and lon (%s) to float", lat, lon)

    # Check for empty or missing coordinates list in geometry
    if geometry and isinstance(geometry, dict):
        coordinates = geometry.get("coordinates")
        if not coordinates:
            logging.warning("Invalid geometry coordinates: %s", geometry)
            return None

    return geometry


def item_to_geojson_feature(item: Item) -> dict:
    feature = {
        "id": compute_hash(item),
        "type": "Feature",
        "properties": item_to_properties(item),
        "geometry": item_to_geometry(item),
    }

    return feature


def compute_hash(item: Item) -> str:
    ref = str(item.get("ref") or uuid.uuid1()).encode("utf8")
    sha1 = hashlib.sha1(ref)

    if spider_name := item.get("extras", {}).get("@spider"):
        sha1.update(spider_name.encode("utf8"))

    return base64.urlsafe_b64encode(sha1.digest()).decode("utf8")


def find_spider_class(spider_name: str):
    if not spider_name:
        return None
    for spider_class in iter_spider_classes_in_modules():
        if spider_name == spider_class.name:
            return spider_class
    return None


def iter_spider_classes_in_modules(modules=SPIDER_MODULES) -> Generator[Type[Spider], Any, None]:
    for mod in modules:
        for module in walk_modules(mod):
            for spider_class in iter_spider_classes(module):
                yield spider_class


def get_dataset_attributes(spider_name) -> {}:
    spider_class = find_spider_class(spider_name)
    dataset_attributes = getattr(spider_class, "dataset_attributes", {})
    settings = getattr(spider_class, "custom_settings", {}) or {}
    if not settings.get("ROBOTSTXT_OBEY", True):
        # See https://github.com/alltheplaces/alltheplaces/issues/4537
        dataset_attributes["spider:robots_txt"] = "ignored"
    if not dataset_attributes.get("lineage"):
        dataset_attributes["spider:lineage"] = spider_class_to_lineage(spider_class).value
    dataset_attributes["@spider"] = spider_name
    dataset_attributes["spider:collection_time"] = datetime.datetime.now().isoformat()

    return dataset_attributes


class GeoJsonExporter(JsonItemExporter):
    def __init__(self, file, **kwargs):
        super().__init__(file, **kwargs)
        self.spider_name = None

    def start_exporting(self):
        pass

    def export_item(self, item):
        spider_name = item.get("extras", {}).get("@spider")

        if self.first_item:
            self.spider_name = spider_name
            self.write_geojson_header()

        if spider_name != self.spider_name:
            # It really should not happen that a single exporter instance
            # handles output from different spiders. If it does happen,
            # we rather crash than emit GeoJSON with the wrong dataset
            # properties, which may include legally relevant license tags.
            raise ValueError(
                f"harvest from multiple spiders ({spider_name, self.spider_name}) cannot be written to same GeoJSON file"
            )

        super().export_item(item)

    def _get_serialized_fields(self, item, default_value=None, include_empty=None):
        feature = [
            ("type", "Feature"),
            ("id", compute_hash(item)),
            ("properties", item_to_properties(item)),
            ("geometry", item_to_geometry(item)),
        ]

        return feature

    def write_geojson_header(self):
        header = io.StringIO()
        header.write('{"type":"FeatureCollection","dataset_attributes":')
        json.dump(
            get_dataset_attributes(self.spider_name), header, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        header.write(',"features":[\n')
        self.file.write(to_bytes(header.getvalue(), self.encoding))

    def finish_exporting(self):
        if not self.first_item:
            self.file.write(b"\n]}\n")
