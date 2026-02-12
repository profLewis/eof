"""GeoJSON loading utilities."""

import json
from shapely.geometry import shape


def load_geojson(file_path: str):
    """Load a GeoJSON file and return the first feature's geometry as a shapely object."""
    with open(file_path) as f:
        features = json.load(f)["features"]
    geom = shape(features[0]["geometry"])
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom
