import json
from pathlib import Path
from os import PathLike
import re
from shapely import wkt as swkt, make_valid, force_2d
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

def _load_file(path: Path) -> BaseGeometry:
    """
    Read a GeoJSON file and return a valid Shapely geometry.

    Parameters
    ----------
    path : Path
        Path to a GeoJSON file containing either a geometry or
        a FeatureCollection.

    Returns
    -------
    BaseGeometry
        A (validated) Shapely geometry.
    """
    data = json.loads(path.read_text())
    geom = (data["features"][0]["geometry"] if "features" in data
            else data)
    return _valid(shape(geom))

def _valid(geom: BaseGeometry) -> BaseGeometry:
    """
    Ensure geometry validity using shapely.make_valid if needed.

    Parameters
    ----------
    geom : BaseGeometry

    Returns
    -------
    BaseGeometry
    """
    return geom if geom.is_valid else make_valid(geom)

def load_geojson(
    source: str | PathLike | BaseGeometry
) -> BaseGeometry:
    """
    Load a geometry from WKT string, GeoJSON file, or BaseGeometry.

    Supports:
    - WKT strings (including messy QGIS copy/paste)
    - File paths (str or pathlib.Path) to GeoJSON
    - BaseGeometry objects (validated and returned)

    Parameters
    ----------
    source : str, PathLike, or BaseGeometry
        Either:
        - a WKT string
        - a path to a GeoJSON file
        - a BaseGeometry object

    Returns
    -------
    BaseGeometry
        A validated Shapely geometry.

    Raises
    ------
    ValueError
        If the input cannot be parsed as WKT or a valid file.
    """
    # Pass-through: if already BaseGeometry, validate and return
    if isinstance(source, BaseGeometry):
        return _valid(force_2d(source))

    if isinstance(source, PathLike):
        return _load_file(Path(source))

    if isinstance(source, str):
        s = source.strip()  # Remove extraneous whitespace

        # Try WKT first, fall back to file if that fails
        try:
            # Extract WKT from messy QGIS paste if needed
            wkt_keywords = (
                r'(?:POINT|LINESTRING|POLYGON|MULTIPOINT|'
                r'MULTILINESTRING|MULTIPOLYGON|'
                r'GEOMETRYCOLLECTION)'
            )
            pattern = rf'{wkt_keywords}(?:\s+Z)?\s*\(.*\)'
            match = re.search(pattern, s, re.IGNORECASE)
            wkt_text = match.group(0) if match else s
            geom = _valid(force_2d(swkt.loads(wkt_text)))
            return geom
        except Exception:
            pass

        # If WKT failed, try as a file path
        p = Path(s)
        if p.exists():
            return _load_file(p)

    raise ValueError(
        "Input must be valid WKT, a GeoJSON file path, or a "
        "BaseGeometry object"
    )

