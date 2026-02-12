"""GDAL warp/read utilities with semaphore-based concurrency."""

import threading
import numpy as np
from osgeo import gdal


def build_vsi_path(href: str, vsi_prefix: str = "/vsicurl",
                   transform_href=None, s3_endpoint: str = None) -> str:
    """
    Build a GDAL VSI path from an asset href.

    Args:
        href: The asset href (S3 URI or HTTP URL).
        vsi_prefix: GDAL virtual filesystem prefix ('/vsicurl' or '/vsis3').
        transform_href: Optional function to transform the href (e.g. sign URL).
        s3_endpoint: S3 endpoint host for building HTTPS URLs from S3 hrefs.
    """
    if transform_href:
        href = transform_href(href)

    if vsi_prefix == "/vsis3":
        if href.startswith("s3://"):
            return f"/vsis3/{href[5:]}"
        elif href.startswith("/"):
            return f"/vsis3{href}"
        return f"/vsis3/{href}"
    elif vsi_prefix == "/vsicurl" and href.startswith("s3://") and s3_endpoint:
        # Convert s3://bucket/path to https://endpoint/bucket/path
        path = href[5:]
        return f"/vsicurl/https://{s3_endpoint}/{path}"
    else:
        if href.startswith("http"):
            return f"/vsicurl/{href}"
        return href


def read_and_crop_band(href: str, geojson_cutline: str, semaphore: threading.Semaphore,
                       vsi_prefix: str = "/vsicurl",
                       transform_href=None,
                       s3_endpoint: str = None,
                       target_resolution: int = 10,
                       resample_alg: str = 'bilinear') -> tuple:
    """
    Read a single band, crop to field boundary, resample to target resolution.

    Returns:
        tuple: (data, geotransform, crs)
    """
    vsi_path = build_vsi_path(href, vsi_prefix, transform_href, s3_endpoint)

    resample_map = {
        'bilinear': gdal.GRA_Bilinear,
        'nearest': gdal.GRA_NearestNeighbour,
    }

    with semaphore:
        ds = gdal.Warp(
            '', vsi_path,
            format='MEM',
            cutlineDSName=geojson_cutline,
            cropToCutline=True,
            xRes=target_resolution,
            yRes=target_resolution,
            resampleAlg=resample_map.get(resample_alg, gdal.GRA_Bilinear),
            dstNodata=0,
            outputType=gdal.GDT_Int16,
        )
        if ds is None:
            raise IOError(f"Failed to read band from {vsi_path}")

        data = ds.ReadAsArray()
        gt = ds.GetGeoTransform()
        crs = ds.GetProjection()
        ds = None

    return data, gt, crs
