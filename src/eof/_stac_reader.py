"""Unified STAC-based Sentinel-2 reader.

Replaces the three near-identical readers (CDSE, AWS, Planetary) with a
single implementation parameterized by SourceConfig.
"""

import os
import json
import tempfile
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from tqdm.auto import tqdm
from osgeo import gdal
import shapely
import mgrs
import pystac_client

from eof._types import S2Result
from eof._source_configs import SourceConfig
from eof._geojson import load_geojson
from eof._cache import get_cache_path, save_to_cache, load_from_cache
from eof._scl import apply_scl_cloud_mask, dn_to_reflectance
from eof._gdal_io import read_and_crop_band

gdal.PushErrorHandler('CPLQuietErrorHandler')


class STACReader:
    """Fetch Sentinel-2 L2A data via STAC API, parameterized by SourceConfig."""

    def __init__(self, config: SourceConfig):
        self.config = config
        self._semaphore = threading.Semaphore(config.max_concurrent_reads)

    def fetch(self, start_date: str, end_date: str, geojson_path: str,
              data_folder: str = None, max_cloud_cover: int = 80) -> S2Result:
        """
        Fetch Sentinel-2 L2A data for a field boundary.

        Args:
            start_date: Start date 'YYYY-MM-DD'.
            end_date: End date 'YYYY-MM-DD'.
            geojson_path: Path to GeoJSON field boundary.
            data_folder: Cache directory. None creates a temp dir.
            max_cloud_cover: Max cloud cover percentage.

        Returns:
            S2Result with reflectance, uncertainty, angles, doys, mask, geotransform, crs.
        """
        cfg = self.config

        # Configure GDAL
        overrides = cfg.configure_gdal()
        vsi_prefix = overrides.get("vsi_prefix", cfg.vsi_prefix)

        # Set up data folder
        if data_folder is None:
            data_folder = tempfile.mkdtemp(prefix="s2_")
            print(f"Cache folder: {data_folder}")
        os.makedirs(data_folder, exist_ok=True)

        # Load geometry
        geometry = load_geojson(geojson_path)
        centroid = geometry.centroid
        geojson_dict = json.loads(shapely.to_geojson(geometry))

        # STAC search
        items = self._search(geojson_dict, start_date, end_date, max_cloud_cover)
        print(f"STAC search ({cfg.name}): {len(items)} items found")

        # Filter to single MGRS tile
        items = self._filter_mgrs(items, centroid.x, centroid.y)

        if not items:
            raise RuntimeError(
                f"No Sentinel-2 L2A items found on {cfg.name} for the given "
                f"field and date range ({start_date} to {end_date})."
            )

        # Report tile
        tile_code = "unknown"
        for key in cfg.mgrs_property_keys:
            tc = items[0].properties.get(key, "")
            if tc:
                tile_code = tc
                break
        print(f"Filtered to {len(items)} items on tile {tile_code}")

        # Sort by datetime
        items.sort(key=lambda x: x.datetime)

        # Extract angles and DOYs
        szas, vzas, raas, doys = [], [], [], []
        for item in items:
            sza, vza, raa = self._extract_angles(item)
            szas.append(sza)
            vzas.append(vza)
            raas.append(raa)
            doys.append(item.datetime.timetuple().tm_yday)

        s2_angles = np.array([szas, vzas, raas], dtype=np.float64)
        doys = np.array(doys, dtype=np.int64)

        # Cache status
        n_cached = sum(
            1 for item in items
            if os.path.exists(get_cache_path(item.id, data_folder))
        )
        print(f"Cache: {n_cached}/{len(items)} items cached, "
              f"{len(items) - n_cached} to download")

        # Process items with two-level executor
        band_pool_size = cfg.max_concurrent_reads + 4
        process_fn = partial(
            self._process_item,
            geojson_cutline=geojson_path,
            vsi_prefix=vsi_prefix,
            data_folder=data_folder,
        )

        with ThreadPoolExecutor(max_workers=band_pool_size) as band_executor:
            process_with_bands = partial(process_fn, band_executor=band_executor)
            with ThreadPoolExecutor(max_workers=len(items)) as item_executor:
                future_to_idx = {
                    item_executor.submit(process_with_bands, item): i
                    for i, item in enumerate(items)
                }
                results = [None] * len(items)
                for future in tqdm(
                    as_completed(future_to_idx),
                    total=len(items),
                    desc=f"Processing S2 items ({cfg.name})",
                    unit="item",
                ):
                    idx = future_to_idx[future]
                    results[idx] = future.result()

        # Assemble output
        s2_reflectances = []
        geotransform = None
        crs = None
        for refl, gt, proj in results:
            s2_reflectances.append(refl)
            if geotransform is None:
                geotransform = gt
                crs = proj

        s2_refs = np.array(s2_reflectances, dtype=np.float32)
        s2_uncs = np.abs(s2_refs) * 0.1
        mask = np.all(np.isnan(s2_refs), axis=(0, 1))

        return S2Result(
            reflectance=s2_refs,
            uncertainty=s2_uncs,
            angles=s2_angles,
            doys=doys,
            mask=mask,
            geotransform=geotransform,
            crs=crs,
        )

    # -------------------------------------------------------------------
    # Internal methods
    # -------------------------------------------------------------------

    def _search(self, geojson_geometry: dict, start_date: str, end_date: str,
                max_cloud_cover: int) -> list:
        """Search STAC catalogue for S2 L2A items."""
        client = pystac_client.Client.open(self.config.stac_url)
        search = client.search(
            collections=[self.config.collection],
            intersects=geojson_geometry,
            datetime=f"{start_date}/{end_date}",
            query={"eo:cloud_cover": {"lte": max_cloud_cover}},
            max_items=500,
        )
        items = list(search.items())
        items.sort(key=lambda x: x.datetime)
        return items

    def _filter_mgrs(self, items: list, centroid_lon: float,
                     centroid_lat: float) -> list:
        """Filter items to the MGRS tile containing the field centroid."""
        m = mgrs.MGRS()
        target_tile = m.toMGRS(centroid_lat, centroid_lon)[:5]

        filtered = []
        for item in items:
            props = item.properties
            for key in self.config.mgrs_property_keys:
                val = props.get(key, "")
                if val == f"MGRS-{target_tile}" or val == target_tile:
                    filtered.append(item)
                    break
        return filtered

    def _extract_angles(self, item) -> tuple:
        """Extract (SZA, VZA, RAA) in degrees from STAC item properties."""
        props = item.properties
        sun_elevation = props.get("view:sun_elevation",
                                  props.get("s2:mean_solar_zenith", 0.0))
        sun_azimuth = props.get("view:sun_azimuth",
                                props.get("s2:mean_solar_azimuth", 0.0))
        view_zenith = props.get("view:incidence_angle", 0.0)
        view_azimuth = props.get("view:azimuth", 0.0)

        # Handle elevation vs zenith
        if sun_elevation < 90:
            sza = 90.0 - sun_elevation
        else:
            sza = sun_elevation

        raa = (view_azimuth - sun_azimuth) % 360.0
        return sza, view_zenith, raa

    def _read_all_bands(self, item, geojson_cutline: str, vsi_prefix: str,
                        band_executor: ThreadPoolExecutor = None):
        """Read all 10 spectral bands + SCL for a single STAC item."""
        cfg = self.config

        band_tasks = []
        for asset_key in cfg.band_assets:
            asset = item.assets.get(asset_key)
            if asset is None:
                raise KeyError(f"Asset '{asset_key}' not found in item {item.id}")
            band_tasks.append((asset_key, asset.href, 'bilinear'))

        scl_asset = item.assets.get(cfg.scl_asset_key)
        if scl_asset is None:
            raise KeyError(f"Asset '{cfg.scl_asset_key}' not found in item {item.id}")
        band_tasks.append((cfg.scl_asset_key, scl_asset.href, 'nearest'))

        read_fn = partial(
            read_and_crop_band,
            geojson_cutline=geojson_cutline,
            semaphore=self._semaphore,
            vsi_prefix=vsi_prefix,
            transform_href=cfg.transform_href,
            s3_endpoint=cfg.s3_endpoint,
        )

        if band_executor is not None:
            futures = {}
            for asset_key, href, resample in band_tasks:
                future = band_executor.submit(
                    read_fn, href, resample_alg=resample,
                )
                futures[future] = asset_key
            results = {}
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        else:
            results = {}
            for asset_key, href, resample in band_tasks:
                results[asset_key] = read_fn(href, resample_alg=resample)

        # Assemble in band order
        bands = []
        geotransform = None
        crs = None
        for asset_key in cfg.band_assets:
            data, gt, proj = results[asset_key]
            bands.append(data)
            if geotransform is None:
                geotransform = gt
                crs = proj

        scl_data, _, _ = results[cfg.scl_asset_key]
        band_data = np.stack(bands, axis=0)
        return band_data, scl_data, geotransform, crs

    def _process_item(self, item, geojson_cutline: str, vsi_prefix: str,
                      data_folder: str,
                      band_executor: ThreadPoolExecutor = None):
        """Process a single STAC item: read, convert, mask."""
        cache_path = get_cache_path(item.id, data_folder)

        if os.path.exists(cache_path):
            band_data, scl_data, geotransform, crs = load_from_cache(cache_path)
        else:
            band_data, scl_data, geotransform, crs = self._read_all_bands(
                item, geojson_cutline, vsi_prefix, band_executor,
            )
            save_to_cache(cache_path, band_data, scl_data, geotransform, crs)

        baseline = item.properties.get(
            self.config.processing_baseline_property, '05.00'
        )
        reflectance = dn_to_reflectance(band_data, baseline)
        reflectance = apply_scl_cloud_mask(reflectance, scl_data)

        return reflectance, geotransform, crs
