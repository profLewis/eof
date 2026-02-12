"""Unified STAC-based EO data reader.

Replaces the three near-identical readers (CDSE, AWS, Planetary) with a
single implementation parameterized by SourceConfig. Supports Sentinel-2
(original API) and generic multi-sensor fetch via fetch_sensor().
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

from eof._types import S2Result, EOResult
from eof._source_configs import SourceConfig
from eof._sensor_configs import SensorConfig
from eof._platform_bindings import SensorPlatformBinding
from eof._geojson import load_geojson
from eof._cache import get_cache_path, save_to_cache, load_from_cache
from eof._scl import apply_scl_cloud_mask, dn_to_reflectance
from eof._gdal_io import read_and_crop_band
from eof._footprints import compute_all_footprints
from eof._bandpass import load_srf

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
        overrides = cfg.configure_gdal(sensor="sentinel2")
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

        failed_items = []
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
                    try:
                        results[idx] = future.result()
                    except Exception as e:
                        item_id = items[idx].id
                        print(f"\nWarning: failed to process {item_id}: {e}")
                        failed_items.append(idx)

        if failed_items:
            print(f"Skipped {len(failed_items)}/{len(items)} items due to errors")

        # Assemble output (skip failed items)
        s2_reflectances = []
        valid_indices = []
        geotransform = None
        crs = None
        for i, result_item in enumerate(results):
            if result_item is None:
                continue
            refl, gt, proj = result_item
            s2_reflectances.append(refl)
            valid_indices.append(i)
            if geotransform is None:
                geotransform = gt
                crs = proj

        if not s2_reflectances:
            raise RuntimeError(
                f"All {len(items)} S2 items failed to download."
            )

        s2_angles = s2_angles[:, valid_indices]
        doys = doys[valid_indices]

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

    def fetch_sensor(self, sensor_config: SensorConfig,
                     binding: SensorPlatformBinding,
                     start_date: str, end_date: str, geojson_path: str,
                     data_folder: str = None,
                     max_cloud_cover: int = 80) -> EOResult:
        """
        Fetch multi-sensor EO data via STAC API.

        Args:
            sensor_config: Sensor-specific configuration.
            binding: Platform-specific binding for this sensor.
            start_date: Start date 'YYYY-MM-DD'.
            end_date: End date 'YYYY-MM-DD'.
            geojson_path: Path to GeoJSON field boundary.
            data_folder: Cache directory. None creates a temp dir.
            max_cloud_cover: Max cloud cover percentage.

        Returns:
            EOResult with reflectance, footprints, etc.
        """
        cfg = self.config
        sensor = sensor_config.name

        # Configure GDAL (pass sensor for source-specific setup, e.g. AWS requester-pays)
        overrides = cfg.configure_gdal(sensor=sensor)
        vsi_prefix = overrides.get("vsi_prefix", cfg.vsi_prefix)

        # Set up data folder
        if data_folder is None:
            data_folder = tempfile.mkdtemp(prefix=f"{sensor}_")
            print(f"Cache folder: {data_folder}")
        os.makedirs(data_folder, exist_ok=True)

        # Load geometry
        geometry = load_geojson(geojson_path)
        geojson_dict = json.loads(shapely.to_geojson(geometry))

        # STAC search — some sensors (MODIS, VIIRS, OLCI) don't have eo:cloud_cover
        has_cloud_cover = sensor in ("sentinel2", "landsat")
        items = self._search_collection(
            binding.collection, geojson_dict, start_date, end_date,
            max_cloud_cover, extra_query=binding.extra_query or None,
            has_cloud_cover=has_cloud_cover,
        )

        # Filter out items that don't have the required band assets
        # (e.g. Landsat 7 in a Landsat 8/9 query)
        if items:
            filtered = []
            for item in items:
                if all(k in item.assets for k in binding.band_asset_keys):
                    filtered.append(item)
            if len(filtered) < len(items):
                print(f"Filtered {len(items) - len(filtered)} items missing "
                      f"required bands")
            items = filtered

        print(f"STAC search ({cfg.name}/{sensor}): {len(items)} items found")

        if not items:
            raise RuntimeError(
                f"No {sensor} items found on {cfg.name} for the given "
                f"field and date range ({start_date} to {end_date})."
            )

        # Sort by datetime (some items use start_datetime instead)
        items.sort(key=lambda x: x.datetime or x.properties.get(
            "start_datetime", "9999"))

        # Extract angles and DOYs
        szas, vzas, raas, doys = [], [], [], []
        for item in items:
            sza, vza, raa = self._extract_angles(item)
            szas.append(sza)
            vzas.append(vza)
            raas.append(raa)
            dt = item.datetime
            if dt is None:
                from datetime import datetime
                dt = datetime.fromisoformat(
                    item.properties.get("start_datetime", "2000-01-01T00:00:00Z")
                    .replace("Z", "+00:00")
                )
            doys.append(dt.timetuple().tm_yday)

        angles = np.array([szas, vzas, raas], dtype=np.float64)
        doys = np.array(doys, dtype=np.int64)

        # Cache status
        n_cached = sum(
            1 for item in items
            if os.path.exists(get_cache_path(item.id, data_folder, sensor))
        )
        print(f"Cache: {n_cached}/{len(items)} items cached, "
              f"{len(items) - n_cached} to download")

        # Process items
        band_pool_size = cfg.max_concurrent_reads + 4
        process_fn = partial(
            self._process_sensor_item,
            sensor_config=sensor_config,
            binding=binding,
            geojson_cutline=geojson_path,
            vsi_prefix=vsi_prefix,
            data_folder=data_folder,
        )

        failed_items = []
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
                    desc=f"Processing {sensor} items ({cfg.name})",
                    unit="item",
                ):
                    idx = future_to_idx[future]
                    try:
                        results[idx] = future.result()
                    except Exception as e:
                        item_id = items[idx].id
                        print(f"\nWarning: failed to process {item_id}: {e}")
                        failed_items.append(idx)

        if failed_items:
            print(f"Skipped {len(failed_items)}/{len(items)} items due to errors")

        # Assemble output (skip failed items)
        reflectances = []
        valid_indices = []
        geotransform = None
        crs = None
        for i, result_item in enumerate(results):
            if result_item is None:
                continue
            refl, gt, proj = result_item
            reflectances.append(refl)
            valid_indices.append(i)
            if geotransform is None:
                geotransform = gt
                crs = proj

        if not reflectances:
            raise RuntimeError(
                f"All {len(items)} {sensor} items failed to download."
            )

        # Filter angles/doys to match successful items
        angles = angles[:, valid_indices]
        doys = doys[valid_indices]

        refs = np.array(reflectances, dtype=np.float32)
        uncs = sensor_config.uncertainty_fn(refs)
        mask = np.all(np.isnan(refs), axis=(0, 1))

        # Compute footprint ID maps (with edge exclusion using mask)
        shape_hw = refs.shape[2:]
        footprints = compute_all_footprints(
            geotransform, shape_hw, sensor_config.resolution_groups,
            sensor_config.target_resolution, mask=mask,
        )

        # Load bandpass / spectral response functions
        bandpass = load_srf(sensor)

        return EOResult(
            reflectance=refs,
            uncertainty=uncs,
            angles=angles,
            doys=doys,
            mask=mask,
            geotransform=geotransform,
            crs=crs,
            sensor=sensor,
            band_names=sensor_config.band_names,
            native_resolutions=sensor_config.resolution_groups,
            footprints=footprints,
            bandpass=bandpass,
        )

    # -------------------------------------------------------------------
    # Internal methods
    # -------------------------------------------------------------------

    def _search_collection(self, collection: str, geojson_geometry: dict,
                           start_date: str, end_date: str,
                           max_cloud_cover: int,
                           extra_query: dict = None,
                           has_cloud_cover: bool = True) -> list:
        """Search STAC catalogue for items in a given collection."""
        query = {}
        if has_cloud_cover:
            query["eo:cloud_cover"] = {"lte": max_cloud_cover}
        if extra_query:
            query.update(extra_query)
        client = pystac_client.Client.open(self.config.stac_url)
        search_kwargs = dict(
            collections=[collection],
            intersects=geojson_geometry,
            datetime=f"{start_date}/{end_date}",
            max_items=500,
        )
        if query:
            search_kwargs["query"] = query
        search = client.search(**search_kwargs)
        items = list(search.items())
        # Some items (e.g. MODIS) use start_datetime instead of datetime
        items.sort(key=lambda x: x.datetime or x.properties.get(
            "start_datetime", "9999"))
        return items

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
        items.sort(key=lambda x: x.datetime or x.properties.get(
            "start_datetime", "9999"))
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

    def _read_sensor_bands(self, item, binding: SensorPlatformBinding,
                           geojson_cutline: str, vsi_prefix: str,
                           target_resolution: int = 10,
                           band_executor: ThreadPoolExecutor = None):
        """Read all spectral bands + QA for a generic sensor item."""
        cfg = self.config

        band_tasks = []
        for asset_key in binding.band_asset_keys:
            asset = item.assets.get(asset_key)
            if asset is None:
                raise KeyError(f"Asset '{asset_key}' not found in item {item.id}")
            band_tasks.append((asset_key, asset.href, binding.resample_alg))

        qa_asset = item.assets.get(binding.qa_asset_key)
        if qa_asset is None:
            raise KeyError(
                f"QA asset '{binding.qa_asset_key}' not found in item {item.id}"
            )
        band_tasks.append((binding.qa_asset_key, qa_asset.href,
                           binding.qa_resample_alg))

        read_fn = partial(
            read_and_crop_band,
            geojson_cutline=geojson_cutline,
            semaphore=self._semaphore,
            vsi_prefix=vsi_prefix,
            transform_href=cfg.transform_href,
            s3_endpoint=cfg.s3_endpoint,
            target_resolution=target_resolution,
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
        for asset_key in binding.band_asset_keys:
            data, gt, proj = results[asset_key]
            bands.append(data)
            if geotransform is None:
                geotransform = gt
                crs = proj

        qa_data, _, _ = results[binding.qa_asset_key]
        band_data = np.stack(bands, axis=0)
        return band_data, qa_data, geotransform, crs

    def _process_sensor_item(self, item, sensor_config: SensorConfig,
                             binding: SensorPlatformBinding,
                             geojson_cutline: str, vsi_prefix: str,
                             data_folder: str,
                             band_executor: ThreadPoolExecutor = None):
        """Process a single STAC item for a generic sensor."""
        sensor = sensor_config.name
        cache_path = get_cache_path(item.id, data_folder, sensor)

        if os.path.exists(cache_path):
            band_data, qa_data, geotransform, crs = load_from_cache(cache_path)
        else:
            band_data, qa_data, geotransform, crs = self._read_sensor_bands(
                item, binding, geojson_cutline, vsi_prefix,
                target_resolution=sensor_config.target_resolution,
                band_executor=band_executor,
            )
            save_to_cache(cache_path, band_data, qa_data, geotransform, crs)

        # DN to reflectance
        metadata = dict(item.properties)
        metadata["processing_baseline"] = metadata.get(
            self.config.processing_baseline_property, "05.00"
        )
        reflectance = sensor_config.dn_to_reflectance(band_data, metadata)

        # Cloud masking
        if sensor_config.cloud_mask_fn is not None:
            cloud_mask = sensor_config.cloud_mask_fn(qa_data)
            reflectance[:, cloud_mask] = np.nan
        elif sensor == "sentinel2":
            # Use existing SCL-based masking for S2
            reflectance = apply_scl_cloud_mask(reflectance, qa_data)

        return reflectance, geotransform, crs
