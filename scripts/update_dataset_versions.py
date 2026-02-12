#!/usr/bin/env python3
"""Check and update dataset versions for all eof sensor × platform combinations.

Queries each STAC endpoint to find available collections and compares them
against the versions in src/eof/data/dataset_versions.json.

Run: python scripts/update_dataset_versions.py [--update]

Without --update, only reports what's available. With --update, writes
changes back to the config file.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

# STAC endpoints to query
STAC_ENDPOINTS = {
    "aws": "https://earth-search.aws.element84.com/v1",
    "cdse": "https://stac.dataspace.copernicus.eu/v1",
    "planetary": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "earthdata": "https://cmr.earthdata.nasa.gov/stac/LPCLOUD",
}

# Collection name patterns to look for per sensor per platform.
# Maps (sensor, platform) -> list of regex patterns to match collection IDs.
COLLECTION_PATTERNS = {
    ("sentinel2", "aws"): [r"sentinel-2-l2a"],
    ("sentinel2", "cdse"): [r"sentinel-2-l2a"],
    ("sentinel2", "planetary"): [r"sentinel-2-l2a"],
    ("sentinel2", "earthdata"): [r"HLSS30_\d+\.\d+"],
    ("landsat", "aws"): [r"landsat-c2-l2"],
    ("landsat", "planetary"): [r"landsat-c2-l2"],
    ("landsat", "earthdata"): [r"HLSL30_\d+\.\d+"],
    ("modis", "planetary"): [r"modis-09A1-\d+", r"modis-09GA-\d+"],
    ("modis", "earthdata"): [r"MOD09GA_\d+"],
    ("viirs", "earthdata"): [r"VNP09GA_\d+"],
}


def fetch_collections(stac_url, timeout=30):
    """Fetch all collection IDs from a STAC endpoint."""
    import requests

    collections = []
    url = f"{stac_url}/collections"
    params = {"limit": 250}

    while url:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Warning: failed to fetch {url}: {e}")
            break

        for col in data.get("collections", []):
            cid = col.get("id", "")
            title = col.get("title", "")
            collections.append({"id": cid, "title": title})

        # Follow pagination
        url = None
        params = {}
        for link in data.get("links", []):
            if link.get("rel") == "next":
                url = link["href"]
                break

    return collections


def extract_version(collection_id):
    """Try to extract a version string from a collection ID."""
    # Pattern: name_VERSION or name-VERSION-xxx
    # e.g. MOD09GA_061 -> 061, HLSL30_2.0 -> 2.0, modis-09A1-061 -> 061
    m = re.search(r'[_-](\d+(?:\.\d+)?)\s*$', collection_id)
    if m:
        return m.group(1)
    m = re.search(r'[_-]v?(\d+(?:\.\d+)?)', collection_id)
    if m:
        return m.group(1)
    return None


def find_matching_collections(collections, patterns):
    """Find collections matching any of the given patterns."""
    matches = []
    for col in collections:
        for pat in patterns:
            if re.match(pat, col["id"]):
                matches.append(col)
                break
    return matches


def check_for_updates(config_path, verbose=True):
    """Check all sensor × platform combinations for newer dataset versions.

    Returns:
        list of dicts with keys: sensor, platform, current_collection,
        current_version, available_collections, suggested_update
    """
    with open(config_path) as f:
        config = json.load(f)

    # Cache fetched collections per endpoint
    endpoint_cache = {}
    results = []

    for sensor, platforms in config.items():
        if sensor.startswith("_"):
            continue
        if not isinstance(platforms, dict):
            continue

        for platform, info in platforms.items():
            if not isinstance(info, dict):
                continue

            key = (sensor, platform)
            if key not in COLLECTION_PATTERNS:
                continue

            stac_url = STAC_ENDPOINTS.get(platform)
            if not stac_url:
                continue

            # Fetch collections (cached)
            if platform not in endpoint_cache:
                if verbose:
                    print(f"Fetching collections from {platform} ({stac_url})...")
                endpoint_cache[platform] = fetch_collections(stac_url)
                if verbose:
                    print(f"  Found {len(endpoint_cache[platform])} collections")

            all_cols = endpoint_cache[platform]
            patterns = COLLECTION_PATTERNS[key]
            matches = find_matching_collections(all_cols, patterns)

            current_col = info.get("collection", "")
            current_ver = info.get("version", "")

            result = {
                "sensor": sensor,
                "platform": platform,
                "current_collection": current_col,
                "current_version": current_ver,
                "available": matches,
                "suggested_update": None,
            }

            # Check if current collection is still available
            available_ids = [m["id"] for m in matches]
            if current_col not in available_ids and matches:
                # Current collection not found; suggest the latest match
                # Sort by version (higher = newer)
                sorted_matches = sorted(
                    matches,
                    key=lambda m: extract_version(m["id"]) or "",
                    reverse=True,
                )
                result["suggested_update"] = sorted_matches[0]

            # Check if there's a newer version
            for m in matches:
                if m["id"] == current_col:
                    continue
                m_ver = extract_version(m["id"])
                if m_ver and current_ver and m_ver > current_ver:
                    result["suggested_update"] = m

            results.append(result)

    return results


def print_report(results):
    """Print a formatted report of version checks."""
    print("\n" + "=" * 70)
    print("Dataset Version Report")
    print("=" * 70)

    any_updates = False
    for r in results:
        sensor = r["sensor"]
        platform = r["platform"]
        current = r["current_collection"]
        available = r["available"]

        status = "OK"
        if r["suggested_update"]:
            status = f"UPDATE AVAILABLE -> {r['suggested_update']['id']}"
            any_updates = True

        available_ids = [m["id"] for m in available]

        print(f"\n  {sensor:12s} / {platform:12s}")
        print(f"    Current:   {current} (v{r['current_version']})")
        print(f"    Available: {', '.join(available_ids) if available_ids else 'none found'}")
        print(f"    Status:    {status}")

    if not any_updates:
        print("\nAll datasets are up to date.")
    else:
        print("\nRun with --update to apply suggested updates.")


def apply_updates(config_path, results):
    """Apply suggested updates to the config file."""
    with open(config_path) as f:
        config = json.load(f)

    updated = 0
    for r in results:
        if r["suggested_update"] is None:
            continue

        sensor = r["sensor"]
        platform = r["platform"]
        new_col = r["suggested_update"]["id"]
        new_ver = extract_version(new_col) or r["current_version"]

        old_col = config[sensor][platform]["collection"]
        config[sensor][platform]["collection"] = new_col
        config[sensor][platform]["version"] = new_ver
        print(f"  Updated {sensor}/{platform}: {old_col} -> {new_col}")
        updated += 1

    if updated > 0:
        config["_updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        print(f"\n{updated} collection(s) updated in {config_path}")
    else:
        print("\nNo updates to apply.")


def main():
    parser = argparse.ArgumentParser(
        description="Check and update eof dataset versions."
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Apply suggested updates to the config file.",
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to dataset_versions.json. Default: auto-detect.",
    )
    args = parser.parse_args()

    if args.config:
        config_path = args.config
    else:
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "eof", "data",
            "dataset_versions.json",
        )
        config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Config: {config_path}")
    results = check_for_updates(config_path)
    print_report(results)

    if args.update:
        apply_updates(config_path, results)


if __name__ == "__main__":
    main()
