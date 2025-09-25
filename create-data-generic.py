#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Synthetic data generator (refactored)
- Single-source-of-truth configs for campaign & geography
- No behavioral changes to core logic, only reads from config
"""

from faker import Faker
import uuid
import random
import base64
import json
import math
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import time
import requests
import logging
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional, List, Any, Union

# ========================
# Logging
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')

# ========================
# Config Models
# ========================

@dataclass(frozen=True)
class CampaignConfig:
    campaign_id: str
    campaign_number: str
    project_type: str
    project_type_id: str
    tenant_id: str = "dev"


@dataclass
class GeoConfig:
    FORCE_COORDS: Optional[Tuple[float, float]] = None
    USER_LOCATION_COORDS: List[Dict[str, Any]] = field(default_factory=list)

    # NEW: safe circle just for Mozambique
    COUNTRY_CIRCLES: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        # Center of Mozambique (inland), ~250 km radius
        "Mozambique": {"lat": -17.75, "lon": 35.50, "radius_km": 250},
        # Nigeria (new)
        "Nigeria": {"lat": 9.0, "lon": 8.5, "radius_km": 400}
    })

    COUNTRY_LATLON_RANGES: Dict[str, Tuple[Tuple[float, float], Tuple[float, float]]] = field(default_factory=lambda: {
        "Mozambique": ((-26.9, -10.5), (30.2, 41.5)),  # keep as fallback
        "Nigeria": ((4.2, 13.9), (2.7, 14.7))
    })

    DEFAULT_COUNTRY: str = "Nigeria"

@dataclass
class ESConfig:
    host: str = "http://elasticsearch-master.es-upgrade:9200/"
    # Basic auth header value (Base64 of "user:pass"), keep configurable
    basic_auth_b64: str = "ZWxhc3RpYzpaRFJsT0RJME1UQTNNV1ppTVRGbFptRms="
    verify_ssl: bool = False
    # Bulk settings
    bulk_chunk_lines: int = 50000
    bulk_chunk_retries: int = 5
    bulk_retry_delay: int = 5


@dataclass
class Settings:
    # Campaign constants used by all documents
    CAMPAIGN: CampaignConfig = CampaignConfig(
        campaign_id="43d8cfbe-17d6-46f0-a960-2f959b9e23b9",
        campaign_number="CMP-2025-09-18-006990",
        project_type="MR-DN",
        project_type_id="ea1bb2e7-06d8-4fe4-ba1e-f4a6363a21be",
        tenant_id="dev",
    )
    # Geo behavior
    GEO: GeoConfig = GeoConfig()
    # Elasticsearch behavior
    ES: ESConfig = ESConfig()
    # Retries for simple GET probes
    max_retries: int = 10
    retry_delay: int = 5
    # Generation sizes
    num_households: int = 100


SETTINGS = Settings()  # global single source of truth

# ========================
# ES Constants (indexes)
# ========================
HOUSEHOLD_INDEX = "household-index-v1"
MEMBER_INDEX = "household-member-index-v1"
PROJECT_TASK_INDEX = "project-task-index-v1"
TRANSFORMER_PGR_SERVICES_INDEX = "transformer-pgr-services"
PROJECT_INDEX = "project-index-v1"
POPULATION_COVERAGE_INDEX = "population-coverage-summary-1"
POP_SUMMARY_DATEWISE_INDEX = "population-coverage-summary-datewise-1_v2"
STOCK_INDEX = "stock-index-v1"
SERVICE_TASK_INDEX = "service-task-v1"
ATTENDANCE_LOG_INDEX = "attendance-log-index-v1"
PROJECT_STAFF_INDEX = "project-staff-index-v1"
HOUSEHOLD_COVERAGE_DAILY_ICCD_INDEX = "household-coverage-daily-iccd-v2"
HOUSEHOLD_COVERAGE_SUMMARY_ICCD_INDEX = "household-coverage-summary-iccd"
INELIGIBLE_SUMMARY_INDEX = "ineligible-summary-v2"
USER_SYNC_INDEX = "user-sync-index-v1"
REFERRAL_INDEX = "referral-index-v1"
SIDE_EFFECT_INDEX = "side-effect-index-v1"
CENSUS_INDEX = "census-index-v1"
PLAN_INDEX = "plan-index-v1"
HF_REFERRAL_INDEX = "hf-referral-index-v1"
STOCK_RECON_INDEX = "stock-reconciliation-index-v1"
PLAN_FACILITY_INDEX = "plan-facility-index-v1"

# ========================
# File Paths
# ========================
HOUSEHOLD_FILE = "bulk_households.jsonl"
MEMBER_FILE = "bulk_members.jsonl"
PROJECT_TASK_FILE = "bulk_projectTasks.jsonl"
TRANSFORMER_PGR_SERVICES_FILE = "bulk_transformer_pgr_services.jsonl"
PROJECT_FILE = "project.json"
POPULATION_COVERAGE_FILE = "bulk_population_coverage.jsonl"
POP_SUMMARY_DATEWISE_FILE = "population_coverage_summary_datewise_bulk.json"
STOCK_FILE = "stock.json"
SERVICE_TASK_FILE = "bulk_service_tasks.jsonl"
ATTENDANCE_LOG_FILE = "bulk_attendance_logs.jsonl"
PROJECT_STAFF_FILE = "bulk_project_staff.jsonl"
HOUSEHOLD_COVERAGE_DAILY_ICCD_FILE = "bulk_household_coverage_daily_iccd.jsonl"
HOUSEHOLD_COVERAGE_SUMMARY_ICCD_FILE = "bulk_household_coverage_summary_iccd.jsonl"
INELIGIBLE_SUMMARY_FILE = "bulk_ineligible_summary.jsonl"
USER_SYNC_FILE = "bulk_user_sync.jsonl"
REFERRAL_FILE = "bulk_referrals.jsonl"
SIDE_EFFECT_FILE = "bulk_side_effects.jsonl"
CENSUS_FILE = "bulk_census.jsonl"
PLAN_FILE = "bulk_plan.jsonl"
HF_REFERRAL_FILE  = "bulk_hf_referrals.jsonl"
STOCK_RECON_FILE  = "bulk_stock_reconciliation.jsonl"
PLAN_FACILITY_FILE = "bulk_plan_facilities.jsonl"

# ========================
# Boundary seeds (unchanged)
# ========================
boundary_data = [
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Kaluway",
            "administrativeProvince": "Pleebo Health Center",
            "locality": "Hospital Camp/Camp 3",
            "village": "Hospital Camp",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_05_KALUWAY__2",
            "administrativeProvince": "NEWTEST00222_MO_11_05_03_YEDIAKEN_CLINIC",
            "locality": "NEWTEST00222_MO_11_06_05_14_HOSPITAL_CAMP_CAMP_3",
            "village": "NEWTEST00222_MO_11_06_05_14_01_HOSPITAL_CAMP"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Kaluway"
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_05_KALUWAY__2"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Kaluway",
            "administrativeProvince": "Boniken",
            "locality": "Hospital Camp/Camp 3",
            "village": "Hospital Camp",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_05_KALUWAY__2",
            "administrativeProvince": "NEWTEST00222_MO_11_05_01_BONIKEN",
            "locality": "NEWTEST00222_MO_11_06_05_14_HOSPITAL_CAMP_CAMP_3",
            "village": "NEWTEST00222_MO_11_06_05_14_01_HOSPITAL_CAMP"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND"
        }
    },
    {
        "hierarchy": {"country": "Mozambique"},
        "codes": {"country": "NEWTEST00222_MO"}
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Pleebo",
            "administrativeProvince": "Pleebo Health Center",
            "locality": "Hospital Camp/Camp 3",
            "village": "Hospital Camp",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_06_PLEEBO",
            "administrativeProvince": "NEWTEST00222_MO_11_06_05_PLEEBO_HEALTH_CENTER",
            "locality":"NEWTEST00222_MO_11_06_05_14_HOSPITAL_CAMP_CAMP_3",
            "village": "NEWTEST00222_MO_11_06_05_14_01_HOSPITAL_CAMP"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Pleebo",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_06_PLEEBO"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Pleebo",
            "administrativeProvince": "Pleebo Health Center"
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_06_PLEEBO",
            "administrativeProvince": "NEWTEST00222_MO_11_06_05_PLEEBO_HEALTH_CENTER"
        }
    },
    {
        "hierarchy": {
            "country": "Mozambique",
            "province": "Maryland",
            "district": "Pleebo",
            "administrativeProvince": "Gbloken Clinic",
            "locality": "Hospital Camp/Camp 3",
            "village": "Hospital Camp",
        },
        "codes": {
            "country": "NEWTEST00222_MO",
            "province": "NEWTEST00222_MO_11_MARYLAND",
            "district": "NEWTEST00222_MO_11_06_PLEEBO",
            "administrativeProvince": "NEWTEST00222_MO_11_06_04_GBLOKEN_CLINIC",
            "locality":"NEWTEST00222_MO_11_06_05_14_HOSPITAL_CAMP_CAMP_3",
            "village": "NEWTEST00222_MO_11_06_05_14_01_HOSPITAL_CAMP"
        }
    }
]

project_type = ["MR-DN"]
product_name = ["SP 500mg", "SP 250mg", "AQ 500mg"]
project_names = ["SMC Campaign 1", "SMC Campaign 2", "SMC Campaign 3", "SMC Campaign 4", "Malaria Control Drive", "Seasonal Immunization", "Child Health Program", "ICCD SMC Campaign"]
names = ["Lata", "Ram", "Sita", "John", "Priya", "Nina", "Amit", "Ravi", "Suresh", "Geeta"]

# ========================
# Helpers
# ========================

def _random_point_in_circle(center_lat: float, center_lon: float, radius_km: float) -> Tuple[float, float]:
    """Uniformly sample a point within a circle in km around center."""
    KM_PER_DEG_LAT = 110.574
    u = random.random()
    r_km = radius_km * math.sqrt(u)
    theta = random.random() * 2 * math.pi

    dlat = (r_km * math.sin(theta)) / KM_PER_DEG_LAT
    dlon = (r_km * math.cos(theta)) / (111.320 * math.cos(math.radians(center_lat)))

    return (round(center_lat + dlat, 6), round(center_lon + dlon, 6))


def random_epoch(start_year=2020, end_year=2026):
    start = int(time.mktime(datetime(start_year, 1, 1).timetuple()))
    end = int(time.mktime(datetime(end_year, 12, 31).timetuple()))
    return random.randint(start, end)

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def cur_timestamp():
    return int(datetime.now().timestamp() * 1000)

def random_timestamp_range(start = datetime(2025, 9, 20), end = datetime(2025, 9, 25)) -> int:
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    random_ts = random.randint(start_ts, end_ts)
    return random_ts * 1000

# def random_date_str(start_year=2025, end_year=2025):
#     start_date = datetime(start_year, 9, 20)
#     end_date = datetime(end_year, 9, 25)
#     return random_date(start_date, end_date).strftime('%Y-%m-%d')

from datetime import datetime, timedelta, timezone

def random_date_str(a=None, b=None) -> str:
    """
    Flexible helper:
      - random_date_str(2025, 2025) -> uses year bounds (keeps your old behavior: Sep 20..25)
      - random_date_str(datetime_obj) -> picks a date in [datetime_obj - 3d, datetime_obj + 3d]
      - random_date_str(datetime_start, datetime_end) -> picks a date in [start, end]
      - random_date_str() -> defaults to this year Sep 20..25
    """
    # Case 1: either argument is a datetime -> treat as datetime bounds
    if isinstance(a, datetime) or isinstance(b, datetime):
        if isinstance(a, datetime) and isinstance(b, datetime):
            start = a
            end = b
        elif isinstance(a, datetime) and b is None:
            # symmetric ±3 days around 'a'
            start = a - timedelta(days=3)
            end = a + timedelta(days=3)
        elif a is None and isinstance(b, datetime):
            start = b - timedelta(days=3)
            end = b + timedelta(days=3)
        else:
            # one is datetime, the other is not; default a 6-day window around the datetime
            dt = a if isinstance(a, datetime) else b
            start = dt - timedelta(days=3)
            end = dt + timedelta(days=3)

        # normalize if inverted
        if end < start:
            start, end = end, start

    else:
        # Case 2: legacy behavior with year ints (or None)
        start_year = a if isinstance(a, int) else 2025
        end_year = b if isinstance(b, int) else start_year
        start = datetime(start_year, 9, 20)
        end = datetime(end_year, 9, 25)

    # clamp to UTC (optional)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    # reuse your existing random_date()
    return random_date(start, end).strftime('%Y-%m-%d')


def most_specific_locality_code(codes: Dict[str, str], hierarchy: Dict[str, str]) -> Optional[str]:
    """Pick the most specific available code in the usual order."""
    for key in ["village", "locality", "administrativeProvince", "district", "province", "country"]:
        if key in codes and codes[key] and key in hierarchy:
            return codes[key]
    for k in ["district", "province", "country"]:
        if k in codes and codes[k]:
            return codes[k]
    return None

def boundary_slice(h: Dict[str, str], c: Dict[str, str], level: str):
    """
    Slice boundary to a given level (country/province/district/administrativeProvince/locality/village)
    """
    order = ["country", "province", "district", "administrativeProvince", "locality", "village"]
    if level not in order:
        return h, c
    idx = order.index(level) + 1
    h2 = {k: v for k, v in h.items() if k in order[:idx] and v}
    c2 = {k: v for k, v in c.items() if k in order[:idx] and v}
    return h2, c2

def _coords_from_user_locations(hierarchy: Dict[str, Any], user_mappings: List[Dict[str, Any]]):
    """Try exact match from USER_LOCATION_COORDS."""
    def matches(entry):
        m = entry.get("match", {})
        for k, v in m.items():
            if hierarchy.get(k) != v:
                return False
        return True
    for entry in user_mappings:
        if matches(entry):
            return float(entry["lat"]), float(entry["lon"])
    return None

def _random_in_country(country: str, geo_cfg: GeoConfig):
    lat_rng, lon_rng = geo_cfg.COUNTRY_LATLON_RANGES.get(country, geo_cfg.COUNTRY_LATLON_RANGES.get(geo_cfg.DEFAULT_COUNTRY))
    return round(random.uniform(*lat_rng), 6), round(random.uniform(*lon_rng), 6)

def pick_lat_lon_for_boundary(hierarchy: Dict[str, Any], geo_cfg: GeoConfig = SETTINGS.GEO) -> Tuple[float, float]:
    """Always pick a point inside Mozambique's safe circle (or fallback)."""
    if geo_cfg.FORCE_COORDS:
        return geo_cfg.FORCE_COORDS

    country = hierarchy.get("country", geo_cfg.DEFAULT_COUNTRY)

    # Use safe circle if defined
    circle = geo_cfg.COUNTRY_CIRCLES.get(country)
    if circle:
        return _random_point_in_circle(circle["lat"], circle["lon"], circle["radius_km"])

    # Fallback to bbox mid-point (shouldn’t happen if country is Mozambique)
    lat_rng, lon_rng = geo_cfg.COUNTRY_LATLON_RANGES[country]
    return (
        round((lat_rng[0] + lat_rng[1]) / 2, 6),
        round((lon_rng[0] + lon_rng[1]) / 2, 6)
    )


def clean_source(source):
    return {k: v for k, v in source.items() if not callable(v)}

def write_bulk_file(data: List[Dict[str, Any]], file_path: str):
    with open(file_path, 'w') as f:
        for item in data:
            f.write(json.dumps({"index": {"_index": item["_index"], "_id": item["_id"]}}) + '\n')
            f.write(json.dumps(clean_source(item["_source"])) + '\n')

def get_resp(url, es=False):
    failed = False
    for attempt in range(1, SETTINGS.max_retries + 1):
        if failed:
            logger.info(f"{attempt-1} retry out of {SETTINGS.max_retries}")
        try:
            headers = {"Content-Type": "application/json"}
            if es:
                headers["Authorization"] = f"Basic {SETTINGS.ES.basic_auth_b64}"
            response = requests.get(url, headers=headers, verify=SETTINGS.ES.verify_ssl)
            if response.status_code in [200, 202]:
                return response
            try:
                logger.warning(response.json())
            except Exception:
                logger.warning("Non-JSON response from probe.")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error. Retrying in {SETTINGS.retry_delay} seconds... (attempt {attempt})")
            failed = True
        time.sleep(SETTINGS.retry_delay)
    return None

def weighted_bool(true_ratio=0.9):
    return random.random() < true_ratio

def iso_z(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    # samples show a 'Z' suffix and nanoseconds sometimes; we keep simple ISO + Z
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def ms(dt: Optional[datetime] = None) -> int:
    dt = dt or datetime.now(timezone.utc)
    return int(dt.timestamp() * 1000)

def make_id() -> str:
    return str(uuid.uuid4())

def r2(x):
    # keep sample-like precision for floats
    return round(float(x), 2)

def build_jurisdiction_mapping_from_codes(codes: Dict[str, str]) -> Optional[Dict[str, str]]:
    """
    Map your boundaryHierarchyCode to the exact keys used in samples:
    COUNTRY, PROVINCE, DISTRICT, ADMINSTRATIVEPOST, LOCALITY, VILLAGE
    """
    mapping: Dict[str, str] = {}
    if "country" in codes:                   mapping["COUNTRY"] = codes["country"]
    if "province" in codes:                  mapping["PROVINCE"] = codes["province"]
    if "district" in codes:                  mapping["DISTRICT"] = codes["district"]
    if "administrativeProvince" in codes:    mapping["ADMINSTRATIVEPOST"] = codes["administrativeProvince"]
    if "locality" in codes:                  mapping["LOCALITY"] = codes["locality"]
    if "village" in codes:                   mapping["VILLAGE"] = codes["village"]
    return mapping or None

# keep a simple in-memory counter; you can seed from ENV/SETTINGS if needed
_SR_SEQ = random.randint(80_000, 99_999)  # looks like 6 digits in your samples

def next_sr_id(at: datetime) -> str:
    global _SR_SEQ
    _SR_SEQ += 1
    return f"SR-{at.strftime('%Y-%m-%d')}-{_SR_SEQ:06d}"

BOUNDARY_ORDER = ["country", "province", "district", "administrativeProvince", "locality", "village"]

def build_boundary_ancestral_path(codes: Dict[str, str], hierarchy: Dict[str, str]) -> str:
    """
    Join codes from country→…→most-specific present in this slice.
    Matches samples like: "MICROPLAN_MO|...|MICROPLAN_MO_17_01_01_03_01_KE_AKWARA_HEALTH_POST"
    """
    parts: List[str] = []
    for k in BOUNDARY_ORDER:
        if k in hierarchy and k in codes and codes[k]:
            parts.append(codes[k])
    return "|".join(parts) if parts else ""

def random_facility_id() -> str:
    # Sample: F-2025-07-31-008941
    dt = datetime.now().strftime("%Y-%m-%d")
    return f"F-{dt}-{random.randint(890000, 999999):06d}"

def maybe_service_boundaries(codes_pool: List[str]) -> str:
    """
    Build serviceBoundaries as:
      - "" (most docs), or
      - a CSV string of 1..N codes.
    """
    if random.random() < 0.65:
        return ""  # most docs in samples use empty string
    pick = random.sample(codes_pool, k=min(len(codes_pool), random.randint(1, min(8, len(codes_pool) or 1))))
    return ",".join(pick) if pick else ""

PLAN_RESOURCE_TYPES = [
    # Core targets / totals
    "TOTAL_TARGET_POPULATION",
    "TOTAL_SPAQ_REQUIRED",
    "TOTAL_SPAQ_REQUIRED_WITH_BUFFER",
    "TOTAL_SPAQ_1_FOR_AGE_3_TO_11_MONTHS",
    "TOTAL_SPAQ_1_FOR_AGE_3_TO_11_MONTHS_WITH_BUFFER",
    "TOTAL_SPAQ_2_FOR_AGE_12_TO_59_MONTHS",
    "TOTAL_SPAQ_2_FOR_AGE_12_TO_59_MONTHS_WITH_BUFFER",

    # Teams & supervisors (household + fixed post; registration + distribution)
    "NO_OF_HOUSEHOLD_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN",
    "NO_OF_HOUSEHOLD_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY",
    "NO_OF_HOUSEHOLD_REGISTRATION_TEAM_MEMBERS_PER_BOUNDARY",
    "NO_OF_SUPERVISORS_FOR_HOUSEHOLD_REGISTRATION_TEAM_PER_BOUNDARY",

    "NO_OF_HOUSEHOLD_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN",
    "NO_OF_HOUSEHOLD_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY",
    "NO_OF_HOUSEHOLD_DISTRIBUTION_TEAM_MEMBERS_PER_BOUNDARY",
    "NO_OF_SUPERVISORS_FOR_HOUSEHOLD_DISTRIBUTION_TEAM_PER_BOUNDARY",

    "NO_OF_FIXED_POST_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN",
    "NO_OF_FIXED_POST_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY",
    "NO_OF_FIXED_POST_REGISTRATION_TEAM_MEMBERS_PER_BOUNDARY",
    "NO_OF_FIXED_POST_REGISTRATION_TEAM_SUPERVISORS_PER_BOUNDARY",

    "NO_OF_FIXED_POST_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN",
    "NO_OF_FIXED_POST_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY",
    "NO_OF_FIXED_POST_DISTRIBUTION_TEAM_MEMBERS_PER_BOUNDARY",
    "NO_OF_FIXED_POST_DISTRIBUTION_TEAM_SUPERVISORS_PER_BOUNDARY",

    # Stationary per-boundary (household vs fixed post; pens/chalk/bags)
    "TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM",
    "TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM",
    "TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM",

    "TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM",
    "TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM",
    "TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM",

    "TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM",
    "TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM",
    "TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM",

    "TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM",
    "TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM",
    "TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM",
]

def _res(rt, val):
    return {
        "activityCode": None,
        "id": make_id(),
        "estimatedNumber": val,
        "resourceType": rt
    }

def build_stock_recon_additional_fields(received: float, issued: float,
                                        returned: float, lost: float,
                                        damaged: float, in_hand: float) -> dict:
    f = lambda v: f"{float(v):.1f}"  # strings with one decimal place
    return {
        "schema": "StockReconciliation",
        "fields": [
            {"value": f(received), "key": "received"},
            {"value": f(issued),   "key": "issued"},
            {"value": f(returned), "key": "returned"},
            {"value": f(lost),     "key": "lost"},
            {"value": f(damaged),  "key": "damaged"},
            {"value": f(in_hand),  "key": "inHand"},
        ],
        "version": 1
    }

def build_plan_resources():
    """
    Produces a resources list similar to your samples with consistent internal math.
    """
    # Target pop similar scale to samples
    tp = random.choice([70, 80, 90, 100, 110, 120])
    # Reasonable split to match examples
    age_3_11   = int(tp * random.choice([0.4, 0.5, 0.6]))
    age_12_59  = tp - age_3_11

    # SPAQ basics (toy math to match magnitudes in samples)
    spaq1 = age_3_11 * random.choice([1, 1, 1, 2/3, 3/4])
    spaq2 = age_12_59 * random.choice([1, 1.5, 2])
    spaq_required = spaq1 + spaq2
    buffer_factor = random.choice([3.5, 4.0])
    spaq1_buf = spaq1 * buffer_factor
    spaq2_buf = spaq2 * buffer_factor
    spaq_total_buf = spaq_required * buffer_factor

    # Teams (fractions per boundary)
    reg_teams_campaign  = r2(tp / random.choice([180, 200]))
    reg_teams_day       = r2(reg_teams_campaign * 4.0)
    reg_team_members    = r2(reg_teams_campaign * random.choice([50, 55, 60]))
    reg_supervisors     = r2(reg_teams_campaign * 0.8)

    dist_teams_campaign = random.choice([None, r2(tp / 250.0)])
    dist_teams_day      = random.choice([None, r2((dist_teams_campaign or 0) * 4.0)])
    dist_team_members   = random.choice([None, r2((dist_teams_campaign or 0) * 55.0)])
    dist_supervisors    = random.choice([None, r2((dist_teams_campaign or 0) * 0.7)])

    # Fixed post (smaller)
    fpr_campaign = r2(tp / random.choice([320, 360, 400]))
    fpr_day      = r2(fpr_campaign * random.choice([4.5, 5.0]))
    fpr_members  = r2(fpr_campaign * 24.0)
    fpr_sup      = r2(fpr_campaign * random.choice([0.05, 0.08]))
    fpd_campaign = r2(tp / random.choice([3500, 5000]))
    fpd_day      = r2(fpd_campaign * random.choice([60, 70]))
    fpd_members  = r2(fpd_campaign * 60.0)
    fpd_sup      = r2(fpd_campaign * random.choice([0.05, 0.1]))

    # Stationary (scale like examples)
    pens_household_reg  = r2(reg_team_members * 0.8)
    chalk_household_reg = r2(reg_team_members * 0.8)
    bags_household_reg  = r2(reg_team_members * 0.95)

    pens_household_dist  = random.choice([None, r2((dist_team_members or 0) * 0.4)])
    chalk_household_dist = random.choice([None, r2((dist_team_members or 0) * 0.4)])
    bags_household_dist  = random.choice([None, r2((dist_team_members or 0) * 0.45)])

    pens_fixed_reg  = r2(fpr_members * 4.2)
    chalk_fixed_reg = r2(fpr_members * 4.2)
    bags_fixed_reg  = r2(fpr_members * 3.4)

    pens_fixed_dist  = r2(fpd_members * 0.9)
    chalk_fixed_dist = r2(fpd_members * 0.9)
    bags_fixed_dist  = r2(fpd_members * 0.7)

    resources = [
        _res("TOTAL_TARGET_POPULATION", tp),
        _res("TOTAL_SPAQ_REQUIRED", int(spaq_required)),
        _res("TOTAL_SPAQ_REQUIRED_WITH_BUFFER", int(spaq_total_buf)),
        _res("TOTAL_SPAQ_1_FOR_AGE_3_TO_11_MONTHS", int(spaq1)),
        _res("TOTAL_SPAQ_1_FOR_AGE_3_TO_11_MONTHS_WITH_BUFFER", int(spaq1_buf)),
        _res("TOTAL_SPAQ_2_FOR_AGE_12_TO_59_MONTHS", int(spaq2)),
        _res("TOTAL_SPAQ_2_FOR_AGE_12_TO_59_MONTHS_WITH_BUFFER", int(spaq2_buf)),

        _res("NO_OF_HOUSEHOLD_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN", reg_teams_campaign),
        _res("NO_OF_HOUSEHOLD_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY", reg_teams_day),
        _res("NO_OF_HOUSEHOLD_REGISTRATION_TEAM_MEMBERS_PER_BOUNDARY", reg_team_members),
        _res("NO_OF_SUPERVISORS_FOR_HOUSEHOLD_REGISTRATION_TEAM_PER_BOUNDARY", reg_supervisors),

        _res("NO_OF_HOUSEHOLD_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN", dist_teams_campaign),
        _res("NO_OF_HOUSEHOLD_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY", dist_teams_day),
        _res("NO_OF_HOUSEHOLD_DISTRIBUTION_TEAM_MEMBERS_PER_BOUNDARY", dist_team_members),
        _res("NO_OF_SUPERVISORS_FOR_HOUSEHOLD_DISTRIBUTION_TEAM_PER_BOUNDARY", dist_supervisors),

        _res("NO_OF_FIXED_POST_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN", fpr_campaign),
        _res("NO_OF_FIXED_POST_REGISTRATION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY", fpr_day),
        _res("NO_OF_FIXED_POST_REGISTRATION_TEAM_MEMBERS_PER_BOUNDARY", fpr_members),
        _res("NO_OF_FIXED_POST_REGISTRATION_TEAM_SUPERVISORS_PER_BOUNDARY", fpr_sup),

        _res("NO_OF_FIXED_POST_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_THE_CAMPAIGN", fpd_campaign),
        _res("NO_OF_FIXED_POST_DISTRIBUTION_TEAMS_PER_BOUNDARY_FOR_ONE_DAY", fpd_day),
        _res("NO_OF_FIXED_POST_DISTRIBUTION_TEAM_MEMBERS_PER_BOUNDARY", fpd_members),
        _res("NO_OF_FIXED_POST_DISTRIBUTION_TEAM_SUPERVISORS_PER_BOUNDARY", fpd_sup),

        _res("TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM", pens_household_reg),
        _res("TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM", chalk_household_reg),
        _res("TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_REGISTRATION_TEAM", bags_household_reg),

        _res("TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM", pens_household_dist),
        _res("TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM", chalk_household_dist),
        _res("TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_HOUSEHOLD_DISTRIBUTION_TEAM", bags_household_dist),

        _res("TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM", pens_fixed_reg),
        _res("TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM", chalk_fixed_reg),
        _res("TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_REGISTRATION_TEAM", bags_fixed_reg),

        _res("TOTAL_NO_OF_PENS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM", pens_fixed_dist),
        _res("TOTAL_NO_OF_CHALKS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM", chalk_fixed_dist),
        _res("TOTAL_NO_OF_BAGS_REQUIRED_PER_BOUNDARY_FOR_FIXED_POST_DISTRIBUTION_TEAM", bags_fixed_dist),
    ]

    # Keep only a subset sometimes (your samples vary by document)
    keep_ratio = random.choice([0.45, 0.6, 0.75, 1.0])
    resources = [r for r in resources if random.random() <= keep_ratio]

    return resources

def emit_plan_facilities_for_all_levels(common_data: Dict[str, Any], user_id: str,
                                        min_per_level: int = 1, max_per_level: int = 1) -> List[Dict[str, Any]]:
    """
    For the given boundary, create plan-facility docs at every existing level:
    country → province → district → administrativeProvince → locality → village.
    """
    levels = ["country", "province", "district", "administrativeProvince", "locality", "village"]
    out: List[Dict[str, Any]] = []

    base_h = common_data["boundaryHierarchy"]
    base_c = common_data["boundaryHierarchyCode"]

    for lvl in levels:
        if lvl not in base_h:
            continue

        h_slice, c_slice = boundary_slice(base_h, base_c, lvl)

        cd = dict(common_data)
        cd["boundaryHierarchy"] = h_slice
        cd["boundaryHierarchyCode"] = c_slice
        cd["localityCode"] = most_specific_locality_code(c_slice, h_slice) or c_slice.get("country")

        for _ in range(random.randint(min_per_level, max_per_level)):
            # expects you have generate_plan_facility(...) implemented elsewhere
            doc = generate_plan_facility(cd, user_id)
            if doc:
                out.append(doc)

    return out

def emit_hf_referrals_for_all_levels(common_data: Dict[str, Any], user_id: str,
                                     min_per_level: int = 1, max_per_level: int = 2) -> List[Dict[str, Any]]:
    """
    For the given boundary in common_data, create HF-referral docs at every boundary depth
    (country, province, district, administrativeProvince, locality, village) that exists.
    Reuses your generate_hf_referral() and ensures localityCode matches the slice.
    """
    levels = ["country", "province", "district", "administrativeProvince", "locality", "village"]
    out: List[Dict[str, Any]] = []

    base_h = common_data["boundaryHierarchy"]
    base_c = common_data["boundaryHierarchyCode"]

    for lvl in levels:
        if lvl not in base_h:
            continue

        h_slice, c_slice = boundary_slice(base_h, base_c, lvl)
        loc_code = most_specific_locality_code(c_slice, h_slice) or c_slice.get("country")

        cd = dict(common_data)
        cd["boundaryHierarchy"] = h_slice
        cd["boundaryHierarchyCode"] = c_slice
        cd["localityCode"] = loc_code

        for _ in range(random.randint(min_per_level, max_per_level)):
            # expects you have generate_hf_referral(...) implemented elsewhere
            doc = generate_hf_referral(cd, user_id)
            if doc:
                out.append(doc)

    return out



# ========================
# Shared data per main loop (boundary locked here)
# ========================
def getSharedData(user_id, loop_index):
    """
    Ensure ONE boundary per main loop (household + all children).
    Also rotate boundaries across loops for variety.
    """
    selected_boundary = boundary_data[loop_index % len(boundary_data)]
    timestamp = random_timestamp_range()
    return {
        "auditDetails": {
            "createdBy": user_id,
            "lastModifiedBy": user_id,
            "createdTime": timestamp,
            "lastModifiedTime": timestamp
        },
        "nameOfUser": random.choice(names),
        "userName": f"USR-{random.randint(1, 999999):06}",
        "boundaryHierarchy": selected_boundary["hierarchy"],
        "boundaryHierarchyCode": selected_boundary["codes"],
        "projectType": SETTINGS.CAMPAIGN.project_type,
        "projectTypeId": SETTINGS.CAMPAIGN.project_type_id,
        "projectId": str(uuid.uuid4()),
        "projectName": random.choice(project_names),
        "household_id": f"H-2025-07-29-{random.randint(100000, 999999)}",
        "householdClientRefId": str(uuid.uuid4())
    }

# ========================
# Generators (read campaign/geo from SETTINGS)
# ========================

# helper: weighted choice using counts
def _weighted_choice(weight_map: dict) -> str:
    keys = list(weight_map.keys())
    weights = list(weight_map.values())
    # random.choices is available in stdlib; fallback if needed
    return random.choices(keys, weights=weights, k=1)[0]

def generate_project_task(common_data, individual_client_ref_id, individual_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    latitude, longitude = pick_lat_lon_for_boundary(boundary)

    now = datetime.now(timezone.utc)
    ingestion_time = now.isoformat() + 'Z'
    timestamp_iso = now.isoformat().replace('+00:00', 'Z')
    last_modified_time = int(now.timestamp() * 1000)
    synced_time = last_modified_time - random.randint(100, 1000)
    synced_timestamp_iso = datetime.fromtimestamp(synced_time / 1000, timezone.utc).isoformat().replace('+00:00', 'Z')

    client_reference_id = str(uuid.uuid4())
    task_client_reference_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    product_names = ["Bednet - Grade-1", "SP - 500mg", "SP - 250mg", "AQ 500mg"]
    product_name = random.choice(product_names)

    user_names = ["USR-006230", "USR-006345", "USR-006362"]
    name_of_users = ["Lata", "SK1", "ICD User One"]

    product_variants = [
        "PVAR-2025-07-30-000139",
        f"PVAR-{now.strftime('%Y-%m-%d')}-{random.randint(100000, 999999):06d}"
    ]

    project_id = f"PT-{now.strftime('%Y-%m-%d')}-{random.randint(900000, 999999):06d}"

    # ---- distributions from your ES ----
    status_weights = {
        "ADMINISTRATION_SUCCESS":    829,
        "ADMINISTRATION_FAILED":     816,
        "CLOSED_HOUSEHOLD":          742,
        "BENEFICIARY_REFUSED":       266,
        "BENEFICIARY_REFERRED":       83,
        "BENEFICIARY_INELIGIBLE":     82,
        "NOT_ADMINISTERED":           70,
        "DELIVERED":                  29,
        "INELIGIBLE":                 15,
    }
    delivered_to_weights = {
        "HOUSEHOLD":  78503,
        "INDIVIDUAL": 77067
        # (Your data shows only these two; exclude OTHER)
    }

    # pick values according to weights
    chosen_status = _weighted_choice(status_weights)
    delivered_to = _weighted_choice(delivered_to_weights)

    additional_details_options = [
        {
            "houseStructureTypes": random.choice(["REEDS", "CLAY", "METAL", "GLASS", "CEMENT"]),
            "children": random.randint(0, 3),
            "latitude": str(latitude),
            "isVulnerable": True,
            "test_b9aa6f50056e": "test_dcfafb1be02f",
            "cycleIndex": "01",
            "noOfRooms": random.randint(1, 15),
            "pregnantWomen": random.randint(0, 1),
            "longitude": str(longitude)
        },
        {
            "memberCount": str(random.randint(1, 3)),
            "dateOfRegistration": f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{random.randint(100000, 999999)}",
            "cycleIndex": random.choice([None, "01"]),
            "pregnantWomenCount": str(random.randint(0, 2)),
            "administrativeArea": boundary.get("village", boundary.get("locality")),
            "childrenCount": str(random.randint(0, 4)),
            "gender": random.choice(["MALE", "FEMALE"])
        }
    ]
    additional_details = random.choice(additional_details_options)
    delivery_comments = random.choice(["SUCCESSFUL_DELIVERY", None])
    complex_id = f"{client_reference_id}{individual_client_ref_id}mz"

    return {
        "_index": PROJECT_TASK_INDEX,
        "_id": complex_id,
        "_source": {
            "ingestionTime": ingestion_time,
            "Data": {
                "boundaryHierarchy": boundary,
                "boundaryHierarchyCode": codes,
                "role": "DISTRIBUTOR",
                "lastModifiedTime": last_modified_time,
                "taskDates": random_date_str(),
                # keep both fields and keep them aligned
                "administrationStatus": chosen_status,
                "status": chosen_status,

                "syncedTime": synced_time,
                "latitude": latitude,
                "projectType": c.project_type,
                "individualId": None,
                "clientReferenceId": client_reference_id,
                "geoPoint": [longitude, latitude],
                "productName": product_name,
                "householdId": common_data["household_id"],
                "taskType": "DELIVERY",
                "syncedDate": random_date_str(),
                "taskClientReferenceId": task_client_reference_id,
                "createdTime": last_modified_time,
                "id": task_id,
                "syncedTimeStamp": synced_timestamp_iso,
                "longitude": longitude,
                "locationAccuracy": random.choice([65.67, None, round(random.uniform(10.0, 100.0), 2)]),
                "quantity": 1,
                "projectBeneficiaryClientReferenceId": individual_client_ref_id,
                "campaignId": c.campaign_id,
                "deliveredTo": delivered_to,
                "lastModifiedBy": str(uuid.uuid4()),
                "memberCount": random.randint(1, 3),
                "localityCode": most_specific_locality_code(codes, boundary),
                "dateOfBirth": None,
                "nameOfUser": random.choice(name_of_users),
                "userName": random.choice(user_names),
                "additionalDetails": additional_details,
                "userAddress": None,
                "isDelivered": random.choice([True, False]),
                "projectTypeId": c.project_type_id,
                "@timestamp": timestamp_iso,
                "productVariant": random.choice(product_variants),
                "createdBy": str(uuid.uuid4()),
                "tenantId": c.tenant_id,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": project_id,
                "taskId": project_id,
                "deliveryComments": delivery_comments
            }
        }
    }

def generate_household(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary_hierarchy = common_data["boundaryHierarchy"]
    boundary_codes = common_data["boundaryHierarchyCode"]

    for k in ["province", "district", "administrativeProvince", "locality"]:
        if k in boundary_hierarchy and k not in boundary_codes:
            return None

    now_utc = datetime.now(timezone.utc)
    ingestion_iso = now_utc.isoformat() + "Z"
    client_audit_ms = int(now_utc.timestamp() * 1000)
    audit_ms = client_audit_ms - 900000
    synced_date = now_utc.date().isoformat()
    task_date = (now_utc + timedelta(days=1)).date().isoformat()
    ts_iso = datetime.fromtimestamp(client_audit_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    es_id = str(uuid.uuid4())
    addr_id = str(uuid.uuid4())

    latitude, longitude = pick_lat_lon_for_boundary(boundary_hierarchy)
    locality_code_for_address = most_specific_locality_code(boundary_codes, boundary_hierarchy)

    project_id = common_data.get("projectId") or str(uuid.uuid4())
    project_name = common_data.get("projectName") or f"MR-DN_{now_utc.strftime('%B_%Y').lower()}"
    name_of_user = common_data.get("nameOfUser") or "ICD User One"
    user_name = common_data.get("userName") or "USR-006362"

    member_count_num = random.randint(2, 6)
    member_count_str = str(member_count_num)
    preg_count_str = str(random.randint(0, 2))
    child_count_str = str(random.randint(0, 3))
    administrative_area = boundary_hierarchy.get("locality") or boundary_hierarchy.get("district") or ""

    doc = {
        "_index": HOUSEHOLD_INDEX,
        "_id": es_id,
        "_source": {
            "ingestionTime": ingestion_iso,
            "Data": {
                "boundaryHierarchy": {k: boundary_hierarchy[k] for k in ["country","province","district","locality","administrativeProvince"] if k in boundary_hierarchy},
                "role": "DISTRIBUTOR",
                "taskDates": task_date,
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "nameOfUser": name_of_user,
                "userName": user_name,
                "boundaryHierarchyCode": {k: boundary_codes[k] for k in ["country","province","district","locality","administrativeProvince"] if k in boundary_codes},
                "geoPoint": [longitude, latitude],
                "additionalDetails": {
                    "memberCount": member_count_str,
                    "pregnantWomenCount": preg_count_str,
                    "cycleIndex": "01",
                    "administrativeArea": administrative_area,
                    "childrenCount": child_count_str
                },
                "userAddress": "",
                "projectTypeId": c.project_type_id,
                "syncedDate": synced_date,
                "@timestamp": ts_iso,
                "household": {
                    "clientAuditDetails": {
                        "lastModifiedTime": client_audit_ms,
                        "createdBy": user_id,
                        "lastModifiedBy": user_id,
                        "createdTime": client_audit_ms
                    },
                    "address": {
                        "locationAccuracy": round(random.uniform(3.0, 6.0), 10),
                        "pincode": "",
                        "city": "",
                        "latitude": latitude,
                        "locality": {
                            "code": locality_code_for_address or "",
                            "materializedPath": "",
                            "latitude": "",
                            "name": "",
                            "label": "",
                            "longitude": ""
                        },
                        "clientReferenceId": "",
                        "type": "PERMANENT",
                        "buildingName": "",
                        "street": "",
                        "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                        "addressLine1": "",
                        "addressLine2": "",
                        "id": addr_id,
                        "doorNo": "",
                        "landmark": "",
                        "longitude": longitude
                    },
                    "additionalFields": {
                        "schema": "Household",
                        "fields": [
                            {"value": child_count_str,  "key": "childrenCount"},
                            {"value": preg_count_str,   "key": "pregnantWomenCount"},
                            {"value": member_count_str, "key": "memberCount"},
                            {"value": administrative_area, "key": "administrativeArea"}
                        ],
                        "version": 1
                    },
                    "isDeleted": False,
                    "rowVersion": 1,
                    "memberCount": member_count_num,
                    "auditDetails": {
                        "lastModifiedTime": audit_ms,
                        "createdBy": user_id,
                        "lastModifiedBy": user_id,
                        "createdTime": audit_ms
                    },
                    "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                    "id": f"H-{synced_date}-{random.randint(100000,999999):06d}",
                    "clientReferenceId": es_id
                },
                "syncedTimeStamp": ingestion_iso,
                "projectName": project_name,
                "campaignNumber": c.campaign_number,
                "projectId": project_id
            }
        }
    }
    return doc

def generate_member(common_data, household_ref_id, individual_client_ref_id, individual_id, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    now = datetime.now(timezone.utc)
    ingestion_time = now.isoformat() + 'Z'
    timestamp_iso = now.isoformat().replace('+00:00', 'Z')
    client_id = str(uuid.uuid4())

    latitude, longitude = pick_lat_lon_for_boundary(boundary)

    dob = random_epoch(1980, 2020) * 1000
    current_timestamp = int(now.timestamp() * 1000)
    age_ms = current_timestamp - dob
    age_years = int(age_ms / (365.25 * 24 * 3600 * 1000))
    gender = random.choice(["MALE", "FEMALE", None]) if random.random() > 0.1 else None

    additional_details_options = [
        {
            "memberCount": str(random.randint(1, 6)),
            "pregnantWomenCount": str(random.randint(0, 2)),
            "cycleIndex": random.choice(["01", None]),
            "administrativeArea": boundary.get("village", boundary.get("locality")),
            "childrenCount": str(random.randint(0, 4))
        },
        {
            "houseStructureTypes": random.choice(["CEMENT", "MUD", "WOOD", "REEDS", "CLAY", "METAL"]),
            "children": str(random.randint(0, 5)),
            "latitude": str(latitude),
            "cycleIndex": "01",
            "noOfRooms": str(random.randint(1, 9)),
            "pregnantWomen": str(random.randint(0, 2)),
            "longitude": str(longitude)
        }
    ]
    additional_details = random.choice(additional_details_options)

    user_names = ["USR-006362", "USR-006187", "USR-006230"]
    name_of_users = ["ICD User One", "Lata"]

    return {
        "_index": MEMBER_INDEX,
        "_id": client_id,
        "_source": {
            "ingestionTime": ingestion_time,
            "Data": {
                "boundaryHierarchy": boundary,
                "boundaryHierarchyCode": codes,
                "role": "DISTRIBUTOR",
                "taskDates": random_date_str(),
                "gender": gender,
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "localityCode": most_specific_locality_code(codes, boundary),
                "dateOfBirth": dob if gender is not None else None,
                "nameOfUser": random.choice(name_of_users),
                "userName": random.choice(user_names),
                "geoPoint": [longitude, latitude],
                "additionalDetails": additional_details,
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "syncedDate": random_date_str(),
                "@timestamp": timestamp_iso,
                "householdMember": {
                    "individualClientReferenceId": individual_client_ref_id,
                    "additionalFields": None,
                    "rowVersion": random.randint(1, 2),
                    "individualId": individual_id,
                    "clientReferenceId": client_id,
                    "householdId": common_data["household_id"],
                    "clientAuditDetails": {
                        "lastModifiedTime": common_data["auditDetails"]["lastModifiedTime"],
                        "createdBy": user_id,
                        "lastModifiedBy": user_id,
                        "createdTime": common_data["auditDetails"]["createdTime"]
                    },
                    "isDeleted": False,
                    "auditDetails": {
                        "lastModifiedTime": common_data["auditDetails"]["lastModifiedTime"],
                        "createdBy": user_id,
                        "lastModifiedBy": user_id,
                        "createdTime": common_data["auditDetails"]["createdTime"]
                    },
                    "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                    "householdClientReferenceId": household_ref_id,
                    "id": client_id,
                    "isHeadOfHousehold": random.choice([True, False])
                },
                "syncedTimeStamp": timestamp_iso,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"],
                "age": age_years if gender is not None else None
            }
        }
    }

def generate_transformer_pgr_services(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    use_minimal_boundary = random.choice([True, False])
    if use_minimal_boundary:
        bh, bc = boundary_slice(boundary, codes, "country")
        locality_code = most_specific_locality_code(bc, bh)
    else:
        bh, bc = boundary, codes
        locality_code = most_specific_locality_code(bc, bh)

    now = datetime.now(timezone.utc)
    ingestion_time = now.isoformat() + 'Z'
    timestamp_ms = int(now.timestamp() * 1000)
    timestamp_iso = now.isoformat().replace('+00:00', 'Z')
    service_request_id = f"PGR-{now.strftime('%Y-%m-%d')}-{random.randint(1000, 9999):06d}"

    service_codes = ["PerformanceIssue", "SecurityIssues", "DataIssues"]
    service_code = random.choice(service_codes)
    application_status = random.choice(["PENDING_ASSIGNMENT", "RESOLVED"])
    description = random.choice(["One Time Evaluation", "dfdf", "Issue"])
    source = random.choice(["web", "mobile"])
    role = random.choice(["DISTRIBUTOR", "PROVINCIAL_SUPERVISOR"])

    user_data_options = [
        {
            "mobileNumber": "7892489611",
            "name": "Sudesh",
            "userName": "7892489611",
            "emailId": None,
            "id": random.randint(12000, 13000),
            "roles": [{"code": "DISTRIBUTOR", "name": "Distributor", "tenantId": SETTINGS.CAMPAIGN.tenant_id, "id": None}]
        },
        {
            "mobileNumber": "9977882643",
            "name": "Abhishek",
            "userName": "9977882643",
            "emailId": "raj@gmail.com",
            "id": random.randint(9000, 10000),
            "roles": [{"code": "CITIZEN", "name": "Citizen", "tenantId": SETTINGS.CAMPAIGN.tenant_id, "id": None}]
        },
        {
            "mobileNumber": "8689982982",
            "name": "HF Referral",
            "userName": "8689982982",
            "emailId": None,
            "id": random.randint(12000, 13000),
            "roles": [{"code": "CITIZEN", "name": "Citizen", "tenantId": SETTINGS.CAMPAIGN.tenant_id, "id": None}]
        }
    ]
    user_data = random.choice(user_data_options)
    name_of_users = ["Sudesh", "Abhishek", "Lata"]
    user_names = ["7892489611", "ProvSup-1", "USR-006054"]

    additional_detail_options = [
        f'{{"household":{{"id":"H-{now.strftime("%Y-%m-%d")}-{random.randint(10000, 99999):06d}","contactNo":"{user_data["mobileNumber"]}","image_1":"{str(uuid.uuid4())}"}}}}',
        f'{{"supervisorName":null,"supervisorContactNumber":null}}',
        f'{{"supervisorName":null,"supervisorContactNumber":null,"otherComplaintDescription":null}}'
    ]
    additional_detail = random.choice(additional_detail_options)

    project_data_options = [
        {
            "campaignId": None,
            "projectType": None,
            "projectTypeId": None,
            "projectName": None,
            "campaignNumber": None,
            "projectId": None
        },
        {
            "campaignId": c.campaign_id,
            "projectType": c.project_type,
            "projectTypeId": c.project_type_id,
            "projectName": common_data.get("projectName", "SMC Campaign"),
            "campaignNumber": c.campaign_number,
            "projectId": common_data.get("projectId")
        },
        {
            "campaignId": None,
            "projectType": "LLIN-mz",
            "projectTypeId": "b1107f0c-7a91-4c76-afc2-a279d8a7b76a",
            "projectName": "SMC Campaign",
            "campaignNumber": "f5F6xAskA05",
            "projectId": "da8c009a-1f75-43f0-8f0c-926d351f16ab"
        }
    ]
    project_data = random.choice(project_data_options)
    user_address = random.choice([None, "dg"])
    self_complaint = random.choice([None, False])

    service_id = str(uuid.uuid4())
    account_id = str(uuid.uuid4())
    user_uuid = str(uuid.uuid4())

    return {
        "_index": TRANSFORMER_PGR_SERVICES_INDEX,
        "_id": service_request_id,
        "_source": {
            "ingestionTime": ingestion_time,
            "Data": {
                "boundaryHierarchy": bh,
                "role": role,
                "taskDates": random_date_str(),
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "localityCode": locality_code,
                "nameOfUser": random.choice(name_of_users),
                "userName": random.choice(user_names),
                "boundaryHierarchyCode": bc,
                "userAddress": user_address,
                "projectTypeId": c.project_type_id,
                "@timestamp": timestamp_iso,
                "service": {
                    "serviceRequestId": service_request_id,
                    "address": None,
                    "serviceCode": service_code,
                    "rating": None,
                    "active": True,
                    "description": description,
                    "source": source,
                    "accountId": account_id,
                    "additionalDetail": additional_detail,
                    "applicationStatus": application_status,
                    "auditDetails": {
                        "lastModifiedTime": timestamp_ms,
                        "createdBy": user_uuid,
                        "lastModifiedBy": user_uuid,
                        "createdTime": timestamp_ms - random.randint(1000, 10000)
                    },
                    "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                    "id": service_id,
                    "user": {
                        "mobileNumber": user_data["mobileNumber"],
                        "roles": user_data["roles"],
                        "name": user_data["name"],
                        "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                        "active": True,
                        "emailId": user_data["emailId"],
                        "id": user_data["id"],
                        "userName": user_data["userName"],
                        "type": "EMPLOYEE",
                        "uuid": user_uuid
                    },
                    "selfComplaint": self_complaint
                },
                "projectName": project_data["projectName"],
                "campaignNumber": project_data["campaignNumber"],
                "projectId": project_data["projectId"]
            }
        }
    }

def generate_project(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat() + 'Z'
    project_id = str(uuid.uuid4())
    project_number = f"PJT-{now.strftime('%Y-%m-%d')}-{random.randint(100000, 999999)}"
    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)
    duration_days = random.randint(5, 8)
    start_date_dt = datetime.fromtimestamp(start_timestamp / 1000, tz=timezone.utc)
    task_dates = [(start_date_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(duration_days)]
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]
    created_time = int(time.time() * 1000)
    
    # Generate random distribution data for aggregation queries
    total_administered_resources = random.randint(50, 500)  # Added for ES aggregation field

    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    base_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    date_str = base_date.strftime('%Y-%m-%dT00:00:00.000Z')
    
    return {
        "_index": PROJECT_INDEX,
        "_id": project_id + SETTINGS.CAMPAIGN.tenant_id,
        "_source": {
            "ingestionTime": timestamp,
            "Data": {
                "boundaryHierarchy": boundary,
                "boundaryHierarchyCode": codes,
                "campaignDurationInDays": len(task_dates),
                "taskDates": task_dates,
                "startDate": start_timestamp,
                "endDate": end_timestamp,
                "projectType": c.project_type,
                "subProjectType": "IRS-mz",
                "productName": "Sumishield - 1litre,Fludora - 1litre,Delt - 1litre",
                "createdTime": created_time,
                "id": project_id,
                "projectId": project_id,
                "projectNumber": project_number,
                "campaignId": c.campaign_id,
                "campaignNumber": c.campaign_number,
                "referenceID": c.campaign_number,
                "targetPerDay": 25,
                "overallTarget": 250,
                "targetType": random.choice(["HOUSEHOLD", "INDIVIDUAL", "PRODUCT"]),
                "projectBeneficiaryType": random.choice(["HOUSEHOLD", "INDIVIDUAL"]),
                "productVariant": "PVAR-2025-01-09-000103,PVAR-2025-01-09-000104",
                "additionalDetails": {"doseIndex": ["01"], "cycleIndex": ["01"]},
                "localityCode": most_specific_locality_code(codes, boundary),
                "projectName": common_data.get("projectName", "IRS Sep"),
                "projectTypeId": c.project_type_id,
                "@timestamp": timestamp,
                "createdBy": user_id,
                "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                "date": date_str,  # Added: Extract date part (YYYY-MM-DD) for date_histogram aggregation
                "total_administered_resources": total_administered_resources  # Added: Field for sum aggregation in ES query
            }
        }
    }
def generate_population_coverage_summary(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    if not ("district" in boundary and "province" in boundary):
        return None

    total_admin = random.randint(5, 50)
    total_pop = random.randint(1, total_admin)
    total_male = random.randint(0, total_pop)
    total_female = total_pop - total_male
    refused = random.randint(0, 10)

    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    random_date_obj = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    date_str = random_date_obj.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    document = {
        "_index": POPULATION_COVERAGE_INDEX,
        "_id": str(uuid.uuid4()),
        "_score": None,
        "_source": {
            "total_administered_resources": total_admin,
            "refused": {"count": refused},
            "date": date_str,
            "total_population_refused": refused,
            "campaignId": c.campaign_id,
            "total_female_population_administered": total_female,
            "cycle": 1,
            "total_population_administered": total_pop,
            "dose": random.randint(1, 3),
            "province": boundary["province"],
            "total_male_population_administered": total_male,
            "district": boundary["district"],
            "administered_resources": {"total_quantity": total_admin},
            "administered": {
                "total_population_administered": total_pop,
                "male_population_administered": {"count": total_male},
                "female_population_administered": {"count": total_female},
            },
        },
        "sort": [-9223372036854776000],
    }
    return document

def generate_population_coverage_summary_datewise(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    if not boundary.get("province") or not boundary.get("district"):
        return None

    province = boundary["province"]
    district = boundary["district"]

    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    base_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    date_str = base_date.strftime('%Y-%m-%dT00:00:00.000Z')

    total_admin = random.randint(5, 50)
    total_pop = random.randint(1, total_admin)
    total_male = random.randint(0, total_pop)
    total_female = total_pop - total_male
    refused = random.randint(0, 10)

    return {
        "_index": POP_SUMMARY_DATEWISE_INDEX,
        "_id": str(uuid.uuid4()),
        "_score": None,
        "_source": {
            "date": date_str,
            "total_administered_resources": total_admin,
            "refused": {"count": refused},
            "total_population_refused": refused,
            "campaignId": c.campaign_id,
            "total_female_population_administered": total_female,
            "cycle": 1,
            "projectTypeId": c.project_type_id,
            "total_population_administered": total_pop,
            "dose": random.randint(1, 3),
            "province": province,
            "total_male_population_administered": total_male,
            "district": district,
            "administered_resources": {"total_quantity": total_admin},
            "administered": {
                "total_population_administered": total_pop,
                "female_population_administered": {"count": total_female},
                "male_population_administered": {"count": total_male},
            },
        },
        "sort": [-9223372036854776000],
    }

def generate_stock(common_data, stock_id=None, client_ref_id=None, facility_id=None, product_variant=None):
    c = SETTINGS.CAMPAIGN
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat() + 'Z'
    created_time = int(now.timestamp() * 1000)
    client_id = str(uuid.uuid4())

    boundary = common_data["boundaryHierarchy"]
    boundary_codes = common_data["boundaryHierarchyCode"]

    latitude, longitude = pick_lat_lon_for_boundary(boundary)

    return {
        "_index": STOCK_INDEX,
        "_id": stock_id if stock_id else f"{client_id}mz",
        "_score": None,
        "_source": {
            "ingestionTime": timestamp,
            "Data": {
                "boundaryHierarchy": boundary,
                "boundaryHierarchyCode": boundary_codes,
                "reason": "RECEIVED",
                "role": "WAREHOUSE_MANAGER",
                "lastModifiedTime": created_time,
                "taskDates": now.strftime("%Y-%m-%d"),
                "waybillNumber": f"WBL-{random.randint(10000, 99999)}",
                "transactingFacilityId": facility_id if facility_id else "F-2025-01-16-008408",
                "additionalFields": {
                    "schema": "Stock",
                    "fields": [
                        {"value": common_data["nameOfUser"], "key": "name"},
                        {"value": str(latitude), "key": "lat"},
                        {"value": str(longitude), "key": "lng"}
                    ],
                    "version": 1
                },
                "syncedTime": created_time,
                "projectType": c.project_type,
                "clientReferenceId": client_ref_id if client_ref_id else client_id,
                "productName": "SP - 250mg",
                "transactingFacilityLevel": None,
                "dateOfEntry": created_time,
                "transactingFacilityType": "WAREHOUSE",
                "syncedDate": now.strftime("%Y-%m-%d"),
                "createdTime": created_time,
                "id": stock_id if stock_id else f"S-{now.strftime('%Y-%m-%d')}-000{random.randint(600,700)}",
                "facilityName": "Bednet L5",
                "syncedTimeStamp": timestamp,
                "facilityLevel": None,
                "facilityTarget": None,
                "physicalCount": random.choice([30, 100, 150]),
                "facilityId": facility_id if facility_id else "F-2025-07-31-008941",
                "transactingFacilityName": "LLIN Facilities",
                "facilityType": "WAREHOUSE",
                "campaignId": c.campaign_id,
                "lastModifiedBy": common_data["auditDetails"]["lastModifiedBy"],
                "eventType": "RECEIVED",
                "nameOfUser": common_data["nameOfUser"],
                "userName": common_data["userName"],
                "additionalDetails": {
                    "lng": longitude,
                    "name": common_data["nameOfUser"],
                    "cycleIndex": "01",
                    "lat": latitude
                },
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "@timestamp": timestamp,
                "productVariant": product_variant if product_variant else f"PVAR-{now.strftime('%Y-%m-%d')}-000{random.randint(130,150)}",
                "createdBy": common_data["auditDetails"]["createdBy"],
                "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"]
            }
        },
        "sort": [created_time]
    }

def generate_service_task(common_data, user_id):
    c = SETTINGS.CAMPAIGN

    # ChecklistName distribution from your ES counts
    checklist_names = [
        "ELIGIBLITY_ASSESSMENT", "TRAINING_SUPERVISION", "MOBILIZER_FORM", "SOP",
        "TEAM_SUPERVISION", "EOD_CLEANING", "HEALTH_PROFESSIONAL", "PERFORMANCE_STANDARD",
        "STOCK_MANAGEMENT", "PAYMENT_DATA", "LLINMozambique2.REGISTRATION_BASIC.DISTRICT_SUPERVISOR",
        "LLINMozambique2.MATERIAL_RECEIVED.LOCAL_MONITOR", "ELIGIBILITY", "LLINMozambique2.MATERIAL_ISSUED.LOCAL_MONITOR",
        "HOUSEHOLD", "HF_RF_FEVER", "INDIVIDUAL", "LLINMozambique2.AS_MONITORING.DISTRICT_SUPERVISOR",
        "LLIN Mozambique.REGISTRATION_BASIC.DISTRICT_SUPERVISOR", "LLIN Mozambique.MATERIAL_RECEIVED.WAREHOUSE_MANAGER",
        "HF_RF_SICK", "WAREHOUSE_MANAGER_PERFORMANCE", "LLIN Mozambique.MATERIAL_ISSUED.WAREHOUSE_MANAGER",
        "LLINMozambique2.DISTRICT_MONITOR_TRAINING.DISTRICT_SUPERVISOR", "SPECIAL_SPRAYING", "MMI",
        "LLINMozambique2.REGISTRATION_BASIC.PROVINCIAL_SUPERVISOR", "TRAINING_MONITORING", "IEC",
        "LLINMozambique2.MATERIAL_ISSUED.LOGISTIC_FOCAL_POINT", "WAREHOUSE_MONITORING",
        "LLINMozambique2.REGISTRATION_BASIC.NATIONAL_SUPERVISOR", "ITV",
        "LLIN Mozambique.AS_MONITORING.DISTRICT_SUPERVISOR", "LLINMozambique2.DISTRICT_MONITOR_TRAINING.PROVINCIAL_SUPERVISOR",
        "LLINMozambique2.AS_MONITORING.PROVINCIAL_SUPERVISOR", "LLIN Mozambique.DISTRICT_MONITOR_TRAINING.DISTRICT_SUPERVISOR",
        "PCECAI", "HF_RF_DRUG_SE_CC", "LLIN Mozambique.LOCAL_MONITOR_TRAINING.DISTRICT_SUPERVISOR",
        "LLIN Mozambique.REGISTRATION_TEAM_TRAINING.DISTRICT_SUPERVISOR",
        "LLINMozambique2.AS_MONITORING.NATIONAL_SUPERVISOR",
        "LLINMozambique2.REGISTRATION_TEAM_TRAINING.DISTRICT_SUPERVISOR",
        "LLIN Mozambique.REGISTRATION_BASIC.PROVINCIAL_SUPERVISOR",
        "LLINMozambique2.LOCAL_MONITOR_TRAINING.DISTRICT_SUPERVISOR",
        "LLINMozambique2.LOCAL_MONITOR_TRAINING.NATIONAL_SUPERVISOR",
        "TEAM_FORMATION", "HF_RF_DRUG_SE_PC",
        "LLIN Mozambique.DISTRICT_MONITOR_TRAINING.NATIONAL_SUPERVISOR",
        "LLINMozambique2.REGISTRATION_TEAM_TRAINING.PROVINCIAL_SUPERVISOR"
    ]
    checklist_name = random.choice(checklist_names)

    # Roles from your ES counts
    roles = [
        "COMMUNITY_DISTRIBUTOR", "DISTRIBUTOR", "MOBILIZER", "TEAM_SUPERVISOR",
        "DISTRICT_SUPERVISOR", "HEALTH_FACILITY_SUPERVISOR", "COMMUNITY_SUPERVISOR",
        "PROVINCIAL_SUPERVISOR", "LOCAL_MONITOR", "SUPERVISOR",
        "NATIONAL_SUPERVISOR", "HEALTH_FACILITY_WORKER", "WAREHOUSE_MANAGER",
        "LOGISTIC_FOCAL_POINT"
    ]

    # Attribute value distribution from ES
    attribute_values = [
        "0", "1", "YES", "NO", "NOT_SELECTED", "true", "false", "POSITIVE", "NEGATIVE",
        "SHORTAGES", "MEDICAL_EQUIPMENT", "HOSPITALS", "PERSONAL_PROTECTIVE_EQUIPMENT_(PPE)",
        "CLINICS", "COMMUNITY_HEALTH_CENTERS", "QUALITY_COMPLAINTS", "PHARMACEUTICALS",
        "HEALTH_POST", "OPTION_1", "OTHER", "PRIMARY_HEALTH_CLINIC", "SUMISHIELD", "FLUDORA",
        "Malrial5"
    ]

    # Attribute codes pool (mixing household & eligibility codes + generic)
    attribute_codes = ["SN1", "SN2", "SN3", "SN4", "SN5", "SMC1", "SMC1.YES.SM1", "SMC2", "SMC3"]

    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)
    client_reference_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    # Generate attributes with random value permutations
    attributes = []
    for code in random.sample(attribute_codes, k=random.randint(3, 6)):
        val = random.choice(attribute_values)
        attributes.append({
            "attributeCode": code,
            "auditDetails": common_data["auditDetails"],
            "id": str(uuid.uuid4()),
            "additionalDetails": None,
            "value": {"value": val},
            "referenceId": task_id
        })

    # Boundary & geoPoint handling
    if checklist_name in ["HOUSEHOLD", "HF_RF_FEVER", "HF_RF_SICK", "INDIVIDUAL"]:
        geo_point = list(pick_lat_lon_for_boundary(common_data["boundaryHierarchy"]))[::-1]
        bh = common_data["boundaryHierarchy"]
        bc = common_data["boundaryHierarchyCode"]
    else:
        geo_point = None
        bh, bc = boundary_slice(
            common_data["boundaryHierarchy"],
            common_data["boundaryHierarchyCode"],
            "country"
        )

    return {
        "_index": SERVICE_TASK_INDEX,
        "_id": f"{client_reference_id}mz",
        "_source": {
            "ingestionTime": now.isoformat() + "Z",
            "Data": {
                "supervisorLevel": random.choice(roles),
                "boundaryHierarchy": bh,
                "role": random.choice(roles),
                "taskDates": random_date_str(),
                "syncedTime": timestamp,
                "projectType": c.project_type,
                "clientReferenceId": client_reference_id,
                "geoPoint": geo_point,
                "checklistName": checklist_name,
                "createdTime": timestamp,
                "id": task_id,
                "syncedTimeStamp": now.isoformat() + "Z",
                "campaignId": c.campaign_id,
                "serviceDefinitionId": random.choice([
                    "fe7cdbcf-5818-43a5-91ac-fc682c1255db",
                    "d8c4c518-36bb-432d-9e25-69bb94ec5a5f"
                ]),
                "nameOfUser": common_data["nameOfUser"],
                "userName": common_data["userName"],
                "boundaryHierarchyCode": bc,
                "additionalDetails": {"cycleIndex": "01"},
                "userId": user_id,
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "@timestamp": now.isoformat() + "Z",
                "createdBy": user_id,
                "tenantId": c.tenant_id,
                "attributes": attributes,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"]
            }
        }
    }

def generate_attendance_log(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)

    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    attendance_id = str(uuid.uuid4())
    register_id = str(uuid.uuid4())
    individual_id = str(uuid.uuid4())

    lat, lon = pick_lat_lon_for_boundary(boundary)

    attendance_type = random.choice(["ENTRY", "EXIT"])
    status = random.choice(["ACTIVE", "INACTIVE"])
    role = random.choice(["DISTRICT_SUPERVISOR", "TEAM_SUPERVISOR", "DISTRIBUTOR"])

    attendee_given_names = ["Distributor One", "Distributor Two", "Distributor Three", "Team Leader Alpha", "Supervisor Beta"]
    given_name = random.choice(attendee_given_names)

    user_names = ["heal-att-taker-1", "health-att-taker-4578", "att-supervisor-001"]
    attendance_user_names = ["heal-demo-1", "heal-demo-2", "heal-demo-3", "health-demo-4790", "health-demo-4791"]

    service_codes = ["heal-demo-test-12", "health-demo-test-001", "att-service-001"]
    register_numbers = [
        f"WR/2025-26/{datetime.now().strftime('%m/%d')}/{random.randint(600000, 699999)}",
        f"AR/2025-{random.randint(10, 30)}/{datetime.now().strftime('%m/%d')}/{random.randint(600000, 699999)}"
    ]

    attendance_time_offset = random.randint(1, 48) * 3600 * 1000
    attendance_time = timestamp + attendance_time_offset

    return {
        "_index": ATTENDANCE_LOG_INDEX,
        "_id": attendance_id,
        "_source": {
            "boundaryHierarchy": boundary,
            "role": role,
            "attendanceTime": datetime.fromtimestamp(attendance_time/1000, timezone.utc).isoformat().replace('+00:00', 'Z'),
            "campaignId": c.campaign_id,
            "givenName": None,
            "projectType": c.project_type,
            "userName": random.choice(user_names),
            "boundaryHierarchyCode": codes,
            "attendeeName": {"otherNames": None, "givenName": given_name, "familyName": None},
            "projectTypeId": c.project_type_id,
            "@timestamp": now.isoformat().replace('+00:00', 'Z'),
            "attendanceLog": {
                "registerId": register_id,
                "auditDetails": common_data["auditDetails"],
                "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                "id": attendance_id,
                "individualId": individual_id,
                "time": attendance_time,
                "userName": random.choice(attendance_user_names),
                "type": attendance_type,
                "additionalDetails": {
                    "boundaryCode": most_specific_locality_code(codes, boundary),
                    "latitude": lat,
                    "comment": random.choice(["attendance taken", "marked", "registered", "logged entry", "confirmed attendance"]),
                    "longitude": lon
                },
                "status": status,
                "documentIds": []
            },
            "ingestionTime": now.isoformat() + "Z",
            "familyName": None,
            "registerServiceCode": random.choice(service_codes),
            "registerNumber": random.choice(register_numbers),
            "projectName": common_data["projectName"],
            "campaignNumber": c.campaign_number,
            "projectId": common_data["projectId"],
            "registerName": "Demo Health New Training Attendnace"
        }
    }

def generate_project_staff(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)

    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    staff_id = f"PTS-{now.strftime('%Y-%m-%d')}-{random.randint(100000, 999999):06d}"
    roles = ["DISTRICT_SUPERVISOR", "PROVINCIAL_SUPERVISOR", "NATIONAL_SUPERVISOR", "WAREHOUSE_MANAGER", "TEAM_SUPERVISOR"]
    role = random.choice(roles)

    staff_names = ["Margaret Barrett", "Erick Jarvis", "Lata", "John Smith", "Sarah Wilson", "Ahmed Hassan", "Maria Santos"]
    staff_usernames = ["ASS-DSL-1", "ASS-DSL-2", "ASS-DSL-3", "USR-006214", "USR-006360", "USR-006359", "USR-006358"]
    name_of_user = random.choice(staff_names)
    user_name = random.choice(staff_usernames)

    base_date = datetime.now()
    if role == "NATIONAL_SUPERVISOR":
        duration_days = random.randint(60, 120)
        bh, bc = boundary_slice(boundary, codes, "country")
    elif role == "PROVINCIAL_SUPERVISOR":
        duration_days = random.randint(20, 40)
        bh, bc = boundary_slice(boundary, codes, "province")
    else:
        duration_days = random.randint(10, 25)
        bh, bc = boundary_slice(boundary, codes, "locality" if "locality" in boundary else ("district" if "district" in boundary else "province"))

    task_dates = [(base_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(duration_days)]
    locality_code = most_specific_locality_code(bc, bh)

    created_by_options = ["Bednet", "3496b299-06f3-4dd2-8340-573cf8c4f1fc", user_id]
    created_by = random.choice(created_by_options)

    return {
        "_index": PROJECT_STAFF_INDEX,
        "_id": staff_id,
        "_source": {
            "ingestionTime": now.isoformat() + "Z",
            "Data": {
                "boundaryHierarchy": bh,
                "role": role,
                "taskDates": task_dates,
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "localityCode": locality_code,
                "nameOfUser": name_of_user,
                "userName": user_name,
                "boundaryHierarchyCode": bc,
                "additionalDetails": {"doseIndex": ["01"], "cycleIndex": ["01"]},
                "userId": str(uuid.uuid4()),
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "@timestamp": now.isoformat().replace('+00:00', 'Z'),
                "isDeleted": False,
                "createdBy": created_by,
                "tenantId": None,
                "createdTime": timestamp,
                "id": staff_id,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"]
            }
        }
    }

def generate_household_coverage_daily_iccd(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    if "province" not in boundary:
        return None
    province = boundary["province"]

    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    random_date_obj = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    date_str = random_date_obj.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    return {
        "_index": HOUSEHOLD_COVERAGE_DAILY_ICCD_INDEX,
        "_id": str(uuid.uuid4()),
        "_source": {
            "date": date_str,
            "province": province,
            "campaignId": c.campaign_id,
            "cycle": "01",
            "total_households_visited": random.randint(1, 25)
        }
    }

def generate_household_coverage_summary_iccd(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    if "province" not in boundary:
        return None
    province = boundary["province"]

    return {
        "_index": HOUSEHOLD_COVERAGE_SUMMARY_ICCD_INDEX,
        "_id": str(uuid.uuid4()),
        "_source": {
            "province": province,
            "campaignId": c.campaign_id,
            "cycle": "01",
            "total_households_visited": random.randint(1, 8)
        }
    }

def upload_bulk_to_es(file_path, es_url, index_name, chunk_size=None, max_chunk_retries=None, retry_delay=None):
    # Use centralized ES settings with overridable args
    chunk_size = chunk_size or SETTINGS.ES.bulk_chunk_lines
    max_chunk_retries = max_chunk_retries or SETTINGS.ES.bulk_chunk_retries
    retry_delay = retry_delay or SETTINGS.ES.bulk_retry_delay

    logger.info(f"➡️ Uploading data from {file_path} to index '{index_name}' in chunks of {chunk_size} lines.")

    def _upload_chunk(chunk_lines):
        headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Basic {SETTINGS.ES.basic_auth_b64}"
        }
        for attempt in range(1, max_chunk_retries + 1):
            try:
                response = requests.post(
                    url=f"{es_url}{index_name}/_bulk",
                    headers=headers,
                    data=''.join(chunk_lines),
                    verify=SETTINGS.ES.verify_ssl
                )
                if response.status_code == 200:
                    logger.info(f"Chunk upload successful (attempt {attempt}).")
                    return True
                else:
                    logger.warning(f"Chunk upload failed (attempt {attempt}). Status: {response.status_code}. Retrying in {retry_delay}s…")
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception during chunk upload (attempt {attempt}): {e}")
                time.sleep(retry_delay)
        return False

    try:
        with open(file_path, 'r') as f:
            chunk = []
            for line in f:
                chunk.append(line)
                if len(chunk) == chunk_size:
                    _upload_chunk(chunk)
                    chunk = []
            if chunk:
                _upload_chunk(chunk)

        logger.info(f"Finished uploading all chunks for index '{index_name}'.")
    except Exception as e:
        logger.exception(f"Exception during upload to index '{index_name}': {e}")

def generate_ineligible_summary(common_data, user_id):
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    if not boundary.get("province") or not boundary.get("district"):
        return None

    province = boundary["province"]
    district = boundary["district"]

    start_date = datetime(2025, 9, 20)
    end_date = datetime(2025, 9, 25)
    base_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    date_str = base_date.strftime('%Y-%m-%dT00:00:00.000Z')

    total_registered = random.randint(1, 31)
    ineligible_count = random.randint(0, max(1, total_registered // 4))

    return {
        "_index": INELIGIBLE_SUMMARY_INDEX,
        "_id": str(uuid.uuid4()),
        "_source": {
            "date": date_str,
            "ineligible_count": {"count": ineligible_count},
            "province": province,
            "total_population_registered": total_registered,
            "campaignId": c.campaign_id,
            "district": district,
            "cycle": "01",
            "ineligible_population_total": ineligible_count
        }
    }

def generate_user_sync(common_data, user_id):
    """
    Produce a user-sync-index-v1 document matching demo shape.
    Reads campaign & geo from SETTINGS.
    """
    c = SETTINGS.CAMPAIGN
    now = datetime.now(timezone.utc)
    ingestion_time = now.isoformat() + "Z"
    ts_ms = int(now.timestamp() * 1000)

    boundary = common_data["boundaryHierarchy"]

    proj_type_choice = random.choice([common_data.get("projectType", c.project_type), "Bednet"])
    proj_type_id = common_data.get("projectTypeId", c.project_type_id)
    if proj_type_choice == "Bednet":
        proj_type_id = "Bednet"

    latitude, longitude = pick_lat_lon_for_boundary(boundary)

    addl_details_options = [
        {
            "houseStructureTypes": random.choice(["REEDS", "CLAY", "METAL", "GLASS", "CEMENT"]),
            "children": random.randint(0, 3),
            "latitude": str(latitude),
            "isVulnerable": True,
            "test_b9aa6f50056e": "test_dcfafb1be02f",
            "cycleIndex": "01",
            "noOfRooms": random.randint(1, 15),
            "pregnantWomen": random.randint(0, 1),
            "longitude": str(longitude)
        },
        {
            "memberCount": str(random.randint(1, 3)),
            "dateOfRegistration": f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{random.randint(100000, 999999)}",
            "cycleIndex": random.choice([None, "01"]),
            "pregnantWomenCount": str(random.randint(0, 1)),
            "administrativeArea": boundary.get("village") or boundary.get("locality"),
            "childrenCount": str(random.randint(0, 3))
        }
    ]
    addl_details = random.choice(addl_details_options)

    doc_id = str(uuid.uuid4())
    synced_user_id = str(uuid.uuid4())

    return {
        "_index": USER_SYNC_INDEX,
        "_id": doc_id,
        "_score": None,
        "_source": {
            "ingestionTime": ingestion_time,
            "Data": {
                "boundaryHierarchy": {**({k: v for k, v in boundary.items() if v is not None})},
                "syncedUserId": synced_user_id,
                "role": "DISTRIBUTOR",
                "taskDates": random_date_str(),
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "additionalDetails": addl_details,
                "syncedUserName": common_data["userName"],
                "userAddress": None,
                "clientCreatedTime": ts_ms,
                "projectTypeId": c.project_type_id,
                "@timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "createdTime": ts_ms,
                "syncedTimeStamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"]
            }
        },
        "sort": [ts_ms]
    }


def generate_referral(common_data, user_id, individual_id):
    """
    Generate a referral-index-v1 document aligned to the sample you shared.
    - Uses campaign + tenant from SETTINGS
    - Mirrors nested 'referral' object (clientAuditDetails/auditDetails, reasons, ids, etc.)
    - Computes age in *months* from dateOfBirth (like your sample)
    """
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    ts_iso = now.isoformat().replace("+00:00", "Z")

    # random DOB (years 1980..2024), then derive age in months
    dob_ms = random_epoch(1980, 2024) * 1000
    age_months = int((now_ms - dob_ms) / (30.44 * 24 * 3600 * 1000))  # ~months

    # randoms to match sample feel
    gender = random.choice(["MALE", "FEMALE"])
    reasons_pool = ["fever", "vomiting", "convulsions", "rash", "weakness", "loss_of_appetite"]
    reasons = [random.choice(reasons_pool)]
    facility_name = "Bednet L5"
    recipient_facility_id = "F-2025-07-31-008941"  # keep stable for demo parity

    # ids
    es_id = str(uuid.uuid4())
    client_reference_id = str(uuid.uuid4())
    proj_beneficiary_id = f"PTB-{now.strftime('%Y-%m-%d')}-{random.randint(39130, 39999):06d}"
    proj_beneficiary_client_ref_id = str(uuid.uuid4())

    # times (mirror your sample’s audit/clientAudit usage)
    client_audit = {
        "lastModifiedTime": now_ms,
        "createdBy": user_id,
        "lastModifiedBy": user_id,
        "createdTime": now_ms,
    }
    audit_details = {
        "lastModifiedTime": now_ms + random.randint(100, 3000),
        "createdBy": user_id,
        "lastModifiedBy": user_id,
        "createdTime": now_ms + random.randint(100, 3000),
    }

    return {
        "_index": REFERRAL_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": {
            "ingestionTime": (now.isoformat() + "Z"),
            "Data": {
                "boundaryHierarchy": boundary,
                "role": "DISTRIBUTOR",
                "taskDates": random_date_str(),                # e.g., "2025-09-21"
                "gender": gender,
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "dateOfBirth": dob_ms,
                "nameOfUser": common_data["nameOfUser"],
                "individualId": individual_id,
                "userName": common_data["userName"],
                "boundaryHierarchyCode": codes,
                "additionalDetails": {"cycleIndex": "01"},
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "referral": {
                    "reasons": reasons,
                    "rowVersion": 1,
                    "additionalFields": None,
                    "projectBeneficiaryClientReferenceId": proj_beneficiary_client_ref_id,
                    "referrerId": None,
                    "clientReferenceId": client_reference_id,
                    "sideEffect": None,
                    "projectBeneficiaryId": proj_beneficiary_id,
                    "clientAuditDetails": client_audit,
                    "recipientType": "FACILITY",
                    "isDeleted": False,
                    "auditDetails": audit_details,
                    "tenantId": c.tenant_id,
                    "recipientId": recipient_facility_id,
                    "id": es_id,  # make nested id match ES _id (like your sample)
                },
                "syncedDate": random_date_str(),
                "@timestamp": ts_iso,
                "tenantId": c.tenant_id,
                "facilityName": facility_name,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"],
                "age": age_months,
            },
        },
        "sort": [audit_details["lastModifiedTime"]],
    }
    
def generate_side_effect(common_data, user_id, individual_id, task_id=None, task_client_reference_id=None):
    """
    Emit a side-effect-index-v1 document matching the sample shape.
    - Reads campaign/tenant/geo from SETTINGS and common_data
    - Adds both a flat 'symptoms' string and nested sideEffect.symptoms [array]
    - Computes age in months from DOB
    - If you pass a project task's ids, they are reused (else random)
    """
    c = SETTINGS.CAMPAIGN
    boundary = common_data["boundaryHierarchy"]
    codes = common_data["boundaryHierarchyCode"]

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    ts_iso = now.isoformat().replace("+00:00", "Z")

    # Gender + DOB -> age in months (like your sample)
    gender = random.choice(["MALE", "FEMALE"])
    dob_ms = random_epoch(1975, 2025) * 1000  # broad range like samples
    age_months = int((now_ms - dob_ms) / (30.44 * 24 * 3600 * 1000))

    # Symptom vocabulary exactly as in your sample set (including the 'VOMITTING' spelling)
    symptoms_vocab = ["ABDOMINAL_PAIN", "FEVER", "DIARRHOEA", "WEAKNESS", "VOMITTING", "SKIN_REACTION"]
    symptom = random.choice(symptoms_vocab)

    # IDs
    es_id = str(uuid.uuid4())
    project_beneficiary_id = f"PTB-{now.strftime('%Y-%m-%d')}-{random.randint(40000, 40999):06d}"
    project_beneficiary_client_ref_id = str(uuid.uuid4())
    client_reference_id = str(uuid.uuid4())

    # Optionally bind to a real project task (preferred) else synthesize
    task_id = task_id or f"PT-{now.strftime('%Y-%m-%d')}-{random.randint(90000, 99999):06d}"
    task_client_reference_id = task_client_reference_id or str(uuid.uuid4())

    client_audit = {
        "lastModifiedTime": now_ms,
        "createdBy": user_id,
        "lastModifiedBy": user_id,
        "createdTime": now_ms
    }
    audit_details = {
        "lastModifiedTime": now_ms + random.randint(100, 3000),
        "createdBy": user_id,
        "lastModifiedBy": user_id,
        "createdTime": now_ms + random.randint(100, 3000)
    }

    locality_code = most_specific_locality_code(codes, boundary)

    return {
        "_index": SIDE_EFFECT_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": {
            "ingestionTime": now.isoformat() + "Z",
            "Data": {
                "boundaryHierarchy": boundary,
                "role": "DISTRIBUTOR",
                "taskDates": random_date_str(),
                "gender": gender,
                "campaignId": c.campaign_id,
                "projectType": c.project_type,
                "localityCode": locality_code,
                "dateOfBirth": dob_ms,
                "individualId": individual_id,
                "nameOfUser": common_data["nameOfUser"],
                "boundaryHierarchyCode": codes,
                "userName": common_data["userName"],
                "additionalDetails": {},
                "sideEffect": {
                    "rowVersion": 1,
                    "additionalFields": None,
                    "projectBeneficiaryClientReferenceId": project_beneficiary_client_ref_id,
                    "clientReferenceId": client_reference_id,
                    "projectBeneficiaryId": project_beneficiary_id,
                    "symptoms": [symptom],
                    "clientAuditDetails": client_audit,
                    "isDeleted": False,
                    "taskClientReferenceId": task_client_reference_id,
                    "auditDetails": audit_details,
                    "tenantId": c.tenant_id,
                    "id": es_id,             # nested id mirrors ES _id (like your sample)
                    "taskId": task_id
                },
                "symptoms": symptom,         # flat field also present in your sample docs
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "syncedDate": random_date_str(),
                "@timestamp": ts_iso,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": common_data["projectId"],
                "age": age_months
            }
        },
        "sort": [audit_details["lastModifiedTime"]]
    }

def build_census_additional_fields(total_pop, tgt_3to11, tgt_12to59, include_latlong_opts=False):
    """
    Matches your sample keys and ordering.
    Set include_latlong_opts=True to include the hidden LAT/LONG option fields (appear in some docs).
    """
    fields = []
    order = 1

    # Uploaded vs Confirmed total population
    fields.append({
        "editable": False,
        "id": make_id(),
        "showOnUi": True,
        "value": total_pop,
        "key": "UPLOADED_HCM_ADMIN_CONSOLE_TOTAL_POPULATION",
        "order": order
    }); order += 1

    fields.append({
        "editable": True,
        "id": make_id(),
        "showOnUi": True,
        "value": total_pop,
        "key": "CONFIRMED_HCM_ADMIN_CONSOLE_TOTAL_POPULATION",
        "order": order
    }); order += 1

    # Target 3–11
    fields.append({
        "editable": False,
        "id": make_id(),
        "showOnUi": True,
        "value": tgt_3to11,
        "key": "UPLOADED_HCM_ADMIN_CONSOLE_TARGET_POPULATION_AGE_3TO11",
        "order": order
    }); order += 1

    fields.append({
        "editable": True,
        "id": make_id(),
        "showOnUi": True,
        "value": tgt_3to11,
        "key": "CONFIRMED_HCM_ADMIN_CONSOLE_TARGET_POPULATION_AGE_3TO11",
        "order": order
    }); order += 1

    # Target 12–59
    fields.append({
        "editable": False,
        "id": make_id(),
        "showOnUi": True,
        "value": tgt_12to59,
        "key": "UPLOADED_HCM_ADMIN_CONSOLE_TARGET_POPULATION_AGE_12TO59",
        "order": order
    }); order += 1

    fields.append({
        "editable": True,
        "id": make_id(),
        "showOnUi": True,
        "value": tgt_12to59,
        "key": "CONFIRMED_HCM_ADMIN_CONSOLE_TARGET_POPULATION_AGE_12TO59",
        "order": order
    }); order += 1

    # Optional hidden lat/long option fields (appear in some docs)
    if include_latlong_opts:
        fields.append({
            "editable": False,
            "id": make_id(),
            "showOnUi": False,
            "value": 10,
            "key": "HCM_ADMIN_CONSOLE_TARGET_LAT_OPT",
            "order": order
        }); order += 1

        fields.append({
            "editable": False,
            "id": make_id(),
            "showOnUi": False,
            "value": 10,
            "key": "HCM_ADMIN_CONSOLE_TARGET_LONG_OPT",
            "order": order
        }); order += 1

    return fields



def build_boundary_ancestral_path_from_codes(codes):
    # Build the pipe-separated path exactly like samples
    parts = []
    for k in ("country", "province", "district", "administrativeProvince", "locality", "village"):
        v = codes.get(k)
        if v:
            parts.append(v)
    return "|".join(parts) if parts else None

def generate_census(common_data, *, boundary_code=None, with_hidden_latlong=False, with_accessibility=False,
                    with_security=False, force_mapping=True):
    """
    Emit a census-index-v1 document matching provided samples.
    - boundary_code: if None, we use the most specific code from common_data["boundaryHierarchyCode"]
    - with_hidden_latlong: include HCM_ADMIN_CONSOLE_TARGET_LAT_OPT / LONG_OPT fields (some docs have these)
    - with_accessibility / with_security: include the nested accessibilityDetails / securityDetails blocks like samples
    - force_mapping: True -> include jurisdictionMapping; False -> include boundaryAncestralPath
    """
    c = SETTINGS.CAMPAIGN

    # Decide population numbers similar to your examples
    total_population = random.choice([100, 200, 300, 500])
    # Split into targets (keep them <= total for realism)
    tgt_3to11   = random.choice([30, 40, 50, 60]) if total_population >= 60 else min(40, max(30, total_population // 2))
    tgt_12to59  = random.choice([30, 40, 50])     if total_population >= 50 else min(50, total_population // 2)

    add_fields = build_census_additional_fields(total_population, tgt_3to11, tgt_12to59, include_latlong_opts=with_hidden_latlong)

    # Jurisdiction info from your common_data
    codes = common_data.get("boundaryHierarchyCode", {})
    mapping = build_jurisdiction_mapping_from_codes(codes)
    ancestral = build_boundary_ancestral_path_from_codes(codes)

    # Facility bits from your samples (you can also draw from a facility list you maintain)
    facility_id = random.choice([
        "F-2025-07-31-008943", "F-2025-02-11-008917", "F-2025-05-20-009001"
    ])
    facility_name = random.choice(["Bednet L7", "AS Pemba", "Primary HC West"])

    additional_details = {
        "facilityId": facility_id,
        "facilityName": facility_name
    }

    # Some docs include latitude/longitude at this level as well
    if with_accessibility or with_hidden_latlong or random.random() < 0.4:
        lat, lon = pick_lat_lon_for_boundary(common_data.get("boundaryHierarchy", {}))
        additional_details["latitude"] = lat
        additional_details["longitude"] = lon

    if with_accessibility:
        additional_details["accessibilityDetails"] = {
            "roadCondition": random.choice([
                {"code": "HCM_MICROPLAN_NO_ROAD", "name": "NO_ROAD", "active": True},
                {"code": "HCM_MICROPLAN_DIRT",    "name": "DIRT",    "active": True},
                {"code": "HCM_MICROPLAN_GRAVEL",  "name": "GRAVEL",  "active": True},
            ]),
            "terrain": random.choice([
                {"code": "HCM_MICROPLAN_PLAIN",   "name": "PLAIN",   "active": True},
                {"code": "HCM_MICROPLAN_DESERT",  "name": "DESERT",  "active": True},
                {"code": "HCM_MICROPLAN_FOREST",  "name": "FOREST",  "active": True},
            ])
        }

    if with_security:
        additional_details["securityDetails"] = {
            "1": {"code": random.choice(["ALL_THE_TIME","OFTEN"]), "value": random.choice(["ALL_THE_TIME","OFTEN"])},
            "2": {"code": random.choice(["EVERYDAY1","OFTEN1"]),   "value": random.choice(["EVERYDAY1","OFTEN1"])},
        }

    # Times & audit
    created_dt = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 60*24))
    created_ms = ms(created_dt)
    last_mod_ms = created_ms + random.randint(10000, 300000)

    created_by = random.choice([
        "b017d9f0-3ced-4541-86cf-91e9584499e1",
        "fa33f72b-6cde-487e-a594-d719b90da21b"
    ])
    last_mod_by = random.choice([
        "a5e20b10-31a0-4b87-a626-94efaec9df2b",
        created_by
    ])

    # ID & boundary code
    es_id = make_id()
    boundary_code = boundary_code or common_data.get("localityCode") or common_data.get("boundaryCode") \
                    or codes.get("village") or codes.get("locality") or codes.get("district")

    # effective ranges (some docs show effectiveTo=0 => active; others roll forward)
    # We'll randomly choose active vs superseded to mimic your mix.
    is_active = random.random() < 0.7
    effective_to = 0 if is_active else created_ms + random.randint(60_000, 2_000_000)

    data_obj = {
        "facilityAssigned": True,
        "hierarchyType": "MICROPLAN",
        "totalPopulation": total_population,
        "additionalFields": add_fields,
        "workflow": None,
        "jurisdictionMapping": mapping if force_mapping else None,
        "populationByDemographics": None,
        # Samples sometimes include boundaryAncestralPath when mapping is null
        "boundaryAncestralPath": None if force_mapping else ancestral,
        "source": random.choice([
            "79ee9f34-7357-43de-8714-00c82bf5b70b",
            "a7fb45f6-c643-47f9-86a7-d3933e0163fc"
        ]),
        "type": "people",
        "additionalDetails": additional_details,
        "effectiveTo": effective_to,
        "boundaryCode": boundary_code,
        "@timestamp": iso_z(created_dt),
        "auditDetails": {
            "lastModifiedTime": last_mod_ms,
            "createdBy": created_by,
            "lastModifiedBy": last_mod_by,
            "createdTime": created_ms
        },
        "tenantId": c.tenant_id,
        "id": es_id,
        "assignee": None,
        "effectiveFrom": created_ms,
        "status": "VALIDATED"
    }

    src = {
        "ingestionTime": iso_z(datetime.now(timezone.utc)),
        "Data": data_obj
    }

    return {
        "_index": CENSUS_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": src,
        "sort": [data_obj["effectiveFrom"]]
    }

def generate_plan(common_data, *, campaign_id=None, plan_config_id=None, locality_code=None, add_comment=False):
    """
    Create one plan-index-v1 document that mirrors your demo structure.
    common_data: expected to carry boundaryHierarchyCode (for mapping), and optionally facility hints.
    """
    codes = common_data.get("boundaryHierarchyCode", {})
    mapping = build_jurisdiction_mapping_from_codes(codes)

    # Choose locality from the most specific code unless given
    locality = locality_code or codes.get("village") or codes.get("locality")

    # Facility and IDs
    facility_id = random.choice(["F-2025-02-11-008917", "F-2025-07-31-008943"])
    campaign_id = campaign_id or random.choice([
        "ae8a7a19-ab74-4cf6-9db1-4cfbeb6583d8",
        "17d06b2a-857f-4fab-aac1-bc4b2001c665",
    ])
    plan_config_id = plan_config_id or random.choice([
        "a7fb45f6-c643-47f9-86a7-d3933e0163fc",
        "79ee9f34-7357-43de-8714-00c82bf5b70b"
    ])

    # Times & audit
    created_dt = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 60*24))
    created_ms = ms(created_dt)
    last_mod_ms = created_ms + random.randint(10_000, 300_000)
    user_id = random.choice([
        "a5e20b10-31a0-4b87-a626-94efaec9df2b",
        "fa33f72b-6cde-487e-a594-d719b90da21b"
    ])

    es_id = make_id()

    workflow = {
        "comments": "validate" if add_comment else None,
        "documents": None,
        "rating": None,
        "action": "VALIDATE",
        "assignes": None
    }

    data_obj = {
        "additionalFields": None,
        "workflow": workflow,
        "jurisdictionMapping": mapping,
        "campaignId": campaign_id,
        "locality": locality,
        "resources": build_plan_resources(),
        "additionalDetails": {
            "facilityId": facility_id
        },
        "targets": [],
        "@timestamp": iso_z(created_dt),
        "planConfigurationId": plan_config_id,
        "activities": [],
        "auditDetails": {
            "lastModifiedTime": last_mod_ms,
            "createdBy": user_id,
            "lastModifiedBy": user_id,
            "createdTime": created_ms
        },
        "tenantId": SETTINGS.CAMPAIGN.tenant_id,   # reuse your existing SETTINGS object
        "id": es_id,
        "assignee": [],
        "status": "VALIDATED"
    }

    src = {
        "ingestionTime": iso_z(datetime.now(timezone.utc)),
        "Data": data_obj
    }

    return {
        "_index": PLAN_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": src,
        "sort": [data_obj["auditDetails"]["createdTime"]]
    }

def emit_plans_for_locality(common_data):
    # 1–3 plan docs per locality
    for k in range(random.randint(1, 3)):
        doc = generate_plan(
            common_data,
            campaign_id=None,        # or pass a real ID
            plan_config_id=None,     # or pass a real ID
            locality_code=None,      # or set explicitly if you want
            add_comment=(k == 0 and random.random() < 0.3),
        )
        plan_docs.append(doc)

def build_hf_referral_additional_fields(*, coordinator_name: str, date_of_eval_ms: int,
                                        name_of_referral: str, age_years: int, gender: str,
                                        cycle: str = "1"):
    """
    Matches the sample ordering and keys for HFReferral.additionalFields.fields.
    Keeps values as strings like in your sample.
    """
    return {
        "schema": "HFReferral",
        "fields": [
            {"value": coordinator_name,         "key": "hfCoordinator"},
            {"value": str(date_of_eval_ms),     "key": "dateOfEvaluation"},
            {"value": name_of_referral,         "key": "nameOfReferral"},
            {"value": str(age_years),           "key": "age"},
            {"value": gender,                   "key": "gender"},
            {"value": cycle,                    "key": "cycle"},
        ],
        "version": 1
    }


def generate_hf_referral(common_data, user_id, collapse_to_country: bool = False):
    """
    Emit an hf-referral-index-v1 document.
    - If collapse_to_country=True, mimics your original 'country-only' behavior.
    - Otherwise, uses boundaryHierarchy / boundaryHierarchyCode from common_data (including localityCode if provided).
    """
    c = SETTINGS.CAMPAIGN

    # Inputs coming from emit_hf_referrals_for_all_levels(...)
    boundary = (common_data.get("boundaryHierarchy") or {}).copy()
    codes    = (common_data.get("boundaryHierarchyCode") or {}).copy()

    # Determine the boundary/code to store
    if collapse_to_country:
        # old behavior
        bh = {"country": boundary.get("country") or SETTINGS.GEO.DEFAULT_COUNTRY}
        bhc = {"country": codes.get("country") or "MICROPLAN_MO"}
    else:
        # keep the slice you passed in (country→…→village)
        bh = boundary or {"country": SETTINGS.GEO.DEFAULT_COUNTRY}
        # keep codes as-is; if none, fall back to a minimal country code
        bhc = codes or {"country": "MICROPLAN_MO"}

    # localityCode: prefer what the caller set; else compute from the slice
    loc_code = common_data.get("localityCode")
    if not loc_code:
        loc_code = most_specific_locality_code(bhc, bh) or bhc.get("country") or "MICROPLAN_MO"

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    ts_iso = now.isoformat().replace("+00:00", "Z")

    coordinator = random.choice(["User with all roles", "HF Supervisor", "HF Coordinator"])
    referral_name = random.choice(["Kanishq", "Asha", "Samuel", "Mary"])
    gender = random.choice(["MALE", "FEMALE"])
    age_years = random.randint(1, 80)
    symptom = random.choice(["fever", "vomiting", "rash", "weakness"])

    es_id = make_id()
    client_ref_id = make_id()

    client_time_ms = now_ms
    audit_time_ms  = now_ms + random.randint(150, 2500)

    additional_fields = build_hf_referral_additional_fields(
        coordinator_name=coordinator,
        date_of_eval_ms=now_ms,
        name_of_referral=referral_name,
        age_years=age_years,
        gender=gender,
        cycle="1"
    )

    user_name = common_data.get("userName") or "USR-006057"
    project_id = common_data.get("projectId") or make_id()
    project_name = common_data.get("projectName") or "MR-DN Campaign"

    src = {
        "ingestionTime": iso_z(datetime.now(timezone.utc)),
        "Data": {
            # ✅ keep the passed-in slice instead of collapsing to country only
            "boundaryHierarchy": bh,
            "role": "HEALTH_FACILITY_WORKER",
            "taskDates": random_date_str(),
            "campaignId": c.campaign_id,
            "projectType": c.project_type,
            "userName": user_name,
            # ✅ keep the passed-in code slice
            "boundaryHierarchyCode": bhc,
            "localityCode": loc_code,  # ✅ now present
            "additionalDetails": {"cycleIndex": "01"},
            "userAddress": None,
            "projectTypeId": c.project_type_id,
            "syncedDate": random_date_str(),
            "@timestamp": ts_iso,

            "hfReferral": {
                "projectFacilityId": None,
                "rowVersion": 1,
                "additionalFields": additional_fields,
                "symptomSurveyId": "abc1",
                "clientReferenceId": client_ref_id,
                "symptom": symptom,
                "clientAuditDetails": {
                    "lastModifiedTime": client_time_ms,
                    "createdBy": user_id,
                    "lastModifiedBy": user_id,
                    "createdTime": client_time_ms
                },
                "isDeleted": False,
                "referralCode": None,
                "auditDetails": {
                    "lastModifiedTime": audit_time_ms,
                    "createdBy": user_id,
                    "lastModifiedBy": user_id,
                    "createdTime": audit_time_ms
                },
                "tenantId": c.tenant_id,
                "id": es_id,
                "projectId": project_id,
                "nationalLevelId": None,
                "beneficiaryId": ""
            },

            "projectName": project_name,
            "campaignNumber": c.campaign_number,
            "projectId": project_id
        }
    }

    return {
        "_index": HF_REFERRAL_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": src,
        "sort": [client_time_ms]
    }


# ------- FIXED: generate_stock_reconciliation (3.8-safe types) -------
def generate_stock_reconciliation(common_data: dict,
                                  *,
                                  user_id: str,
                                  # override knobs (optional)
                                  product_variant_id: Optional[str] = None,
                                  product_name: Optional[str] = None,
                                  facility_id: Optional[str] = None,
                                  facility_name: Optional[str] = None,
                                  project_id: Optional[str] = None,
                                  project_name: Optional[str] = None,
                                  project_type: Optional[str] = None,
                                  project_type_id: Optional[str] = None,
                                  tenant_id: Optional[str] = None,
                                  campaign_id: Optional[str] = None,
                                  campaign_number: Optional[str] = None,
                                  user_name: Optional[str] = None,
                                  name_of_user: Optional[str] = None,
                                  locality_code: Optional[str] = None,
                                  boundary_hierarchy: Optional[Dict[str, Any]] = None,
                                  boundary_hierarchy_code: Optional[Dict[str, Any]] = None,
                                  # stock numbers (optional; if None they’re randomized)
                                  received: Optional[float] = None,
                                  issued: Optional[float] = None,
                                  returned: Optional[float] = None,
                                  lost: Optional[float] = None,
                                  damaged: Optional[float] = None,
                                  physical_count: Optional[int] = None,
                                  comments: Optional[str] = None
                                  ) -> dict:
    """
    Builds one ES doc for stock-reconciliation-index-v1, faithful to your samples.
    - Sort key = syncedTime (ms)
    - @timestamp and syncedTimeStamp = ISO of syncedTime
    - id = SR-YYYY-MM-DD-###### and mirrored inside nested.stockReconciliation.id
    """
    now = datetime.now(timezone.utc)

    # time choreography (close to your samples)
    date_of_recon = now - timedelta(minutes=random.randint(5, 60))
    client_time   = now - timedelta(minutes=random.randint(1, 3))
    synced_time   = now

    date_of_recon_ms = ms(date_of_recon)
    client_time_ms   = ms(client_time)
    synced_time_ms   = ms(synced_time)

    es_id = next_sr_id(synced_time)

    # pull or default context fields
    project_id       = project_id       or common_data.get("projectId")       or make_id()
    project_name     = project_name     or common_data.get("projectName")     or "MR-DN-August-Dec"
    project_type     = project_type     or common_data.get("projectType")     or "MR-DN"
    project_type_id  = project_type_id  or common_data.get("projectTypeId")   or "ea1bb2e7-06d8-4fe4-ba1e-f4a6363a21be"
    tenant_id        = tenant_id        or common_data.get("tenantId")        or "mz"
    campaign_id      = campaign_id      or common_data.get("campaignId")      or make_id()
    campaign_number  = campaign_number  or common_data.get("campaignNumber")  or "CMP-2025-08-25-001466"
    user_name        = user_name        or common_data.get("userName")        or "USR-006196"
    name_of_user     = name_of_user     or common_data.get("nameOfUser")      or random.choice(["Lata", "James Brown"])
    facility_id      = facility_id      or common_data.get("facilityId")      or random.choice(["F-2025-07-31-008941","F-2025-07-31-008938","F-2025-04-08-008932"])
    facility_name    = facility_name    or common_data.get("facilityName")    or random.choice(["Bednet L5","Bednet L2","Facility Storage","Facility Storage Grand Gedeh","Destination Warehouse 5","LLIN Facilities"])
    product_variant_id = product_variant_id or common_data.get("productVariantId") or random.choice(["PVAR-2025-01-09-000099","PVAR-2025-01-08-000094","PVAR-2025-07-30-000134"])
    product_name     = product_name     or common_data.get("productName")     or random.choice(["SP - 250mg","Bednet - Grade 1"])
    locality_code    = locality_code    or common_data.get("localityCode")    or "MICROPLAN_MO"
    boundary_hierarchy = boundary_hierarchy or common_data.get("boundaryHierarchy") or {"country": "Nigeria"}
    boundary_hierarchy_code = boundary_hierarchy_code or common_data.get("boundaryHierarchyCode") or {"country": "MICROPLAN_MO"}

    # --- stock math (align with samples) ---
    if received is None and issued is None and returned is None and lost is None and damaged is None:
        pattern = random.choice(["all100","some_loss","big","zero","negative"])
        if pattern == "all100":
            received, issued, returned, lost, damaged = 100.0, 0.0, 0.0, 0.0, 0.0
        elif pattern == "some_loss":
            received, issued, returned, lost, damaged = 600.0, 25.0, 4.0, 24.0, 5.0
        elif pattern == "big":
            received, issued, returned, lost, damaged = 10000.0, 5000.0, 1000.0, 0.0, 0.0
        elif pattern == "zero":
            received, issued, returned, lost, damaged = 0.0, 0.0, 0.0, 0.0, 0.0
        else:  # negative inHand case like sample
            received, issued, returned, lost, damaged = 0.0, 0.0, 0.0, 500.0, 500.0

    # calculated/inHand
    in_hand_calc = received - issued + returned - lost - damaged

    # physicalCount default
    if physical_count is None:
        if random.random() < 0.3:
            physical_count = max(0, int(in_hand_calc) - random.choice([5, 2, 1, 0]))
        else:
            physical_count = int(in_hand_calc)

    add_fields = build_stock_recon_additional_fields(received, issued, returned, lost, damaged, in_hand_calc)
    reference_id = project_id
    comments = comments if comments is not None else random.choice([None, "Reconciled the stock", ""])

    nested = {
        "calculatedCount": int(in_hand_calc) if float(in_hand_calc).is_integer() else in_hand_calc,
        "dateOfReconciliation": date_of_recon_ms,
        "facilityId": facility_id,
        "productVariantId": product_variant_id,
        "additionalFields": add_fields,
        "rowVersion": 1,
        "clientReferenceId": make_id(),
        "referenceId": reference_id,
        "clientAuditDetails": {
            "lastModifiedTime": client_time_ms,
            "createdBy": user_id,
            "lastModifiedBy": user_id,
            "createdTime": client_time_ms
        },
        "commentsOnReconciliation": comments if comments else None,
        "isDeleted": False,
        "auditDetails": {
            "lastModifiedTime": synced_time_ms,
            "createdBy": user_id,
            "lastModifiedBy": user_id,
            "createdTime": synced_time_ms
        },
        "tenantId": tenant_id,
        "id": es_id,
        "referenceIdType": "PROJECT",
        "physicalCount": physical_count
    }

    src = {
        "ingestionTime": iso_z(datetime.now(timezone.utc)),
        "Data": {
            "boundaryHierarchy": boundary_hierarchy,
            "taskDates": random_date_str(synced_time),  # fixed helper handles datetime
            "role": "WAREHOUSE_MANAGER",
            "syncedTime": synced_time_ms,
            "campaignId": campaign_id,

            "stockReconciliation": nested,

            "projectType": project_type,
            "localityCode": locality_code,
            "nameOfUser": name_of_user,
            "boundaryHierarchyCode": boundary_hierarchy_code,
            "additionalDetails": {
                "lost":     int(lost) if float(lost).is_integer() else lost,
                "damaged":  int(damaged) if float(damaged).is_integer() else damaged,
                "inHand":   int(in_hand_calc) if float(in_hand_calc).is_integer() else in_hand_calc,
                "received": int(received) if float(received).is_integer() else received,
                "issued":   int(issued) if float(issued).is_integer() else issued,
                "returned": int(returned) if float(returned).is_integer() else returned
            },
            "userName": user_name,
            "productName": product_name,
            "userAddress": common_data.get("userAddress"),
            "projectTypeId": project_type_id,
            "syncedDate": random_date_str(synced_time),
            "@timestamp": iso_z(synced_time),
            "facilityName": facility_name,
            "facilityTarget": common_data.get("facilityTarget"),
            "facilityLevel": common_data.get("facilityLevel"),
            "syncedTimeStamp": iso_z(synced_time),
            "projectName": project_name,
            "campaignNumber": campaign_number,
            "projectId": project_id
        }
    }

    return {
        "_index": STOCK_RECON_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": src,
        "sort": [synced_time_ms]
    }

# ------- FIXED: generate_plan_facility (3.8-safe types) -------
def generate_plan_facility(common_data: dict, user_id: str,
                           facility_name: Optional[str] = None,
                           fixed_post_yes_no: Optional[str] = None,
                           facility_type: Optional[str] = None) -> dict:
    """
    Emit a plan-facility-index-v1 document following your samples.
    Uses the boundary *slice* provided in common_data (so call via emit_*_for_all_levels).
    """
    h: Dict[str, Any] = (common_data.get("boundaryHierarchy") or {}).copy()
    codes: Dict[str, Any] = (common_data.get("boundaryHierarchyCode") or {}).copy()

    residing_code = most_specific_locality_code(codes, h) or codes.get("country") or "MICROPLAN_MO"
    boundary_path = build_boundary_ancestral_path(codes, h)

    code_pool = [v for v in codes.values() if v]
    if boundary_data:
        for bd in random.sample(boundary_data, k=min(5, len(boundary_data))):
            code_pool.extend([cv for cv in bd.get("codes", {}).values() if cv])

    service_boundaries_value = maybe_service_boundaries(code_pool)  # "" or CSV

    facility_id = random_facility_id()
    facility_name = facility_name or random.choice([
        "New Facility", "AS Pemba", "Armazem distrital de Pemba ",
        "Armazem distrital de Mecufi", "Dundun Hp New",
        "Bednet L1", "Bednet L2", "Bednet L3", "Bednet L4", "Bednet L5", "Bednet L6", "Bednet L7",
        "NPHC IYERE"
    ])
    fixed_post_yes_no = fixed_post_yes_no or random.choice(["Yes", "No"])
    facility_type = facility_type or random.choice(["Warehouse", "Health Facility", "Storing Resource"])

    lat = None if random.random() < 0.7 else 10
    lon = None if random.random() < 0.7 else 10
    capacity = random.choice([0, 100, 200, 400, 500, 2500, 20000])
    serving_pop = random.choice([0, 220, 810, 2000])

    plan_cfg_id = make_id()
    plan_cfg_name = f"{common_data.get('projectName','Malaria-SMC Campaign')}-" \
                    f"{random.choice(['House-To-House','Fixed Post & House-to-House'])}-" \
                    f"{datetime.now().strftime('%d %b %y')}"

    now = datetime.now(timezone.utc)
    now_ms = ms(now)
    es_id = make_id()

    init_sb = None if service_boundaries_value == "" else []

    src = {
        "ingestionTime": iso_z(datetime.now(timezone.utc)),
        "Data": {
            "residingBoundary": residing_code,
            "facilityId":      facility_id,
            "serviceBoundaries": service_boundaries_value,
            "planConfigurationName": plan_cfg_name,
            "jurisdictionMapping": build_jurisdiction_mapping_from_codes(codes) or {},
            "boundaryAncestralPath": boundary_path,
            "active": True,
            "additionalDetails": {
                "assignedVillages": [],
                "hierarchyType": "MICROPLAN",
                "facilityType": facility_type,
                "latitude": lat,
                "fixedPost": fixed_post_yes_no,
                "facilityName": facility_name,
                "facilityStatus": random.choice(["Permanent", "Temporary"]),
                "capacity": capacity,
                "servingPopulation": serving_pop,
                "longitude": lon
            },
            "initiallySetServiceBoundaries": init_sb,
            "@timestamp": iso_z(now),
            "planConfigurationId": plan_cfg_id,
            "auditDetails": {
                "lastModifiedTime": now_ms,
                "createdBy": user_id,
                "lastModifiedBy": user_id,
                "createdTime": now_ms
            },
            "tenantId": "mz",
            "id": es_id,
            "facilityName": facility_name
        }
    }

    return {
        "_index": PLAN_FACILITY_INDEX,
        "_id": es_id,
        "_score": None,
        "_source": src,
        "sort": [now_ms]
    }



# ========================
# Main
# ========================
if __name__ == "__main__":
    start_time = time.time()
    logger.info("Starting synthetic data generation process.")
    num_households = SETTINGS.num_households

    households, members, projectTasks, transformer_pgr_services, projects = [], [], [], [], []
    population_coverage_docs, population_coverage_datewise_docs, stock_docs, service_tasks = [], [], [], []
    attendance_logs, project_staff, stocks = [], [], []
    household_coverage_daily_iccd, household_coverage_summary_iccd, ineligible_summary = [], [], []
    user_sync_docs = []
    referrals = [] 
    side_effect_docs = []
    census_docs = []
    plan_docs = []
    hf_referrals = []
    stock_recons: list[dict] = []
    plan_facilities = []  # NEW


    user_id = str(uuid.uuid4())

    for i in range(num_households):
        common_data = getSharedData(user_id, i)  # boundary locked per loop

        household_doc = generate_household(common_data, user_id)
        if household_doc is None:
            continue

        households.append(household_doc)
        individual_client_ref_uuid_list = [str(uuid.uuid4()) for _ in range(5)]

        for j in range(5):
            individual_id = str(uuid.uuid4())
            individual_client_ref_id = individual_client_ref_uuid_list[j]

            task_doc = generate_project_task(common_data, individual_client_ref_id, individual_id)
            if task_doc is not None:
                projectTasks.append(task_doc)

            member_doc = generate_member(common_data, household_doc["_id"], individual_client_ref_id, individual_id, user_id)
            if member_doc is not None:
                members.append(member_doc)

                # --- NEW: side-effect tied to the task (if present) ---
            task_id = None
            task_client_ref = None
            if task_doc is not None:
                try:
                    task_id = task_doc["_source"]["Data"]["taskId"]
                    task_client_ref = task_doc["_source"]["Data"]["taskClientReferenceId"]
                except KeyError:
                    pass

            se_doc = generate_side_effect(
                common_data,
                user_id=user_id,
                individual_id=individual_id,
                task_id=task_id,
                task_client_reference_id=task_client_ref
            )
            if se_doc is not None:
                side_effect_docs.append(se_doc)

            service_doc = generate_transformer_pgr_services(common_data, user_id)
            if service_doc is not None:
                transformer_pgr_services.append(service_doc)

            project = generate_project(common_data, user_id)
            if project is not None:
                projects.append(project)

            pop_doc = generate_population_coverage_summary(common_data, user_id)
            if pop_doc is not None:
                population_coverage_docs.append(pop_doc)

            pop_datewise_doc = generate_population_coverage_summary_datewise(common_data, user_id)
            if pop_datewise_doc is not None:
                population_coverage_datewise_docs.append(pop_datewise_doc)

            service_task_doc = generate_service_task(common_data, user_id)
            if service_task_doc is not None:
                service_tasks.append(service_task_doc)

            attendance_log_doc = generate_attendance_log(common_data, user_id)
            if attendance_log_doc is not None:
                attendance_logs.append(attendance_log_doc)

            project_staff_doc = generate_project_staff(common_data, user_id)
            if project_staff_doc is not None:
                project_staff.append(project_staff_doc)

            stock_doc = generate_stock(common_data)
            if stock_doc is not None:
                stocks.append(stock_doc)

            household_coverage_summary_doc = generate_household_coverage_summary_iccd(common_data, user_id)
            if household_coverage_summary_doc is not None:
                household_coverage_summary_iccd.append(household_coverage_summary_doc)

            ineligible_summary_doc = generate_ineligible_summary(common_data, user_id)
            if ineligible_summary_doc is not None:
                ineligible_summary.append(ineligible_summary_doc)

            household_coverage_daily_doc = generate_household_coverage_daily_iccd(common_data, user_id)
            if household_coverage_daily_doc is not None:
                household_coverage_daily_iccd.append(household_coverage_daily_doc)

            user_sync_doc = generate_user_sync(common_data, user_id)
            if user_sync_doc is not None:
                user_sync_docs.append(user_sync_doc)

            referral_doc = generate_referral(common_data, user_id, individual_id)
            if referral_doc is not None:
                referrals.append(referral_doc)
            
            # Toggle variations to mimic your dataset
            with_hidden = random.random() < 0.4
            with_acc    = random.random() < 0.5
            with_sec    = random.random() < 0.3
            force_map   = random.random() < 0.7  # most have jurisdictionMapping; some use boundaryAncestralPath

            census_doc = generate_census(
                common_data,
                boundary_code=common_data.get("localityCode"),  # or pick codes["village"] for finer granularity
                with_hidden_latlong=with_hidden,
                with_accessibility=with_acc,
                with_security=with_sec,
                force_mapping=force_map
            )
            census_docs.append(census_doc)

            emit_plans_for_locality(common_data)
            # NEW: create HF referrals for ALL available boundary hierarchies (country→…→village)
            hf_referrals.extend(
                emit_hf_referrals_for_all_levels(common_data, user_id, min_per_level=1, max_per_level=2)
            )

            # --- NEW: stock reconciliation docs for this locality/facility/product ---
            sr_common = {
                # reuse boundary & codes from the loop so all child docs align
                "boundaryHierarchy": common_data["boundaryHierarchy"],
                "boundaryHierarchyCode": common_data["boundaryHierarchyCode"],
                # campaign/project context (fallbacks match your samples if SETTINGS lacks them)
                "projectId": common_data.get("projectId"),
                "projectName": common_data.get("projectName"),
                "projectType": common_data.get("projectType"),
                "projectTypeId": common_data.get("projectTypeId"),
                "tenantId": "mz",
                "campaignId": getattr(SETTINGS.CAMPAIGN, "campaign_id", "f51ed8bc-2f40-425c-83f8-6dea5d31c169"),
                "campaignNumber": getattr(SETTINGS.CAMPAIGN, "campaign_number", "CMP-2025-08-25-001466"),
                # locality code (fallback to country-level code like in samples)
                "localityCode": common_data["boundaryHierarchyCode"].get("district")
                                or common_data["boundaryHierarchyCode"].get("country")
                                or "MICROPLAN_MO",
                # facility/product (you can wire these to real lists if you have them)
                "facilityId": random.choice(["F-2025-07-31-008941","F-2025-07-31-008938","F-2025-04-08-008932"]),
                "facilityName": random.choice(["Bednet L5","Bednet L2","Facility Storage","Destination Warehouse 5","LLIN Facilities"]),
                "productVariantId": random.choice(["PVAR-2025-07-30-000134","PVAR-2025-01-09-000099","PVAR-2025-01-08-000094"]),
                "productName": random.choice(["SP - 250mg","Bednet - Grade 1"]),
                "userName": common_data["userName"],
                "nameOfUser": common_data["nameOfUser"],
            }

            for _ in range(random.randint(1, 3)):
                sr_doc = generate_stock_reconciliation(sr_common, user_id=user_id)
                stock_recons.append(sr_doc)
            
            # NEW: Plan-Facility docs for all available boundary hierarchies
            plan_facilities.extend(
                emit_plan_facilities_for_all_levels(common_data, user_id, min_per_level=1, max_per_level=1)
            )


    logger.info(f"Generated {len(households)} households, {len(members)} members, {len(projectTasks)} project tasks.")

    # Write files
    write_bulk_file(households, HOUSEHOLD_FILE)
    write_bulk_file(members, MEMBER_FILE)
    write_bulk_file(projectTasks, PROJECT_TASK_FILE)
    write_bulk_file(transformer_pgr_services, TRANSFORMER_PGR_SERVICES_FILE)
    write_bulk_file(projects, PROJECT_FILE)
    write_bulk_file(population_coverage_docs, POPULATION_COVERAGE_FILE)
    write_bulk_file(population_coverage_datewise_docs, POP_SUMMARY_DATEWISE_FILE)
    write_bulk_file(stocks, STOCK_FILE)
    write_bulk_file(service_tasks, SERVICE_TASK_FILE)
    write_bulk_file(attendance_logs, ATTENDANCE_LOG_FILE)
    write_bulk_file(project_staff, PROJECT_STAFF_FILE)
    write_bulk_file(household_coverage_daily_iccd, HOUSEHOLD_COVERAGE_DAILY_ICCD_FILE)
    write_bulk_file(household_coverage_summary_iccd, HOUSEHOLD_COVERAGE_SUMMARY_ICCD_FILE)
    write_bulk_file(ineligible_summary, INELIGIBLE_SUMMARY_FILE)
    write_bulk_file(user_sync_docs, USER_SYNC_FILE)
    write_bulk_file(referrals, REFERRAL_FILE)
    write_bulk_file(side_effect_docs, SIDE_EFFECT_FILE)
    write_bulk_file(census_docs, CENSUS_FILE)
    write_bulk_file(plan_docs, PLAN_FILE)
    write_bulk_file(hf_referrals, HF_REFERRAL_FILE)
    write_bulk_file(stock_recons, STOCK_RECON_FILE)
    write_bulk_file(plan_facilities, PLAN_FACILITY_FILE)  # NEW

    # Upload to Elasticsearch
    if get_resp(SETTINGS.ES.host, es=True):
        logger.info("Elasticsearch is up. Starting upload.")
        upload_bulk_to_es(HOUSEHOLD_FILE, SETTINGS.ES.host, HOUSEHOLD_INDEX)
        upload_bulk_to_es(PROJECT_TASK_FILE, SETTINGS.ES.host, PROJECT_TASK_INDEX)
        upload_bulk_to_es(TRANSFORMER_PGR_SERVICES_FILE, SETTINGS.ES.host, TRANSFORMER_PGR_SERVICES_INDEX)
        upload_bulk_to_es(MEMBER_FILE, SETTINGS.ES.host, MEMBER_INDEX)
        upload_bulk_to_es(PROJECT_FILE, SETTINGS.ES.host, PROJECT_INDEX)
        upload_bulk_to_es(POPULATION_COVERAGE_FILE, SETTINGS.ES.host, POPULATION_COVERAGE_INDEX)
        upload_bulk_to_es(POP_SUMMARY_DATEWISE_FILE, SETTINGS.ES.host, POP_SUMMARY_DATEWISE_INDEX)
        upload_bulk_to_es(STOCK_FILE, SETTINGS.ES.host, STOCK_INDEX)
        upload_bulk_to_es(SERVICE_TASK_FILE, SETTINGS.ES.host, SERVICE_TASK_INDEX)
        upload_bulk_to_es(ATTENDANCE_LOG_FILE, SETTINGS.ES.host, ATTENDANCE_LOG_INDEX)
        upload_bulk_to_es(PROJECT_STAFF_FILE, SETTINGS.ES.host, PROJECT_STAFF_INDEX)
        upload_bulk_to_es(HOUSEHOLD_COVERAGE_DAILY_ICCD_FILE, SETTINGS.ES.host, HOUSEHOLD_COVERAGE_DAILY_ICCD_INDEX)
        upload_bulk_to_es(HOUSEHOLD_COVERAGE_SUMMARY_ICCD_FILE, SETTINGS.ES.host, HOUSEHOLD_COVERAGE_SUMMARY_ICCD_INDEX)
        upload_bulk_to_es(INELIGIBLE_SUMMARY_FILE, SETTINGS.ES.host, INELIGIBLE_SUMMARY_INDEX)
        upload_bulk_to_es(USER_SYNC_FILE, SETTINGS.ES.host, USER_SYNC_INDEX)
        upload_bulk_to_es(REFERRAL_FILE, SETTINGS.ES.host, REFERRAL_INDEX)
        upload_bulk_to_es(SIDE_EFFECT_FILE, SETTINGS.ES.host, SIDE_EFFECT_INDEX)
        upload_bulk_to_es(CENSUS_FILE, SETTINGS.ES.host, CENSUS_INDEX)
        upload_bulk_to_es(PLAN_FILE, SETTINGS.ES.host, PLAN_INDEX)
        upload_bulk_to_es(HF_REFERRAL_FILE, SETTINGS.ES.host, HF_REFERRAL_INDEX)
        upload_bulk_to_es(STOCK_RECON_FILE, SETTINGS.ES.host, STOCK_RECON_INDEX)
        upload_bulk_to_es(PLAN_FACILITY_FILE, SETTINGS.ES.host, PLAN_FACILITY_INDEX)  # NEW

        
        logger.info("Data uploaded to Elasticsearch successfully.")
    else:
        logger.warning("Elasticsearch is not reachable. Skipping upload.")

    end_time = time.time()
    minutes = int((end_time - start_time) // 60)
    seconds = int((end_time - start_time) % 60)
    logger.info(f"Total time taken: {minutes} minute(s) and {seconds} second(s)")
