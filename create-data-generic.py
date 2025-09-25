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
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import time
import requests
import logging
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional, List, Any

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
    """
    Central place to control latitude/longitude generation.
      Priority order:
        1) FORCE_COORDS if provided (global fixed lat/lon used everywhere)
        2) USER_LOCATION_COORDS (exact boundary tuple matches)
        3) COUNTRY_LATLON_RANGES fallback
    """
    # Set to a tuple (lat, lon) to force the same coordinates everywhere.
    FORCE_COORDS: Optional[Tuple[float, float]] = None

    # Exact coordinates keyed by partial or full boundary tuples
    USER_LOCATION_COORDS: List[Dict[str, Any]] = field(default_factory=lambda: [
        {
            "match": {
                "country": "Mozambique",
                "province": "Maryland",
                "district": "Pleebo",
                "administrativeProvince": "Pleebo Health Center",
                "locality": "Hospital Camp/Camp 3",
                "village": "Hospital Camp"
            },
            "lat": -7.0345,
            "lon": 37.1234
        },
        # Add more entries as needed…
    ])

    # Country bounds (lat_rng, lon_rng)
    COUNTRY_LATLON_RANGES: Dict[str, Tuple[Tuple[float, float], Tuple[float, float]]] = field(default_factory=lambda: {
        # Mozambique approx bounds (lat: -26.9..-10.5, lon: 30.2..41.5)
        "Mozambique": ((-26.9, -10.5), (30.2, 41.5)),
        # India approx bounds
        "India": ((8.0, 37.0), (68.0, 97.0))
    })

    # Default country to use if none present
    DEFAULT_COUNTRY: str = "India"


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
    num_households: int = 10


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

# ========================
# USER-EDITABLE LOCATION INPUTS (moved into GeoConfig)
# ========================

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

def random_date_str(start_year=2025, end_year=2025):
    start_date = datetime(start_year, 9, 20)
    end_date = datetime(end_year, 9, 25)
    return random_date(start_date, end_date).strftime('%Y-%m-%d')

def most_specific_locality_code(codes: dict, hierarchy: dict):
    """Pick the most specific available code in the usual order."""
    for key in ["village", "locality", "administrativeProvince", "district", "province", "country"]:
        if key in codes and codes[key] and key in hierarchy:
            return codes[key]
    for k in ["district", "province", "country"]:
        if k in codes and codes[k]:
            return codes[k]
    return None

def boundary_slice(h, c, level: str):
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

def _coords_from_user_locations(hierarchy: dict, user_mappings: List[Dict[str, Any]]):
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

def pick_lat_lon_for_boundary(hierarchy: dict, geo_cfg: GeoConfig = SETTINGS.GEO) -> Tuple[float, float]:
    """Single place that controls how lat/lon are chosen."""
    # 0) Forced—use the same lat/lon everywhere
    if geo_cfg.FORCE_COORDS is not None:
        return geo_cfg.FORCE_COORDS

    # 1) Exact user mapping
    mapped = _coords_from_user_locations(hierarchy, geo_cfg.USER_LOCATION_COORDS)
    if mapped:
        return mapped

    # 2) Country based fallback
    country = hierarchy.get("country", geo_cfg.DEFAULT_COUNTRY)
    return _random_in_country(country, geo_cfg)

def clean_source(source):
    return {k: v for k, v in source.items() if not callable(v)}

def write_bulk_file(data, file_path):
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

    _id = str(uuid.uuid4())
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

    additional_details_options = [
        {
            "houseStructureTypes": random.choice(["REEDS", "CLAY", "METAL", "GLASS", "CEMENT"]),
            "children": random.randint(0, 3),
            "latitude": "11.094015445728362",
            "isVulnerable": True,
            "test_b9aa6f50056e": "test_dcfafb1be02f",
            "cycleIndex": "01",
            "noOfRooms": random.randint(1, 15),
            "pregnantWomen": random.randint(0, 1),
            "longitude": "4.41527528930878"
        },
        {
            "memberCount": str(random.randint(1, 3)),
            "dateOfRegistration": f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{random.randint(100000, 999999)}",
            "cycleIndex": random.choice([None, "01"]),
            "pregnantWomenCount": str(random.randint(0, 2)),
            "administrativeArea": boundary.get("village", boundary.get("locality")),
            "childrenCount": str(random.randint(0, 4))
        }
    ]
    additional_details = random.choice(additional_details_options)
    administration_status = "ADMINISTRATION_SUCCESS"
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
                "administrationStatus": administration_status,
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
                "deliveredTo": "HOUSEHOLD",
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
                "tenantId": SETTINGS.CAMPAIGN.tenant_id,
                "projectName": common_data["projectName"],
                "campaignNumber": c.campaign_number,
                "projectId": project_id,
                "taskId": project_id,
                "deliveryComments": delivery_comments,
                "status": administration_status
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
                "projectBeneficiaryType": "HOUSEHOLD",
                "productVariant": "PVAR-2025-01-09-000103,PVAR-2025-01-09-000104",
                "additionalDetails": {"doseIndex": ["01"], "cycleIndex": ["01"]},
                "localityCode": most_specific_locality_code(codes, boundary),
                "projectName": common_data.get("projectName", "IRS Sep"),
                "projectTypeId": c.project_type_id,
                "@timestamp": timestamp,
                "createdBy": user_id,
                "tenantId": SETTINGS.CAMPAIGN.tenant_id
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

    document = {
        "_index": POPULATION_COVERAGE_INDEX,
        "_id": str(uuid.uuid4()),
        "_score": None,
        "_source": {
            "total_administered_resources": total_admin,
            "refused": {"count": refused},
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
    checklist_names = ["HOUSEHOLD", "ELIGIBILITY"]
    supervisor_levels = ["DISTRIBUTOR", "TEAM_SUPERVISOR", "DISTRICT_SUPERVISOR"]
    service_definition_ids = [
        "fe7cdbcf-5818-43a5-91ac-fc682c1255db",
        "d8c4c518-36bb-432d-9e25-69bb94ec5a5f"
    ]

    household_attribute_codes = ["SN1", "SN2", "SN3", "SN4", "SN5"]
    eligibility_attribute_codes = ["SMC1", "SMC1.YES.SM1", "SMC2", "SMC3"]
    household_attribute_values = ["0", "1"]

    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1000)

    checklist_name = random.choice(checklist_names)
    service_definition_id = service_definition_ids[0] if checklist_name == "HOUSEHOLD" else service_definition_ids[1]

    client_reference_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    if checklist_name == "HOUSEHOLD":
        attributes = [{
            "attributeCode": code,
            "auditDetails": common_data["auditDetails"],
            "id": str(uuid.uuid4()),
            "additionalDetails": None,
            "value": {"value": random.choice(household_attribute_values)},
            "referenceId": task_id
        } for code in household_attribute_codes]
        geo_point = list(pick_lat_lon_for_boundary(common_data["boundaryHierarchy"]))[::-1]  # [lon, lat]
        bh = common_data["boundaryHierarchy"]
        bc = common_data["boundaryHierarchyCode"]
    else:
        attributes = []
        for code in eligibility_attribute_codes:
            val = "NOT_SELECTED" if code == "SMC1.YES.SM1" else random.choice(["YES", "NO"])
            attributes.append({
                "attributeCode": code,
                "auditDetails": common_data["auditDetails"],
                "id": str(uuid.uuid4()),
                "additionalDetails": None,
                "value": {"value": val},
                "referenceId": task_id
            })
        geo_point = None
        bh, bc = boundary_slice(common_data["boundaryHierarchy"], common_data["boundaryHierarchyCode"], "country")

    return {
        "_index": SERVICE_TASK_INDEX,
        "_id": f"{client_reference_id}mz",
        "_source": {
            "ingestionTime": now.isoformat() + "Z",
            "Data": {
                "supervisorLevel": random.choice(supervisor_levels),
                "boundaryHierarchy": bh,
                "role": "DISTRIBUTOR",
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
                "serviceDefinitionId": service_definition_id,
                "nameOfUser": common_data["nameOfUser"],
                "userName": common_data["userName"],
                "boundaryHierarchyCode": bc,
                "additionalDetails": {"cycleIndex": "01"},
                "userId": user_id,
                "userAddress": None,
                "projectTypeId": c.project_type_id,
                "@timestamp": now.isoformat() + "Z",
                "createdBy": user_id,
                "tenantId": SETTINGS.CAMPAIGN.tenant_id,
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

    addl_details_options = [
        {
            "houseStructureTypes": random.choice(["REEDS", "CLAY", "METAL", "GLASS", "CEMENT"]),
            "children": random.randint(0, 3),
            "latitude": "11.094015445728362",
            "isVulnerable": True,
            "test_b9aa6f50056e": "test_dcfafb1be02f",
            "cycleIndex": "01",
            "noOfRooms": random.randint(1, 15),
            "pregnantWomen": random.randint(0, 1),
            "longitude": "4.41527528930878"
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

    Upload to Elasticsearch
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

        logger.info("Data uploaded to Elasticsearch successfully.")
    else:
        logger.warning("Elasticsearch is not reachable. Skipping upload.")

    end_time = time.time()
    minutes = int((end_time - start_time) // 60)
    seconds = int((end_time - start_time) % 60)
    logger.info(f"Total time taken: {minutes} minute(s) and {seconds} second(s)")
