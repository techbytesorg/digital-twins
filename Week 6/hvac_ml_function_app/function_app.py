import json
import logging
import os
from io import BytesIO
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from azure.identity import DefaultAzureCredential
from azure.digitaltwins.core import DigitalTwinsClient
from azure.functions import EventHubEvent, FunctionApp
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import IngestionProperties, QueuedIngestClient
from azure.kusto.data.data_format import DataFormat

COLOR_MAP = {
    "heat": "#FFA500",  # orange
    "cool": "#1E90FF",  # blue
    "off": "#808080",  # grey
}

# Shared configuration
ADT_SERVICE_URL = os.getenv("ADT_SERVICE_URL")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
# EVENT_HUB_CONNECTION_STRING = os.getenv("EventHubConnectionString")
ADX_DATABASE = os.getenv("ADX_DATABASE")

# ML-specific configuration
HVAC_ENDPOINT_URL = os.getenv("HVAC_ENDPOINT_URL")
HVAC_ENDPOINT_KEY = os.getenv("HVAC_ENDPOINT_KEY")
ADX_INGEST_URI = os.getenv("ADX_INGEST_URI")
ADX_QUERY_URI = os.getenv("ADX_QUERY_URI")
ML_INFERRED_ADX_TABLE = os.getenv("ML_INFERRED_ADX_TABLE")
ML_ADX_TABLE = os.getenv("ML_ADX_TABLE")

# Legacy-specific configuration
LEGACY_ADX_TABLE = os.getenv("LEGACY_ADX_TABLE")

if not ADT_SERVICE_URL:
    raise RuntimeError("ADT_SERVICE_URL is not configured")

credential = DefaultAzureCredential()
adt_client = DigitalTwinsClient(ADT_SERVICE_URL, credential)

app = FunctionApp()


def map_color(action: str) -> str:
    return COLOR_MAP.get(action, "#808080")


# ML PIPELINE

ml_ingest_client: Optional[QueuedIngestClient] = None
ml_ingest_properties: Optional[IngestionProperties] = None
ml_query_client: Optional[KustoClient] = None

if not (HVAC_ENDPOINT_URL and HVAC_ENDPOINT_KEY):
    raise RuntimeError("HVAC_ENDPOINT_URL/HVAC_ENDPOINT_KEY must be configured")
if not (ML_ADX_TABLE and ADX_INGEST_URI):
    raise RuntimeError("ML_ADX_TABLE and ADX_INGEST_URI must be configured for ADX-backed features")

kcsb_query = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(ADX_QUERY_URI)
ml_query_client = KustoClient(kcsb_query)

kcsb_ingest = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(ADX_INGEST_URI)
ml_ingest_client = QueuedIngestClient(kcsb_ingest)

if ADX_DATABASE and ML_INFERRED_ADX_TABLE:
    ml_ingest_properties = IngestionProperties(
        database=ADX_DATABASE,
        table=ML_INFERRED_ADX_TABLE,
        data_format=DataFormat.JSON,
    )
else:
    logging.warning("ML inferred rows will not be logged to ADX (missing ADX_DATABASE or ML_INFERRED_ADX_TABLE)")


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def build_feature_vector_from_record(record: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = record.get("Timestamp")
    parsed = timestamp if isinstance(timestamp, datetime) else parse_timestamp(timestamp)
    return {
            "RoomID": record.get("RoomID"),
            "AmbientTemperature": record.get("AmbientTemperature"),
            "AmbientHumidity": record.get("AmbientHumidity"),
            "CurrentTemperature": record.get("CurrentTemperature"),
            "TargetTemperature": record.get("TargetTemperature"),
            "FanSpeed": record.get("FanSpeed"),
            "Humidity": record.get("Humidity"),
            "Occupancy": record.get("Occupancy"),
            "PowerConsumption": record.get("PowerConsumption"),
            "TotalOccupantCount": record.get("TotalOccupantCount"),
            "Hour": record.get("Hour") if record.get("Hour") is not None else (parsed.hour if parsed else None),
            "DayOfWeek": record.get("DayOfWeek") if record.get("DayOfWeek") is not None else (parsed.weekday() if parsed else None),
            "DayOfYear": record.get("DayOfYear") if record.get("DayOfYear") is not None else (parsed.timetuple().tm_yday if parsed else None),
            "TotalPowerConsumption": record.get("TotalPowerConsumption"),
            }

def call_ml_inference(features: Dict[str, Any], fallback: str) -> Dict[str, Any]:
    if not (HVAC_ENDPOINT_URL and HVAC_ENDPOINT_KEY):
        return {"ControlAction": fallback}
    payload = {"Inputs": {"input1": [features]},
    "GlobalParameters": {}}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {HVAC_ENDPOINT_KEY}",
    }
    try:
        response = requests.post(HVAC_ENDPOINT_URL, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        body = response.json()
        results = body.get("Results") or body.get("results")
        if isinstance(results, list) and results:
            result = results[0]
            if "ControlAction" not in result:
                result["ControlAction"] = fallback
            return result
        if isinstance(body, dict):
            body.setdefault("ControlAction", fallback)
            return body
    except requests.HTTPError as http_exc:
        resp_text = ""
        if http_exc.response is not None:
            resp_text = http_exc.response.text
            logging.error(
                "ML inference failed: %s (status %s). Response body: %s",
                http_exc,
                http_exc.response.status_code,
                resp_text,
            )
        else:
            logging.error("ML inference failed: %s", http_exc)
    except Exception as exc:
        logging.error("ML inference failed: %s", exc)
    return {"ControlAction": fallback}


def enqueue_ml_record(record: Dict[str, Any]) -> None:
    if not (ml_ingest_client and ml_ingest_properties):
        return
    try:
        payload = json.dumps(record).encode("utf-8")
        ml_ingest_client.ingest_from_stream(BytesIO(payload), ml_ingest_properties)
    except Exception as exc:
        logging.error("Failed to ingest ML record: %s", exc)


def format_table_name(table: str) -> str:
    if table and table.startswith("["):
        return table
    return f"['{table}']"


def latest_ml_room_query(room_id: str) -> str:
    table_ref = format_table_name(ML_ADX_TABLE)
    safe_room = room_id.replace("'", "''")
    return f"""
{table_ref}
| where isnotempty(RoomID) and RoomID == '{safe_room}'
| summarize arg_max(Timestamp, *) by RoomID
"""


def fetch_latest_ml_record(room_id: str) -> Optional[Dict[str, Any]]:
    if not (ml_query_client and ADX_DATABASE and ML_ADX_TABLE):
        return None
    query = latest_ml_room_query(room_id)
    try:
        result = ml_query_client.execute(ADX_DATABASE, query)
    except Exception as exc:
        logging.error("Failed ML ADX query for %s: %s", room_id, exc)
        return None
    tables = result.primary_results
    if not tables:
        return None
    data = tables[0].to_dict().get("data", [])
    return data[0] if data else None


def build_patch_from_message(message: Dict[str, Any], action: str, fan_speed: Optional[int], power: Optional[float]) -> List[Dict[str, Any]]:
    # Testing message.get("Timestamp").isoformat() here
    timestamp = message.get("Timestamp").isoformat() or datetime.now(timezone.utc).isoformat()
    occupancy = message.get("Occupancy")
    if isinstance(occupancy, str):
        occupancy_value = occupancy.strip().lower() in ("true", "1", "yes")
    else:
        occupancy_value = bool(occupancy) if occupancy is not None else False

    occupant_count = message.get("TotalOccupantCount")
    try:
        occupant_count_value = int(occupant_count) if occupant_count is not None else 0
    except (TypeError, ValueError):
        occupant_count_value = 0

    return [
        {"op": "replace", "path": "/currentTemp", "value": message.get("CurrentTemperature")},
        {"op": "replace", "path": "/targetTemp", "value": message.get("TargetTemperature")},
        {"op": "replace", "path": "/humidity", "value": message.get("Humidity")},
        {"op": "replace", "path": "/occupancy", "value": occupancy_value},
        {"op": "replace", "path": "/occupantCount", "value": occupant_count_value},
        {"op": "replace", "path": "/ambientTemp", "value": message.get("AmbientTemperature")},
        {"op": "replace", "path": "/ambientHumidity", "value": message.get("AmbientHumidity")},
        {"op": "replace", "path": "/powerConsumption", "value": power if power is not None else message.get("PowerConsumption")},
        {"op": "replace", "path": "/fanSpeed", "value": fan_speed if fan_speed is not None else message.get("FanSpeed")},
        {"op": "replace", "path": "/hvacAction", "value": action},
        {"op": "replace", "path": "/hvacActionColor", "value": map_color(action)},
        {"op": "replace", "path": "/timestamp", "value": timestamp},
    ]

@app.event_hub_message_trigger(
    arg_name="ml_events",
    event_hub_name=EVENT_HUB_NAME,
    connection="EventHubConnectionString",
    consumer_group="adx-ml-ingestion",
)
def process_ml_pipeline(ml_events: List[EventHubEvent]) -> None:
    if not isinstance(ml_events, list):
        ml_events = [ml_events]

    for event in ml_events:
        try:
            payload = event.get_body().decode("utf-8")
            message = json.loads(payload)
        except Exception as exc:
            logging.error("ML pipeline failed to decode Event Hub message: %s", exc)
            continue

        event_type = (message.get("EventType") or message.get("event_type") or "").lower()
        if event_type and event_type != "sensor_reading":
            continue

        room_id = message.get("RoomID") or message.get("room_id")
        if not room_id:
            logging.warning("ML pipeline missing RoomID: %s", message)
            continue

        ml_record = fetch_latest_ml_record(room_id)
        if not ml_record:
            logging.warning("No ADX record found for room %s; skipping inference.", room_id)
            continue

        features = build_feature_vector_from_record(ml_record)
        fallback_action = ml_record.get("ControlAction", "N/A")
        inference = call_ml_inference(features, fallback_action)
        action = inference.get("ControlAction", fallback_action)
        fan_speed_raw = inference.get("FanSpeed", ml_record.get("FanSpeed"))
        power_raw = inference.get("PowerConsumption", ml_record.get("PowerConsumption"))

        try:
            fan_speed_value = int(fan_speed_raw) if fan_speed_raw is not None else None
        except (TypeError, ValueError):
            fan_speed_value = None
        try:
            power_value = float(power_raw) if power_raw is not None else None
        except (TypeError, ValueError):
            power_value = None

        source_timestamp = ml_record.get("Timestamp")
        if isinstance(source_timestamp, datetime):
            timestamp_value = source_timestamp.isoformat()
        else:
            timestamp_value = source_timestamp or datetime.now(timezone.utc).isoformat()

        occupancy_raw = ml_record.get("Occupancy")
        if isinstance(occupancy_raw, str):
            occupancy_value = occupancy_raw.strip().lower() in ("true", "1", "yes")
        else:
            occupancy_value = bool(occupancy_raw) if occupancy_raw is not None else False

        ingest_record = {
            "EventType": ml_record.get("EventType", "sensor_reading"),
            "Timestamp": timestamp_value,
            "UnitID": ml_record.get("UnitID"),
            "RoomID": room_id,
            "AmbientTemperature": ml_record.get("AmbientTemperature"),
            "AmbientHumidity": ml_record.get("AmbientHumidity"),
            "CurrentTemperature": ml_record.get("CurrentTemperature"),
            "TargetTemperature": ml_record.get("TargetTemperature"),
            "FanSpeed": fan_speed_value if fan_speed_value is not None else ml_record.get("FanSpeed"),
            "Humidity": ml_record.get("Humidity"),
            "Occupancy": occupancy_value,
            "PowerConsumption": power_value if power_value is not None else ml_record.get("PowerConsumption"),
            "TotalOccupantCount": ml_record.get("TotalOccupantCount"),
            "Hour": features.get("Hour"),
            "DayOfWeek": features.get("DayOfWeek"),
            "DayOfYear": features.get("DayOfYear"),
            "TotalPowerConsumption": features.get("TotalPowerConsumption")
            if features.get("TotalPowerConsumption") is not None
            else ml_record.get("TotalPowerConsumption"),
            "ControlAction": inference.get("Scored Labels", action),
            "Scored_Probabilities_Cooling": inference.get("Scored Probabilities_cooling"),
            "Scored_Probabilities_Heating": inference.get("Scored Probabilities_heating"),
            "Scored_Probabilities_Off": inference.get("Scored Probabilities_off"),
        }

        patch = build_patch_from_message(ml_record, action, fan_speed_value, power_value)

        try:
            adt_client.update_digital_twin(room_id, patch)
            logging.info("ML pipeline updated %s with action %s", room_id, action)
        except Exception as exc:
            logging.error("Failed to update ML twin %s: %s", room_id, exc)
        finally:
            enqueue_ml_record(ingest_record)
