# This file is part of the "Smartville" project.
# Copyright (c) 2024 University of Insubria
# Licensed under the Apache License 2.0.
# SPDX-License-Identifier: Apache-2.0
# For the full text of the license, visit:
# https://www.apache.org/licenses/LICENSE-2.0

# Smartville is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# Apache License 2.0 for more details.

# You should have received a copy of the Apache License 2.0
# along with Smartville. If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

# Additional licensing information for third-party dependencies
# used in this file can be found in the accompanying `NOTICE` file.


from prometheus_manager import (
    config_prometheus,
    start_prometheus,
    check_prometheus,
    stop_prometheus,
)
from grafana_manager import (
    config_grafana,
    start_grafana,
    check_grafana,
    stop_grafana,
)
from kafka_manager import (
    config_kafka,
    start_kafka,
    check_kafka,
    stop_kafka,
)
from zookeeper_manager import (
    config_zookeeper,
    start_zookeeper,
    check_zookeeper,
    stop_zookeeper,
)


from fastapi import FastAPI
from fastapi.responses import JSONResponse

import uvicorn
import threading
import os
import atexit
import signal
from threading import Lock
import logging 
import netifaces as ni


SUPPRESSED_ENDPOINTS = [
   '/check_zookeeper', 
   '/check_kafka',
   '/check_prometheus',
   '/check_grafana',
   '/metrics'
 ]

class SuppressEndpointFilter(logging.Filter):
    def filter(self, record):
        # Check if the log record has the necessary arguments (for Uvicorn access logs)
        if record.args and len(record.args) >= 3:
            # record.args[2] contains the path (including query parameters)
            path = record.args[2]
            # Check if the path is in the list of suppressed endpoints
            if path in SUPPRESSED_ENDPOINTS:
                return False  # Suppress this log entry
        return True  # Allow other log entries

# Get the Uvicorn access logger and add the filter
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.addFilter(SuppressEndpointFilter())
uvicorn_access_logger.name = "MonitorServer"

logger = logging.getLogger("monitor_server")
logger.name = "MonitorServer"

def get_static_source_ip_address(interface='eth0'):
    try:
        ip = ni.ifaddresses(interface)[ni.AF_INET][0]['addr']
        return ip
    except ValueError:
        return "Interface not found"


app = FastAPI(title="Monitor Server API", description="API for node health monitoring")  # FastAPI app instance
args = None
openflow_connection = None  # openflow connection to switch is stored here
FLOWSTATS_FREQ_SECS = None  # Interval in which the FLOW stats request is triggered
traffic_dict = None
rewards = None
smart_switch = None
container_ips = None
flow_logger = None
metrics_logger = None
controller_brain = None
stop_tiger_threads = True
flowstatreq_thread = None
inference_thread = None
tiger_lock = Lock()


def pprint(obj):
    for key, value in obj.items():
        if isinstance(value, dict):
            pprint(value)
        else:
          logger.debug(f"{key}: {value}")


@app.get("/")
async def root():
    logger.info("Root endpoint called")
    return {"msg": "Hello World from the MonitorServer!"}


@app.post("/stop")
async def shutdown():
    logger.info("Shutdown command received")
    return {"status_code": 200, "msg": "MonitorServer is stopped"}

def cleanup():
    logger.info("Cleaning up before exit")
    return shutdown()

def handle_sigterm(signum, frame):
    cleanup()
    os._exit(0)  # Force exit


@app.post("/start_zookeeper")
async def api_start_zookeeper(cfg: dict):
    zookeeper_running, pid, last_exit_status = check_zookeeper()
    if not zookeeper_running:
        config_zookeeper_response = config_zookeeper(cfg)
        if config_zookeeper_response.status_code == 200:
            return start_zookeeper()
        else:
            return config_zookeeper_response
        
    return JSONResponse(
        content={"msg": f"Zookeeper is already running (PID={pid})"},
        status_code=200)


@app.post("/start_kafka")
async def api_start_kafka(cfg: dict):
    kafka_running, pid, last_exit_status = check_kafka()
    if not kafka_running:
        config_kafka_response = config_kafka(cfg)
        if config_kafka_response.status_code == 200:
            return start_kafka()
        else:
            return config_kafka_response
        
    return JSONResponse(
        content={"msg": f"Kafka is already running (PID={pid})"},
        status_code=200)


@app.post("/start_prometheus")
async def api_config_prometheus(cfg: dict):
    prometheus_running, pid, last_exit_status = check_prometheus()
    if not prometheus_running:
        config_prometheus_response = config_prometheus(cfg)
        if config_prometheus_response.status_code == 200:
            return start_prometheus()
        else:
            return config_prometheus_response
    
    return JSONResponse(
        content={"msg": f"Prometheus is already running (PID={pid})"},
        status_code=200)


@app.post("/start_grafana")
async def api_config_grafana(cfg: dict):
    grafana_running, pid, last_exit_status = check_grafana()
    if not grafana_running:
        config_grafana_response = config_grafana(cfg)
        if config_grafana_response.status_code == 200:
            return start_grafana()
        else:
            return config_grafana_response
    
    return JSONResponse(
        content={"msg": f"Grafana is already running (PID={pid})"},
        status_code=200)

@app.get("/check_zookeeper")
async def api_check_zookeeper():
    zookeeper_running, pid, last_exit_status = check_zookeeper()
    return JSONResponse(content={"running": zookeeper_running, "pid": pid, "last_exit_status": last_exit_status}, status_code=200)

@app.get("/check_kafka")
async def api_check_kafka():
    kafka_running, pid, last_exit_status = check_kafka()
    return JSONResponse(content={"running": kafka_running, "pid": pid, "last_exit_status": last_exit_status}, status_code=200)

@app.get("/check_prometheus")
async def api_check_prometheus():
    prometheus_running, pid, last_exit_status = check_prometheus()
    return JSONResponse(content={"running": prometheus_running, "pid": pid, "last_exit_status": last_exit_status}, status_code=200)

@app.get("/check_grafana")
async def api_check_grafana():
    grafana_running, pid, last_exit_status = check_grafana()
    return JSONResponse(content={"running": grafana_running, "pid": pid, "last_exit_status": last_exit_status}, status_code=200)


@app.post("/stop_zookeeper")
async def api_stop_services():
    return stop_zookeeper()

@app.post("/stop_kafka")
async def api_stop_services():
    return stop_kafka()

@app.post("/stop_prometheus")
async def api_stop_services():
    return stop_prometheus()

@app.post("/stop_grafana")
async def api_stop_services():
    return stop_grafana()

    
if __name__ == "__main__":
    logger.info("Starting MonitorServer")

    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    os.environ['no_proxy'] = os.environ['no_proxy']+','+get_static_source_ip_address()
    try:
        port = int(os.environ.get("SERVER_PORT"))
    except Exception as e:
        print(f"Error parsing SERVER_PORT env var: {e}")
        assert False

    uvicorn.run(app, host="0.0.0.0", port=port)
