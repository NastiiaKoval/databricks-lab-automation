#!/usr/bin/env python3
"""
Databricks Platform Management CLI
Usage: python cli.py <command> [options]
"""

import os
import time
import argparse
import requests
import sys
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")
EXISTING_CLUSTER_ID = os.getenv("KOVAL_CLUSTER_ID")

if not HOST or not TOKEN:
    print("❌ Missing env vars: DATABRICKS_HOST and DATABRICKS_TOKEN are required.")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


# ── Core functions (same logic as databricks_automation.py) ──────────────────

def create_cluster(name: str = "koval-api-cluster", workers: int = 0,
                   autotermination: int = 15) -> str | None:
    url = f"{HOST}/api/2.1/clusters/create"
    payload = {
        "cluster_name": name,
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_F4",
        "num_workers": workers,
        "data_security_mode": "SINGLE_USER",
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {"ResourceClass": "SingleNode"},
        "autotermination_minutes": autotermination,
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        print(f"❌ Error: {resp.text}")
        return None
    cluster_id = resp.json().get("cluster_id")
    print(f"✅ Cluster created: {cluster_id}")
    return cluster_id


def run_notebook(notebook_path: str, parameters: dict = None,
                 cluster_id: str = None) -> int | None:
    url = f"{HOST}/api/2.2/jobs/runs/submit"
    payload = {
        "run_name": f"CLI Notebook Run - {time.strftime('%Y-%m-%d %H:%M')}",
        "existing_cluster_id": cluster_id or EXISTING_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": parameters or {}
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        print(f"❌ Error: {resp.text}")
        return None
    run_id = resp.json()["run_id"]
    print(f"✅ Notebook running — run_id={run_id}")
    return run_id


def trigger_job(job_id: int) -> int | None:
    url = f"{HOST}/api/2.2/jobs/run-now"
    resp = requests.post(url, headers=headers, json={"job_id": job_id})
    if resp.status_code != 200:
        print(f"❌ Error: {resp.text}")
        return None
    run_id = resp.json()["run_id"]
    print(f"✅ Job {job_id} triggered — run_id={run_id}")
    return run_id


def trigger_pipeline(pipeline_id: str, full_refresh: bool = False) -> str | None:
    url = f"{HOST}/api/2.0/pipelines/{pipeline_id}/updates"
    resp = requests.post(url, headers=headers, json={"full_refresh": full_refresh})
    if resp.status_code != 200:
        print(f"❌ Error: {resp.text}")
        return None
    update_id = resp.json().get("update_id")
    print(f"✅ Pipeline {pipeline_id} triggered — update_id={update_id}")
    return update_id


def monitor_run(run_id: int, timeout: int = 600) -> dict | None:
    url = f"{HOST}/api/2.2/jobs/runs/get"
    start = time.time()
    print(f"⏳ Monitoring run {run_id}...")
    while time.time() - start < timeout:
        resp = requests.get(url, headers=headers, params={"run_id": run_id})
        data = resp.json()
        state = data["state"]
        lc = state.get("life_cycle_state")
        result = state.get("result_state", "")
        print(f"   state={lc}  result={result}")
        if lc in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            if result == "SUCCESS":
                print("✅ Run completed successfully!")
            else:
                print(f"❌ Run ended with: {result}")
            return data
        time.sleep(10)
    print("⏰ Monitoring timeout reached.")
    return None


def monitor_pipeline(pipeline_id: str, update_id: str, timeout: int = 600) -> str | None:
    url = f"{HOST}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
    start = time.time()
    print(f"⏳ Monitoring pipeline {pipeline_id} / update {update_id}...")
    while time.time() - start < timeout:
        resp = requests.get(url, headers=headers)
        state = resp.json().get("update", {}).get("state", "UNKNOWN")
        print(f"   state={state}")
        if state in ["COMPLETED", "FAILED", "CANCELED"]:
            if state == "COMPLETED":
                print("✅ Pipeline completed successfully!")
            else:
                print(f"❌ Pipeline ended with: {state}")
            return state
        time.sleep(15)
    print("⏰ Monitoring timeout reached.")
    return None


def terminate_cluster(cluster_id: str) -> None:
    url = f"{HOST}/api/2.1/clusters/delete"
    resp = requests.post(url, headers=headers, json={"cluster_id": cluster_id})
    if resp.status_code == 200:
        print(f"✅ Cluster {cluster_id} termination requested.")
    else:
        print(f"❌ Error: {resp.text}")


# ── CLI argument parsing ─────────────────────────────────────────────────────

def parse_params(raw: list[str]) -> dict:
    """Convert ['key=value', ...] to {'key': 'value', ...}"""
    result = {}
    for item in raw or []:
        if "=" not in item:
            print(f"⚠️  Skipping malformed param (expected key=value): {item}")
            continue
        k, v = item.split("=", 1)
        result[k] = v
    return result


def cmd_create_cluster(args):
    create_cluster(
        name=args.name,
        workers=args.workers,
        autotermination=args.autotermination
    )


def cmd_run_notebook(args):
    params = parse_params(args.param)
    run_id = run_notebook(
        notebook_path=args.path,
        parameters=params,
        cluster_id=args.cluster_id
    )
    if run_id and args.wait:
        monitor_run(run_id, timeout=args.timeout)


def cmd_trigger_job(args):
    run_id = trigger_job(args.job_id)
    if run_id and args.wait:
        monitor_run(run_id, timeout=args.timeout)


def cmd_trigger_pipeline(args):
    update_id = trigger_pipeline(args.pipeline_id, full_refresh=args.full_refresh)
    if update_id and args.wait:
        monitor_pipeline(args.pipeline_id, update_id, timeout=args.timeout)


def cmd_terminate_cluster(args):
    terminate_cluster(args.cluster_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="databricks-cli",
        description="🔧 Databricks Platform Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py create-cluster --name my-cluster --autotermination 30
  python cli.py run-notebook --path /Workspace/Users/you/notebook --param date=2026-04-21 --wait
  python cli.py trigger-job --job-id 235502095043739 --wait
  python cli.py trigger-pipeline --pipeline-id e6749830-f5a5-4182-a0c8-cb8c056cee85 --wait
  python cli.py terminate-cluster --cluster-id 1234-567890-abc123
        """
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # create-cluster
    p_cc = sub.add_parser("create-cluster", help="Create a new Databricks cluster")
    p_cc.add_argument("--name", default="koval-api-cluster", help="Cluster name")
    p_cc.add_argument("--workers", type=int, default=0, help="Number of workers (0 = single-node)")
    p_cc.add_argument("--autotermination", type=int, default=15,
                      help="Auto-termination in minutes (default: 15)")
    p_cc.set_defaults(func=cmd_create_cluster)

    # run-notebook
    p_nb = sub.add_parser("run-notebook", help="Submit a notebook as a one-time run")
    p_nb.add_argument("--path", required=True, help="Workspace path to the notebook")
    p_nb.add_argument("--cluster-id", default=None,
                      help="Cluster ID (defaults to KOVAL_CLUSTER_ID env var)")
    p_nb.add_argument("--param", nargs="*", metavar="KEY=VALUE",
                      help="Notebook parameters, e.g. --param date=2026-04-21 env=prod")
    p_nb.add_argument("--wait", action="store_true", help="Wait for the run to finish")
    p_nb.add_argument("--timeout", type=int, default=600, help="Monitoring timeout in seconds")
    p_nb.set_defaults(func=cmd_run_notebook)

    # trigger-job
    p_job = sub.add_parser("trigger-job", help="Trigger an existing Databricks job")
    p_job.add_argument("--job-id", type=int, required=True, help="Job ID to trigger")
    p_job.add_argument("--wait", action="store_true", help="Wait for the run to finish")
    p_job.add_argument("--timeout", type=int, default=600, help="Monitoring timeout in seconds")
    p_job.set_defaults(func=cmd_trigger_job)

    # trigger-pipeline
    p_pipe = sub.add_parser("trigger-pipeline", help="Trigger a DLT / Lakeflow pipeline")
    p_pipe.add_argument("--pipeline-id", required=True, help="Pipeline UUID")
    p_pipe.add_argument("--full-refresh", action="store_true",
                        help="Force a full refresh (default: incremental)")
    p_pipe.add_argument("--wait", action="store_true", help="Wait for the update to finish")
    p_pipe.add_argument("--timeout", type=int, default=600, help="Monitoring timeout in seconds")
    p_pipe.set_defaults(func=cmd_trigger_pipeline)

    # terminate-cluster
    p_term = sub.add_parser("terminate-cluster", help="Terminate a running cluster")
    p_term.add_argument("--cluster-id", required=True, help="Cluster ID to terminate")
    p_term.set_defaults(func=cmd_terminate_cluster)

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)