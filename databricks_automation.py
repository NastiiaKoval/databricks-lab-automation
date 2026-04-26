import os
import time
import requests
import json
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")
EXISTING_CLUSTER_ID = os.getenv("KOVAL_CLUSTER_ID")

if not HOST or not TOKEN:
    raise ValueError("Add DATABRICKS_HOST і DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

print(f"Connected to: {HOST}")
print(f"Token: {'***' + TOKEN[-4:] if TOKEN else 'NOT FOUND'}")


def create_cluster():
    url = f"{HOST}/api/2.1/clusters/create"
    payload = {
        "cluster_name": "koval-api-cluster",
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_F4",
        "num_workers": 0,
        "data_security_mode": "SINGLE_USER",  
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {"ResourceClass": "SingleNode"},
        "autotermination_minutes": 15,
    }
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code != 200:
        print(f"❌ Помилка: {resp.text}")
        return None

    cluster_id = resp.json().get("cluster_id")
    print(f"✅ Cluster created: {cluster_id}")
    return cluster_id


def run_notebook(notebook_path: str, parameters: dict = None):
    """2. Запуск notebook (one-time run)"""
    url = f"{HOST}/api/2.2/jobs/runs/submit"
    payload = {
        "run_name": f"Lab Notebook Run - {time.strftime('%Y-%m-%d %H:%M')}",
        "existing_cluster_id": EXISTING_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook_path,         
            "base_parameters": parameters or {}
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        print("❌ Error notebook:")
        print(resp.text)     
        return None


    data = resp.json()
    run_id = data["run_id"]
    print(f"✅ Notebook is running, run_id = {run_id}")
    return run_id


def trigger_job(job_id: int):
    """3. Запуск існуючого Job"""
    url = f"{HOST}/api/2.2/jobs/run-now"
    payload = {"job_id": job_id}
    resp = requests.post(url, headers=headers, json=payload)
    run_id = resp.json()["run_id"]
    print(f"✅ Job {job_id} is running, run_id = {run_id}")
    return run_id


def trigger_pipeline(pipeline_id: str):
    """4. Запуск Pipeline (DLT / Lakeflow)"""
    # Для Lakeflow / DLT pipelines
    url = f"{HOST}/api/2.0/pipelines/{pipeline_id}/updates"   
    payload = {"full_refresh": False} 
    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()
    update_id = data.get("update_id")
    print(f"✅ Pipeline {pipeline_id} is running, update_id = {update_id}")
    return update_id


def monitor_run(run_id: int, timeout=600):
    """5. Моніторинг статусу run (job/notebook)"""
    url = f"{HOST}/api/2.2/jobs/runs/get"
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(url, headers=headers, params={"run_id": run_id})
        data = resp.json()
        
        state = data["state"]
        life_cycle = state.get("life_cycle_state")
        result = state.get("result_state")
                
        if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            if result == "SUCCESS":
                print("Run completed with SUCCESS!")
            else:
                print("❌ Error or cancelled")
            return data
        time.sleep(10)
    print("Monitoring timeout")
    return None

def monitor_pipeline(pipeline_id: str, update_id: str, timeout=600):
    """Моніторинг статусу DLT pipeline update."""
    url = f"{HOST}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
    start = time.time()

    while time.time() - start < timeout:
        resp = requests.get(url, headers=headers)
        data = resp.json()
        state = data.get("update", {}).get("state", "UNKNOWN")
        print(f"⏳ Pipeline state: {state}")

        if state in ["COMPLETED", "FAILED", "CANCELED"]:
            if state == "COMPLETED":
                print("✅ Pipeline completed successfully!")
            else:
                print(f"❌ Pipeline ended with: {state}")
            return state

        time.sleep(15)

    print("Monitoring timeout")
    return None    


def terminate_cluster(cluster_id: str) -> None:
    """Зупиняє кластер (можна перезапустити пізніше)."""
    url = f"{HOST}/api/2.1/clusters/delete"
    resp = requests.post(url, headers=headers, json={"cluster_id": cluster_id})
    if resp.status_code == 200:
        print(f"✅ Cluster {cluster_id} termination requested")
    else:
        print(f"❌ Error: {resp.text}")    



# EXECUTION
if __name__ == "__main__":
    print("Run all processes..\n")

    # Create cluster
    new_cluster_id = create_cluster()

    # Run notebook 
    notebook_run_id = run_notebook(
        notebook_path="/Workspace/Users/kovalnasta919@softserve.academy/setup_rls_cls", 
        parameters={"date": "2026-04-21"}
    )
    monitor_run(notebook_run_id)
    
    # Run job
    job_run_id = trigger_job(235502095043739)
    
    # Run pipeline
    PIPELINE_ID = "e6749830-f5a5-4182-a0c8-cb8c056cee85"
    pipeline_update_id = trigger_pipeline(PIPELINE_ID)

    if pipeline_update_id:
        monitor_pipeline(PIPELINE_ID, pipeline_update_id)

    # Terminate cluster
    if new_cluster_id:
        terminate_cluster(new_cluster_id)
        print(f"Cluster {new_cluster_id} terminated.")


print("Done.")