import time
import requests

BASE_URL = "http://localhost:8000"
NODE_ID = "laptop-node-alpha"

print("[1] Registering virtual node...")
reg_resp = requests.post(f"{BASE_URL}/api/nodes/register", json={
    "node_id": NODE_ID,
    "specs": {"cores": 8, "ram_gb": 16.0, "device_type": "cuda"}
})
print("Registered:", reg_resp.json().get("node_id"))

print("\n[2] Submitting batch job (12 sample tasks)...")
sample_prompts = [f"Classify citizen complaint document #{i}" for i in range(1, 13)]
job_resp = requests.post(f"{BASE_URL}/api/jobs/submit", json={
    "title": "Municipal Civic Ingestion",
    "prompts": sample_prompts
})

if job_resp.status_code != 200:
    print(f"[!] Server Error ({job_resp.status_code}): {job_resp.text}")
    exit(1)

job_data = job_resp.json()
job_id = job_data["id"]
print(f"Job submitted successfully! ID: {job_id}")

print("\n[3] Polling and executing task chunks...")
while True:
    poll_resp = requests.get(f"{BASE_URL}/api/tasks/poll?node_id={NODE_ID}")
    
    if poll_resp.status_code != 200:
        print(f"[!] Polling error ({poll_resp.status_code}): {poll_resp.text}")
        break

    chunk_data = poll_resp.json()
    if not chunk_data:
        print("[*] No more chunks available in queue.")
        break

    tasks_list = chunk_data.get("tasks", [])
    chunk_id = chunk_data.get("chunk_id")
    
    if not tasks_list:
        print("[*] Chunk returned with 0 tasks.")
        break

    print(f"-> Processing chunk {chunk_id} ({len(tasks_list)} tasks)...")
    results = [
        {"task_id": t["task_id"], "output": f"Processed item {t['task_id'][:8]} via {NODE_ID}"}
        for t in tasks_list
    ]

    comp_resp = requests.post(f"{BASE_URL}/api/tasks/complete", json={
        "chunk_id": chunk_id,
        "node_id": NODE_ID,
        "results": results
    })
    print("   Chunk acknowledged:", comp_resp.json())

print("\n[4] Checking final job status...")
status_resp = requests.get(f"{BASE_URL}/api/jobs/{job_id}")
print("Final Job State:", status_resp.json())