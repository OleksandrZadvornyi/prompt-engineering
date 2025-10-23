from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import datetime
import json
from pathlib import Path
import time
import yaml
import re

# Load environment variables
load_dotenv()

# Load the entire configuration
with open("config/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

# --- Get Current Experiment Settings from Config ---
experiment_config = config['experiment']
model_key = experiment_config['model_key']           # e.g., "gpt-4o-mini"
prompt_variant = experiment_config['prompt_variant'] # e.g., "zero-shot-clusters"

# --- Get Model-Specific Information ---
model_identifier = config['models'][model_key]['identifier'] # e.g., "openai/gpt-4o-mini"
model_provider = config['models'][model_key]['provider']     # e.g., "nebius"
print(f"Running experiment for model: {model_identifier}")

# --- Build the Results Path Dynamically ---
base_results_dir = Path(config['project_paths']['results_dir'])
base_data_dir = Path(config['project_paths']['data_dir'])
results_path = base_results_dir / "stories-to-code" / model_key / prompt_variant

print(f"Saving results to: {results_path}")
results_path.mkdir(parents=True, exist_ok=True)

# Initialize LLM
llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model=model_identifier
).bind(
    logprobs=True,
    extra_body={
        "provider": {
            "order": [
                model_provider
            ]
        }
    }
)

REQUEST_START = 1
REQUEST_END = 21
# for i in range(REQUEST_START, REQUEST_END):
request_number = 16
run_dir = results_path / f"report_{request_number}"
run_dir.mkdir(parents=True, exist_ok=True)

# --- Step 1: Read user stories ---
if "clusters" in prompt_variant:
    with open(base_data_dir / "clustered_stories.json", "r", encoding="utf-8") as f:
        clustered_stories = json.load(f)
    
    # Build stories text from clusters
    stories_parts = []
    for cluster_id, stories_text in clustered_stories.items():
        stories_parts.append(f"Cluster {cluster_id}:")
        stories_parts.append(stories_text)
        stories_parts.append("")  # Empty line between clusters
else:
    with open(base_data_dir / "user-stories.txt", "r", encoding="utf-8") as f:
        stories_parts = [line.strip() for line in f if line.strip()]

stories_text = "\n".join(stories_parts).strip()

# --- Step 2: Build the prompt ---
prompt = (
    "Generate fully functional Python code that implements the following user stories. "
    "The code should realistically reflect the described functionality.\n\n"
    f"{stories_text}\n\n"
    "Output only Python code (no markdown formatting or extra text). "
    "Do not leave functions empty — implement reasonable logic where needed."
)

# --- Step 3: LLM call ---
print(f"Making API call #{request_number} to {model_identifier}...")
msg = llm.invoke(("human", prompt))

# --- Step 4: Clean code output ---
response_text = msg.content

# Regex to find the content of a Python code block (handles ```python, ```py, or ```)
code_pattern = r"```(?:python|py)?\s*\n(.*?)```"
match = re.search(code_pattern, response_text, re.DOTALL)

if match:
    code = match.group(1).strip()
    print("Successfully extracted code block.")
else:
    code = response_text.strip()
    print("Warning: No markdown code block found. Using the entire response as code.")

# --- Step 5: Save raw response data ---
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Save all data needed for analysis
raw_data = {
    "request_number": request_number,
    "timestamp": timestamp,
    "model": model_identifier,
    "prompt": prompt,
    "prompt_variant": prompt_variant,
    "stories_text": stories_text,
    "code": code,
    "logprobs": msg.response_metadata.get("logprobs", {}),
    "response_metadata": msg.response_metadata
}

# Save raw response
with open(run_dir / "raw_response.json", "w", encoding="utf-8") as f:
    json.dump(raw_data, f, indent=4, ensure_ascii=False)

print(f"\n✅ Raw response saved in folder: {run_dir}")
print("   - raw_response.json")

print("\nContinuing after 5 seconds.")
time.sleep(5)  # Pauses execution for 10 seconds