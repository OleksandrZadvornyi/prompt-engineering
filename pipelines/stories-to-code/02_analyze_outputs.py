import math
import json
import yaml
from pathlib import Path

from utils.compute_code_structure_metrics import compute_code_structure_metrics
from utils.compute_code_semantic_metrics import compute_code_semantic_metrics
from utils.compute_code_execution_metrics import compute_code_execution_metrics
from utils.compute_credibility import compute_credibility

# Configuration
with open("config/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

experiment_config = config['experiment']
model_key = experiment_config['model_key']           # e.g., "gpt-4o-mini"
prompt_variant = experiment_config['prompt_variant'] # e.g., "zero-shot-clusters"
base_results_dir = Path(config['project_paths']['results_dir'])

results_path = base_results_dir / "stories-to-code" / model_key / prompt_variant

request_number = 2
run_dir = results_path / f"report_{request_number}"


# --- Step 1: Load raw response data ---
print(f"\n\nLoading data from {run_dir}...")
with open(run_dir / "raw_response.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

# Extract data
request_number = raw_data["request_number"]
timestamp = raw_data["timestamp"]
model = raw_data["model"]
prompt = raw_data["prompt"]
stories_text = raw_data["stories_text"]
code = raw_data["code"]
logprobs_data = raw_data["logprobs"].get("content", [])

print(f"Loaded response from {timestamp}")
print(f"Model: {model}")


# --- Step 2: Analyze log probabilities ---
MIN_LOGPROB = -10

# Clip logprobs in the original data structure
anomalous_count = 0
for item in logprobs_data:
    raw_logp = item.get("logprob", float("-inf"))
    clipped_logp = max(raw_logp, MIN_LOGPROB) if math.isfinite(raw_logp) else MIN_LOGPROB
    
    if math.isfinite(raw_logp) and raw_logp < MIN_LOGPROB:
        anomalous_count += 1
    
    # Modify the original data structure
    item["logprob"] = clipped_logp

if anomalous_count > 0:
    print(f"⚠️  Warning: Clipped {anomalous_count} tokens with logprob < {MIN_LOGPROB}")

# Calculate summary statistics 
total_logprob = sum(item.get("logprob", 0.0) for item in logprobs_data) if logprobs_data else 0.0
total_tokens = len(logprobs_data) if logprobs_data else len(code.split())
avg_logprob = (total_logprob / total_tokens) if total_tokens else 0.0
avg_prob = math.exp(avg_logprob) if math.isfinite(avg_logprob) else 0.0
perplexity = math.exp(-avg_logprob) if math.isfinite(avg_logprob) else float("inf")

# --- Step 3: Compute metrics ---
print("Computing metrics...")

# Structural & length metrics
struct_metrics = compute_code_structure_metrics(code)

# Semantic quality metrics
semantic_metrics = compute_code_semantic_metrics(code)

# Execution-based metrics
execution_metrics = compute_code_execution_metrics(code, 15)

# Total credibility
credibility = compute_credibility(struct_metrics, semantic_metrics, execution_metrics, avg_prob, perplexity)


# --- Step 4: Save JSON summary ---
summary = {
    "request_number": request_number,
    "timestamp": timestamp,
    "model": model,
    "total_tokens": total_tokens,
    "total_logprob": total_logprob,
    "avg_prob": avg_prob,
    "perplexity": perplexity,
    "struct_metrics": {**struct_metrics},
    "semantic_metrics": {**semantic_metrics},
    "execution_metrics": {**execution_metrics},
    "total_credibility": credibility
}

with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=4, ensure_ascii=False)

print(f"\n✅ Analysis complete! Report saved in folder: {run_dir}")
print("   - summary.json")