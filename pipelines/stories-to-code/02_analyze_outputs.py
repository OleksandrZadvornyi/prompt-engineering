import math
import json
import yaml
from pathlib import Path

from utils.compute_code_structure_metrics import compute_code_structure_metrics
from utils.compute_code_semantic_metrics import compute_code_semantic_metrics
from utils.compute_code_execution_metrics import compute_code_execution_metrics
from utils.compute_credibility import compute_credibility

def load_config():
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        # Fallback to current directory
        config_path = Path("config.yaml")
    
    if not config_path.exists():
        print("Error: Configuration file not found at 'config/config.yaml' or 'config.yaml'")
        return None
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def analyze_all_reports(config):
    """
    Scans the entire results directory, finds all 'raw_response.json' files,
    and generates/overwrites a 'summary.json' for each one.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    search_path = base_results_dir / "stories-to-code"

    if not search_path.exists():
        print(f"Error: Search directory does not exist: {search_path}")
        return

    print(f"Scanning for 'raw_response.json' files in: {search_path}...")
    
    # Use .rglob() to recursively find all matching files
    raw_json_files = list(search_path.rglob("raw_response.json"))
    
    if not raw_json_files:
        print("No 'raw_response.json' files found to analyze.")
        return

    print(f"Found {len(raw_json_files)} reports. Re-analyzing all...")
    
    processed_count = 0
    skipped_count = 0
    
    for raw_json_path in raw_json_files:
        run_dir = raw_json_path.parent  # This is the 'report_{n}' folder
        summary_path = run_dir / "summary.json"
        
        # Check if this report has already been analyzed
        # if summary_path.exists():
        #     print(f"Skipping (summary exists): {run_dir.relative_to(search_path)}")
        #     skipped_count += 1
        #     continue
        
        # --- Step 1: Load raw response data ---
        with open(raw_json_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        # Extract data
        request_number = raw_data.get("request_number", "N/A")
        timestamp = raw_data.get("timestamp", "N/A")
        model = raw_data.get("model", "N/A")
        prompt_variant = raw_data.get("prompt_variant", "N/A")
        code = raw_data.get("code", "")
        logprobs_data = raw_data.get("logprobs", {}).get("content", [])
        
        # --- Step 2: Analyze log probabilities ---
        MIN_LOGPROB = -10
        
        for item in logprobs_data:
            raw_logp = item.get("logprob", float("-inf"))
            clipped_logp = max(raw_logp, MIN_LOGPROB) if math.isfinite(raw_logp) else MIN_LOGPROB
            item["logprob"] = clipped_logp
        
        total_logprob = sum(item.get("logprob", 0.0) for item in logprobs_data) if logprobs_data else 0.0
        total_tokens = len(logprobs_data) if logprobs_data else 0
        
        if total_tokens == 0:
            avg_logprob = 0.0
            avg_prob = 0.0
            perplexity = float("inf")
        else:
            avg_logprob = total_logprob / total_tokens
            avg_prob = math.exp(avg_logprob) if math.isfinite(avg_logprob) else 0.0
            perplexity = math.exp(-avg_logprob) if math.isfinite(avg_logprob) else float("inf")
        
        
        # --- Step 3: Compute metrics ---
        struct_metrics = compute_code_structure_metrics(code)
        semantic_metrics = compute_code_semantic_metrics(code)
        execution_metrics = compute_code_execution_metrics(code, 15) # 15s timeout
        
        credibility = compute_credibility(struct_metrics, semantic_metrics, execution_metrics, avg_prob, perplexity)
        
        
        # --- Step 4: Save JSON summary ---
        summary = {
            "request_number": request_number,
            "timestamp": timestamp,
            "model": model,
            "prompt_variant": prompt_variant,
            "total_tokens": total_tokens,
            "avg_prob": avg_prob,
            "perplexity": perplexity,
            "struct_metrics": {**struct_metrics},
            "semantic_metrics": {**semantic_metrics},
            "execution_metrics": {**execution_metrics},
            "total_credibility": credibility
        }
        
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=4, ensure_ascii=False)
            
        processed_count += 1
        print(f"  Processed: {run_dir.relative_to(search_path)}")


    print("\n--- Analysis Run Complete ---")
    print(f"Successfully processed: {processed_count} reports")
    print(f"Skipped (already exist): {skipped_count}")
    print(f"Total found: {len(raw_json_files)}")


if __name__ == "__main__":
    config = load_config()
    if config:
        analyze_all_reports(config)

