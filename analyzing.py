import math
import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from Functions.plot_llm_confidence import plot_llm_confidence
from Functions.compute_code_structure_metrics import compute_code_structure_metrics
from Functions.compute_code_semantic_metrics import compute_code_semantic_metrics
from Functions.compute_code_execution_metrics import compute_code_execution_metrics
from Functions.compute_credibility import compute_credibility

# Configuration
results_root = Path("Reports/gpt-4o-mini")

for i in range(10, 11):
    # --- Step 0: Get request number 
    request_number = i
    run_dir = results_root / f"report_{request_number}"
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
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
    supports_logprobs = raw_data["supports_logprobs"]
    
    print(f"Loaded response from {timestamp}")
    print(f"Model: {model}")
    print(f"Code length: {len(code)} characters")
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 2: Analyze log probabilities ---
    tokens_info = []
    cumulative_logprob = 0.0
    for idx, item in enumerate(logprobs_data):
        token = item.get("token")
        logp = item.get("logprob", float("-inf"))
        prob = math.exp(logp) if math.isfinite(logp) else 0.0
        cumulative_logprob += logp if math.isfinite(logp) else 0.0
        tokens_info.append({
            "index": idx + 1,
            "token": token,
            "logprob": logp,
            "probability": prob,
            "cumulative_logprob": cumulative_logprob
        })
    
    total_logprob = sum((it.get("logprob", 0.0) for it in logprobs_data)) if logprobs_data else 0.0
    total_tokens = len(logprobs_data) if logprobs_data else len(code.split())
    avg_logprob = (total_logprob / total_tokens) if total_tokens else 0.0
    avg_prob = math.exp(avg_logprob) if math.isfinite(avg_logprob) else 0.0
    perplexity = math.exp(-avg_logprob) if math.isfinite(avg_logprob) else float("inf")
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 3: Compute metrics ---
    print("Computing metrics...")
    
    # Structural & length metrics
    struct_metrics = compute_code_structure_metrics(code, logprobs_data)
    
    # Semantic quality metrics
    semantic_metrics = compute_code_semantic_metrics(code)
    
    # Execution-based metrics
    execution_metrics = compute_code_execution_metrics(code)
    
    # Total credibility
    credibility = compute_credibility(struct_metrics, semantic_metrics, execution_metrics, avg_prob, perplexity)
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 4: Save plots ---
    print("Generating plots...")
    plot_llm_confidence(logprobs_data, output_dir=run_dir, title_prefix="Python Code Generation", code_text=code)
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 5: Save detailed data ---
    print("Saving analysis results...")
    
    # JSON (for reloading in Python)
    with open(run_dir / "tokens.json", "w", encoding="utf-8") as f:
        json.dump(tokens_info, f, indent=4, ensure_ascii=False)
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 6: Generate HTML report ---
    print("Generating HTML report...")
    
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("report_template.html")
    
    html_report = template.render(
        request_number=request_number,
        timestamp=timestamp,
        model=model,
        supports_logprobs=supports_logprobs,
        stories_text=stories_text,
        prompt=prompt,
        code=code,
        total_tokens=total_tokens,
        total_logprob=total_logprob,
        avg_prob=avg_prob,
        perplexity=perplexity,
        struct_metrics=struct_metrics,
        semantic_metrics=semantic_metrics,
        execution_metrics=execution_metrics,
        credibility=credibility
    )
    
    # Write report to file
    report_path = run_dir / "report.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_report)
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 7: Save JSON summary ---
    summary = {
        "request_number": request_number,
        "timestamp": timestamp,
        "model": model,
        "total_tokens": total_tokens,
        "total_logprob": total_logprob,
        "avg_prob": avg_prob,
        "perplexity": perplexity,
        **struct_metrics,
        **semantic_metrics,
        **execution_metrics,
        "total_credibility": credibility
    }
    with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, ensure_ascii=False)
    
    print(f"\n✅ Analysis complete! Report saved in folder: {run_dir}")
    print("   - tokens.json")
    print("   - report.html")
    print("   - summary.json")