from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import math
import datetime
import json
import csv
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from Functions.plot_llm_confidence import plot_llm_confidence
from Functions.compute_code_structure_metrics import compute_code_structure_metrics
from Functions.compute_code_semantic_metrics import compute_code_semantic_metrics
from Functions.compute_code_execution_metrics import compute_code_execution_metrics
from Functions.compute_credibility import compute_credibility

# Load environment variables
load_dotenv()

# Configuration
model = "qwen/qwen3-coder-30b-a3b-instruct"
results_root = Path("Reports/qwen3")
results_root.mkdir(exist_ok=True)



###################################################################
###################################################################
###################################################################



# --- Step 0: Determine next request number ---
request_number = 2
run_dir = results_root / f"report_{request_number}"
run_dir.mkdir(parents=True, exist_ok=True)

# Initialize LLM
llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model=model
).bind(
    logprobs=True,
    extra_body={
        "provider": {
            "order": [
                "nebius-ai-studio",
                "nebius"
            ]
        }
    }
)



###################################################################
###################################################################
###################################################################



# --- Step 1: Read user stories ---
with open("data.txt", "r", encoding="utf-8") as f:
    user_stories = [line.strip() for line in f if line.strip()]



###################################################################
###################################################################
###################################################################



# --- Step 2: Select a few stories ---
sample_stories = user_stories  # [:50]
stories_text = "\n".join(sample_stories)



###################################################################
###################################################################
###################################################################



# --- Step 3: Build the prompt ---
# with open("clustered_stories.json", "r", encoding="utf-8") as f:
#     clusters = json.load(f)

# structured_text = "\n\n".join(
#     f"Module {name}:\n{stories}" for name, stories in clusters.items()
# )

prompt = (
    "Generate fully functional Python code that implements the following user stories. "
    "The code should realistically reflect the described functionality.\n\n"
    f"{stories_text}\n\n"
    "Output only Python code (no markdown formatting or extra text). "
    "Do not leave functions empty — implement reasonable logic where needed."
)



###################################################################
###################################################################
###################################################################



# --- Step 4: LLM call ---
msg = llm.invoke(("human", prompt))



###################################################################
###################################################################
###################################################################



# --- Step 5: Clean code output ---
code = msg.content.strip()
if code.startswith("```python"):
    code = code[len("```python"):].strip()
if code.endswith("```"):
    code = code[:-3].strip()

print("\n--- GENERATED PYTHON CODE ---\n")
print(code)



###################################################################
###################################################################
###################################################################



# --- Step 6: Analyze log probabilities ---
logprobs_data = msg.response_metadata["logprobs"]["content"]
supports_logprobs = bool(msg.response_metadata.get("logprobs"))

# Convert to richer structure for saving 
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



# --- Step 7: Save plots ---
plot_llm_confidence(logprobs_data, output_dir=run_dir, title_prefix="Python Code Generation")



###################################################################
###################################################################
###################################################################



# --- Step 8: Save detailed data ---
# JSON (for reloading in Python)
with open(run_dir / "tokens.json", "w", encoding="utf-8") as f:
    json.dump(tokens_info, f, indent=4, ensure_ascii=False)

# CSV (for spreadsheets / pandas)
with open(run_dir / "tokens.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=tokens_info[0].keys())
    writer.writeheader()
    writer.writerows(tokens_info)

# Save report assets
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

env = Environment(loader=FileSystemLoader("."))  # current directory
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

# Also save JSON summary for easier analysis later
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

# Save raw code separately
with open(run_dir / "generated_code.py", "w", encoding="utf-8") as f:
    f.write(code)

print(f"\n✅ Report saved in folder: {run_dir}")
