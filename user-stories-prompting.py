from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import math
import datetime
import json
import csv
from pathlib import Path
from plot_llm_confidence import plot_llm_confidence

# Load environment variables
load_dotenv()

# Configuration
model = "openai/gpt-oss-20b"
results_root = Path("results")
results_root.mkdir(exist_ok=True)

# --- Step 0: Determine next request number ---
existing_reports = sorted([p for p in results_root.iterdir() if p.is_dir()])
request_number = len(existing_reports) + 1
run_dir = results_root / f"report_{request_number}"
run_dir.mkdir(parents=True, exist_ok=True)

# Initialize LLM
llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model=model
).bind(logprobs=True)

# --- Step 1: Read user stories ---
with open("data.txt", "r", encoding="utf-8") as f:
    user_stories = [line.strip() for line in f if line.strip()]

# --- Step 2: Select a few stories ---
sample_stories = user_stories #[:50]
stories_text = "\n".join(sample_stories)

# --- Step 3: Build the prompt ---
prompt = (
    "Write only Python code (no explanations or comments) "
    "that fulfills the following user stories:\n\n"
    f"{stories_text}\n\n"
    "Do NOT include ```python or ``` anywhere. "
    "The code should be simple, clean, and implement the described functionality as best as possible."
)

# --- Step 4: LLM call ---
msg = llm.invoke(("human", prompt))

# --- Step 5: Clean code output ---
code = msg.content.strip()
if code.startswith("```python"):
    code = code[len("```python"):].strip()
if code.endswith("```"):
    code = code[:-3].strip()

print("\n--- GENERATED PYTHON CODE ---\n")
print(code)

# --- Step 6: Analyze log probabilities ---
#logprobs_data = msg.response_metadata["logprobs"]["content"]
logprobs_meta = msg.response_metadata.get("logprobs")

if not logprobs_meta or not logprobs_meta.get("content"):
    print("\n⚠️ This model did not return logprobs. Skipping probability analysis.\n")
    logprobs_data = []
else:
    logprobs_data = logprobs_meta["content"]

supports_logprobs = bool(msg.response_metadata.get("logprobs"))
print(supports_logprobs)

# Convert to richer structure for saving 
tokens_info = [] 
cumulative_logprob = 0.0 
for idx, item in enumerate(logprobs_data): 
    token = item["token"] 
    logp = item["logprob"] 
    prob = math.exp(logp) 
    cumulative_logprob += logp 
    tokens_info.append({ 
        "index": idx + 1, 
        "token": token, 
        "logprob": logp, 
        "probability": prob, 
        "cumulative_logprob": cumulative_logprob 
    })

total_logprob = sum(item["logprob"] for item in logprobs_data)
total_tokens = len(logprobs_data)
avg_logprob = total_logprob / total_tokens
avg_prob = math.exp(avg_logprob)
perplexity = math.exp(-avg_logprob)

# --- Step 7: Save plots ---
plot_llm_confidence(logprobs_data, output_dir=run_dir, title_prefix="Python Code Generation")

# --- Step 8: Save detailed data ---
# JSON (for reloading in Python)
with open(run_dir / "tokens.json", "w", encoding="utf-8") as f:
    json.dump(tokens_info, f, indent=4)

# CSV (for spreadsheets / pandas)
with open(run_dir / "tokens.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=tokens_info[0].keys())
    writer.writeheader()
    writer.writerows(tokens_info)

# --- Step 9: Save report ---
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

html_report = f"""
<html>
<head>
    <meta charset="utf-8">
    <title>LLM Code Generation Report #{request_number}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #fafafa; }}
        h2 {{ color: #333; }}
        pre {{ background: #f3f3f3; padding: 10px; border-radius: 6px; overflow-x: auto; }}
        .metrics {{ background: #fff; padding: 10px; border-radius: 6px; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>LLM Code Generation Report #{request_number}</h1>
    <p><b>Timestamp:</b> {timestamp}</p>
    <p><b>Model:</b> {model}</p>

    <h2>Selected User Stories</h2>
    <pre>{stories_text}</pre>

    <h2>Prompt Sent to LLM</h2>
    <pre>{prompt}</pre>

    <h2>Generated Code</h2>
    <pre>{code}</pre>

    <h2>Confidence Metrics</h2>
    <div class="metrics">
        <p><b>Total tokens:</b> {total_tokens}</p>
        <p><b>Total log-probability:</b> {total_logprob:.3f}</p>
        <p><b>Average per-token probability:</b> {avg_prob:.2%}</p>
        <p><b>Perplexity:</b> {perplexity:.2f}</p>
    </div>

    <h2>Visualizations</h2>
    <img src="1_token_confidence.png" width="800"><br>
    <img src="2_logprob_trend.png" width="800"><br>
    <img src="3_probability_distribution.png" width="800"><br>
    <img src="4_cumulative_logprob.png" width="800">
</body>
</html>
"""

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
}
with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=4)

# Save raw code separately
with open(run_dir / "generated_code.py", "w", encoding="utf-8") as f:
    f.write(code)

print(f"\n✅ Report saved in folder: {run_dir}")
