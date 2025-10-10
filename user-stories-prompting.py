from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import math
import datetime
import json
import csv
from pathlib import Path
from plot_llm_confidence import plot_llm_confidence
import ast
import statistics

# Load environment variables
load_dotenv()

# Configuration
model = "qwen/qwen3-coder-30b-a3b-instruct"
results_root = Path("Reports/qwen3")
results_root.mkdir(exist_ok=True)

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
            ]
        }
    }
)

# --- Step 1: Read user stories ---
with open("data.txt", "r", encoding="utf-8") as f:
    user_stories = [line.strip() for line in f if line.strip()]

# --- Step 2: Select a few stories ---
sample_stories = user_stories  # [:50]
stories_text = "\n".join(sample_stories)

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

# --- Helper: compute structure/length/token metrics ---
def compute_code_structure_metrics(code_text, logprobs_data):
    """
    Returns dictionary with:
      - token_count
      - function_count
      - class_count
      - num_lines, num_nonempty_lines
      - avg_line_len_all_chars, avg_line_len_nonempty_chars
      - avg_tokens_per_nonempty_line
      - ast_depth
      - import_count
      - import_names (list)
      - per_function_cyclomatic (dict: func_name -> int)
      - avg_cyclomatic_complexity
      - max_cyclomatic_complexity
      - module_cyclomatic_complexity
    """
    # tokens
    token_count = len(logprobs_data) if logprobs_data is not None else len(code_text.split())

    # default values if parse fails
    func_count = class_count = 0
    num_lines = num_nonempty = 0
    avg_line_len_all = avg_line_len_nonempty = 0.0
    avg_tokens_per_line = 0.0

    ast_depth = 0
    import_count = 0
    import_names = []
    per_function_cc = {}
    avg_cc = 0.0
    max_cc = 0.0
    module_cc = 0.0

    # basic line metrics
    lines = code_text.splitlines()
    non_empty_lines = [ln for ln in lines if ln.strip()]
    num_lines = len(lines)
    num_nonempty = len(non_empty_lines)

    if num_lines:
        avg_line_len_all = sum(len(ln) for ln in lines) / num_lines
    if num_nonempty:
        avg_line_len_nonempty = sum(len(ln) for ln in non_empty_lines) / num_nonempty
        tokens_per_line = [len(ln.split()) for ln in non_empty_lines]

        avg_tokens_per_line = statistics.mean(tokens_per_line) if tokens_per_line else 0.0

    # AST parsing and advanced metrics
    try:
        tree = ast.parse(code_text)
        func_count = sum(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) for n in ast.walk(tree))
        class_count = sum(isinstance(n, ast.ClassDef) for n in ast.walk(tree))
    except SyntaxError:
        tree = None

    # AST depth (max nesting)
    def _ast_max_depth(node):
        max_child = 0
        for child in ast.iter_child_nodes(node):
            d = _ast_max_depth(child)
            if d > max_child:
                max_child = d
        return max_child + 1

    if tree is not None:
        try:
            ast_depth = _ast_max_depth(tree)
        except Exception:
            ast_depth = 0

        # Imports
        import_modules = set()
        import_nodes = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                import_nodes += 1
                for alias in node.names:
                    import_modules.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                import_nodes += 1
                if node.module:
                    import_modules.add(node.module.split('.')[0])
        import_count = import_nodes
        import_names = sorted([m for m in import_modules if m])

        # Cyclomatic complexity: try radon first, fallback to heuristic
        try:
            from radon.complexity import cc_visit  # radon must be installed for this block
            cc_results = cc_visit(code_text)
            # cc_visit returns CCResult objects with .name and .complexity
            per_function_cc = {r.name: int(r.complexity) for r in cc_results}
            cc_values = list(per_function_cc.values())
            avg_cc = float(statistics.mean(cc_values)) if cc_values else 0.0
            max_cc = int(max(cc_values)) if cc_values else 0
            # radon also may include module-level entries; compute module_cc approx as sum or take top-level entry:
            module_cc = int(sum(cc_values)) if cc_values else 0
        except Exception:
            # fallback heuristic: CC = 1 + number of branching constructs in the scope
            def _compute_cc_for_node(node):
                branch_nodes = (
                    ast.If, ast.For, ast.While, ast.AsyncFor, ast.With,
                    ast.Try, ast.ExceptHandler, ast.IfExp, ast.Match
                )
                count = 0
                for n in ast.walk(node):
                    if isinstance(n, branch_nodes):
                        count += 1
                # Boolean ops: each additional operand increases complexity (a and b and c -> +2)
                bool_extra = sum((len(n.values) - 1) for n in ast.walk(node) if isinstance(n, ast.BoolOp))
                count += bool_extra
                # comprehensions add branches
                count += sum(1 for n in ast.walk(node) if isinstance(n, ast.comprehension))
                return 1 + count

            # compute per-function CC
            per_function_cc = {}
            for func in (n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
                try:
                    per_function_cc[func.name] = int(_compute_cc_for_node(func))
                except Exception:
                    per_function_cc[func.name] = 1

            cc_values = list(per_function_cc.values())
            avg_cc = float(statistics.mean(cc_values)) if cc_values else 0.0
            max_cc = int(max(cc_values)) if cc_values else 0
            # module-level CC (apply heuristic to whole module)
            try:
                module_cc = int(_compute_cc_for_node(tree))
            except Exception:
                module_cc = 0

    # Build result
    return {
        "token_count": token_count,
        "function_count": func_count,
        "class_count": class_count,
        "num_lines": num_lines,
        "num_nonempty_lines": num_nonempty,
        "avg_line_len_all_chars": avg_line_len_all,
        "avg_line_len_nonempty_chars": avg_line_len_nonempty,
        "avg_tokens_per_nonempty_line": avg_tokens_per_line,
        "ast_depth": ast_depth,
        "import_count": import_count,
        "import_names": import_names,
        "per_function_cyclomatic": per_function_cc,
        "avg_cyclomatic_complexity": avg_cc,
        "max_cyclomatic_complexity": max_cc,
        "module_cyclomatic_complexity": module_cc,
    }


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

# --- NEW: structural & length metrics ---
struct_metrics = compute_code_structure_metrics(code, logprobs_data)

# --- Step 7: Save plots ---
plot_llm_confidence(logprobs_data, output_dir=run_dir, title_prefix="Python Code Generation")


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
        .metrics table {{ width: 100%; border-collapse: collapse; }}
        .metrics td {{ padding: 6px 8px; border-bottom: 1px solid #eee; }}
    </style>
</head>
<body>
    <h1>LLM Code Generation Report #{request_number}</h1>
    <p><b>Timestamp:</b> {timestamp}</p>
    <p><b>Model:</b> {model}</p>
    <p><b>Logprobs available:</b> {supports_logprobs}</p>

    <h2>Selected User Stories</h2>
    <pre>{stories_text}</pre>

    <h2>Prompt Sent to LLM</h2>
    <pre>{prompt}</pre>

    <h2>Generated Code</h2>
    <pre>{code}</pre>

    <h2>Confidence & Basic Metrics</h2>
    <div class="metrics">
        <table>
            <tr><td><b>Total tokens</b></td><td>{total_tokens}</td></tr>
            <tr><td><b>Total log-probability</b></td><td>{total_logprob:.3f}</td></tr>
            <tr><td><b>Average per-token probability</b></td><td>{avg_prob:.2%}</td></tr>
            <tr><td><b>Perplexity</b></td><td>{perplexity:.2f}</td></tr>
        </table>
    </div>

    <h2>Code Structure & Length Metrics</h2>
    <div class="metrics">
        <table>
            <tr><td><b>Token count (source)</b></td><td>{struct_metrics["token_count"]}</td></tr>
            <tr><td><b>Function count (AST)</b></td><td>{struct_metrics["function_count"]}</td></tr>
            <tr><td><b>Class count (AST)</b></td><td>{struct_metrics["class_count"]}</td></tr>
            <tr><td><b>Number of lines</b></td><td>{struct_metrics["num_lines"]}</td></tr>
            <tr><td><b>Non-empty lines</b></td><td>{struct_metrics["num_nonempty_lines"]}</td></tr>
            <tr><td><b>Avg line length (all lines, chars)</b></td><td>{struct_metrics["avg_line_len_all_chars"]:.1f}</td></tr>
            <tr><td><b>Avg line length (non-empty, chars)</b></td><td>{struct_metrics["avg_line_len_nonempty_chars"]:.1f}</td></tr>
            <tr><td><b>Avg tokens per non-empty line</b></td><td>{struct_metrics["avg_tokens_per_nonempty_line"]:.2f}</td></tr>
            <tr><td><b>AST depth (max nesting)</b></td><td>{struct_metrics["ast_depth"]}</td></tr>
            <tr><td><b>Import count</b></td><td>{struct_metrics["import_count"]}</td></tr>
            <tr><td><b>Import names</b></td><td>{", ".join(struct_metrics["import_names"])}</td></tr>
            <tr><td><b>Avg cyclomatic complexity (functions)</b></td><td>{struct_metrics["avg_cyclomatic_complexity"]:.2f}</td></tr>
            <tr><td><b>Max cyclomatic complexity (functions)</b></td><td>{struct_metrics["max_cyclomatic_complexity"]}</td></tr>
            <tr><td><b>Module cyclomatic complexity</b></td><td>{struct_metrics["module_cyclomatic_complexity"]}</td></tr>
        </table>
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
    # structured metrics
    **struct_metrics
}
with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=4, ensure_ascii=False)

# Save raw code separately
with open(run_dir / "generated_code.py", "w", encoding="utf-8") as f:
    f.write(code)

print(f"\n✅ Report saved in folder: {run_dir}")
