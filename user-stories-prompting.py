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
import subprocess
import tempfile
import os
from jinja2 import Environment, FileSystemLoader
import time
import textwrap
import docker

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
request_number = 3
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

def compute_code_execution_metrics(code_text, timeout_sec=5):
    """
    Safely executes generated Python code in a Docker container.
    Returns:
      - execution_success (bool)
      - execution_time_sec (float)
      - exception_type (str)
      - exception_message (str)
      - runtime_output (str)
    """
    # Create a temporary file for the code
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as tmp:
        tmp.write(code_text)
        tmp_path = os.path.abspath(tmp.name)  # Use absolute path for Windows compatibility

    start_time = time.time()
    execution_success = False
    exec_time = 0.0
    exception_type = ""
    exception_message = ""
    runtime_output = ""

    try:
        # Connect to Docker (uses environment defaults, works on Windows)
        client = docker.from_env()

        # Run the container with isolation:
        # - Image: python:3.11-slim (lightweight Python 3.11)
        # - Command: Run the mounted script
        # - Volumes: Mount the temp file read-only
        # - Network: Disabled to prevent network access
        # - Memory/CPU limits: Prevent resource abuse
        # - Detach: True to run in background
        # - Remove: True to auto-cleanup after exit
        container = client.containers.run(
            "python:3.11-slim",
            ["python", "/app/code.py"],  # Command to execute the script
            volumes={tmp_path: {"bind": "/app/code.py", "mode": "ro"}},  # Mount Windows path to container path
            network_disabled=True,  # No network access
            mem_limit="100m",  # 100 MB memory limit
            cpu_period=100000,  # CPU quota (with quota below, limits to ~0.5 CPU)
            cpu_quota=50000,
            detach=True,
            remove=True  # Auto-remove container after stop
        )

        # Wait for container to finish or timeout
        try:
            result = container.wait(timeout=timeout_sec)
            exec_time = round(time.time() - start_time, 3)
            execution_success = result["StatusCode"] == 0
        except Exception as e:  # Handles timeout
            container.kill()  # Force stop if timed out
            exec_time = timeout_sec
            exception_type = "TimeoutError"
            exception_message = f"Execution exceeded {timeout_sec}s limit"

        # Get logs (stdout + stderr)
        logs = container.logs().decode("utf-8").strip()

        if not execution_success:
            # Parse exception from logs if possible
            if "Traceback" in logs:
                exception_lines = logs.splitlines()
                last_line = exception_lines[-1] if exception_lines else ""
                if ":" in last_line:
                    exception_type = last_line.split(":")[0].strip()
                    exception_message = ":".join(last_line.split(":")[1:]).strip()
                else:
                    exception_type = "UnknownError"
                    exception_message = logs
            else:
                exception_type = "RuntimeError"
                exception_message = logs

        # Preview first 10 lines of output, shortened
        stdout_preview = "\n".join(logs.splitlines()[:10])
        runtime_output = textwrap.shorten(stdout_preview, width=300, placeholder="...")

    except docker.errors.DockerException as e:
        exec_time = round(time.time() - start_time, 3)
        exception_type = type(e).__name__
        exception_message = str(e)
    finally:
        # Clean up temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    return {
        "execution_success": execution_success,
        "execution_time_sec": exec_time,
        "exception_type": exception_type,
        "exception_message": exception_message,
        "runtime_output": runtime_output
    }

def compute_code_semantic_metrics(code_text):
    """
    Analyzes semantic quality of the generated code.

    Returns:
      - syntax_valid (bool)
      - flake8_error_count (int)
      - flake8_error_breakdown (dict)
      - mypy_error_count (int)
      - mypy_error_breakdown (dict)
      - semantic_quality_score (float 0–100)
    """

    # --- 1. Syntax check ---
    try:
        ast.parse(code_text)
        syntax_valid = True
    except SyntaxError:
        syntax_valid = False

    # Create a temporary file to run flake8 and mypy on
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
        tmp.write(code_text)
        tmp_path = tmp.name

    # --- 2. Run flake8 for style & lint issues (categorized) ---
    try:
        result = subprocess.run(
            ["flake8", tmp_path, "--ignore=E501"],  # ignore long lines
            capture_output=True, text=True
        )
        lines = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
        # Initialize counters by category
        flake8_counts = {"E": 0, "F": 0, "W": 0, "C": 0, "N": 0, "total": 0}
    
        for line in lines:
            # Typical line: tmp.py:8:1: E302 expected 2 blank lines, found 1
            parts = line.split()
            if len(parts) >= 2:
                code = parts[1]  # E302, F401, etc.
                prefix = code[0].upper()
                if prefix in flake8_counts:
                    flake8_counts[prefix] += 1
                else:
                    flake8_counts[prefix] = 1  # catch other prefixes
                flake8_counts["total"] += 1
    
        flake8_error_count = flake8_counts["total"]
    except Exception as e:
        print("Flake8 error:", e)
        flake8_counts = {"E": -1, "F": -1, "W": -1, "C": -1, "N": -1, "total": -1}
        flake8_error_count = -1

    # --- 3. Run mypy and categorize errors ---
    mypy_error_count = 0
    mypy_breakdown = {
        "return_type": 0,
        "argument_type": 0,
        "missing_return": 0,
        "attribute_error": 0,
        "annotation_issue": 0,
        "other": 0,
        "total": 0,
    }

    try:
        result = subprocess.run(
            ["mypy", "--ignore-missing-imports", "--no-color-output", tmp_path],
            capture_output=True, text=True
        )
        lines = [ln.strip() for ln in result.stdout.splitlines() if "error:" in ln]

        for line in lines:
            mypy_error_count += 1
            msg = line.lower()

            # categorize based on message keywords
            if "return value" in msg or "return type" in msg:
                mypy_breakdown["return_type"] += 1
            elif "argument" in msg or "positional" in msg:
                mypy_breakdown["argument_type"] += 1
            elif "missing return" in msg:
                mypy_breakdown["missing_return"] += 1
            elif "has no attribute" in msg:
                mypy_breakdown["attribute_error"] += 1
            elif "annotation" in msg or "untyped" in msg:
                mypy_breakdown["annotation_issue"] += 1
            else:
                mypy_breakdown["other"] += 1

        mypy_breakdown["total"] = mypy_error_count
    except Exception as e:
        print("Mypy error:", e)
        mypy_breakdown = {
            "return_type": -1,
            "argument_type": -1,
            "missing_return": -1,
            "attribute_error": -1,
            "annotation_issue": -1,
            "other": -1,
            "total": -1,
        }
        mypy_error_count = -1

    # --- 4. Compute composite semantic score ---
    # Base score out of 100
    score = 100.0
    if not syntax_valid:
        score -= 50  # major penalty
    if flake8_error_count > 0:
        score -= min(20, flake8_error_count * 0.5)
    if mypy_error_count > 0:
        score -= min(20, mypy_error_count * 1.0)
    score = max(0.0, score)

    # Cleanup temp file
    try:
        os.remove(tmp_path)
    except OSError:
        pass

    return {
        "syntax_valid": syntax_valid,
        "flake8_error_count": flake8_error_count,
        "flake8_error_breakdown": flake8_counts,
        "mypy_error_count": mypy_error_count,
        "mypy_error_breakdown": mypy_breakdown,
        "semantic_quality_score": round(score, 2),
    }


# --- Helper: compute structure/length/token metrics ---
def compute_code_structure_metrics(code_text, logprobs_data):
    """
    Returns dictionary with:
      - token_count, function_count, class_count
      - num_lines, num_nonempty_lines
      - avg_line_len_all_chars, avg_line_len_nonempty_chars, avg_tokens_per_nonempty_line
      - ast_depth
      - import_count, import_names
      - per_function_cyclomatic, avg_cyclomatic_complexity, max_cyclomatic_complexity, module_cyclomatic_complexity
      - avg_function_size_lines
      - comment_density_percent
      - import_redundancy_ratio
    """

    token_count = len(logprobs_data) if logprobs_data is not None else len(code_text.split())
    lines = code_text.splitlines()
    non_empty_lines = [ln for ln in lines if ln.strip()]
    num_lines = len(lines)
    num_nonempty = len(non_empty_lines)

    avg_line_len_all = sum(len(ln) for ln in lines) / num_lines if num_lines else 0.0
    avg_line_len_nonempty = sum(len(ln) for ln in non_empty_lines) / num_nonempty if num_nonempty else 0.0
    tokens_per_line = [len(ln.split()) for ln in non_empty_lines]
    avg_tokens_per_line = statistics.mean(tokens_per_line) if tokens_per_line else 0.0

    # --- Count comment lines for comment density ---
    comment_lines = sum(1 for ln in lines if ln.strip().startswith("#"))
    comment_density_percent = (comment_lines / num_nonempty * 100) if num_nonempty else 0.0

    # --- AST parsing ---
    try:
        tree = ast.parse(code_text)
    except SyntaxError:
        tree = None

    func_count = class_count = 0
    ast_depth = 0
    import_count = 0
    import_names = []
    per_function_cc = {}
    avg_cc = max_cc = module_cc = 0.0
    avg_function_size = 0.0
    import_redundancy_ratio = 0.0

    if tree is not None:
        # --- Function and class counts ---
        func_nodes = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        class_nodes = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        func_count = len(func_nodes)
        class_count = len(class_nodes)

        # --- Average function size ---
        func_sizes = []
        for fn in func_nodes:
            start = getattr(fn, "lineno", None)
            end = getattr(fn, "end_lineno", None)
            if start is not None and end is not None and end >= start:
                func_sizes.append(end - start + 1)
        avg_function_size = statistics.mean(func_sizes) if func_sizes else 0.0

        # --- AST depth ---
        def _depth(node):
            return 1 + max((_depth(ch) for ch in ast.iter_child_nodes(node)), default=0)
        ast_depth = _depth(tree)

        # --- Imports and redundancy ratio ---
        import_modules = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    import_modules.append(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module:
                import_modules.append(node.module.split(".")[0])

        import_count = len(import_modules)
        import_names = sorted(set(import_modules))
        if import_count > 0:
            duplicates = import_count - len(set(import_modules))
            import_redundancy_ratio = duplicates / import_count
        else:
            import_redundancy_ratio = 0.0

        # --- Cyclomatic complexity (radon if available, fallback heuristic) ---
        try:
            from radon.complexity import cc_visit
            cc_results = cc_visit(code_text)
            per_function_cc = {r.name: int(r.complexity) for r in cc_results}
            cc_values = list(per_function_cc.values())
            avg_cc = statistics.mean(cc_values) if cc_values else 0.0
            max_cc = max(cc_values) if cc_values else 0
            module_cc = sum(cc_values)
        except Exception:
            def _cc_heuristic(node):
                branches = (
                    ast.If, ast.For, ast.While, ast.AsyncFor, ast.With,
                    ast.Try, ast.ExceptHandler, ast.IfExp, ast.Match
                )
                count = sum(1 for n in ast.walk(node) if isinstance(n, branches))
                bool_extra = sum((len(n.values) - 1) for n in ast.walk(node) if isinstance(n, ast.BoolOp))
                count += bool_extra
                return 1 + count

            per_function_cc = {fn.name: _cc_heuristic(fn) for fn in func_nodes}
            cc_values = list(per_function_cc.values())
            avg_cc = statistics.mean(cc_values) if cc_values else 0.0
            max_cc = max(cc_values) if cc_values else 0
            module_cc = _cc_heuristic(tree)

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
        "import_redundancy_ratio": import_redundancy_ratio,
        "avg_function_size_lines": avg_function_size,
        "comment_density_percent": comment_density_percent,
        "per_function_cyclomatic": per_function_cc,
        "avg_cyclomatic_complexity": avg_cc,
        "max_cyclomatic_complexity": max_cc,
        "module_cyclomatic_complexity": module_cc,
    }

def compute_credibility(
    struct_metrics: dict,
    semantic_metrics: dict,
    execution_metrics: dict,
    avg_prob: float,
    perplexity: float,
    *,
    weights: dict = None,
    timeout_seconds: float = 5.0,
    relax_execution_gate: bool = False
) -> float:
    """
    Compute a credibility score C in range [0, 100].
    Parameters:
      - struct_metrics: output of compute_code_structure_metrics()
      - semantic_metrics: output of compute_code_semantic_metrics()
      - execution_metrics: output of compute_code_execution_metrics()
      - avg_prob: average token probability (0..1)
      - perplexity: model perplexity (>=1)
      - weights: optional dict with keys 'confidence','structure','semantic','execution'
      - timeout_seconds: used to normalize execution_time_sec if present
      - relax_execution_gate: if True, do not force exec_score to 0 on failure (give small credit)
    Returns:
      - float score in [0,100] (higher = more credible)
    """

    # Default weights
    if weights is None:
        weights = {"confidence": 0.10, "structure": 0.15, "semantic": 0.40, "execution": 0.35}
    # Normalize weight sum to 1
    total_w = sum(weights.values()) or 1.0
    for k in weights:
        weights[k] = weights[k] / total_w

    # --- Semantic gating: if syntax invalid, credibility = 0 ---
    syntax_valid = semantic_metrics.get("syntax_valid") if semantic_metrics is not None else True
    if syntax_valid is False:
        return 0.0

    # --- Confidence subscore (0..1) ---
    ap = float(avg_prob) if avg_prob is not None else 0.0
    ap = max(0.0, min(1.0, ap))
    # Map perplexity: 1->1, 10->1, 50->0 (linear between 10..50)
    p = float(perplexity) if perplexity is not None else 1.0
    if p <= 10:
        p_norm = 1.0
    else:
        p_norm = 1.0 - min(max((p - 10) / 40.0, 0.0), 1.0)
    confidence = 0.6 * ap + 0.4 * p_norm
    confidence = max(0.0, min(1.0, confidence))

    # --- Structure subscore (0..1) ---
    # Extract structure metrics with safe defaults
    avg_cc = float(struct_metrics.get("avg_cyclomatic_complexity", 0.0)) if struct_metrics else 0.0
    comment_pct = float(struct_metrics.get("comment_density_percent", 0.0)) if struct_metrics else 0.0
    import_redundancy = float(struct_metrics.get("import_redundancy_ratio", 0.0)) if struct_metrics else 0.0
    avg_func_size = float(struct_metrics.get("avg_function_size_lines", 0.0)) if struct_metrics else 0.0

    # Normalize sub-components
    cc_score = 1.0 - min(avg_cc / 10.0, 1.0)            # <=10 good
    # Ideal comment density ~12.5% -> linear drop-off
    comment_score = 1.0 - min(abs(comment_pct - 12.5) / 12.5, 1.0)
    import_score = 1.0 - min(import_redundancy, 1.0)
    func_size_score = 1.0 - min(avg_func_size / 30.0, 1.0)

    # Subweights for structure components
    s_w_cc, s_w_comment, s_w_import, s_w_fsize = 0.40, 0.25, 0.20, 0.15
    structure = (
        s_w_cc * cc_score
        + s_w_comment * comment_score
        + s_w_import * import_score
        + s_w_fsize * func_size_score
    )
    structure = max(0.0, min(1.0, structure))

    # --- Semantic subscore (0..1) ---
    base_sem = float(semantic_metrics.get("semantic_quality_score", 0.0)) / 100.0 if semantic_metrics else 0.0
    flake_count = int(semantic_metrics.get("flake8_error_count", 0)) if semantic_metrics else 0
    mypy_count = int(semantic_metrics.get("mypy_error_count", 0)) if semantic_metrics else 0

    flake_score = 1.0 - min(flake_count / 50.0, 1.0)
    mypy_score = 1.0 - min(mypy_count / 10.0, 1.0)

    semantic = 0.60 * max(0.0, min(1.0, base_sem)) + 0.25 * flake_score + 0.15 * mypy_score
    semantic = max(0.0, min(1.0, semantic))

    # --- Execution subscore (0..1) ---
    exec_succ = bool(execution_metrics.get("execution_success")) if execution_metrics else False
    exec_time = float(execution_metrics.get("execution_time_sec", timeout_seconds)) if execution_metrics else timeout_seconds

    if exec_succ:
        time_norm = 1.0 - min(exec_time / float(timeout_seconds), 1.0)
        exec_score = 0.7 * time_norm + 0.3 * 1.0  # reward success + speed
    else:
        if relax_execution_gate:
            # give a small credit for non-executed runs (environment issues, etc.)
            time_norm = 1.0 - min(exec_time / float(timeout_seconds), 1.0)
            exec_score = 0.1 * time_norm
        else:
            exec_score = 0.0

    exec_score = max(0.0, min(1.0, exec_score))

    # --- Final weighted aggregation ---
    C_raw = (
        weights["confidence"] * confidence
        + weights["structure"] * structure
        + weights["semantic"] * semantic
        + weights["execution"] * exec_score
    )
    C = max(0.0, min(1.0, C_raw)) * 100.0
    return round(C, 3)



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
