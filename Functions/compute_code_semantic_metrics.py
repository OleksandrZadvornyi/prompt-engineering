import ast
import tempfile
import subprocess
import os

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
