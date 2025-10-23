def _normalize_score(value: float, ideal_max: float) -> float:
    """
    Normalizes a metric to a score from 0.0 to 1.0.
    A value of 0 gets a score of 1.0 (best).
    A value of ideal_max or higher gets a score of 0.0 (worst).
    The score decreases linearly in between.
    """
    if ideal_max <= 0:
        return 1.0 if value == 0 else 0.0
    
    score = 1.0 - min(float(value) / float(ideal_max), 1.0)
    return max(0.0, score)


def compute_credibility(
    struct_metrics: dict,
    semantic_metrics: dict,
    execution_metrics: dict,
    avg_prob: float,
    perplexity: float,
    *,
    weights: dict = None,
    timeout_seconds: float = 15.0
) -> float:
    """
    Computes a credibility score from 0 to 100 based on aggregated metrics.
    """
    # Use empty dicts as a safe default to simplify .get() calls
    struct_metrics = struct_metrics or {}
    semantic_metrics = semantic_metrics or {}
    execution_metrics = execution_metrics or {}

    # --- 1. Gating: If syntax is invalid, credibility is 0 ---
    if not semantic_metrics.get("syntax_valid", True):
        print("Syntax is invalid, credibility score is 0.")
        return 0.0

    # --- 2. Sub-score Calculations (each from 0.0 to 1.0) ---

    # Confidence Score (based on model's own certainty)
    p_norm = _normalize_score(max(0, perplexity - 10), 40) # Perplexity is good up to 10
    confidence_score = 0.6 * (avg_prob or 0) + 0.4 * p_norm

    # Structure Score (how well-designed is the code?)
    s_weights = {"cc": 0.4, "depth": 0.3, "import": 0.15, "fsize": 0.15}
    cc_score = _normalize_score(struct_metrics.get("avg_cyclomatic_complexity", 0), 10)
    depth_score = _normalize_score(struct_metrics.get("ast_depth", 0), 6)
    fsize_score = _normalize_score(struct_metrics.get("avg_function_size_lines", 0), 30)
    import_score = 1.0 - struct_metrics.get("import_redundancy_ratio", 0)
    
    structure_score = (
        s_weights["cc"] * cc_score +
        s_weights["depth"] * depth_score +
        s_weights["import"] * import_score +
        s_weights["fsize"] * fsize_score
    )

    # Semantic Score (is the code stylistically correct?)
    flake_score = _normalize_score(semantic_metrics.get("flake8_error_count", 0), 30)
    mypy_score = _normalize_score(semantic_metrics.get("mypy_error_count", 0), 10)
    semantic_score = 0.6 * flake_score + 0.4 * mypy_score

    # Execution Score (does the code run correctly and efficiently?)
    if execution_metrics.get("execution_success"):
        time_taken = execution_metrics.get("execution_time_sec", 0)
        time_score = _normalize_score(time_taken, timeout_seconds)
        # 0.5 base score for succeeding, plus a bonus up to 0.5 for speed
        execution_score = 0.5 + 0.5 * time_score
    else:
        execution_score = 0.0

    # --- 3. Final Weighted Aggregation ---
    if weights is None:
        weights = {"confidence": 0.10, "structure": 0.25, "semantic": 0.30, "execution": 0.35}
    
    total_w = sum(weights.values()) or 1.0
    
    final_score = (
        (weights["confidence"] * confidence_score) +
        (weights["structure"] * structure_score) +
        (weights["semantic"] * semantic_score) +
        (weights["execution"] * execution_score)
    ) / total_w

    # --- START: Added Print Commands ---
    print("\n--- Credibility Calculation ---")
    print(f"  - Confidence: {confidence_score:.2f} (weight: {weights['confidence']:.2f})")
    print(f"  - Structure:  {structure_score:.2f} (weight: {weights['structure']:.2f})")
    print(f"  - Semantic:   {semantic_score:.2f} (weight: {weights['semantic']:.2f})")
    print(f"  - Execution:  {execution_score:.2f} (weight: {weights['execution']:.2f})")
    print("-" * 31)
    print(f"--- Final Score: {final_score * 100:.2f} ---\n")
    # --- END: Added Print Commands ---

    return round(final_score * 100, 2)