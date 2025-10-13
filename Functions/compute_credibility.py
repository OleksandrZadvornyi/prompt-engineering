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