import statistics
import ast

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