import statistics
import ast

def compute_code_structure_metrics(code_text):
    """
    Calculates key objective metrics to assess code structure and maintainability.

    Returns a dictionary with:
      - avg_cyclomatic_complexity (float): The average logical complexity per function.
      - ast_depth (int): The maximum nesting level of the code.
      - avg_function_size_lines (float): The average lines of code per function.
      - import_redundancy_ratio (float): The ratio of duplicate imports.
    """

    # Check if syntax is valid
    try:
        tree = ast.parse(code_text)
    except SyntaxError:
        return {
            "avg_cyclomatic_complexity": -1.0,
            "ast_depth": -1,
            "avg_function_size_lines": -1.0,
            "import_redundancy_ratio": -1.0,
        }

    # --- 1. Get all function nodes from the AST ---
    func_nodes = [
        n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    
    # --- 2. Calculate average function size ---
    func_sizes = []
    for fn in func_nodes:
        start = getattr(fn, "lineno", None)
        end = getattr(fn, "end_lineno", None)
        if start is not None and end is not None:
            func_sizes.append(end - start + 1)
    avg_function_size = statistics.mean(func_sizes) if func_sizes else 0.0
    
    # --- 3. Calculate AST depth ---
    def _depth(node):
        return 1 + max((_depth(ch) for ch in ast.iter_child_nodes(node)), default=0)
    ast_depth = _depth(tree)
    
    # --- 4. Calculate import redundancy ---
    import_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                import_modules.append(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            import_modules.append(node.module.split(".")[0])
    
    import_count = len(import_modules)
    if import_count > 0:
        duplicates = import_count - len(set(import_modules))
        import_redundancy_ratio = duplicates / import_count
    else:
        import_redundancy_ratio = 0.0
    
    # --- 5. Calculate Average Cyclomatic Complexity ---
    avg_cc = 0.0
    try:
        from radon.complexity import cc_visit
        cc_results = cc_visit(code_text)
        cc_values = [r.complexity for r in cc_results]
        avg_cc = statistics.mean(cc_values) if cc_values else 0.0
    except ImportError:
        # Fallback heuristic if radon is not installed
        def _cc_heuristic(node):
            branches = (ast.If, ast.For, ast.While, ast.Try, ast.ExceptHandler)
            return 1 + sum(1 for n in ast.walk(node) if isinstance(n, branches))
        
        cc_values = [_cc_heuristic(fn) for fn in func_nodes]
        avg_cc = statistics.mean(cc_values) if cc_values else 0.0
        
    return {
        "avg_cyclomatic_complexity": avg_cc,
        "ast_depth": ast_depth,
        "avg_function_size_lines": avg_function_size,
        "import_redundancy_ratio": import_redundancy_ratio,
    }