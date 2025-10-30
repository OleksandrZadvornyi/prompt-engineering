import json
import yaml
import math
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import html # Changed from 'cgi'

def load_config():
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        # Fallback to current directory
        config_path = Path("config.yaml")
    
    if not config_path.exists():
        print("Error: Configuration file not found at 'config/config.yaml' or 'config.yaml'")
        return None
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def format_value(value, is_percent=False, is_error=False, reverse_error=False):
    """Formats a numeric value for the HTML table with appropriate styling."""
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return f'<td class="na">N/A</td>'

    css_class = ""
    display_value = ""

    if is_percent:
        display_value = f"{value * 100:.1f}%"
        if value == 1.0:
            css_class = "success"
        elif value >= 0.8:
            css_class = "warn"
        else:
            css_class = "fail"
    elif is_error:
        display_value = f"{value:.2f}"
        if reverse_error: # For metrics where higher is better (e.g., credibility)
            if value >= 80:
                css_class = "success"
            elif value >= 60:
                css_class = "warn"
            else:
                css_class = "fail"
        else: # For metrics where lower is better (e.g., error counts)
            if value == 0.0:
                css_class = "success"
            elif value <= 2.0:
                css_class = "warn"
            else:
                css_class = "fail"
    else:
        display_value = f"{value:.2f}"
        css_class = "neutral"

    return f'<td class="{css_class}">{display_value}</td>'

def generate_html_report(config):
    """
    Loads 'comparison_report.json' and generates a 'llm_comparison_report.html'
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    search_path = base_results_dir / "stories-to-code"
    
    json_path = search_path / "comparison_report.json"
    output_path = search_path / "llm_comparison_report.html"

    if not json_path.exists():
        print(f"Error: Input file not found: {json_path}")
        print("Please run '03_compare_summaries.py' first.")
        return
        
    print(f"Loading data from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # --- HTML Generation Starts ---
    
    html_output = [] # Renamed from 'html' to 'html_output'
    
    # 1. HTML Head and CSS
    html_output.append(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LLM Code Generation Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            background-color: #f4f7f6;
            color: #333;
            margin: 0;
            padding: 0;
        }}
        .container {{
            max-width: 1400px;
            margin: 20px auto;
            padding: 20px;
            background-color: #ffffff;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            border-radius: 10px;
        }}
        h1, h2, h3, h4 {{
            color: #2c3e50;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 8px;
        }}
        h1 {{ font-size: 2.2em; }}
        h2 {{ font-size: 1.8em; margin-top: 40px; }}
        h3 {{ font-size: 1.5em; margin-top: 30px; border-bottom: 1px solid #eee; }}
        h4 {{ font-size: 1.2em; margin-top: 25px; border-bottom: none; }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 25px;
            font-size: 0.95em;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f9f9f9;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        tbody tr:nth-child(even) {{
            background-color: #fdfdfd;
        }}
        td.metric-name {{
            font-weight: 600;
            min-width: 180px;
        }}
        td.success {{ background-color: #e6f7ec; color: #228b22; }}
        td.warn {{ background-color: #fffbe6; color: #8d6e00; }}
        td.fail {{ background-color: #fff0f0; color: #d9534f; }}
        td.neutral {{ background-color: #f8f8f8; }}
        td.na {{ color: #999; }}

        details {{
            margin-top: 15px;
            background-color: #f9f9f9;
            border: 1px solid #eee;
            border-radius: 5px;
        }}
        summary {{
            cursor: pointer;
            padding: 12px;
            font-weight: 600;
            background-color: #f1f1f1;
        }}
        details[open] > summary {{
            border-bottom: 1px solid #ddd;
        }}
        .run-details {{
            padding: 15px;
            background: #fff;
        }}
        .run-item {{
            border-bottom: 1px dashed #ccc;
            padding-bottom: 10px;
            margin-bottom: 10px;
        }}
        pre {{
            background-color: #2d2d2d;
            color: #f1f1f1;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            font-size: 0.85em;
        }}
        footer {{
            text-align: center;
            margin-top: 30px;
            font-size: 0.9em;
            color: #888;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>LLM Code Generation Report</h1>
        <footer>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</footer>
    """)
    
    # 2. Global Stats
    gs = data.get('global_stats', {})
    html_output.append(f"""
        <h2>Overall Stats (All Runs)</h2>
        <table>
            <thead>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                <tr><td class="metric-name">Total Runs</td><td>{gs.get('total_runs', 0)}</td></tr>
                <tr><td class="metric-name">Total Models Compared</td><td>{gs.get('total_models_compared', 0)}</td></tr>
                <tr><td class="metric-name">Prompt Variants Found</td><td>{', '.join(gs.get('prompt_variants_found', []))}</td></tr>
                <tr><td class="metric-name">Avg. Total Credibility</td>{format_value(gs.get('avg_total_credibility'), is_error=True, reverse_error=True)}</tr>
                <tr><td class="metric-name">Execution Success Rate</td>{format_value(gs.get('execution_success_rate'), is_percent=True)}</tr>
                <tr><td class="metric-name">Syntax Valid Rate</td>{format_value(gs.get('syntax_valid_rate'), is_percent=True)}</tr>
                <tr><td class="metric-name">Avg. Flake8 Errors</td>{format_value(gs.get('avg_flake8_error_count'), is_error=True)}</tr>
                <tr><td class="metric-name">Avg. Mypy Errors</td>{format_value(gs.get('avg_mypy_error_count'), is_error=True)}</tr>
            </tbody>
        </table>
    """)
    
    # --- NEW SECTION: Master Comparison Table ---
    
    # Define the metrics we want to show in the comparison table
    metrics_to_display = [
        # (Display Name, JSON Key, is_percent, is_error, reverse_error)
        ("Avg. Credibility", "avg_total_credibility", False, True, True),
        ("Exec. Success Rate", "execution_success_rate", True, False, False),
        ("Syntax Valid Rate", "syntax_valid_rate", True, False, False),
        ("Avg. Flake8 Errors", "avg_flake8_error_count", False, True, False),
        ("Avg. Mypy Errors", "avg_mypy_error_count", False, True, False),
        ("Avg. Exec. Time (Success)", "avg_execution_time_sec_on_success", False, False, False),
        ("Avg. Cyclomatic Complexity", "avg_cyclomatic_complexity", False, False, False),
        ("Avg. AST Depth", "avg_ast_depth", False, False, False),
        ("Avg. Tokens", "avg_total_tokens", False, False, False),
        ("Avg. Perplexity", "avg_perplexity", False, False, False),
        ("Total Runs", "total_runs", False, False, False),
    ]
    
    # Collect all model/variant combinations
    all_combinations = []
    for model_name, model_data in data.get('models', {}).items():
        for variant_name, variant_data in model_data.get('prompt_variants', {}).items():
            all_combinations.append({
                "model": model_name,
                "variant": variant_name,
                "stats": variant_data.get('summary_stats', {})
            })
            
    # Sort by model, then variant
    all_combinations.sort(key=lambda x: (x['model'], x['variant']))

    if all_combinations:
        html_output.append("<h2>Master Comparison Table</h2>")
        html_output.append("<table>")
        
        # Table Header
        html_output.append("<thead><tr><th class=\"metric-name\">Metric</th>")
        for c in all_combinations:
            combo_name = f"{c['model']} / {c['variant']}"
            html_output.append(f"<th>{html.escape(combo_name)}</th>")
        html_output.append("</tr></thead>")
        
        # Table Body
        html_output.append("<tbody>")
        for display_name, key, is_perc, is_err, rev_err in metrics_to_display:
            html_output.append(f"<tr><td class=\"metric-name\">{display_name}</td>")
            for c in all_combinations:
                value = c['stats'].get(key)
                html_output.append(format_value(value, is_percent=is_perc, is_error=is_err, reverse_error=rev_err))
            html_output.append("</tr>")
        html_output.append("</tbody></table>")
    
    # --- END NEW SECTION ---

    # 3. Per-Model Deep Dive
    html_output.append("<h2>Model Deep Dive</h2>")
    
    # Define the metrics we want to show in the comparison table
    # This list is now defined above, so we can remove it from here.
    """
    metrics_to_display = [
        # (Display Name, JSON Key, is_percent, is_error, reverse_error)
        ("Avg. Credibility", "avg_total_credibility", False, True, True),
        ("Exec. Success Rate", "execution_success_rate", True, False, False),
        ("Syntax Valid Rate", "syntax_valid_rate", True, False, False),
        ("Avg. Flake8 Errors", "avg_flake8_error_count", False, True, False),
        ("Avg. Mypy Errors", "avg_mypy_error_count", False, True, False),
        ("Avg. Exec. Time (Success)", "avg_execution_time_sec_on_success", False, False, False),
        ("Avg. Cyclomatic Complexity", "avg_cyclomatic_complexity", False, False, False),
        ("Avg. AST Depth", "avg_ast_depth", False, False, False),
        ("Avg. Tokens", "avg_total_tokens", False, False, False),
        ("Avg. Perplexity", "avg_perplexity", False, False, False),
        ("Total Runs", "total_runs", False, False, False),
    ]
    """

    for model_name, model_data in data.get('models', {}).items():
        html_output.append(f"<h3>Model: {model_name}</h3>")
        
        variants_data = model_data.get('prompt_variants', {})
        variant_names = sorted(variants_data.keys())
        
        if not variant_names:
            html_output.append("<p>No prompt variant data found for this model.</p>")
            continue

        # --- Variant Comparison Table ---
        html_output.append("<h4>Prompt Variant Comparison</h4>")
        html_output.append("<table>")
        
        # Table Header
        html_output.append("<thead><tr><th class=\"metric-name\">Metric</th>")
        for v_name in variant_names:
            html_output.append(f"<th>{html.escape(v_name)}</th>") # This now correctly uses the 'html' module
        html_output.append("</tr></thead>")
        
        # Table Body
        html_output.append("<tbody>")
        for display_name, key, is_perc, is_err, rev_err in metrics_to_display:
            html_output.append(f"<tr><td class=\"metric-name\">{display_name}</td>")
            for v_name in variant_names:
                stats = variants_data.get(v_name, {}).get('summary_stats', {})
                value = stats.get(key)
                html_output.append(format_value(value, is_percent=is_perc, is_error=is_err, reverse_error=rev_err))
            html_output.append("</tr>")
        html_output.append("</tbody></table>")

        # --- Individual Runs Details ---
        html_output.append(f"""
            <details>
                <summary>Show/Hide All Individual Runs for {html.escape(model_name)}</summary>
                <div class="run-details">
        """)
        
        for v_name in variant_names:
            html_output.append(f"<h4>Variant: {html.escape(v_name)}</h4>")
            runs = variants_data.get(v_name, {}).get('all_runs', [])
            if not runs:
                html_output.append("<p>No individual runs found for this variant.</p>")
                continue
                
            for run in runs:
                exec_metrics = run.get('execution_metrics', {})
                exec_success = exec_metrics.get('execution_success', False)
                exec_class = "success" if exec_success else "fail"
                
                html_output.append(f"""
                <div class="run-item">
                    <strong>Request Number:</strong> {run.get('request_number', 'N/A')} |
                    <strong>Timestamp:</strong> {run.get('timestamp', 'N/A')} |
                    <strong>Credibility:</strong> {run.get('total_credibility', 0):.2f} |
                    <strong>Exec. Success:</strong> <span class="{exec_class}">{exec_success}</span>
                """)
                
                if not exec_success:
                    html_output.append(f"""
                    <pre><strong>Exception:</strong> {html.escape(exec_metrics.get('exception_type', 'N/A'))}
{html.escape(exec_metrics.get('exception_message', 'No message'))}</pre>
                    """)
                
                html_output.append(f"""
                    <pre><strong>Runtime Output:</strong>
{html.escape(exec_metrics.get('runtime_output', 'No output'))}</pre>
                </div>
                """)
        
        html_output.append("</div></details>")

    # 4. Close HTML
    html_output.append("""
    </div>
</body>
</html>
    """)
    
    # --- Write to file ---
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(html_output)) # Changed from 'html'
        print(f"\nSuccessfully generated HTML report!")
        print(f"Report saved to: {output_path}")
    except Exception as e:
        print(f"\nError: Could not save HTML report to {output_path}. Error: {e}")


if __name__ == "__main__":
    config = load_config()
    if config:
        generate_html_report(config)



