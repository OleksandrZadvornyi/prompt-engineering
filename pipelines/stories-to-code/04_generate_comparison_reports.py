import json
import yaml
from pathlib import Path
import html
import statistics
from collections import defaultdict

def load_config():
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        config_path = Path("config.yaml")
    
    if not config_path.exists():
        print("Error: Configuration file not found at 'config/config.yaml' or 'config.yaml'")
        return None
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_metric_stats(values):
    """Calculates stats for a list of metric values."""
    if not values:
        return { "avg": 0, "min": 0, "max": 0, "std": 0 }
    
    # Ensure all values are numeric for statistics
    numeric_values = [float(v) for v in values if isinstance(v, (int, float, bool))]
    if not numeric_values:
        return { "avg": "N/A", "min": "N/A", "max": "N/A", "std": "N/A" }

    return {
        "avg": statistics.mean(numeric_values),
        "min": min(numeric_values),
        "max": max(numeric_values),
        "std": statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0
    }

def generate_comparison_html(model_key, prompt_variant, summary_items, output_path):
    """Generates a single HTML comparison report for a group of summaries."""
    
    # --- 1. Calculate Aggregate Statistics ---
    
    # Define which metrics to aggregate (using keys from your summary.json)
    main_metrics_to_agg = {
        "Total Credibility": [s["data"].get("total_credibility", 0) for s in summary_items],
        "Perplexity": [s["data"].get("perplexity", 0) for s in summary_items],
        "Avg. Probability": [s["data"].get("avg_prob", 0) for s in summary_items],
        "Total Tokens": [s["data"].get("total_tokens", 0) for s in summary_items],
        "Execution Success (Rate)": [s["data"].get("execution_metrics", {}).get("execution_success", False) for s in summary_items],
        "Syntax Valid (Rate)": [s["data"].get("semantic_metrics", {}).get("syntax_valid", False) for s in summary_items],
        "Avg. Flake8 Errors": [s["data"].get("semantic_metrics", {}).get("flake8_error_count", 0) for s in summary_items],
        "Avg. MyPy Errors": [s["data"].get("semantic_metrics", {}).get("mypy_error_count", 0) for s in summary_items],
    }

    stats_html = "<h3>Aggregate Statistics</h3><table>"
    stats_html += "<tr><th>Metric</th><th>Average</th><th>Min</th><th>Max</th><th>Std. Dev.</th></tr>"

    for metric_name, values in main_metrics_to_agg.items():
        stats = get_metric_stats(values)
        if stats["avg"] == "N/A":
            row = f"<tr><td>{metric_name}</td><td colspan='4'>N/A</td></tr>"
        else:
            row = f"""
            <tr>
                <td>{metric_name}</td>
                <td>{stats['avg']:.4f}</td>
                <td>{stats['min']:.4f}</td>
                <td>{stats['max']:.4f}</td>
                <td>{stats['std']:.4f}</td>
            </tr>
            """
        stats_html += row
    stats_html += "</table>"


    # --- 2. Build Detailed Runs Table ---
    details_html = "<h3>Detailed Runs</h3><table>"
    details_html += "<tr><th>Run #</th><th>Credibility</th><th>Perplexity</th><th>Exec. Success</th><th>Syntax Valid</th><th>Report Link</th></tr>"

    # Sort by run number
    try:
        sorted_items = sorted(summary_items, key=lambda x: int(x["data"].get("request_number", 0)))
    except ValueError:
         sorted_items = summary_items # Fallback if request_number is not numeric

    for item in sorted_items:
        summary = item["data"]
        link = item["link"]
        
        run_num = summary.get("request_number", "N/A")
        cred = summary.get("total_credibility", 0)
        perp = summary.get("perplexity", 0)
        exec_success = summary.get("execution_metrics", {}).get("execution_success", "N/A")
        syntax_valid = summary.get("semantic_metrics", {}).get("syntax_valid", "N/A")
        
        details_html += f"""
        <tr>
            <td>{run_num}</td>
            <td>{cred:.4f}</td>
            <td>{perp:.4f}</td>
            <td>{exec_success}</td>
            <td>{syntax_valid}</td>
            <td><a href="{link}">View Report</a></td>
        </tr>
        """
    details_html += "</table>"


    # --- 3. Create Full HTML Document ---
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comparison: {model_key} - {prompt_variant}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f9f9f9;
            color: #333;
        }}
        .container {{
            max-width: 900px;
            margin: 20px auto;
            padding: 24px;
            background-color: #ffffff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        h1, h2, h3 {{
            color: #111;
            margin-top: 1.2em;
            margin-bottom: 0.6em;
        }}
        h1 {{
            font-size: 2em;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }}
        h2 {{
            font-size: 1.5em;
            color: #555;
        }}
        h3 {{
            font-size: 1.2em;
            border-bottom: 1px solid #eee;
            padding-bottom: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        th {{
            background-color: #f7f7f7;
            font-weight: 600;
        }}
        td {{
            background-color: #fff;
        }}
        td:first-child {{
            font-weight: 500;
        }}
        a {{
            color: #005fdd;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Comparison Report</h1>
        <h2>{html.escape(model_key)}</h2>
        <p><strong>Prompt Variant:</strong> {html.escape(prompt_variant)}</p>

        {stats_html}
        
        {details_html}
        
    </div>
</body>
</html>
    """

    # --- 4. Write HTML file ---
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  ✅ Comparison report saved to: {output_path}")
    except Exception as e:
        print(f"  [ERROR] Could not write HTML file: {e}")


def main():
    """Finds all summary.json files and generates comparison HTML reports."""
    config = load_config()
    if not config:
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    search_path = base_results_dir / "stories-to-code"

    if not search_path.exists():
        print(f"Error: Search directory does not exist: {search_path}")
        return

    print(f"Scanning for 'summary.json' files in: {search_path}...")
    
    summary_json_files = list(search_path.rglob("summary.json"))
    
    if not summary_json_files:
        print("No 'summary.json' files found to compare.")
        return

    print(f"Found {len(summary_json_files)} summaries. Grouping reports...")
    
    # Group summaries by (model_key, prompt_variant)
    grouped_summaries = defaultdict(list)
    
    for summary_path in summary_json_files:
        run_dir = summary_path.parent
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                summary_data = json.load(f)
            
            # Use directory structure for reliable grouping
            model_key = run_dir.parent.parent.name
            prompt_variant = run_dir.parent.name
            
            # Link to the individual report, relative to the comparison report
            relative_report_path = f"{run_dir.name}/report.html"
            
            group_key = (model_key, prompt_variant)
            grouped_summaries[group_key].append(
                {"data": summary_data, "link": relative_report_path}
            )
                
        except Exception as e:
            print(f"  [WARN] Skipping {summary_path.relative_to(search_path)} due to error: {e}")

    print(f"\nFound {len(grouped_summaries)} groups. Generating comparison reports...")
    
    for (model_key, prompt_variant), summary_items in grouped_summaries.items():
        output_dir = search_path / model_key / prompt_variant
        output_path = output_dir / "comparison_report.html"
        
        print(f"Generating report for: {model_key} / {prompt_variant} ({len(summary_items)} runs)")
        generate_comparison_html(model_key, prompt_variant, summary_items, output_path)

    print("\n--- Comparison Report Generation Complete ---")

if __name__ == "__main__":
    main()

