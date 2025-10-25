import json
import yaml
from pathlib import Path
import html  # For escaping HTML special characters in code/prompt

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

def create_metrics_table(title, metrics_dict):
    """Helper function to create an HTML table from a metrics dictionary."""
    if not metrics_dict:
        return ""
    
    rows = ""
    for key, value in metrics_dict.items():
        if isinstance(value, float):
            value = f"{value:.4f}"
        rows += f"<tr><th>{key.replace('_', ' ').title()}</th><td>{value}</td></tr>\n"
        
    return f"""
    <h3>{title}</h3>
    <table>
        {rows}
    </table>
    """

def generate_html_report(run_dir):
    """Generates a single HTML report for a given report directory."""
    
    summary_path = run_dir / "summary.json"
    raw_path = run_dir / "raw_response.json"
    report_path = run_dir / "report.html"

    # Ensure both source files exist
    if not summary_path.exists():
        print(f"  Skipping (missing summary.json): {run_dir}")
        return
    if not raw_path.exists():
        print(f"  Skipping (missing raw_response.json): {run_dir}")
        return

    # Load data
    with open(summary_path, "r", encoding="utf-8") as f:
        summary_data = json.load(f)
    with open(raw_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Extract data safely
    model = html.escape(summary_data.get("model", "N/A"))
    prompt_variant = html.escape(summary_data.get("prompt_variant", "N/A"))
    request_number = summary_data.get("request_number", "N/A")
    credibility = summary_data.get("total_credibility", 0)
    
    prompt_text = html.escape(raw_data.get("prompt", "Prompt not found."))
    code_text = html.escape(raw_data.get("code", "Code not found."))

    # Build metric tables
    key_metrics = {
        "Perplexity": summary_data.get("perplexity", 0),
        "Avg. Probability": summary_data.get("avg_prob", 0),
        "Total Tokens": summary_data.get("total_tokens", 0),
    }
    
    struct_table = create_metrics_table("Structural Metrics", summary_data.get("struct_metrics", {}))
    semantic_table = create_metrics_table("Semantic Metrics", summary_data.get("semantic_metrics", {}))
    exec_table = create_metrics_table("Execution Metrics", summary_data.get("execution_metrics", {}))
    key_metrics_table = create_metrics_table("Key Metrics", key_metrics)

    # --- HTML Template ---
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analysis Report: Run {request_number}</title>
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
            width: 30%;
        }}
        td {{
            background-color: #fff;
        }}
        details {{
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-bottom: 10px;
            background-color: #fff;
        }}
        summary {{
            padding: 12px;
            font-weight: 600;
            cursor: pointer;
            background-color: #f7f7f7;
        }}
        summary:hover {{
            background-color: #eee;
        }}
        pre {{
            background-color: #fdfdfd;
            border-top: 1px solid #eee;
            padding: 15px;
            margin: 0;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            font-size: 0.9em;
            line-height: 1.6;
        }}
        code {{
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
        }}
        .credibility-score {{
            font-size: 1.8em;
            font-weight: bold;
            color: #005fdd;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Analysis Report #{request_number}</h1>
        <h2>{model}</h2>
        <p><strong>Prompt Variant:</strong> {prompt_variant}</p>

        <h3>Total Credibility Score</h3>
        <p class="credibility-score">{credibility:.2f}%</p>

        {key_metrics_table}
        {exec_table}
        {struct_table}
        {semantic_table}

        <details>
            <summary>View Full Prompt</summary>
            <pre>{prompt_text}</pre>
        </details>
        
        <details>
            <summary>View Generated Code</summary>
            <pre><code>{code_text}</code></pre>
        </details>
    </div>
</body>
</html>
    """
    
    # Write the HTML file
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        return True
    except Exception as e:
        print(f"  [ERROR] Could not write HTML file: {e}")
        return False


def main():
    """Finds all summary.json files and generates HTML reports."""
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
        print("No 'summary.json' files found to report.")
        return

    print(f"Found {len(summary_json_files)} summaries. Generating HTML reports...")
    
    generated_count = 0
    for summary_path in summary_json_files:
        run_dir = summary_path.parent
        print(f"Generating report for: {run_dir.relative_to(search_path)}")
        if generate_html_report(run_dir):
            generated_count += 1

    print("\n--- HTML Report Generation Complete ---")
    print(f"Successfully generated: {generated_count} reports")

if __name__ == "__main__":
    main()
