import pandas as pd
import json
import yaml
from pathlib import Path
import webbrowser
import sys
import os

def load_config():
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        config_path = Path("config.yaml")
    if not config_path.exists():
        print("Error: Configuration file (config.yaml) not found.")
        return None
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading YAML config: {e}")
        return None

def create_html_report_iter2(summary_df, report_stats, output_path):
    """Generates a self-contained HTML report from the Iteration 2 summary DataFrame."""
    
    print("Generating HTML report for Iteration 2...")

    # --- 1. Prepare Data and Styler ---
    
    # Define the columns to display and their user-friendly names
    display_columns = {
        'model': 'Model',
        'prompt_variant': 'Prompt Variant',
        'run_count': 'Runs',
        'mean_us_vs_us1': "Mean (US vs US')",
        'mean_us_vs_us2': "Mean (US vs US'')",
        'mean_us1_vs_us2': "Mean (US' vs US'')",
        'decay_score': 'Score Decay (US\' -> US\'\')'
    }
    
    # Format for display
    summary_display_df = summary_df.rename(columns=display_columns)[list(display_columns.values())]

    # Create a Styler object to add conditional formatting
    styler = summary_display_df.style \
        .format({
            "Mean (US vs US')": '{:.4f}',
            "Mean (US vs US'')": '{:.4f}',
            "Mean (US' vs US'')" : '{:.4f}',
            'Score Decay (US\' -> US\'\')': '{:+.4f}' # Show + or -
        }) \
        .background_gradient(
            subset=["Mean (US vs US')", "Mean (US vs US'')", "Mean (US' vs US'')" ],
            cmap='RdYlGn', # Red -> Yellow -> Green
            vmin=0.5,
            vmax=1.0
        ) \
        .background_gradient(
            subset=['Score Decay (US\' -> US\'\')'],
            cmap='RdYlBu_r', # Red (high decay) -> Blue (low decay)
            gmap=summary_df['decay_score'].abs()
        ) \
        .set_table_attributes('class="results-table"') \
        .set_properties(**{'border': '1px solid #ddd', 'padding': '8px'}) \
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#f2f2f2'), ('font-weight', 'bold')]},
            {'selector': 'tr:hover', 'props': [('background-color', '#f5f5f5')]}
        ])

    table_html = styler.to_html()

    # --- 2. Create Summary Cards ---
    best_convergence = report_stats['best_convergence_combo']
    best_stability = report_stats['best_stability_combo']
    least_decay = report_stats['least_decay_combo']
    
    cards_html = f"""
    <div class="card-grid">
        <div class="card">
            <div class="card-title">Total Runs Analyzed</div>
            <div class="card-value">{report_stats['total_runs']}</div>
        </div>
        <div class="card">
            <div class="card-title">Best Convergence (US vs US'')</div>
            <div class="card-value">{best_convergence['mean_us_vs_us2']:.4f}</div>
            <div class="card-sub">{best_convergence['model']} / {best_convergence['prompt_variant']}</div>
        </div>
        <div class="card">
            <div class="card-title">Best Stability (US' vs US'')</div>
            <div class="card-value">{best_stability['mean_us1_vs_us2']:.4f}</div>
            <div class="card-sub">{best_stability['model']} / {best_stability['prompt_variant']}</div>
        </div>
        <div class="card">
            <div class="card-title">Least Score Decay</div>
            <div class="card-value">{least_decay['decay_score']:+.4f}</div>
            <div class="card-sub">{least_decay['model']} / {least_decay['prompt_variant']}</div>
        </div>
    </div>
    """

    # --- 3. Assemble the Full HTML Page ---
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Iteration 2 - Semantic Consistency Report</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background-color: #f9fafb;
                color: #1f2937;
                margin: 0;
                padding: 2rem;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.05);
                overflow: hidden;
            }}
            header {{
                padding: 2rem 2.5rem;
                border-bottom: 1px solid #e5e7eb;
            }}
            h1 {{
                margin: 0;
                color: #111827;
                font-size: 1.75rem;
            }}
            h2 {{
                font-size: 1.25rem;
                color: #374151;
                margin: 0 0 1.5rem 0;
                border-bottom: 2px solid #60a5fa; /* Blue accent */
                padding-bottom: 0.5rem;
                display: inline-block;
            }}
            .content {{
                padding: 2.5rem;
            }}
            .card-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2.5rem;
            }}
            .card {{
                background-color: #f9fafb;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                padding: 1.5rem;
            }}
            .card-title {{
                font-size: 0.9rem;
                font-weight: 600;
                color: #6b7280;
                margin-bottom: 0.5rem;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            .card-value {{
                font-size: 2rem;
                font-weight: 700;
                color: #111827;
                line-height: 1;
            }}
            .card-sub {{
                font-size: 0.85rem;
                color: #4b5563;
                margin-top: 0.75rem;
                word-wrap: break-word;
            }}
            .table-container {{
                overflow-x: auto;
            }}
            .results-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 0.9rem;
            }}
            .results-table th, .results-table td {{
                text-align: left;
                padding: 12px 15px;
                border-bottom: 1px solid #e5e7eb;
            }}
            .results-table th {{
                background-color: #f9fafb;
                font-weight: 600;
                color: #374151;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Iteration 2 - Semantic Consistency Report</h1>
                <p style="margin: 0.25rem 0 0 0; color: #6b7280;">Analysis of US -> US' -> US'' semantic stability</p>
            </header>
            <div class="content">
                <h2>Overall Summary</h2>
                {cards_html}
                
                <h2>Aggregated Results</h2>
                <p style="margin: 0.25rem 0 0 0; color: #6b7280;">US - original user stories</p>
                <p style="margin: 0.25rem 0 0 0; color: #6b7280;">US' - generated user stories during the first iteration</p>
                <p style="margin: 0.25rem 0 0 0; color: #6b7280;">US'' - generated user stories during the second iteration</p>
                <br>
                <div class="table-container">
                    {table_html}
                </div>
            </div>
        </div>
        <footer>
            Report generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
        </footer>
    </body>
    </html>
    """

    # --- 4. Save and Open the File ---
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"\n✅ HTML report saved to: {output_path.resolve()}")
        
        # Try to open the report in the default web browser
        webbrowser.open(f"file://{output_path.resolve()}")
    except Exception as e:
        print(f"\nError writing or opening HTML file: {e}")

def analyze_and_report_iter2(config):
    """
    Finds all 'semantic_consistency_report.json' files from Iteration 2,
    aggregates the scores, and generates an HTML report.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    # Set the search path to the Iteration 2 results
    analysis_dir = base_results_dir / "code-to-stories-iter2"

    if not analysis_dir.exists():
        print(f"Error: Analysis directory not found: {analysis_dir}")
        print("Please ensure the Iteration 2 analysis script has been run first.")
        return

    print(f"Scanning for 'semantic_consistency_report.json' files in {analysis_dir}...")
    json_files = list(analysis_dir.rglob("semantic_consistency_report.json"))

    if not json_files:
        print("No 'semantic_consistency_report.json' files found to analyze.")
        return

    print(f"Found {len(json_files)} result files. Loading data...")

    # --- Step 1: Load all data into a list ---
    all_data = []
    for f_path in json_files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Flatten the nested score data
                flat_data = {
                    "model": data.get("model", "unknown"),
                    "prompt_variant": data.get("prompt_variant", "unknown"),
                    "original_vs_iter1": data.get("scores", {}).get("original_vs_iter1"),
                    "original_vs_iter2": data.get("scores", {}).get("original_vs_iter2"),
                    "iter1_vs_iter2": data.get("scores", {}).get("iter1_vs_iter2"),
                }
                all_data.append(flat_data)
        except Exception as e:
            print(f"Warning: Could not read or parse {f_path.relative_to(base_results_dir)}: {e}")

    if not all_data:
        print("No valid data was loaded.")
        return

    # --- Step 2: Convert to Pandas DataFrame ---
    df = pd.DataFrame(all_data)

    # --- Step 3: Aggregate the data ---
    print("Aggregating results by model and prompt variant...")
    
    summary_df = df.groupby(['model', 'prompt_variant']).agg(
        mean_us_vs_us1=('original_vs_iter1', 'mean'),
        mean_us_vs_us2=('original_vs_iter2', 'mean'),
        mean_us1_vs_us2=('iter1_vs_iter2', 'mean'),
        run_count=('model', 'count')
    ).reset_index()

    # --- Step 4: Engineer new comparison features ---
    # Calculate the "decay" or "drop-off" from US' to US'' (relative to original US)
    # A smaller number is better (less information loss)
    summary_df['decay_score'] = summary_df['mean_us_vs_us1'] - summary_df['mean_us_vs_us2']
    
    # Sort by model, then by the final convergence score (descending)
    summary_df = summary_df.sort_values(by=['model', 'mean_us_vs_us2'], ascending=[True, False])

    # --- Step 5: Get high-level stats for the report cards ---
    report_stats = {
        'total_runs': int(summary_df['run_count'].sum()),
        'best_convergence_combo': summary_df.loc[summary_df['mean_us_vs_us2'].idxmax()],
        'best_stability_combo': summary_df.loc[summary_df['mean_us1_vs_us2'].idxmax()],
        'least_decay_combo': summary_df.loc[summary_df['decay_score'].idxmin()]
    }

    # --- Step 6: Generate and save the HTML report ---
    output_html_path = base_results_dir / "semantic_consistency_report_iter2.html"
    create_html_report_iter2(summary_df, report_stats, output_html_path)

if __name__ == "__main__":
    # Check for pandas import
    try:
        import pandas as pd
    except ImportError:
        print("---")
        print("Error: The 'pandas' library is required for this script.")
        print("Please install it by running: pip install pandas")
        print("---")
        sys.exit(1)

    print("Loading configuration...")
    config = load_config()
    
    if config:
        print("Configuration loaded. Starting Iteration 2 aggregation...")
        analyze_and_report_iter2(config)
    else:
        print("Failed to load configuration. Exiting.")