import pandas as pd
import json
import yaml
import re
from pathlib import Path
import webbrowser
import os
import sys

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

def create_html_report(summary_df, report_stats, output_path):
    """Generates a self-contained HTML report from the summary DataFrame."""
    
    print("Generating HTML report...")

    # --- 1. Prepare Data and Styler ---
    
    # Define the columns to display and their user-friendly names
    display_columns = {
        'model': 'Model',
        'prompt_variant': 'Prompt Variant',
        'mean_score': 'Mean Score',
        'run_count': 'Runs',
        'mean_generated_stories': 'Mean Gen. Count',
        'mean_count_diff': 'Count Diff (from Orig)',
        'mean_count_ratio': 'Count Ratio (%)'
    }
    
    # Format for display
    summary_display_df = summary_df.rename(columns=display_columns)[list(display_columns.values())]

    # Create a Styler object to add conditional formatting
    styler = summary_display_df.style \
        .format({
            'Mean Score': '{:.4f}',
            'Mean Gen. Count': '{:.1f}',
            'Count Diff (from Orig)': '{:+.1f}', # Show + or -
            'Count Ratio (%)': '{:.1%}' # Format as percentage
        }) \
        .background_gradient(
            subset=['Mean Score'],
            cmap='RdYlGn', # Red -> Yellow -> Green
            vmin=max(0, summary_df['mean_score'].min() - 0.1),
            vmax=min(1, summary_df['mean_score'].max() + 0.1)
        ) \
        .background_gradient(
            subset=['Count Diff (from Orig)'],
            cmap='Reds', # White -> Red
            gmap=summary_df['mean_count_diff_abs'] # Color based on absolute value
        ) \
        .set_table_attributes('class="results-table"') \
        .set_properties(**{'border': '1px solid #ddd', 'padding': '8px'}) \
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#f2f2f2'), ('font-weight', 'bold')]},
            {'selector': 'tr:hover', 'props': [('background-color', '#f5f5f5')]}
        ])

    table_html = styler.to_html()

    # --- 2. Create Summary Cards ---
    best_combo = report_stats['best_score_combo']
    best_count_combo = report_stats['best_count_combo']
    
    cards_html = f"""
    <div class="card-grid">
        <div class="card">
            <div class="card-title">Total Runs Analyzed</div>
            <div class="card-value">{report_stats['total_runs']}</div>
        </div>
        <div class="card">
            <div class="card-title">Original Story Count</div>
            <div class="card-value">{report_stats['original_story_count']}</div>
        </div>
        <div class="card">
            <div class="card-title">Best Semantic Score</div>
            <div class="card-value">{best_combo['mean_score']:.4f}</div>
            <div class="card-sub">{best_combo['model']} / {best_combo['prompt_variant']}</div>
        </div>
        <div class="card">
            <div class="card-title">Closest Story Count</div>
            <div class="card-value">{best_count_combo['mean_count_diff']:+.1f} <span>Diff</span></div>
            <div class="card-sub">{best_count_combo['model']} / {best_count_combo['prompt_variant']}</div>
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
        <title>Semantic Consistency Report</title>
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
            .card-value span {{
                font-size: 1rem;
                font-weight: 500;
                color: #4b5563;
                margin-left: 0.25rem;
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
            .results-table tr:last-child td {{
                border-bottom: none;
            }}
            .results-table tr:nth-child(even) {{
                background-color: #fdfdfd;
            }}
            /* Styler adds inline styles for colors, this just helps layout */
            .results-table td:nth-child(3), /* Mean Score */
            .results-table td:nth-child(6), /* Count Diff */
            .results-table td:nth-child(7) {{ /* Count Ratio */
                font-weight: 500;
            }}
            footer {{
                text-align: center;
                padding: 1.5rem;
                font-size: 0.85rem;
                color: #9ca3af;
                margin-top: 2rem;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Semantic Consistency Report</h1>
                <p style="margin: 0.25rem 0 0 0; color: #6b7280;">Analysis of Code-to-Stories Generation</p>
            </header>
            <div class="content">
                <h2>Overall Summary</h2>
                {cards_html}
                
                <h2>Aggregated Results</h2>
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

def analyze_and_report(config):
    """
    Finds all 'semantic_consistency.json' files, aggregates the scores,
    and generates an HTML report.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    analysis_dir = base_results_dir / "code-to-stories"

    if not analysis_dir.exists():
        print(f"Error: Analysis directory not found: {analysis_dir}")
        print("Please ensure the 'code-to-stories' analysis has been run first.")
        return

    print(f"Scanning for 'semantic_consistency.json' files in {analysis_dir}...")
    json_files = list(analysis_dir.rglob("semantic_consistency.json"))

    if not json_files:
        print("No 'semantic_consistency.json' files found to analyze.")
        return

    print(f"Found {len(json_files)} result files. Loading data...")

    # --- Step 1: Load all data into a list ---
    all_data = []
    for f_path in json_files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data['report_number'] = f_path.parent.name
                all_data.append(data)
        except Exception as e:
            print(f"Warning: Could not read or parse {f_path.relative_to(base_results_dir)}: {e}")

    if not all_data:
        print("No valid data was loaded.")
        return

    # --- Step 2: Convert to Pandas DataFrame ---
    df = pd.DataFrame(all_data)
    
    # Check for consistency in original stories count
    original_counts = df['original_stories_count'].unique()
    if len(original_counts) > 1:
        print(f"Warning: Multiple 'original_stories_count' values found: {original_counts}")
        print(f"Using the first value: {original_counts[0]}")
    original_story_count = original_counts[0]

    # --- Step 3: Engineer new comparison features ---
    df['count_diff'] = df['generated_stories_count'] - original_story_count
    df['count_ratio'] = df['generated_stories_count'] / original_story_count

    # --- Step 4: Aggregate the data ---
    print("Aggregating results by model and prompt variant...")
    
    summary_df = df.groupby(['model', 'prompt_variant']).agg(
        mean_score=('semantic_consistency_score', 'mean'),
        run_count=('semantic_consistency_score', 'count'),
        mean_generated_stories=('generated_stories_count', 'mean'),
        mean_count_diff=('count_diff', 'mean'),
        mean_count_ratio=('count_ratio', 'mean')
    ).reset_index()

    # Add helper columns for styling and sorting
    summary_df['mean_count_diff_abs'] = summary_df['mean_count_diff'].abs()
    
    # Sort by model, then by the mean score (descending)
    summary_df = summary_df.sort_values(by=['model', 'mean_score'], ascending=[True, False])

    # --- Step 5: Get high-level stats for the report cards ---
    report_stats = {
        'total_runs': int(summary_df['run_count'].sum()),
        'original_story_count': int(original_story_count),
        'best_score_combo': summary_df.loc[summary_df['mean_score'].idxmax()],
        'best_count_combo': summary_df.loc[summary_df['mean_count_diff_abs'].idxmin()]
    }

    # --- Step 6: Generate and save the HTML report ---
    output_html_path = base_results_dir / "semantic_consistency_report.html"
    create_html_report(summary_df, report_stats, output_html_path)

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
        print("Configuration loaded. Starting analysis...")
        analyze_and_report(config)
    else:
        print("Failed to load configuration. Exiting.")
