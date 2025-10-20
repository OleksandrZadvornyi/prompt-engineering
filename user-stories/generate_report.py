import json
from pathlib import Path
from datetime import datetime

# --- ⚙️ CONFIGURATION ---
# The script will scan this directory for model-specific summary files.
REPORTS_BASE_DIR = Path("../Reports")
OUTPUT_HTML_FILE = Path("cross_model_report.html")
# ---

def generate_html_report(all_model_data: list):
    """
    Generates a single HTML file from the aggregated model comparison data.
    """
    # Sort data by average score for a ranked view
    all_model_data.sort(key=lambda x: x['score_statistics']['mean'], reverse=True)

    # --- HTML Header and Styling ---
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>LLM Performance Comparison Report</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                background-color: #f8f9fa;
                color: #343a40;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
            }
            .container {
                max-width: 1200px;
                margin: auto;
                background: #ffffff;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            }
            h1, h2 {
                color: #212529;
                border-bottom: 2px solid #dee2e6;
                padding-bottom: 10px;
            }
            h1 { font-size: 2.5em; text-align: center; }
            h2 { font-size: 1.8em; margin-top: 40px; }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }
            th, td {
                padding: 12px 15px;
                text-align: left;
                border: 1px solid #e9ecef;
            }
            th {
                background-color: #f2f3f5;
                font-weight: 600;
            }
            tr:nth-child(even) { background-color: #f8f9fa; }
            .grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
                gap: 25px;
                margin-top: 20px;
            }
            .card {
                background: #ffffff;
                border: 1px solid #e9ecef;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.05);
                transition: transform 0.2s, box-shadow 0.2s;
            }
            .card:hover {
                transform: translateY(-5px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }
            .card h3 {
                margin-top: 0;
                font-size: 1.4em;
                color: #0056b3;
            }
            .card p { margin: 10px 0; }
            .card a {
                display: inline-block;
                margin-top: 15px;
                padding: 8px 15px;
                background-color: #007bff;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                font-weight: 500;
            }
            .card a:hover { background-color: #0056b3; }
            footer {
                text-align: center;
                margin-top: 40px;
                font-size: 0.9em;
                color: #6c757d;
            }
            .highlight-best { background-color: #d4edda; font-weight: bold; }
            .highlight-worst { background-color: #f8d7da; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>LLM Performance Comparison Report</h1>
    """

    # --- Summary Table ---
    html_content += """
            <h2>Overall Model Ranking</h2>
            <p>Models are ranked by their average Semantic Consistency Score. This score measures how well the model's generated code aligns with the original user stories' intent.</p>
            <table>
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Model Name</th>
                        <th>Avg. Consistency Score</th>
                        <th>Avg. Generated Stories</th>
                        <th>Runs Analyzed</th>
                    </tr>
                </thead>
                <tbody>
    """

    # Find best score and highest story count for highlighting
    best_score = max(d['score_statistics']['mean'] for d in all_model_data)
    highest_stories = max(d['story_count_statistics']['mean'] for d in all_model_data)

    for i, data in enumerate(all_model_data):
        score_class = 'highlight-best' if data['score_statistics']['mean'] == best_score else ''
        stories_class = 'highlight-best' if data['story_count_statistics']['mean'] == highest_stories else ''

        html_content += f"""
                    <tr>
                        <td>{i + 1}</td>
                        <td>{data['model_name']}</td>
                        <td class="{score_class}">{data['score_statistics']['mean']:.4f}</td>
                        <td class="{stories_class}">{data['story_count_statistics']['mean']:.2f}</td>
                        <td>{data['total_runs_analyzed']}</td>
                    </tr>
        """
    html_content += "</tbody></table>"

    # --- Individual Model Cards ---
    html_content += "<h2>Detailed Model Breakdown</h2><div class='grid'>"
    for data in all_model_data:
        model_name = data['model_name']
        score_stats = data['score_statistics']
        story_stats = data['story_count_statistics']
        # Relative path from the script's location to the chart
        chart_path = f"../Reports/{model_name}/scores_comparison_chart.png"

        html_content += f"""
            <div class="card">
                <h3>{model_name}</h3>
                <p><strong>Avg. Score:</strong> {score_stats['mean']:.4f} (Min: {score_stats['min']:.4f}, Max: {score_stats['max']:.4f})</p>
                <p><strong>Score Std Dev:</strong> {score_stats['std_dev']:.4f}</p>
                <p><strong>Avg. Stories:</strong> {story_stats['mean']:.2f} (Min: {story_stats['min']}, Max: {story_stats['max']})</p>
                <p><strong>Runs Analyzed:</strong> {data['total_runs_analyzed']}</p>
                <a href="{chart_path}" target="_blank">View Detailed Chart</a>
            </div>
        """
    html_content += "</div>"

    # --- Footer ---
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_content += f"""
            <footer>
                <p>Report generated on {report_date}</p>
            </footer>
        </div>
    </body>
    </html>
    """
    return html_content

def main():
    """
    Main function to find summaries, aggregate data, and generate the report.
    """
    print(f"🔍 Scanning for summary files in '{REPORTS_BASE_DIR.resolve()}'...")
    summary_files = list(REPORTS_BASE_DIR.glob("*/comparison_summary.json"))

    if not summary_files:
        print("❌ Error: No 'comparison_summary.json' files found. Aborting.")
        return

    all_model_data = []
    for file in summary_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_model_data.append(data)
                print(f"  - Loaded data for: {data.get('model_name', 'Unknown Model')}")
        except Exception as e:
            print(f"⚠️ Warning: Could not process file {file}. Reason: {e}")

    if not all_model_data:
        print("❌ Error: No valid data could be loaded. Aborting.")
        return

    print("\n✅ Data aggregation complete. Generating HTML report...")
    html_report = generate_html_report(all_model_data)

    try:
        with open(OUTPUT_HTML_FILE, 'w', encoding='utf-8') as f:
            f.write(html_report)
        print(f"\n🎉 Successfully generated HTML report: '{OUTPUT_HTML_FILE.resolve()}'")
    except Exception as e:
        print(f"\n❌ Error: Failed to write HTML file. Reason: {e}")


if __name__ == "__main__":
    main()
