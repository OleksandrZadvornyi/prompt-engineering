import pandas as pd
import plotly.express as px
from pathlib import Path

# --- 1. Settings ---
CSV_FILE_PATH = "results/cluster_similarity_results.csv"
OUTPUT_HTML_PATH = "results/cluster_report_en.html" # Змінив назву файлу

# --- 2. Function to generate the report ---
def create_html_report_en(csv_path: str, output_path: str):
    print(f"📈 Starting report generation from {csv_path}...")

    # --- 2.1. Load and validate data ---
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"ERROR: File '{csv_path}' not found.")
        print("Please run 'compare_structures.py' first.")
        return
    except pd.errors.EmptyDataError:
        print(f"ERROR: File '{csv_path}' is empty.")
        return

    if "similarity_score" not in df.columns:
        print("ERROR: 'similarity_score' column missing from CSV.")
        return

    print(f"Successfully loaded {len(df)} data rows.")

    # --- 2.2. Aggregate data for plots ---
    
    # Average similarity by model
    df_model_agg = df.groupby('model_name')['similarity_score'].mean().reset_index()
    df_model_agg = df_model_agg.sort_values(by='similarity_score', ascending=False)
    
    # Average similarity by prompt technique
    df_prompt_agg = df.groupby('prompt_variant')['similarity_score'].mean().reset_index()
    df_prompt_agg = df_prompt_agg.sort_values(by='similarity_score', ascending=False)

    # Main plot: Model vs Prompt
    df_combined_agg = df.groupby(['model_name', 'prompt_variant'])['similarity_score'].mean().reset_index()

    # --- 2.3. Create Plotly figures ---
    print("🎨 Creating interactive plots...")

    # Plot 1: Average Similarity by Model
    fig_model = px.bar(
        df_model_agg,
        x='model_name',
        y='similarity_score',
        title='🏆 Average Cluster Similarity by Model', # Translated
        labels={'model_name': 'Model', 'similarity_score': 'Average Similarity'}, # Translated
        text_auto='.3f'
    )

    # Plot 2: Average Similarity by Prompt Technique
    fig_prompt = px.bar(
        df_prompt_agg,
        x='prompt_variant',
        y='similarity_score',
        title='🛠️ Average Cluster Similarity by Prompt Technique', # Translated
        labels={'prompt_variant': 'Prompt Technique', 'similarity_score': 'Average Similarity'}, # Translated
        text_auto='.3f'
    )

    # Plot 3: Detailed Comparison (Model vs Prompt)
    fig_combined = px.bar(
        df_combined_agg,
        x='prompt_variant',
        y='similarity_score',
        color='model_name',
        barmode='group',
        title='📊 Detailed Comparison: Model vs. Prompt Technique', # Translated
        labels={
            'prompt_variant': 'Prompt Technique',
            'similarity_score': 'Average Similarity',
            'model_name': 'Model'
        }, # Translated
        text_auto='.3f'
    )

    # --- 2.4. Convert plots and data to HTML ---
    
    plot_model_html = fig_model.to_html(full_html=False, include_plotlyjs='cdn')
    plot_prompt_html = fig_prompt.to_html(full_html=False, include_plotlyjs=False)
    plot_combined_html = fig_combined.to_html(full_html=False, include_plotlyjs=False)

    # Table with aggregated data
    table_combined_html = df_combined_agg.sort_values(
        by='similarity_score', ascending=False
    ).to_html(index=False, classes='styled-table', float_format='%.4f')
    
    # Table with all raw data (for reference)
    table_raw_html = df.sort_values(
        by='similarity_score', ascending=False
    ).to_html(index=False, classes='styled-table', float_format='%.4f')


    # --- 2.5. Create final HTML file ---
    
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Cluster Similarity Report</title> <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f8f9fa;
                color: #212529;
            }}
            .container {{
                max-width: 1200px;
                margin: 20px auto;
                padding: 20px;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            }}
            h1, h2 {{
                color: #343a40;
                border-bottom: 2px solid #dee2e6;
                padding-bottom: 10px;
            }}
            h1 {{ font-size: 2.5rem; text-align: center; }}
            h2 {{ font-size: 1.8rem; margin-top: 40px; }}
            
            .plot-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
                justify-content: center;
            }}
            .plot {{
                flex: 1 1 45%;
                min-width: 500px;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }}
            .plot-full {{
                flex-basis: 100%;
                min-width: 500px;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }}
            
            .table-container {{
                max-height: 500px;
                overflow-y: auto;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                margin-top: 20px;
            }}
            .styled-table {{
                width: 100%;
                border-collapse: collapse;
            }}
            .styled-table th, .styled-table td {{
                border: 1px solid #dee2e6;
                padding: 10px 12px;
                text-align: left;
            }}
            .styled-table th {{
                background-color: #f1f3f5;
                position: sticky;
                top: 0;
                z-index: 1;
            }}
            .styled-table tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            .styled-table tr:hover {{
                background-color: #e9ecef;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📋 Cluster Structure Similarity Report</h1> <h2>📈 Key Visualizations</h2> <div class="plot-container">
                <div class="plot">{plot_model_html}</div>
                <div class="plot">{plot_prompt_html}</div>
            </div>
            <div class="plot-container" style="margin-top: 20px;">
                <div class="plot-full">{plot_combined_html}</div>
            </div>
            
            <h2>📑 Aggregated Data (Model vs. Prompt)</h2> <div class="table-container" style="max-height: 300px;">
                {table_combined_html}
            </div>

            <h2>🧾 All Results (sorted)</h2> <div class="table-container">
                {table_raw_html}
            </div>
            
        </div>
    </body>
    </html>
    """

    # --- 2.6. Save the file ---
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_template)
        print(f"\n🎉 Success! Report saved to: {Path(output_path).resolve()}")
    except Exception as e:
        print(f"ERROR: Failed to save HTML file: {e}")

# --- 3. Run the script ---
if __name__ == "__main__":
    create_html_report_en(CSV_FILE_PATH, OUTPUT_HTML_PATH)