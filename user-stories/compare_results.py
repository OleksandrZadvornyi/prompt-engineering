import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# --- ⚙️ CONFIGURATION ---
MODEL = "qwen3_clusters"
BASE_REPORTS_DIR = Path(f"../Reports/{MODEL}/")
# ---

def analyze_and_compare_results(base_dir: Path):
    """
    Analyzes the results from multiple runs, calculates statistics,
    and generates a comparison plot.
    """
    print(f"🔍 Starting analysis in directory: {base_dir.resolve()}")

    if not base_dir.is_dir():
        print(f"❌ Error: The directory '{base_dir.resolve()}' does not exist.")
        return

    # --- CHANGED: Use a list of dicts to store all run data ---
    run_data = []
    
    # Loop to find all report directories (increased range for flexibility)
    for i in range(1, 21):
        report_dir = base_dir / f"report_{i}" / "User-stories"
        # --- CHANGED: Updated path to be more generic ---
        results_file = report_dir / "analysis_results.json"

        if results_file.exists():
            try:
                with open(results_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # --- CHANGED: Extract more data points ---
                    score = data.get("semantic_consistency_score")
                    story_count = data.get("generated_stories_count")
                    model_name = data.get("analysis_model")

                    if score is not None and story_count is not None:
                        run_data.append({
                            "run_number": i,
                            "score": score,
                            "story_count": story_count,
                            "model_name": model_name
                        })
                    else:
                        print(f"⚠️ Warning: Missing data in {results_file}")
            except Exception as e:
                print(f"⚠️ An unexpected error occurred while reading {results_file}: {e}")
        else:
            # This is normal if not all runs exist, so no print needed unless debugging
            pass

    if not run_data:
        print("\n❌ No valid result files were found. Cannot perform analysis.")
        return

    # --- Step 1: Calculate Statistics ---
    print(f"\n✅ Successfully loaded data from {len(run_data)} runs.")
    
    # --- CHANGED: Extract data from the list of dicts ---
    scores = [d['score'] for d in run_data]
    story_counts = [d['story_count'] for d in run_data]
    run_numbers = [d['run_number'] for d in run_data]
    
    # Score statistics
    mean_score = np.mean(scores)
    median_score = np.median(scores)
    std_dev_score = np.std(scores)
    min_score = np.min(scores)
    max_score = np.max(scores)

    # --- NEW: Story count statistics ---
    mean_stories = np.mean(story_counts)
    min_stories = np.min(story_counts)
    max_stories = np.max(story_counts)

    # --- Step 2: Print Enhanced Summary to Console ---
    summary_text = f"""
    {"="*60}
    📊 Performance Summary for Model: {MODEL}
    {"="*60}
    - Total Runs Analyzed: {len(run_data)}

    --- Semantic Consistency Score ---
    - Average (Mean): {mean_score:.4f}
    - Median:         {median_score:.4f}
    - Std Deviation:  {std_dev_score:.4f}
    - Min Score:      {min_score:.4f} (Run {run_numbers[np.argmin(scores)]})
    - Max Score:      {max_score:.4f} (Run {run_numbers[np.argmax(scores)]})

    --- Generated User Story Count ---
    - Average Count: {mean_stories:.2f}
    - Min Count:     {min_stories} (in Run {run_numbers[np.argmin(story_counts)]})
    - Max Count:     {max_stories} (in Run {run_numbers[np.argmax(story_counts)]})
    {"="*60}
    """
    print(summary_text)
    
    # --- Step 2b: Save Enhanced Summary Data as JSON ---
    summary_data = {
        "model_name": MODEL,
        "total_runs_analyzed": len(run_data),
        "score_statistics": {
            "mean": float(mean_score),
            "median": float(median_score),
            "std_dev": float(std_dev_score),
            "min": float(min_score),
            "max": float(max_score)
        },
        "story_count_statistics": {
            "mean": float(mean_stories),
            "min": int(min_stories),
            "max": int(max_stories)
        },
        "detailed_runs": run_data # <-- CHANGED: Save all detailed data
    }

    summary_json_path = base_dir / "comparison_summary.json"
    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=4)
    print(f"✅ Enhanced summary data saved to: {summary_json_path}")


    # --- Step 3: Create and Save an Improved Visualization ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax1 = plt.subplots(figsize=(15, 8))

    # --- NEW: Dual-axis plot setup ---
    ax2 = ax1.twinx()

    # Plot bars for scores on the first axis (ax1)
    bars = ax1.bar(run_numbers, scores, color='skyblue', edgecolor='black', zorder=2, label='Consistency Score')
    ax1.set_title(f'Performance Analysis for {MODEL}', fontsize=16, weight='bold')
    ax1.set_xlabel('Run Number', fontsize=12)
    ax1.set_ylabel('Semantic Consistency Score', fontsize=12, color='skyblue')
    ax1.tick_params(axis='y', labelcolor='skyblue')
    ax1.set_xticks(range(1, 21))
    ax1.set_ylim(bottom=0) # Scores are always positive

    # Add a horizontal line for the average score
    ax1.axhline(mean_score, color='dodgerblue', linestyle='--', linewidth=2, label=f'Avg Score: {mean_score:.3f}')
    
    # Plot line for story counts on the second axis (ax2)
    ax2.plot(run_numbers, story_counts, color='coral', marker='o', linestyle='-', zorder=3, label='Generated Stories')
    ax2.set_ylabel('Number of Generated Stories', fontsize=12, color='coral')
    ax2.tick_params(axis='y', labelcolor='coral')
    ax2.set_ylim(bottom=0)

    # Add text labels for scores on top of each bar
    for bar in bars:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2.0, yval + 0.01, f'{yval:.3f}', ha='center', va='bottom', fontsize=9)
    
    # Combine legends from both axes
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.tight_layout()
    plot_path = base_dir / "scores_comparison_chart.png"
    plt.savefig(plot_path, dpi=300)
    print(f"✅ Dual-axis comparison chart saved to: {plot_path}")
    plt.close()

if __name__ == "__main__":
    analyze_and_compare_results(BASE_REPORTS_DIR)