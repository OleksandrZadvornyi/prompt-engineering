import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# --- ⚙️ CONFIGURATION ---
MODEL = "grok-4-fast"
BASE_REPORTS_DIR = Path(f"../Reports/{MODEL}/")
# ---

def analyze_and_compare_results(base_dir: Path):
    """
    Analyzes the results from multiple runs, calculates statistics,
    and generates a comparison plot.

    Args:
        base_dir: The base directory containing the report folders.
    """
    print(f"🔍 Starting analysis in directory: {base_dir.resolve()}")

    if not base_dir.is_dir():
        print(f"❌ Error: The directory '{base_dir.resolve()}' does not exist.")
        print("Please make sure the BASE_REPORTS_DIR is set correctly.")
        return

    scores = []
    run_numbers = []
    found_files = 0

    # Loop to find all report directories
    for i in range(1, 16):
        report_dir = base_dir / f"report_{i}"
        results_file = report_dir / "User-stories" / "analysis_results.json"

        if results_file.exists():
            found_files += 1
            try:
                with open(results_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    score = data.get("semantic_consistency_score")
                    if score is not None:
                        scores.append(score)
                        run_numbers.append(i)
                    else:
                        print(f"⚠️ Warning: 'semantic_consistency_score' not found in {results_file}")
            except json.JSONDecodeError:
                print(f"⚠️ Warning: Could not decode JSON from {results_file}")
            except Exception as e:
                print(f"⚠️ An unexpected error occurred while reading {results_file}: {e}")
        else:
            print(f"ℹ️ Info: Results file not found for report_{i}, skipping.")

    if not scores:
        print("\n❌ No scores were found. Cannot perform analysis.")
        print("Please check the directory structure and file names.")
        return

    # --- Step 1: Calculate Statistics ---
    print(f"\n✅ Successfully loaded scores from {len(scores)} runs.")
    mean_score = np.mean(scores)
    median_score = np.median(scores)
    std_dev = np.std(scores)
    min_score = np.min(scores)
    max_score = np.max(scores)

    # --- Step 2: Print Summary to Console ---
    summary_text = f"""
    {"="*50}
    📊 Performance Summary
    {"="*50}
    - Total Runs Analyzed: {len(scores)}
    - Average Score (Mean): {mean_score:.4f}
    - Median Score:         {median_score:.4f}
    - Standard Deviation:   {std_dev:.4f}
    - Minimum Score:        {min_score:.4f} (Run {run_numbers[np.argmin(scores)]})
    - Maximum Score:        {max_score:.4f} (Run {run_numbers[np.argmax(scores)]})
    {"="*50}
    """
    print(summary_text)
    
    # --- Step 2b: Save Summary Data as JSON ---
    summary_data = {
        "total_runs_analyzed": len(scores),
        "average_score_mean": float(mean_score),
        "median_score": float(median_score),
        "standard_deviation": float(std_dev),
        "minimum_score": {
            "value": float(min_score),
            "run_number": int(run_numbers[np.argmin(scores)])
        },
        "maximum_score": {
            "value": float(max_score),
            "run_number": int(run_numbers[np.argmax(scores)])
        },
        "all_scores": [float(s) for s in scores]
    }

    # Save the summary to a JSON file
    summary_json_path = base_dir / "comparison_summary.json"
    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=4)
    print(f"✅ Summary data saved to: {summary_json_path}")


    # --- Step 3: Create and Save a Visualization ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(15, 8))

    bars = ax.bar(run_numbers, scores, color='skyblue', edgecolor='black', zorder=2)
    ax.set_title(f'Semantic Consistency Score Across 20 Runs ({MODEL})', fontsize=16, weight='bold')
    ax.set_xlabel('Run Number', fontsize=12)
    ax.set_ylabel('Semantic Consistency Score', fontsize=12)
    ax.set_xticks(range(1, 21)) # Ensure all run numbers are shown
    ax.set_ylim(bottom=max(0, min_score - 0.05), top=min(1, max_score + 0.05)) # Dynamic y-axis

    # Add a horizontal line for the average score
    ax.axhline(mean_score, color='r', linestyle='--', linewidth=2, label=f'Average Score: {mean_score:.4f}', zorder=3)
    
    # Add text labels on top of each bar
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.005, f'{yval:.3f}', ha='center', va='bottom', fontsize=9)

    ax.legend()
    plt.tight_layout()

    # Save the plot
    plot_path = base_dir / "scores_comparison_chart.png"
    plt.savefig(plot_path, dpi=300)
    print(f"✅ Comparison chart saved to: {plot_path}")
    plt.close() # Free up memory

if __name__ == "__main__":
    analyze_and_compare_results(BASE_REPORTS_DIR)

