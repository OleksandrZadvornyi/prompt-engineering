import json
from pathlib import Path
import statistics
from jinja2 import Environment, FileSystemLoader
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Configuration
results_root = Path("Reports/qwen3_clusters")
comparison_dir = results_root / "comparison"
comparison_dir.mkdir(exist_ok=True)


###################################################################
# Step 1: Load all summary data
###################################################################

print("Loading summary data from all runs...")
summaries = []

# Load all summaries first
temp_summaries = []
for run_dir in results_root.glob("report_*"):
    summary_file = run_dir / "summary.json"
    if summary_file.exists():
        with open(summary_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            temp_summaries.append(data)

# Sort by request_number numerically
summaries = sorted(temp_summaries, key=lambda x: x["request_number"])
run_numbers = [s["request_number"] for s in summaries]

if not summaries:
    print("❌ No summary files found!")
    exit(1)

print(f"✅ Loaded {len(summaries)} runs")
model_name = summaries[0]["model"]
print(f"Model: {model_name}")


###################################################################
# Step 2: Extract metrics for comparison
###################################################################

# Organize metrics by category based on actual structure
metric_categories = {
    "LLM Confidence": ["total_tokens", "avg_prob", "perplexity", "total_logprob"],
    "Code Structure": ["token_count", "function_count", "class_count", "num_lines", 
                       "num_nonempty_lines", "avg_line_len_nonempty_chars", 
                       "avg_tokens_per_nonempty_line", "ast_depth"],
    "Imports & Dependencies": ["import_count", "import_redundancy_ratio"],
    "Code Complexity": ["avg_function_size_lines", "avg_cyclomatic_complexity", 
                        "max_cyclomatic_complexity", "module_cyclomatic_complexity"],
    "Code Quality": ["comment_density_percent", "syntax_valid", "flake8_error_count", 
                     "mypy_error_count", "semantic_quality_score"],
    "Execution": ["execution_success", "execution_time_sec"],
    "Overall": ["total_credibility"]
}

# Extract all metric values
metrics_data = {}
for category, metric_names in metric_categories.items():
    metrics_data[category] = {}
    for metric in metric_names:
        values = []
        for summary in summaries:
            val = summary.get(metric)
            # Handle boolean values
            if isinstance(val, bool):
                val = 1.0 if val else 0.0
            # Handle None or missing values
            if val is None or val == "N/A":
                val = 0.0
            values.append(val)
        metrics_data[category][metric] = values


###################################################################
# Step 3: Compute statistics
###################################################################

print("Computing statistics...")

def compute_stats(values):
    """Compute statistics for a list of values"""
    if not values:
        return {}
    
    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "range": max(values) - min(values)
    }

statistics_data = {}
for category, metrics in metrics_data.items():
    statistics_data[category] = {}
    for metric, values in metrics.items():
        statistics_data[category][metric] = compute_stats(values)


###################################################################
# Step 4: Generate comparison plots
###################################################################

print("Generating comparison plots...")

def plot_metric_over_runs(metric_name, values, run_numbers, output_path, ylabel="Value"):
    """Plot a metric across all runs"""
    plt.figure(figsize=(12, 6))
    plt.plot(run_numbers, values, marker='o', linewidth=2, markersize=6, color='#667eea')
    plt.xlabel('Run Number', fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.title(f'{metric_name} Across Runs', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    # Set x-axis to show integer ticks
    plt.xticks(run_numbers)
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

def plot_metric_distribution(metric_name, values, output_path):
    """Plot histogram of metric values"""
    plt.figure(figsize=(10, 6))
    plt.hist(values, bins=min(20, max(len(set(values)), 5)), edgecolor='black', alpha=0.7, color='#667eea')
    plt.xlabel('Value', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title(f'Distribution of {metric_name}', fontsize=14, fontweight='bold')
    mean_val = statistics.mean(values)
    plt.axvline(mean_val, color='red', linestyle='--', 
                linewidth=2, label=f'Mean: {mean_val:.2f}')
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

def plot_category_comparison(category_name, metrics_dict, run_numbers, output_path):
    """Plot multiple metrics from same category"""
    num_metrics = len(metrics_dict)
    if num_metrics == 0:
        return
    
    fig, axes = plt.subplots(num_metrics, 1, figsize=(12, 4 * num_metrics))
    if num_metrics == 1:
        axes = [axes]
    
    colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a']
    
    for idx, (metric, values) in enumerate(metrics_dict.items()):
        color = colors[idx % len(colors)]
        axes[idx].plot(run_numbers, values, marker='o', linewidth=2, markersize=6, color=color)
        axes[idx].set_xlabel('Run Number', fontsize=10)
        axes[idx].set_ylabel(metric.replace('_', ' ').title(), fontsize=10)
        axes[idx].set_title(f'{metric.replace("_", " ").title()}', fontsize=11, fontweight='bold')
        axes[idx].grid(True, alpha=0.3)
        # Set x-axis to show integer ticks
        axes[idx].set_xticks(run_numbers)
    
    plt.suptitle(f'{category_name} Metrics Comparison', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

# Generate plots for key metrics
plots_dir = comparison_dir / "plots"
plots_dir.mkdir(exist_ok=True)

# Plot credibility over runs
credibility_values = [s["total_credibility"] for s in summaries]
plot_metric_over_runs("Total Credibility", credibility_values, run_numbers,
                      plots_dir / "credibility_over_runs.png", "Credibility Score")

plot_metric_distribution("Total Credibility", credibility_values,
                         plots_dir / "credibility_distribution.png")

# Plot perplexity over runs
perplexity_values = [s["perplexity"] for s in summaries]
plot_metric_over_runs("Perplexity", perplexity_values, run_numbers,
                      plots_dir / "perplexity_over_runs.png", "Perplexity")

# Plot average probability over runs
avg_prob_values = [s["avg_prob"] for s in summaries]
plot_metric_over_runs("Average Token Probability", avg_prob_values, run_numbers,
                      plots_dir / "avg_prob_over_runs.png", "Probability")

# Plot semantic quality score
semantic_quality_values = [s["semantic_quality_score"] for s in summaries]
plot_metric_over_runs("Semantic Quality Score", semantic_quality_values, run_numbers,
                      plots_dir / "semantic_quality_over_runs.png", "Quality Score")

# Plot cyclomatic complexity
complexity_values = [s["avg_cyclomatic_complexity"] for s in summaries]
plot_metric_over_runs("Average Cyclomatic Complexity", complexity_values, run_numbers,
                      plots_dir / "complexity_over_runs.png", "Complexity")

# Plot category comparisons
for category, metrics in metrics_data.items():
    if category != "Overall":
        plot_category_comparison(category, metrics, run_numbers,
                                plots_dir / f"{category.lower().replace(' ', '_').replace('&', 'and')}_comparison.png")


###################################################################
# Step 5: Identify best and worst runs
###################################################################

print("Identifying best and worst runs...")

# Find runs with highest/lowest credibility
best_idx = credibility_values.index(max(credibility_values))
worst_idx = credibility_values.index(min(credibility_values))

best_run = summaries[best_idx]
worst_run = summaries[worst_idx]

# Find outliers (beyond 1.5 standard deviations)
outliers = []
if len(credibility_values) > 2:
    mean_cred = statistics.mean(credibility_values)
    stdev_cred = statistics.stdev(credibility_values)
    for idx, val in enumerate(credibility_values):
        if abs(val - mean_cred) > 1.5 * stdev_cred:
            outliers.append({
                "run": summaries[idx]["request_number"],
                "credibility": val,
                "deviation": abs(val - mean_cred) / stdev_cred
            })

# Analyze execution success rate
execution_success_count = sum(1 for s in summaries if s.get("execution_success", False))
execution_success_rate = (execution_success_count / len(summaries)) * 100

# Analyze syntax validity
syntax_valid_count = sum(1 for s in summaries if s.get("syntax_valid", False))
syntax_valid_rate = (syntax_valid_count / len(summaries)) * 100


###################################################################
# Step 6: Generate HTML comparison report
###################################################################

print("Generating HTML comparison report...")

# Create HTML template
template_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Model Comparison Report - {{ model_name }}</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .header h1 { margin: 0 0 10px 0; }
        .header p { margin: 5px 0; opacity: 0.9; }
        .section {
            background: white;
            padding: 25px;
            margin-bottom: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .section h2 {
            color: #667eea;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .metric-card {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid #667eea;
        }
        .metric-card h4 {
            margin: 0 0 10px 0;
            color: #333;
            font-size: 14px;
        }
        .stat-row {
            display: flex;
            justify-content: space-between;
            margin: 5px 0;
            font-size: 13px;
        }
        .stat-label { color: #666; }
        .stat-value { font-weight: bold; color: #333; }
        .highlight-box {
            background: #e7f3ff;
            border-left: 4px solid #2196F3;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }
        .warning-box {
            background: #fff3e0;
            border-left: 4px solid #ff9800;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }
        .success-box {
            background: #e8f5e9;
            border-left: 4px solid #4caf50;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }
        .plot-container {
            margin: 20px 0;
            text-align: center;
        }
        .plot-container img {
            max-width: 100%;
            height: auto;
            border-radius: 6px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 14px;
        }
        th, td {
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background: #667eea;
            color: white;
            font-weight: 600;
        }
        tr:hover { background: #f5f5f5; }
        .best-badge {
            display: inline-block;
            padding: 4px 8px;
            background: #4caf50;
            color: white;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
        }
        .worst-badge {
            display: inline-block;
            padding: 4px 8px;
            background: #f44336;
            color: white;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
        }
        .progress-bar {
            width: 100%;
            height: 25px;
            background: #e0e0e0;
            border-radius: 12px;
            overflow: hidden;
            margin: 10px 0;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 13px;
        }
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .kpi-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }
        .kpi-value {
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0;
        }
        .kpi-label {
            font-size: 14px;
            opacity: 0.9;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔬 Model Comparison Report</h1>
        <p><strong>Model:</strong> {{ model_name }}</p>
        <p><strong>Total Runs:</strong> {{ total_runs }}</p>
        <p><strong>Generated:</strong> {{ generation_time }}</p>
    </div>

    <!-- Key Performance Indicators -->
    <div class="section">
        <h2>🎯 Key Performance Indicators</h2>
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Mean Credibility</div>
                <div class="kpi-value">{{ "%.1f"|format(stats.Overall.total_credibility.mean) }}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Syntax Valid Rate</div>
                <div class="kpi-value">{{ "%.1f"|format(syntax_valid_rate) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Execution Success Rate</div>
                <div class="kpi-value">{{ "%.1f"|format(execution_success_rate) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Mean Quality Score</div>
                <div class="kpi-value">{{ "%.1f"|format(stats['Code Quality'].semantic_quality_score.mean) }}</div>
            </div>
        </div>
    </div>

    <!-- Summary Statistics -->
    <div class="section">
        <h2>📊 Summary Statistics</h2>
        
        <div class="highlight-box">
            <h3 style="margin-top: 0;">Overall Credibility</h3>
            <div class="metrics-grid">
                <div class="metric-card">
                    <h4>Mean Credibility</h4>
                    <div class="stat-value" style="font-size: 24px; color: #667eea;">
                        {{ "%.2f"|format(stats.Overall.total_credibility.mean) }}
                    </div>
                </div>
                <div class="metric-card">
                    <h4>Range</h4>
                    <div class="stat-value" style="font-size: 18px;">
                        {{ "%.2f"|format(stats.Overall.total_credibility.min) }} - 
                        {{ "%.2f"|format(stats.Overall.total_credibility.max) }}
                    </div>
                </div>
                <div class="metric-card">
                    <h4>Standard Deviation</h4>
                    <div class="stat-value" style="font-size: 18px;">
                        {{ "%.2f"|format(stats.Overall.total_credibility.stdev) }}
                    </div>
                </div>
                <div class="metric-card">
                    <h4>Median</h4>
                    <div class="stat-value" style="font-size: 18px;">
                        {{ "%.2f"|format(stats.Overall.total_credibility.median) }}
                    </div>
                </div>
            </div>
        </div>

        <h3>Best and Worst Runs</h3>
        <table>
            <tr>
                <th>Category</th>
                <th>Run</th>
                <th>Credibility</th>
                <th>Perplexity</th>
                <th>Quality Score</th>
                <th>Functions</th>
                <th>Syntax Valid</th>
                <th>Executed</th>
            </tr>
            <tr style="background: #e8f5e9;">
                <td><span class="best-badge">BEST</span></td>
                <td><a href="../report_{{ best_run.request_number }}/report.html">Run {{ best_run.request_number }}</a></td>
                <td><strong>{{ "%.2f"|format(best_run.total_credibility) }}</strong></td>
                <td>{{ "%.2f"|format(best_run.perplexity) }}</td>
                <td>{{ "%.1f"|format(best_run.semantic_quality_score) }}</td>
                <td>{{ best_run.function_count }}</td>
                <td>{{ "✓" if best_run.syntax_valid else "✗" }}</td>
                <td>{{ "✓" if best_run.execution_success else "✗" }}</td>
            </tr>
            <tr style="background: #ffebee;">
                <td><span class="worst-badge">WORST</span></td>
                <td><a href="../report_{{ worst_run.request_number }}/report.html">Run {{ worst_run.request_number }}</a></td>
                <td><strong>{{ "%.2f"|format(worst_run.total_credibility) }}</strong></td>
                <td>{{ "%.2f"|format(worst_run.perplexity) }}</td>
                <td>{{ "%.1f"|format(worst_run.semantic_quality_score) }}</td>
                <td>{{ worst_run.function_count }}</td>
                <td>{{ "✓" if worst_run.syntax_valid else "✗" }}</td>
                <td>{{ "✓" if worst_run.execution_success else "✗" }}</td>
            </tr>
        </table>

        {% if outliers %}
        <div class="warning-box">
            <h4 style="margin-top: 0;">⚠️ Outlier Runs Detected</h4>
            <p>The following runs have credibility scores more than 1.5 standard deviations from the mean:</p>
            <ul>
            {% for outlier in outliers %}
                <li><a href="../report_{{ outlier.run }}/report.html">Run {{ outlier.run }}</a>: 
                    {{ "%.2f"|format(outlier.credibility) }} 
                    ({{ "%.2f"|format(outlier.deviation) }}σ from mean)</li>
            {% endfor %}
            </ul>
        </div>
        {% endif %}

        <div class="success-box">
            <h4 style="margin-top: 0;">✅ Code Quality Metrics</h4>
            <p><strong>Syntax Validity:</strong> {{ syntax_valid_count }}/{{ total_runs }} runs ({{ "%.1f"|format(syntax_valid_rate) }}%)</p>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {{ syntax_valid_rate }}%">{{ "%.0f"|format(syntax_valid_rate) }}%</div>
            </div>
            <p><strong>Execution Success:</strong> {{ execution_success_count }}/{{ total_runs }} runs ({{ "%.1f"|format(execution_success_rate) }}%)</p>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {{ execution_success_rate }}%">{{ "%.0f"|format(execution_success_rate) }}%</div>
            </div>
        </div>
    </div>

    <!-- Visualizations -->
    <div class="section">
        <h2>📈 Trends Over Runs</h2>
        
        <div class="plot-container">
            <h3>Credibility Score Progression</h3>
            <img src="plots/credibility_over_runs.png" alt="Credibility Over Runs">
        </div>

        <div class="plot-container">
            <h3>Credibility Distribution</h3>
            <img src="plots/credibility_distribution.png" alt="Credibility Distribution">
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
            <div class="plot-container">
                <h3>Perplexity</h3>
                <img src="plots/perplexity_over_runs.png" alt="Perplexity Over Runs">
            </div>
            <div class="plot-container">
                <h3>Token Probability</h3>
                <img src="plots/avg_prob_over_runs.png" alt="Average Probability Over Runs">
            </div>
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
            <div class="plot-container">
                <h3>Semantic Quality Score</h3>
                <img src="plots/semantic_quality_over_runs.png" alt="Quality Score Over Runs">
            </div>
            <div class="plot-container">
                <h3>Cyclomatic Complexity</h3>
                <img src="plots/complexity_over_runs.png" alt="Complexity Over Runs">
            </div>
        </div>
    </div>

    <!-- Detailed Metrics by Category -->
    {% for category, metrics in stats.items() %}
    {% if category != "Overall" %}
    <div class="section">
        <h2>{{ category }}</h2>
        
        <div class="plot-container">
            <img src="plots/{{ category.lower().replace(' ', '_').replace('&', 'and') }}_comparison.png" 
                 alt="{{ category }} Comparison">
        </div>

        <div class="metrics-grid">
        {% for metric_name, metric_stats in metrics.items() %}
            <div class="metric-card">
                <h4>{{ metric_name.replace('_', ' ').title() }}</h4>
                <div class="stat-row">
                    <span class="stat-label">Mean:</span>
                    <span class="stat-value">{{ "%.3f"|format(metric_stats.mean) }}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Median:</span>
                    <span class="stat-value">{{ "%.3f"|format(metric_stats.median) }}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Min:</span>
                    <span class="stat-value">{{ "%.3f"|format(metric_stats.min) }}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Max:</span>
                    <span class="stat-value">{{ "%.3f"|format(metric_stats.max) }}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">Std Dev:</span>
                    <span class="stat-value">{{ "%.3f"|format(metric_stats.stdev) }}</span>
                </div>
            </div>
        {% endfor %}
        </div>
    </div>
    {% endif %}
    {% endfor %}

    <!-- All Runs Table -->
    <div class="section">
        <h2>📋 All Runs Summary</h2>
        <div style="overflow-x: auto;">
            <table>
                <tr>
                    <th>Run</th>
                    <th>Timestamp</th>
                    <th>Credibility</th>
                    <th>Perplexity</th>
                    <th>Quality</th>
                    <th>Functions</th>
                    <th>Classes</th>
                    <th>Lines</th>
                    <th>Complexity</th>
                    <th>Syntax</th>
                    <th>Executed</th>
                </tr>
                {% for run in all_runs %}
                <tr>
                    <td><a href="../report_{{ run.request_number }}/report.html">{{ run.request_number }}</a></td>
                    <td style="font-size: 12px;">{{ run.timestamp }}</td>
                    <td><strong>{{ "%.1f"|format(run.total_credibility) }}</strong></td>
                    <td>{{ "%.2f"|format(run.perplexity) }}</td>
                    <td>{{ "%.1f"|format(run.semantic_quality_score) }}</td>
                    <td>{{ run.function_count }}</td>
                    <td>{{ run.class_count }}</td>
                    <td>{{ run.num_lines }}</td>
                    <td>{{ "%.1f"|format(run.avg_cyclomatic_complexity) }}</td>
                    <td>{{ "✓" if run.syntax_valid else "✗" }}</td>
                    <td>{{ "✓" if run.execution_success else "✗" }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>

    <div style="text-align: center; padding: 20px; color: #666; font-size: 14px;">
        <p>Generated by Model Comparison Tool</p>
    </div>
</body>
</html>
"""

# Save template
template_path = comparison_dir / "comparison_template.html"
with open(template_path, "w", encoding="utf-8") as f:
    f.write(template_content)

# Render template
env = Environment(loader=FileSystemLoader(str(comparison_dir)))
template = env.get_template("comparison_template.html")

import datetime
html_report = template.render(
    model_name=model_name,
    total_runs=len(summaries),
    generation_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    stats=statistics_data,
    best_run=best_run,
    worst_run=worst_run,
    outliers=outliers,
    all_runs=summaries,
    syntax_valid_count=syntax_valid_count,
    syntax_valid_rate=syntax_valid_rate,
    execution_success_count=execution_success_count,
    execution_success_rate=execution_success_rate
)

# Write comparison report
report_path = comparison_dir / "comparison_report.html"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(html_report)


###################################################################
# Step 7: Save comparison summary JSON
###################################################################

comparison_summary = {
    "model": model_name,
    "total_runs": len(summaries),
    "statistics": statistics_data,
    "best_run": {
        "run_number": best_run["request_number"],
        "credibility": best_run["total_credibility"],
        "timestamp": best_run["timestamp"],
        "quality_score": best_run["semantic_quality_score"],
        "syntax_valid": best_run["syntax_valid"],
        "execution_success": best_run["execution_success"]
    },
    "worst_run": {
        "run_number": worst_run["request_number"],
        "credibility": worst_run["total_credibility"],
        "timestamp": worst_run["timestamp"],
        "quality_score": worst_run["semantic_quality_score"],
        "syntax_valid": worst_run["syntax_valid"],
        "execution_success": worst_run["execution_success"]
    },
    "outliers": outliers,
    "quality_rates": {
        "syntax_valid_rate": syntax_valid_rate,
        "execution_success_rate": execution_success_rate
    }
}

with open(comparison_dir / "comparison_summary.json", "w", encoding="utf-8") as f:
    json.dump(comparison_summary, f, indent=4, ensure_ascii=False)

print(f"\n✅ Comparison complete! Report saved in: {comparison_dir}")
print("   - comparison_report.html")
print("   - comparison_summary.json")
print(f"   - plots/ (contains {len(list(plots_dir.glob('*.png')))} visualization files)")
print(f"\n📊 Summary:")
print(f"   Model: {model_name}")
print(f"   Runs analyzed: {len(summaries)}")
print(f"   Mean credibility: {statistics_data['Overall']['total_credibility']['mean']:.2f}")
print(f"   Mean quality score: {statistics_data['Code Quality']['semantic_quality_score']['mean']:.1f}")
print(f"   Syntax valid rate: {syntax_valid_rate:.1f}%")
print(f"   Execution success rate: {execution_success_rate:.1f}%")
print(f"   Best run: #{best_run['request_number']} (credibility: {best_run['total_credibility']:.2f})")
print(f"   Worst run: #{worst_run['request_number']} (credibility: {worst_run['total_credibility']:.2f})")