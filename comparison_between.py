import json
from pathlib import Path
import statistics
from jinja2 import Environment, FileSystemLoader
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Configuration
reports_root = Path("Reports")
comparison_dir = reports_root / "models_comparison"
comparison_dir.mkdir(exist_ok=True)


###################################################################
# Step 1: Load all models' comparison data
###################################################################

print("Scanning for models...")
models_data = {}

for model_dir in reports_root.iterdir():
    if not model_dir.is_dir() or model_dir.name == "models_comparison":
        continue
    
    comparison_summary = model_dir / "comparison" / "comparison_summary.json"
    if comparison_summary.exists():
        with open(comparison_summary, "r", encoding="utf-8") as f:
            data = json.load(f)
            model_name = model_dir.name
            models_data[model_name] = data
            print(f"  ✓ Loaded: {model_name}")

if len(models_data) < 2:
    print(f"\n❌ Need at least 2 models to compare. Found: {len(models_data)}")
    print("Please run compare_model_runs.py for each model first.")
    exit(1)

print(f"\n✅ Found {len(models_data)} models to compare")


###################################################################
# Step 2: Extract comparison metrics
###################################################################

print("Extracting metrics for comparison...")

# Define metrics we want to compare
comparison_metrics = {
    "Overall Performance": {
        "mean_credibility": ("Overall", "total_credibility", "mean"),
        "median_credibility": ("Overall", "total_credibility", "median"),
        "credibility_stability": ("Overall", "total_credibility", "stdev")
    },
    "LLM Confidence": {
        "mean_perplexity": ("LLM Confidence", "perplexity", "mean"),
        "mean_avg_prob": ("LLM Confidence", "avg_prob", "mean"),
        "mean_tokens": ("LLM Confidence", "total_tokens", "mean")
    },
    "Code Structure": {
        "mean_functions": ("Code Structure", "function_count", "mean"),
        "mean_classes": ("Code Structure", "class_count", "mean"),
        "mean_lines": ("Code Structure", "num_lines", "mean"),
        "mean_ast_depth": ("Code Structure", "ast_depth", "mean")
    },
    "Code Complexity": {
        "mean_cyclomatic": ("Code Complexity", "avg_cyclomatic_complexity", "mean"),
        "max_cyclomatic": ("Code Complexity", "max_cyclomatic_complexity", "mean"),
        "mean_function_size": ("Code Complexity", "avg_function_size_lines", "mean")
    },
    "Code Quality": {
        "mean_quality_score": ("Code Quality", "semantic_quality_score", "mean"),
        "syntax_valid_rate": ("quality_rates", "syntax_valid_rate", None),
        "execution_success_rate": ("quality_rates", "execution_success_rate", None),
        "mean_flake8_errors": ("Code Quality", "flake8_error_count", "mean"),
        "mean_comment_density": ("Code Quality", "comment_density_percent", "mean")
    }
}

# Extract values for each model
models_metrics = {}
for model_name, model_data in models_data.items():
    models_metrics[model_name] = {
        "total_runs": model_data["total_runs"],
        "best_run_credibility": model_data["best_run"]["credibility"],
        "worst_run_credibility": model_data["worst_run"]["credibility"]
    }
    
    for category, metrics in comparison_metrics.items():
        for metric_key, (cat, metric, stat) in metrics.items():
            try:
                if cat == "quality_rates":
                    value = model_data.get(cat, {}).get(metric, 0.0)
                else:
                    value = model_data["statistics"][cat][metric][stat]
                models_metrics[model_name][metric_key] = value
            except (KeyError, TypeError):
                models_metrics[model_name][metric_key] = 0.0


###################################################################
# Step 3: Rank models by various metrics
###################################################################

print("Ranking models...")

def rank_models_by_metric(metric_key, higher_is_better=True):
    """Rank models by a specific metric"""
    ranked = sorted(
        models_metrics.items(),
        key=lambda x: x[1].get(metric_key, 0),
        reverse=higher_is_better
    )
    return [(model, data[metric_key]) for model, data in ranked]

# Create rankings
rankings = {
    "Overall Credibility": rank_models_by_metric("mean_credibility", True),
    "Code Quality Score": rank_models_by_metric("mean_quality_score", True),
    "Execution Success": rank_models_by_metric("execution_success_rate", True),
    "Syntax Validity": rank_models_by_metric("syntax_valid_rate", True),
    "Code Complexity": rank_models_by_metric("mean_cyclomatic", False),
    "Perplexity": rank_models_by_metric("mean_perplexity", False)
}

# Find overall winner (by mean credibility)
overall_winner = rankings["Overall Credibility"][0][0]


###################################################################
# Step 4: Generate comparison plots
###################################################################

print("Generating comparison plots...")
plots_dir = comparison_dir / "plots"
plots_dir.mkdir(exist_ok=True)

def plot_models_comparison_bar(metric_key, metric_label, output_path, higher_is_better=True):
    """Create bar chart comparing models"""
    models = list(models_metrics.keys())
    values = [models_metrics[m][metric_key] for m in models]
    
    # Sort by value
    sorted_pairs = sorted(zip(models, values), key=lambda x: x[1], reverse=higher_is_better)
    models_sorted = [p[0] for p in sorted_pairs]
    values_sorted = [p[1] for p in sorted_pairs]
    
    # Shorten model names for display
    labels = [m.split('/')[-1] if '/' in m else m for m in models_sorted]
    
    plt.figure(figsize=(12, 6))
    colors = ['#667eea' if i == 0 else '#764ba2' for i in range(len(models_sorted))]
    bars = plt.bar(range(len(labels)), values_sorted, color=colors, edgecolor='black', linewidth=1.5)
    
    plt.xlabel('Model', fontsize=12, fontweight='bold')
    plt.ylabel(metric_label, fontsize=12, fontweight='bold')
    plt.title(f'Models Comparison: {metric_label}', fontsize=14, fontweight='bold')
    plt.xticks(range(len(labels)), labels, rotation=45, ha='right')
    plt.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, values_sorted)):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.2f}',
                ha='center', va='bottom', fontweight='bold', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

def plot_models_comparison_grouped(metrics_dict, output_path, title):
    """Create grouped bar chart for multiple metrics"""
    models = list(models_metrics.keys())
    labels = [m.split('/')[-1] if '/' in m else m for m in models]
    
    x = range(len(models))
    width = 0.8 / len(metrics_dict)
    
    fig, ax = plt.subplots(figsize=(14, 7))
    colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a']
    
    for idx, (metric_key, metric_label) in enumerate(metrics_dict.items()):
        values = [models_metrics[m][metric_key] for m in models]
        offset = width * (idx - len(metrics_dict)/2 + 0.5)
        bars = ax.bar([i + offset for i in x], values, width, 
                      label=metric_label, color=colors[idx % len(colors)],
                      edgecolor='black', linewidth=0.5)
    
    ax.set_xlabel('Model', fontsize=12, fontweight='bold')
    ax.set_ylabel('Value', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

def plot_radar_chart(models_list, metrics_dict, output_path):
    """Create radar chart comparing models across multiple dimensions"""
    from math import pi
    
    categories = list(metrics_dict.keys())
    N = len(categories)
    
    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]
    
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
    colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b']
    
    for idx, model in enumerate(models_list):
        values = []
        for metric_key in metrics_dict.values():
            val = models_metrics[model][metric_key]
            # Normalize to 0-100 scale
            all_vals = [models_metrics[m][metric_key] for m in models_metrics.keys()]
            min_val, max_val = min(all_vals), max(all_vals)
            normalized = 100 * (val - min_val) / (max_val - min_val) if max_val > min_val else 50
            values.append(normalized)
        
        values += values[:1]
        label = model.split('/')[-1] if '/' in model else model
        ax.plot(angles, values, 'o-', linewidth=2, label=label, color=colors[idx % len(colors)])
        ax.fill(angles, values, alpha=0.15, color=colors[idx % len(colors)])
    
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, size=10)
    ax.set_ylim(0, 100)
    ax.set_title('Multi-Dimensional Model Comparison', size=14, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    ax.grid(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()

# Generate individual metric comparisons
plot_models_comparison_bar("mean_credibility", "Mean Credibility Score",
                           plots_dir / "credibility_comparison.png", True)

plot_models_comparison_bar("mean_quality_score", "Semantic Quality Score",
                           plots_dir / "quality_comparison.png", True)

plot_models_comparison_bar("execution_success_rate", "Execution Success Rate (%)",
                           plots_dir / "execution_comparison.png", True)

plot_models_comparison_bar("mean_perplexity", "Mean Perplexity",
                           plots_dir / "perplexity_comparison.png", False)

plot_models_comparison_bar("mean_cyclomatic", "Mean Cyclomatic Complexity",
                           plots_dir / "complexity_comparison.png", False)

# Generate grouped comparisons
plot_models_comparison_grouped(
    {
        "mean_credibility": "Credibility",
        "mean_quality_score": "Quality Score",
        "syntax_valid_rate": "Syntax Valid %"
    },
    plots_dir / "overall_metrics_comparison.png",
    "Overall Performance Metrics"
)

plot_models_comparison_grouped(
    {
        "mean_functions": "Functions",
        "mean_classes": "Classes",
        "mean_lines": "Lines of Code"
    },
    plots_dir / "code_structure_comparison.png",
    "Code Structure Metrics"
)

# Generate radar chart
radar_metrics = {
    "Credibility": "mean_credibility",
    "Quality": "mean_quality_score",
    "Execution": "execution_success_rate",
    "Syntax": "syntax_valid_rate"
}
plot_radar_chart(list(models_metrics.keys())[:5], radar_metrics,
                plots_dir / "radar_comparison.png")


###################################################################
# Step 5: Generate HTML comparison report
###################################################################

print("Generating HTML comparison report...")

template_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Models Comparison Report</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
        }
        .header h1 { margin: 0 0 10px 0; font-size: 36px; }
        .header p { margin: 5px 0; opacity: 0.9; font-size: 18px; }
        .winner-banner {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .winner-banner h2 { margin: 0; font-size: 28px; }
        .winner-banner .model-name { font-size: 32px; font-weight: bold; margin: 10px 0; }
        .section {
            background: white;
            padding: 30px;
            margin-bottom: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .section h2 {
            color: #667eea;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
            font-size: 24px;
        }
        .models-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .model-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .model-card h3 {
            margin: 0 0 15px 0;
            font-size: 20px;
            border-bottom: 2px solid rgba(255,255,255,0.3);
            padding-bottom: 10px;
        }
        .model-stat {
            display: flex;
            justify-content: space-between;
            margin: 8px 0;
            font-size: 14px;
        }
        .model-stat-label { opacity: 0.9; }
        .model-stat-value { font-weight: bold; font-size: 16px; }
        .plot-container {
            margin: 30px 0;
            text-align: center;
        }
        .plot-container h3 {
            color: #333;
            margin-bottom: 15px;
            font-size: 20px;
        }
        .plot-container img {
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .plot-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 30px;
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background: #667eea;
            color: white;
            font-weight: 600;
            position: sticky;
            top: 0;
        }
        tr:hover { background: #f5f5f5; }
        .rank-1 { font-weight: bold; color: #4caf50; }
        .rank-2 { color: #2196F3; }
        .rank-3 { color: #ff9800; }
        .medal {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            margin-left: 5px;
        }
        .gold { background: #FFD700; color: #333; }
        .silver { background: #C0C0C0; color: #333; }
        .bronze { background: #CD7F32; color: white; }
        .comparison-table {
            overflow-x: auto;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏆 Models Comparison Report</h1>
        <p>Comprehensive comparison of {{ models_count }} LLM models</p>
        <p><strong>Generated:</strong> {{ generation_time }}</p>
    </div>

    <div class="winner-banner">
        <h2>🥇 Overall Winner</h2>
        <div class="model-name">{{ overall_winner }}</div>
        <p>Highest mean credibility score</p>
    </div>

    <!-- Models Overview -->
    <div class="section">
        <h2>📊 Models Overview</h2>
        <div class="models-grid">
        {% for model_name, metrics in models_metrics.items() %}
            <div class="model-card">
                <h3>{{ model_name.split('/')[-1] if '/' in model_name else model_name }}</h3>
                <div class="model-stat">
                    <span class="model-stat-label">Total Runs:</span>
                    <span class="model-stat-value">{{ metrics.total_runs }}</span>
                </div>
                <div class="model-stat">
                    <span class="model-stat-label">Mean Credibility:</span>
                    <span class="model-stat-value">{{ "%.2f"|format(metrics.mean_credibility) }}</span>
                </div>
                <div class="model-stat">
                    <span class="model-stat-label">Quality Score:</span>
                    <span class="model-stat-value">{{ "%.1f"|format(metrics.mean_quality_score) }}</span>
                </div>
                <div class="model-stat">
                    <span class="model-stat-label">Execution Success:</span>
                    <span class="model-stat-value">{{ "%.1f"|format(metrics.execution_success_rate) }}%</span>
                </div>
                <div class="model-stat">
                    <span class="model-stat-label">Syntax Valid:</span>
                    <span class="model-stat-value">{{ "%.1f"|format(metrics.syntax_valid_rate) }}%</span>
                </div>
            </div>
        {% endfor %}
        </div>
    </div>

    <!-- Rankings -->
    <div class="section">
        <h2>🏅 Rankings by Category</h2>
        
        {% for category, ranking in rankings.items() %}
        <h3>{{ category }}</h3>
        <table>
            <tr>
                <th style="width: 60px;">Rank</th>
                <th>Model</th>
                <th style="text-align: right;">Score</th>
            </tr>
            {% for model, score in ranking %}
            <tr class="rank-{{ loop.index if loop.index <= 3 else 4 }}">
                <td>
                    #{{ loop.index }}
                    {% if loop.index == 1 %}<span class="medal gold">🥇</span>
                    {% elif loop.index == 2 %}<span class="medal silver">🥈</span>
                    {% elif loop.index == 3 %}<span class="medal bronze">🥉</span>
                    {% endif %}
                </td>
                <td><strong>{{ model }}</strong></td>
                <td style="text-align: right;"><strong>{{ "%.2f"|format(score) }}</strong></td>
            </tr>
            {% endfor %}
        </table>
        {% endfor %}
    </div>

    <!-- Visual Comparisons -->
    <div class="section">
        <h2>📈 Visual Comparisons</h2>
        
        <div class="plot-container">
            <h3>Multi-Dimensional Comparison</h3>
            <img src="plots/radar_comparison.png" alt="Radar Comparison">
        </div>

        <div class="plot-container">
            <h3>Overall Performance Metrics</h3>
            <img src="plots/overall_metrics_comparison.png" alt="Overall Metrics">
        </div>

        <div class="plot-grid">
            <div class="plot-container">
                <h3>Mean Credibility Score</h3>
                <img src="plots/credibility_comparison.png" alt="Credibility Comparison">
            </div>
            <div class="plot-container">
                <h3>Semantic Quality Score</h3>
                <img src="plots/quality_comparison.png" alt="Quality Comparison">
            </div>
        </div>

        <div class="plot-grid">
            <div class="plot-container">
                <h3>Execution Success Rate</h3>
                <img src="plots/execution_comparison.png" alt="Execution Comparison">
            </div>
            <div class="plot-container">
                <h3>Mean Perplexity (Lower is Better)</h3>
                <img src="plots/perplexity_comparison.png" alt="Perplexity Comparison">
            </div>
        </div>

        <div class="plot-container">
            <h3>Code Structure Comparison</h3>
            <img src="plots/code_structure_comparison.png" alt="Code Structure">
        </div>

        <div class="plot-container">
            <h3>Cyclomatic Complexity (Lower is Better)</h3>
            <img src="plots/complexity_comparison.png" alt="Complexity Comparison">
        </div>
    </div>

    <!-- Detailed Metrics Table -->
    <div class="section">
        <h2>📋 Detailed Metrics Comparison</h2>
        <div class="comparison-table">
            <table>
                <tr>
                    <th>Model</th>
                    <th>Runs</th>
                    <th>Credibility</th>
                    <th>Quality</th>
                    <th>Perplexity</th>
                    <th>Avg Prob</th>
                    <th>Functions</th>
                    <th>Classes</th>
                    <th>Lines</th>
                    <th>Complexity</th>
                    <th>Syntax %</th>
                    <th>Exec %</th>
                </tr>
                {% for model, metrics in models_metrics.items() %}
                <tr>
                    <td><strong>{{ model }}</strong></td>
                    <td>{{ metrics.total_runs }}</td>
                    <td>{{ "%.2f"|format(metrics.mean_credibility) }}</td>
                    <td>{{ "%.1f"|format(metrics.mean_quality_score) }}</td>
                    <td>{{ "%.2f"|format(metrics.mean_perplexity) }}</td>
                    <td>{{ "%.3f"|format(metrics.mean_avg_prob) }}</td>
                    <td>{{ "%.1f"|format(metrics.mean_functions) }}</td>
                    <td>{{ "%.1f"|format(metrics.mean_classes) }}</td>
                    <td>{{ "%.0f"|format(metrics.mean_lines) }}</td>
                    <td>{{ "%.2f"|format(metrics.mean_cyclomatic) }}</td>
                    <td>{{ "%.1f"|format(metrics.syntax_valid_rate) }}%</td>
                    <td>{{ "%.1f"|format(metrics.execution_success_rate) }}%</td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>

    <div style="text-align: center; padding: 20px; color: #666; font-size: 14px;">
        <p>Generated by Models Comparison Tool</p>
        <p>Compare individual model reports in their respective folders</p>
    </div>
</body>
</html>
"""

# Save template
template_path = comparison_dir / "models_comparison_template.html"
with open(template_path, "w", encoding="utf-8") as f:
    f.write(template_content)

# Render template
env = Environment(loader=FileSystemLoader(str(comparison_dir)))
template = env.get_template("models_comparison_template.html")

import datetime
html_report = template.render(
    models_count=len(models_data),
    generation_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    overall_winner=overall_winner,
    models_metrics=models_metrics,
    rankings=rankings
)

# Write report
report_path = comparison_dir / "models_comparison_report.html"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(html_report)


###################################################################
# Step 6: Save JSON summary
###################################################################

comparison_summary = {
    "total_models": len(models_data),
    "overall_winner": overall_winner,
    "models_metrics": models_metrics,
    "rankings": {k: [(m, float(s)) for m, s in v] for k, v in rankings.items()}
}

with open(comparison_dir / "models_comparison_summary.json", "w", encoding="utf-8") as f:
    json.dump(comparison_summary, f, indent=4, ensure_ascii=False)

print(f"\n✅ Models comparison complete! Report saved in: {comparison_dir}")
print("   - models_comparison_report.html")
print("   - models_comparison_summary.json")
print(f"   - plots/ (contains {len(list(plots_dir.glob('*.png')))} visualization files)")
print(f"\n🏆 Summary:")
print(f"   Models compared: {len(models_data)}")
print(f"   Overall winner: {overall_winner}")
print(f"   Winner's mean credibility: {models_metrics[overall_winner]['mean_credibility']:.2f}")
print("\n📊 Top 3 by Credibility:")
for idx, (model, score) in enumerate(rankings["Overall Credibility"][:3], 1):
    medal = ["🥇", "🥈", "🥉"][idx-1]
    print(f"   {medal} {idx}. {model}: {score:.2f}")