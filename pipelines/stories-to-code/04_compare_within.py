import json
import yaml
import math
from pathlib import Path
from collections import defaultdict
from statistics import mean

def load_config():
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        # Fallback to current directory
        config_path = Path("config.yaml")
    
    if not config_path.exists():
        print("Error: Configuration file not found at 'config/config.yaml' or 'config.yaml'")
        return None
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def compute_aggregate_stats(runs_list):
    """Calculates aggregate metrics for a list of summary runs."""
    if not runs_list:
        return {"total_runs": 0}
    
    total_runs = len(runs_list)
    
    def safe_mean(values):
        """Calculates mean, ignoring None and non-finite numbers."""
        valid_values = [v for v in values if v is not None and isinstance(v, (int, float)) and math.isfinite(v)]
        return mean(valid_values) if valid_values else 0.0

    # Core metrics
    avg_credibility = safe_mean([r.get('total_credibility') for r in runs_list])
    avg_tokens = safe_mean([r.get('total_tokens') for r in runs_list])
    avg_perplexity = safe_mean([r.get('perplexity') for r in runs_list])

    # Semantic metrics
    avg_flake8 = safe_mean([r.get('semantic_metrics', {}).get('flake8_error_count') for r in runs_list])
    avg_mypy = safe_mean([r.get('semantic_metrics', {}).get('mypy_error_count') for r in runs_list])
    syntax_valid_rate = safe_mean([1 if r.get('semantic_metrics', {}).get('syntax_valid', False) else 0 for r in runs_list])

    # Execution metrics
    exec_success_rate = safe_mean([1 if r.get('execution_metrics', {}).get('execution_success', False) else 0 for r in runs_list])
    successful_runs = [r for r in runs_list if r.get('execution_metrics', {}).get('execution_success', False)]
    avg_exec_time_on_success = safe_mean([
        r.get('execution_metrics', {}).get('execution_time_sec') for r in successful_runs
    ])

    # Structural metrics
    avg_complexity = safe_mean([r.get('struct_metrics', {}).get('avg_cyclomatic_complexity') for r in runs_list])
    avg_ast_depth = safe_mean([r.get('struct_metrics', {}).get('ast_depth') for r in runs_list])

    return {
        "total_runs": total_runs,
        "avg_total_credibility": avg_credibility,
        "execution_success_rate": exec_success_rate,
        "syntax_valid_rate": syntax_valid_rate,
        "avg_execution_time_sec_on_success": avg_exec_time_on_success,
        "avg_total_tokens": avg_tokens,
        "avg_perplexity": avg_perplexity,
        "avg_flake8_error_count": avg_flake8,
        "avg_mypy_error_count": avg_mypy,
        "avg_cyclomatic_complexity": avg_complexity,
        "avg_ast_depth": avg_ast_depth
    }

def generate_comparison_report(config):
    """
    Scans for all 'summary.json' files, aggregates them by model and 
    prompt_variant, and saves a single 'comparison_report.json'.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    search_path = base_results_dir / "stories-to-code"

    if not search_path.exists():
        print(f"Error: Search directory does not exist: {search_path}")
        return

    print(f"Scanning for 'summary.json' files in: {search_path}...")
    summary_files = list(search_path.rglob("summary.json"))
    
    if not summary_files:
        print("No 'summary.json' files found to compare.")
        return

    print(f"Found {len(summary_files)} summary files. Aggregating...")

    # Step 1: Collect all data, grouped by model and variant
    all_data = defaultdict(lambda: defaultdict(list))
    for summary_path in summary_files:
        try:
            with open(summary_path, 'r', encoding="utf-8") as f:
                data = json.load(f)
            
            model = data.get('model', 'unknown-model')
            variant = data.get('prompt_variant', 'unknown-variant')
            all_data[model][variant].append(data)
        except Exception as e:
            print(f"Warning: Could not process {summary_path}. Error: {e}")

    # Step 2: Build the comparison report
    comparison_report = {"models": {}}
    all_models = sorted(all_data.keys())
    all_variants_global = set()
    all_runs_global = []

    for model in all_models:
        comparison_report["models"][model] = {
            "prompt_variants": {},
            "model_overall_stats": {}
        }
        prompt_variants = sorted(all_data[model].keys())
        model_total_runs_list = []
        
        for variant in prompt_variants:
            all_variants_global.add(variant)
            runs = all_data[model][variant]
            model_total_runs_list.extend(runs)
            
            # Get aggregate stats for this specific model/variant pair
            stats = compute_aggregate_stats(runs)
            
            comparison_report["models"][model]["prompt_variants"][variant] = {
                "summary_stats": stats,
                "all_runs": runs  # Include the raw data for all runs
            }
        
        # Calculate model-level overall stats (aggregating all its variants)
        model_overall_stats = compute_aggregate_stats(model_total_runs_list)
        comparison_report["models"][model]["model_overall_stats"] = model_overall_stats
        all_runs_global.extend(model_total_runs_list)

    # Step 3: Add global stats (aggregating all models)
    global_stats = compute_aggregate_stats(all_runs_global)
    comparison_report["global_stats"] = {
        **global_stats,  # Unpack all the avg stats
        "total_models_compared": len(all_models),
        "prompt_variants_found": sorted(list(all_variants_global)),
    }
    
    # Step 4: Save the final report
    output_path = search_path / "comparison_report.json"
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(comparison_report, f, indent=4, ensure_ascii=False)
        print(f"\nSuccessfully generated comparison report!")
        print(f"Report saved to: {output_path}")
    except Exception as e:
        print(f"\nError: Could not save report to {output_path}. Error: {e}")


if __name__ == "__main__":
    config = load_config()
    if config:
        generate_comparison_report(config)
