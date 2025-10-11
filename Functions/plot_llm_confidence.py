import math
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.ndimage import uniform_filter1d

def plot_llm_confidence(logprobs_data, output_dir=".", title_prefix="LLM Output", code_text=None):
    """Generates and saves comprehensive confidence plots to output_dir."""

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logprobs = [item["logprob"] for item in logprobs_data]
    probs = [math.exp(lp) for lp in logprobs]
    tokens = [item.get("token", "") for item in logprobs_data]

    # --- 1. Log-Probability Trend ---
    plt.figure(figsize=(12, 4))
    plt.plot(logprobs, marker="o", linestyle="-", color="orange")
    plt.title(f"{title_prefix}: Token Log-Probability Trend")
    plt.xlabel("Token Index")
    plt.ylabel("Log Probability")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/1_logprob_trend.png", bbox_inches="tight")
    plt.close()

    # --- 2. Probability Distribution Histogram ---
    plt.figure(figsize=(6, 4))
    plt.hist(probs, bins=30, color="lightgreen", edgecolor="black")
    plt.title(f"{title_prefix}: Distribution of Token Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Token Count")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/2_probability_distribution.png", bbox_inches="tight")
    plt.close()

    # --- 3. Cumulative Log-Probability Curve ---
    cumulative = np.cumsum(logprobs)
    plt.figure(figsize=(12, 4))
    plt.plot(cumulative, color="purple")
    plt.title(f"{title_prefix}: Cumulative Log-Probability")
    plt.xlabel("Token Index")
    plt.ylabel("Cumulative Log Probability")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/3_cumulative_logprob.png", bbox_inches="tight")
    plt.close()
    
    # --- 4. NEW: Moving Average Confidence (Smoothed Trend) ---
    window_size = min(50, len(probs) // 10)
    if window_size > 1:
        smoothed_probs = uniform_filter1d(probs, size=window_size, mode='nearest')
        
        plt.figure(figsize=(12, 4))
        plt.plot(probs, alpha=0.3, color='gray', label='Raw Probability')
        plt.plot(smoothed_probs, color='blue', linewidth=2, label=f'Moving Avg (window={window_size})')
        plt.title(f"{title_prefix}: Smoothed Confidence Trend")
        plt.xlabel("Token Index")
        plt.ylabel("Probability")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/4_smoothed_confidence.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    # --- 5. NEW: Low Confidence Regions (Uncertainty Heatmap) ---
    # Highlight tokens where model was uncertain
    plt.figure(figsize=(12, 4))
    threshold = 0.5
    colors = ['red' if p < threshold else 'green' for p in probs]
    plt.scatter(range(len(probs)), probs, c=colors, alpha=0.6, s=10)
    plt.axhline(y=threshold, color='black', linestyle='--', label=f'Threshold ({threshold})')
    plt.title(f"{title_prefix}: Uncertainty Detection (Red = Low Confidence)")
    plt.xlabel("Token Index")
    plt.ylabel("Probability")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/5_uncertainty_heatmap.png", bbox_inches="tight", dpi=150)
    plt.close()
    
    # --- 6. NEW: Perplexity Over Time (Rolling Window) ---
    window = min(100, len(logprobs) // 5)
    if window > 1:
        rolling_perplexity = []
        for i in range(len(logprobs)):
            start = max(0, i - window + 1)
            window_logprobs = logprobs[start:i+1]
            avg_logprob = np.mean(window_logprobs)
            perplexity = math.exp(-avg_logprob) if math.isfinite(avg_logprob) else float('inf')
            rolling_perplexity.append(min(perplexity, 1000))  # Cap for visualization
        
        plt.figure(figsize=(12, 4))
        plt.plot(rolling_perplexity, color='crimson')
        plt.title(f"{title_prefix}: Rolling Perplexity (window={window})")
        plt.xlabel("Token Index")
        plt.ylabel("Perplexity")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/6_rolling_perplexity.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    # --- 7. NEW: Confidence Percentiles (Box Plot by Segments) ---
    # Divide output into segments and compare confidence distributions
    n_segments = min(10, len(probs) // 50)
    if n_segments > 1:
        segment_size = len(probs) // n_segments
        segments = [probs[i*segment_size:(i+1)*segment_size] for i in range(n_segments)]
        
        plt.figure(figsize=(12, 5))
        plt.boxplot(segments, labels=[f"{i*segment_size}-{(i+1)*segment_size}" for i in range(n_segments)])
        plt.title(f"{title_prefix}: Confidence Distribution Across Output Segments")
        plt.xlabel("Token Range")
        plt.ylabel("Probability")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/7_confidence_by_segment.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    # --- 8. NEW: Token Type Analysis (if code provided) ---
    if code_text:
        # Categorize tokens by type
        special_chars = set('(){}[],:;=+-*/<>!&|')
        token_categories = []
        
        for token in tokens:
            if token.strip() == '':
                token_categories.append('whitespace')
            elif token in special_chars:
                token_categories.append('operator')
            elif token.isdigit():
                token_categories.append('number')
            elif token.isalpha():
                token_categories.append('keyword/identifier')
            else:
                token_categories.append('other')
        
        # Calculate average confidence per category
        category_probs = {}
        for cat, prob in zip(token_categories, probs):
            if cat not in category_probs:
                category_probs[cat] = []
            category_probs[cat].append(prob)
        
        avg_by_category = {cat: np.mean(probs_list) for cat, probs_list in category_probs.items()}
        
        plt.figure(figsize=(8, 5))
        categories = list(avg_by_category.keys())
        averages = list(avg_by_category.values())
        plt.bar(categories, averages, color='steelblue', edgecolor='black')
        plt.title(f"{title_prefix}: Average Confidence by Token Type")
        plt.xlabel("Token Category")
        plt.ylabel("Average Probability")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"{output_dir}/8_confidence_by_token_type.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    # --- 9. NEW: Confidence Volatility (Standard Deviation in Windows) ---
    window = min(50, len(probs) // 10)
    if window > 1:
        rolling_std = []
        for i in range(len(probs)):
            start = max(0, i - window + 1)
            window_probs = probs[start:i+1]
            rolling_std.append(np.std(window_probs))
        
        plt.figure(figsize=(12, 4))
        plt.plot(rolling_std, color='darkorange')
        plt.title(f"{title_prefix}: Confidence Volatility (Rolling Std Dev)")
        plt.xlabel("Token Index")
        plt.ylabel("Standard Deviation")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/9_confidence_volatility.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    # --- 10. NEW: Top Uncertain Tokens (Bottom 20 by probability) ---
    if len(probs) >= 20:
        indexed_probs = [(i, p, tokens[i]) for i, p in enumerate(probs)]
        bottom_20 = sorted(indexed_probs, key=lambda x: x[1])[:20]
        
        indices = [x[0] for x in bottom_20]
        probabilities = [x[1] for x in bottom_20]
        token_labels = [f"{x[2][:10]}..." if len(x[2]) > 10 else x[2] for x in bottom_20]
        
        plt.figure(figsize=(10, 6))
        plt.barh(range(len(indices)), probabilities, color='coral')
        plt.yticks(range(len(indices)), [f"#{idx}: '{tok}'" for idx, tok in zip(indices, token_labels)])
        plt.xlabel("Probability")
        plt.title(f"{title_prefix}: Top 20 Most Uncertain Tokens")
        plt.tight_layout()
        plt.savefig(f"{output_dir}/10_top_uncertain_tokens.png", bbox_inches="tight", dpi=150)
        plt.close()
    
    print(f"✅ Generated {10 if code_text else 9} plots in {output_dir}")
