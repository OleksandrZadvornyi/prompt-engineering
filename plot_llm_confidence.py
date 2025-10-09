import math
import numpy as np
import matplotlib.pyplot as plt

def plot_llm_confidence(logprobs_data, title_prefix="LLM Output"):
    # Extract tokens and probabilities
    tokens = [item["token"] for item in logprobs_data]
    logprobs = [item["logprob"] for item in logprobs_data]
    probs = [math.exp(lp) for lp in logprobs]

    # --- 1. Token Confidence Bar Chart ---
    plt.figure(figsize=(12, 4))
    plt.bar(range(len(tokens)), probs, color="skyblue")
    plt.xticks(range(len(tokens)), tokens, rotation=90, fontsize=8)
    plt.ylabel("Probability")
    plt.xlabel("Token Index")
    plt.title(f"{title_prefix}: Per-token Confidence")
    plt.tight_layout()
    plt.show()

    # --- 2. Log-Probability Trend ---
    plt.figure(figsize=(12, 4))
    plt.plot(logprobs, marker="o", linestyle="-", color="orange")
    plt.title(f"{title_prefix}: Token Log-Probability Trend")
    plt.xlabel("Token Index")
    plt.ylabel("Log Probability")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # --- 3. Probability Distribution Histogram ---
    plt.figure(figsize=(6, 4))
    plt.hist(probs, bins=30, color="lightgreen", edgecolor="black")
    plt.title(f"{title_prefix}: Distribution of Token Probabilities")
    plt.xlabel("Probability")
    plt.ylabel("Token Count")
    plt.tight_layout()
    plt.show()

    # --- 4. Cumulative Log-Probability Curve ---
    cumulative = np.cumsum(logprobs)
    plt.figure(figsize=(12, 4))
    plt.plot(cumulative, color="purple")
    plt.title(f"{title_prefix}: Cumulative Log-Probability")
    plt.xlabel("Token Index")
    plt.ylabel("Cumulative Log Probability")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # --- 5. Summary Metrics ---
    total_logprob = sum(logprobs)
    avg_logprob = total_logprob / len(logprobs)
    avg_prob = math.exp(avg_logprob)
    perplexity = math.exp(-avg_logprob)

    print("\n=== Confidence Summary ===")
    print(f"Total tokens: {len(logprobs)}")
    print(f"Total log-probability: {total_logprob:.3f}")
    print(f"Average per-token probability: {avg_prob:.2%}")
    print(f"Perplexity: {perplexity:.2f}")
    print("==========================\n")