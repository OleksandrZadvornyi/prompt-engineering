from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import math

load_dotenv()

llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model="openai/gpt-4o"
).bind(logprobs=True)

# Ask for code generation
msg = llm.invoke(("human", "Write only the Python code (no explanation): a function that returns the square of a number."))

logprobs_data = msg.response_metadata["logprobs"]["content"]

print("\n--- MODEL OUTPUT ---\n")
print(msg.content)

# Token probability table
print("\n" + "="*70)
print(f"{'Token':<12} {'Log Prob':<12} {'Probability':<12}")
print("="*70)

for item in logprobs_data:
    prob = math.exp(item['logprob'])
    print(f"{item['token']:<12} {item['logprob']:>11.6f} {prob:>11.2%}")

print("="*70)

# Confidence metrics
total_logprob = sum(item["logprob"] for item in logprobs_data)
total_tokens = len(logprobs_data)
avg_logprob = total_logprob / total_tokens
avg_prob = math.exp(avg_logprob)
perplexity = math.exp(-avg_logprob)

print(f"\nTotal tokens: {total_tokens}")
print(f"Total log-probability: {total_logprob:.6f}")
print(f"Average per-token probability: {avg_prob:.2%}")
print(f"Perplexity: {perplexity:.2f}")
print("=" * 70)
