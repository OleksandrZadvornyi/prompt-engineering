from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import math
from plot_llm_confidence import plot_llm_confidence

# Load environment variables
load_dotenv()

# Initialize LLM
llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model="openai/gpt-4o"
).bind(logprobs=True)

# --- Step 1: Read user stories file ---
with open("data.txt", "r", encoding="utf-8") as f:
    user_stories = [line.strip() for line in f if line.strip()]

# --- Step 2: Select a few user stories for testing ---
# You can adjust the number of stories here
sample_stories = user_stories[:8]

# Combine them into one prompt
stories_text = "\n".join(sample_stories)

# --- Step 3: Build the LLM prompt ---
prompt = (
    "Write only Python code (no explanations, no markdown formatting or comments) that fulfills the following user stories:\n\n"
    f"{stories_text}\n\n"
    "Do NOT include ```python or ``` anywhere. "
    "The code should be simple, clean, and implement the described functionality as best as possible."
)

# --- Step 4: Get model response ---
msg = llm.invoke(("human", prompt))

# --- Step 5: Display output ---
print("\n--- GENERATED PYTHON CODE ---\n")
print(msg.content)

# --- Step 6: Analyze log probabilities ---
logprobs_data = msg.response_metadata["logprobs"]["content"]

total_logprob = sum(item["logprob"] for item in logprobs_data)
total_tokens = len(logprobs_data)
avg_logprob = total_logprob / total_tokens
avg_prob = math.exp(avg_logprob)
perplexity = math.exp(-avg_logprob)

print("\n--- CONFIDENCE METRICS ---")
print(f"Total tokens: {total_tokens}")
print(f"Total log-probability: {total_logprob:.3f}")
print(f"Average per-token probability: {avg_prob:.2%}")
print(f"Perplexity: {perplexity:.2f}")

plot_llm_confidence(logprobs_data)
