from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

# Initialize model
model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")

# Step 1: generate multiple reasoning paths (thoughts)
thoughts_prompt = ChatPromptTemplate.from_messages([
    ("system", "Give 3 different possible ways to solve the problem: {problem}")
])

# Use .to_string() to convert the prompt to a string
thoughts = model.invoke(thoughts_prompt.invoke({"problem": "How can we reduce traffic in a city?"}).to_string())

# Step 2: evaluate and choose the best path
evaluate_prompt = ChatPromptTemplate.from_messages([
    ("system", "Here are possible solutions:\n{options}\nPick the most effective one and explain why.")
])

# Use .to_string() to convert the prompt to a string
decision = model.invoke(evaluate_prompt.invoke({"options": thoughts.content}).to_string())

print("Thoughts:\n", thoughts.content)
print()
print("\nDecision:\n", decision.content)