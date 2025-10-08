from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    
    load_dotenv()
except ImportError:
    pass

# Initialize model
model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")

# Chain of thought prompt
cot_prompt = ChatPromptTemplate.from_messages([
    ("system", "Solve the problem step by step, then give the final answer."),
    ("user", "If a train travels 60 km in 1 hour, how far will it travel in 3.5 hours?")
])

# Invoke
response = model.invoke(cot_prompt.invoke({}))
print(response.content)
