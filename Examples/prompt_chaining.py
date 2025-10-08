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

# First prompt: summarize text
summarize = ChatPromptTemplate.from_messages([
    ("system", "Summarize this text in one sentence"),
    ("user", "{text}")
])

# Second prompt: translate summary to Ukrainian
translate = ChatPromptTemplate.from_messages([
    ("system", "Translate English to Ukrainian"),
    ("user", "{summary}")
])

# Run chain
summary = model.invoke(summarize.invoke({"text": "LangChain is a framework for building LLM-powered apps."}))
translation = model.invoke(translate.invoke({"summary": summary.content}))

print("Summary:", summary.content)
print("Translation:", translation.content)
