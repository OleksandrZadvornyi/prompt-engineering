from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    
    load_dotenv()
except ImportError:
    pass


import getpass
import os

if not os.environ.get("GOOGLE_API_KEY"):
  os.environ["GOOGLE_API_KEY"] = getpass.getpass("Enter API key for Google Gemini: ")

# Initialize model
model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")

# Few-shot prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "Translate English to Ukrainian"),
    ("user", "cat"),
    ("assistant", "кіт"),
    ("user", "dog"),
    ("assistant", "собака"),
    ("user", "{word}")
])

# Invoke with new input
response = model.invoke(prompt.invoke({"word": "tree"}))
print(response.content)
