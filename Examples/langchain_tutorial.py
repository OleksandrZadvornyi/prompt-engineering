# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    
    load_dotenv()
except ImportError:
    pass


import getpass
import os

if not os.environ.get("OPENAI_API_KEY"):
  os.environ["OPENAI_API_KEY"] = getpass.getpass("Enter API key for OpenAI: ")

# Initialize the Gemini model
from langchain.chat_models import init_chat_model

model = init_chat_model("gpt-4o-mini", model_provider="openai")
model

# Example usage
from langchain_core.messages import HumanMessage, SystemMessage

messages = [
    SystemMessage(content="Translate the following from English into Italian"),
    HumanMessage(content="hi!"),
]

model.invoke(messages)


# Prompt template
from langchain_core.prompts import ChatPromptTemplate

system_template = "Translate the following from English into {language}"

prompt_template = ChatPromptTemplate.from_messages(
    [("system", system_template), ("user", "{text}")]
)


# Example usage of prompt template
prompt = prompt_template.invoke({"language": "Ukrainian", "text": "hi!"})

prompt


# Convert prompt back to messages
prompt.to_messages()


# Get model response
response = model.invoke(prompt)
print(response.content)



import google.generativeai as genai
import os

genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

response = genai.GenerativeModel("gemini-2.5-flash").generate_content(
    "5 + 8 = ?",
    generation_config={
        "response_logprobs": True,  # увімкнути логпроби
        "logprobs": 5               # топ-5 токенів для кожного кроку
    }
)

print(response.candidates[0].content.parts[0].text)

# Вивести logprobs
for part in response.candidates[0].content.parts:
    if hasattr(part, "logprobs") and part.logprobs:
        print(part.logprobs)
