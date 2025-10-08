from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate

# Initialize model
model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")

# Step 1: generate background knowledge
knowledge_prompt = ChatPromptTemplate.from_messages([
    ("system", "List three quick facts about {topic}"),
])

# Create the prompt value object
knowledge_prompt_value = knowledge_prompt.invoke({"topic": "black holes"})

# Get the string content from the prompt value
knowledge_prompt_content = knowledge_prompt_value.to_string()

# Invoke the model with the string content
knowledge = model.invoke(knowledge_prompt_content)

# Step 2: use that knowledge to answer a question
qa_prompt = ChatPromptTemplate.from_messages([
    ("system", "Use these facts:\n{facts}"),
    ("user", "Why are black holes invisible?")
])

# Create the prompt value object
qa_prompt_value = qa_prompt.invoke({"facts": knowledge.content})

# Get the string content from the prompt value
qa_prompt_content = qa_prompt_value.to_string()

# Invoke the model with the string content
answer = model.invoke(qa_prompt_content)

print("Knowledge:", knowledge.content)
print("\n")
print("Answer:", answer.content)