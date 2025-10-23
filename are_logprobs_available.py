from os import getenv
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

model = "x-ai/grok-4-fast"  # replace with e.g. "x-ai/grok-4-fast"
provider = "xai" # replace with e.g. "nebius"

load_dotenv()
llm = ChatOpenAI(
    api_key=getenv("OPENROUTER_API_KEY"),
    base_url=getenv("OPENROUTER_BASE_URL"),
    model=model
).bind(
    logprobs=True,
    extra_body={
        "provider": {
            "order": [
                provider
            ]
        }
    }
)

msg = llm.invoke(("human", "Hello"))
print("Raw logprobs field:", msg.response_metadata["logprobs"])
if msg.response_metadata["logprobs"]:
    print(f"✅ Model {model} supports logprobs with provider: {provider}")
else:
    print(f"❌ Model {model} does NOT support logprobs with provider: {provider}")
