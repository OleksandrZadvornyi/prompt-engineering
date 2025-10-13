from langchain_openai import ChatOpenAI
from os import getenv
from dotenv import load_dotenv
import datetime
import json
from pathlib import Path
import time

# Load environment variables
load_dotenv()

# Configuration
model = "x-ai/grok-4-fast"
results_root = Path("Reports/grok-4-fast_clusters")
results_root.mkdir(exist_ok=True)



###################################################################
###################################################################
###################################################################

for i in range(16, 21):
    # --- Step 0: Determine next request number ---
    request_number = i
    run_dir = results_root / f"report_{request_number}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize LLM
    llm = ChatOpenAI(
        api_key=getenv("OPENROUTER_API_KEY"),
        base_url=getenv("OPENROUTER_BASE_URL"),
        model=model
    ).bind(
        logprobs=True,
        extra_body={
            "provider": {
                "order": [
                    "nebius"
                ]
            }
        }
    )
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 1: Read clustered user stories ---
    # with open("data.txt", "r", encoding="utf-8") as f:
    #     user_stories = [line.strip() for line in f if line.strip()]
        
    with open("clustered_stories.json", "r", encoding="utf-8") as f:
        clustered_stories = json.load(f)
    
    # Build stories text from clusters
    stories_parts = []
    for cluster_id, stories_text in clustered_stories.items():
        stories_parts.append(f"Cluster {cluster_id}:")
        stories_parts.append(stories_text)
        stories_parts.append("")  # Empty line between clusters
    
    stories_text = "\n".join(stories_parts).strip()
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 3: Build the prompt ---
    prompt = (
        "Generate fully functional Python code that implements the following user stories. "
        "The code should realistically reflect the described functionality.\n\n"
        f"{stories_text}\n\n"
        "Output only Python code (no markdown formatting or extra text). "
        "Do not leave functions empty — implement reasonable logic where needed."
    )
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 4: LLM call ---
    print(f"Making API call #{request_number} to {model}...")
    msg = llm.invoke(("human", prompt))
    bool(msg.response_metadata.get("logprobs"))
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 5: Clean code output ---
    code = msg.content.strip()
    if code.startswith("```python"):
        code = code[len("```python"):].strip()
    if code.endswith("```"):
        code = code[:-3].strip()
    
    
    
    ###################################################################
    ###################################################################
    ###################################################################
    
    
    
    # --- Step 6: Save raw response data ---
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save all data needed for analysis
    raw_data = {
        "request_number": request_number,
        "timestamp": timestamp,
        "model": model,
        "prompt": prompt,
        "stories_text": stories_text,
        "code": code,
        "logprobs": msg.response_metadata.get("logprobs", {}),
        "supports_logprobs": bool(msg.response_metadata.get("logprobs")),
        "response_metadata": msg.response_metadata
    }
    
    # Save raw response
    with open(run_dir / "raw_response.json", "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=4, ensure_ascii=False)
    
    # Save generated code separately
    with open(run_dir / "generated_code.py", "w", encoding="utf-8") as f:
        f.write(code)
    
    print(f"\n✅ Raw response saved in folder: {run_dir}")
    print("   - raw_response.json")
    print("   - generated_code.py")
    
    print("\nContinuing after 10 seconds.")
    time.sleep(10)  # Pauses execution for 3 seconds
    print("Continuing...")