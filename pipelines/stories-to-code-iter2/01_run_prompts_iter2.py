import datetime
import json
import re
import time
import yaml
from pathlib import Path
from os import getenv
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

# Load environment variables
load_dotenv()

def load_config():
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        config_path = Path("config.yaml")
    if not config_path.exists():
        print("Error: Configuration file not found.")
        return None
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def extract_user_stories(text):
    """
    Extracts only valid user stories from text that might contain
    LLM conversational filler.
    """
    stories = []
    # Regex to find "As a... I want to... so that..." pattern.
    # re.IGNORECASE handles variations in capitalization.
    pattern = re.compile(r'(As a .*? I want to .*? so that .*)', re.IGNORECASE)
    
    for line in text.splitlines():
        # Clean up standard markdown list markers if present
        clean_line = line.strip().lstrip('-*1234567890. ')
        clean_line = clean_line.strip().replace("**", "")
        match = pattern.search(clean_line)
        if match:
            stories.append(match.group(1).strip())
            
    return "\n".join(stories)

def run_iteration_2(config):
    # --- 1. Setup Configuration & Paths ---
    experiment_config = config['experiment']
    model_key = experiment_config['model_key']
    prompt_variant = experiment_config['prompt_variant']

    model_identifier = config['models'][model_key]['identifier']
    model_provider = config['models'][model_key]['provider']

    base_results_dir = Path(config['project_paths']['results_dir'])
    
    # INPUT: Iteration 1 results (US')
    input_base_dir = base_results_dir / "code-to-stories" / model_key / prompt_variant
    # OUTPUT: Iteration 2 results (CODE')
    output_base_dir = base_results_dir / "stories-to-code-iter2" / model_key / prompt_variant

    print(f"--- Starting Iteration 2 (US' -> CODE') ---")
    print(f"Model: {model_key}")
    print(f"Input (US'): {input_base_dir}")
    print(f"Output (CODE'): {output_base_dir}")

    if not input_base_dir.exists():
        print(f"Error: Input directory not found. Run Iteration 1 first.")
        return

    # --- 2. Initialize LLM ---
    llm = ChatOpenAI(
        api_key=getenv("OPENROUTER_API_KEY"),
        base_url=getenv("OPENROUTER_BASE_URL"),
        model=model_identifier
    ).bind(
        logprobs=True,
        temperature=0.8,
        extra_body={"provider": {"order": [model_provider]}}
    )

    # --- 3. Scan for completed Iteration 1 reports ---
    report_dirs = sorted([d for d in input_base_dir.iterdir() if d.is_dir() and d.name.startswith("report_")])
    if not report_dirs:
        print("No 'report_{n}' directories found to process.")
        return

    print(f"Found {len(report_dirs)} reports to process.")

    for report_dir in report_dirs:
        report_name = report_dir.name
        output_dir = output_base_dir / report_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "raw_response.json"

        if output_file.exists():
             print(f"Skipping {report_name} (already exists)")
             continue

        # --- 4. Load and CLEAN the User Stories (US') ---
        us_prime_file = report_dir / "generated_user_stories.txt"
        if not us_prime_file.exists():
            print(f"Warning: {report_name} missing generated_user_stories.txt. Skipping.")
            continue

        with open(us_prime_file, "r", encoding="utf-8") as f:
            raw_stories_text = f.read()

        # Apply regex cleaning to get only the stories
        cleaned_stories_text = extract_user_stories(raw_stories_text)

        if not cleaned_stories_text:
             print(f"Warning: No valid 'As a...' stories found in {report_name} after cleaning. Skipping.")
             continue

        # --- 5. Build Prompt and Run LLM ---
        print(f"Processing {report_name} -> Generating CODE'...")
        
        # Using the exact same prompt template as Iteration 1 for fairness
        prompt = (
            "First, think step-by-step to outline the necessary classes, functions, and logic. "
            "Then, generate the fully functional Python code that implements the following user stories. "
            "The code should realistically reflect the described functionality.\n\n"
            f"{cleaned_stories_text}\n\n"
            "Output only Python code (no markdown formatting or extra text). "
            "Do not leave functions empty — implement reasonable logic where needed."
        )

        try:
            msg = llm.invoke(("human", prompt))
            
            # Extract code from potential markdown blocks
            response_text = msg.content
            code_pattern = r"```(?:python|py)?\s*\n(.*?)```"
            match = re.search(code_pattern, response_text, re.DOTALL)
            code = match.group(1).strip() if match else response_text.strip()

            # --- 6. Save Results ---
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            raw_data = {
                "iteration": 2,
                "source_report": report_name,
                "timestamp": timestamp,
                "model": model_identifier,
                "prompt_variant": prompt_variant,
                "input_stories_source": str(us_prime_file),
                "stories_text_raw": raw_stories_text,   # Save original noisy input for debugging
                "stories_text_cleaned": cleaned_stories_text, # Save what was actually sent to LLM
                "code": code,
                "logprobs": msg.response_metadata.get("logprobs", {}),
                "response_metadata": msg.response_metadata
            }

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(raw_data, f, indent=4, ensure_ascii=False)
            
            print(f"  ✅ Saved CODE' to: {output_dir.relative_to(base_results_dir)}")
            time.sleep(5)

        except Exception as e:
            print(f"  ❌ Error processing {report_name}: {e}")

if __name__ == "__main__":
    config = load_config()
    if config:
        run_iteration_2(config)