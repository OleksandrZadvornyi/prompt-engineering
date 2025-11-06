import json
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

def run_code_to_stories_iter2(config):
    # --- 1. Setup Configuration & Paths ---
    experiment_config = config['experiment']
    model_key = experiment_config['model_key']
    prompt_variant = experiment_config['prompt_variant']

    model_identifier = config['models'][model_key]['identifier']
    model_provider = config['models'][model_key]['provider']

    base_results_dir = Path(config['project_paths']['results_dir'])
    
    # INPUT: Iteration 2 CODE' (from stories-to-code-iter2)
    input_base_dir = base_results_dir / "stories-to-code-iter2" / model_key / prompt_variant
    # OUTPUT: Iteration 2 US'' (to code-to-stories-iter2)
    output_base_dir = base_results_dir / "code-to-stories-iter2" / model_key / prompt_variant

    print(f"--- Starting Iteration 2 Part B (CODE' -> US'') ---")
    print(f"Model: {model_key}")
    print(f"Input (CODE'): {input_base_dir}")
    print(f"Output (US''): {output_base_dir}")

    if not input_base_dir.exists():
        print(f"Error: Input directory not found. Run 'run_prompts_iter2.py' first.")
        return

    # --- 2. Initialize LLM ---
    llm = ChatOpenAI(
        api_key=getenv("OPENROUTER_API_KEY"),
        base_url=getenv("OPENROUTER_BASE_URL"),
        model=model_identifier
    ).bind(
        logprobs=False, # Not strictly needed for text generation, but can be enabled if desired
        temperature=0.8,
        extra_body={"provider": {"order": [model_provider]}}
    )

    # --- 3. Scan for Iteration 2 CODE' reports ---
    report_dirs = sorted([d for d in input_base_dir.iterdir() if d.is_dir() and d.name.startswith("report_")])
    if not report_dirs:
        print("No 'report_{n}' directories found to process.")
        return

    print(f"Found {len(report_dirs)} reports to process.")

    for report_dir in report_dirs:
        report_name = report_dir.name
        output_dir = output_base_dir / report_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "generated_user_stories.txt"

        if output_file.exists():
             print(f"Skipping {report_name} (already exists)")
             continue

        # --- 4. Load CODE' ---
        raw_json_path = report_dir / "raw_response.json"
        if not raw_json_path.exists():
            print(f"Warning: {report_name} missing raw_response.json. Skipping.")
            continue

        with open(raw_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            code_prime = data.get("code")

        if not code_prime:
             print(f"Warning: No code found in {report_name} JSON. Skipping.")
             continue

        # --- 5. Build Prompt (Same as Iteration 1 for consistency) ---
        print(f"Processing {report_name} -> Generating US''...")
        
        prompt = (
            "Analyze the following Python code and generate a list of user stories that "
            "accurately describe its functionality. Each user story should be on a new line "
            "and follow the format: 'As a [role], I want to [action], so that [benefit]'. "
            "Only output the user stories.\n\n"
            "Python Code:\n"
            "\n```python\n"
            f"{code_prime}"
            "\n```"
        )

        try:
            # --- 6. Run LLM ---
            msg = llm.invoke(("human", prompt))
            us_double_prime = msg.content.strip()

            # --- 7. Save Results (US'') ---
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(us_double_prime)
            
            print(f"  ✅ Saved US'' to: {output_dir.relative_to(base_results_dir)}")
            time.sleep(5)

        except Exception as e:
            print(f"  ❌ Error processing {report_name}: {e}")

if __name__ == "__main__":
    config = load_config()
    if config:
        run_code_to_stories_iter2(config)