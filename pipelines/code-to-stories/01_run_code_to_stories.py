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
    """Loads the YAML configuration file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        config_path = Path("config.yaml")
    if not config_path.exists():
        print("Error: Configuration file not found.")
        return None
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_model_provider(model_identifier, config):
    """Finds the provider for a given model identifier from the config."""
    if not config or 'models' not in config:
        return None
    
    for model_key, model_info in config['models'].items():
        if model_info.get('identifier') == model_identifier:
            return model_info.get('provider')
    
    print(f"Warning: Provider not found for model {model_identifier}. Defaulting to 'None'.")
    return None

def initialize_llm(model_identifier, provider):
    """Initializes and returns a ChatOpenAI instance for the given model."""
    print(f"Initializing LLM for: {model_identifier} (Provider: {provider})")
    
    extra_body = {}
    if provider:
        extra_body["provider"] = {"order": [provider]}

    return ChatOpenAI(
        api_key=getenv("OPENROUTER_API_KEY"),
        base_url=getenv("OPENROUTER_BASE_URL"),
        model=model_identifier
    ).bind(
        logprobs=False, # We don't need logprobs for this task
        temperature=0.8,
        extra_body=extra_body
    )

def run_code_to_stories_generation(config):
    """
    Scans for all 'raw_response.json' files, sends the code to an LLM
    to generate user stories, and saves the stories.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    input_search_path = base_results_dir / "stories-to-code"
    output_base_path = base_results_dir / "code-to-stories"

    if not input_search_path.exists():
        print(f"Error: Input directory does not exist: {input_search_path}")
        return

    print(f"Scanning for 'raw_response.json' files in: {input_search_path}...")
    
    raw_json_files = list(input_search_path.rglob("raw_response.json"))
    
    if not raw_json_files:
        print("No 'raw_response.json' files found to analyze.")
        return

    print(f"Found {len(raw_json_files)} reports to process.")
    
    processed_count = 0
    skipped_count = 0
    llm_cache = {} # Cache to avoid re-initializing the same LLM

    for raw_json_path in raw_json_files:
        try:
            # --- 1. Determine paths and identifiers ---
            run_dir = raw_json_path.parent
            report_number = run_dir.name # e.g., "report_1"
            prompt_variant = run_dir.parent.name
            model_key = run_dir.parent.parent.name
            
            # --- 2. Define Output Path ---
            output_dir = output_base_path / model_key / prompt_variant / report_number
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file_path = output_dir / "generated_user_stories.txt"
            
            # --- 3. Check if already processed ---
            if output_file_path.exists():
                print(f"Skipping (exists): {output_file_path.relative_to(base_results_dir)}")
                skipped_count += 1
                continue

            # --- 4. Load input code ---
            with open(raw_json_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            
            code = raw_data.get("code")
            model_identifier = raw_data.get("model")
            
            if not code or not model_identifier:
                print(f"Skipping (missing code or model): {raw_json_path}")
                continue

            # --- 5. Initialize LLM (use cache) ---
            if model_identifier not in llm_cache:
                provider = get_model_provider(model_identifier, config)
                if provider is None:
                    print(f"Skipping (could not find provider config for model): {model_identifier}")
                    continue
                llm_cache[model_identifier] = initialize_llm(model_identifier, provider)
            
            llm = llm_cache[model_identifier]

            # --- 6. Build the prompt ---
            # Updated prompt as requested by user
            prompt = (
                "Analyze the following Python code and generate a list of user stories that "
                "accurately describe its functionality. Each user story should be on a new line "
                "and follow the format: 'As a [role], I want to [action], so that [benefit]'. "
                "Only output the user stories.\n\n"
                "Python Code:\n"
                "\n```python\n"
                f"{code}"
                "\n```"
            )
            
            # --- 7. LLM call ---
            print(f"Making API call for: {run_dir.relative_to(input_search_path)}")
            msg = llm.invoke(("human", prompt))
            user_stories_text = msg.content.strip()
            
            # --- 8. Save output ---
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write(user_stories_text)
                
            processed_count += 1
            print(f"  ✅ Saved: {output_file_path.relative_to(base_results_dir)}")
            
            time.sleep(5) # Be kind to the API

        except Exception as e:
            print(f"Error processing {raw_json_path}: {e}")

    print("\n--- Code-to-Stories Generation Complete ---")
    print(f"Successfully processed: {processed_count} reports")
    print(f"Skipped (already exist): {skipped_count}")
    print(f"Total found: {len(raw_json_files)}")


if __name__ == "__main__":
    config = load_config()
    if config:
        run_code_to_stories_generation(config)

