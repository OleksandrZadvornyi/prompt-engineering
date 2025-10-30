import numpy as np
import json
import yaml
import re
from pathlib import Path
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.manifold import TSNE
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for saving plots
import matplotlib.pyplot as plt

# --- Step 1: Initialize Models ---
print("Initializing embedding model (all-MiniLM-L6-v2)...")
try:
    EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
except Exception as e:
    print(f"Error initializing SentenceTransformer: {e}")
    print("Please ensure you have network access and 'sentence-transformers' is installed.")
    exit()
print("Model loaded.")

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

def parse_original_stories(story_text_block):
    """Parses the original user-stories.txt file into a list of stories."""
    stories = []
    for line in story_text_block.split('\n'):
        cleaned_line = line.strip()
        # Filter out empty lines and cluster headers
        if cleaned_line and not re.match(r'^Cluster \d+:$', cleaned_line):
            stories.append(cleaned_line)
    return stories

def parse_generated_stories(story_text_block):
    """
    Parses a block of text from an LLM into a list of user stories,
    handling prefixes (e.g., "1.") and markdown ("**").
    """
    stories = []
    # Regex to find "As a..." stories, ignoring optional prefixes and markdown
    story_pattern = re.compile(r'(As a .*? I want to .*? so that .*)', re.IGNORECASE)
    
    for line in story_text_block.split('\n'):
        # Clean the line of markdown and extra whitespace
        cleaned_line = line.strip().replace("**", "")
        
        match = story_pattern.search(cleaned_line)
        if match:
            # Extract the core story, strip extra chars
            story = match.group(1).strip().rstrip('.,')
            stories.append(story)
    return stories

def analyze_all_reports(config):
    """
    Scans for generated stories, compares them to originals,
    and saves consistency scores and plots.
    """
    if not config or 'project_paths' not in config or 'results_dir' not in config['project_paths']:
        print("Error: 'project_paths.results_dir' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    base_data_dir = Path(config['project_paths']['data_dir'])
    code_to_stories_path = base_results_dir / "code-to-stories"

    if not code_to_stories_path.exists():
        print(f"Error: Input directory does not exist: {code_to_stories_path}")
        return

    print(f"Scanning for 'generated_user_stories.txt' files in: {code_to_stories_path}...")
    
    generated_story_files = list(code_to_stories_path.rglob("generated_user_stories.txt"))
    
    if not generated_story_files:
        print("No 'generated_user_stories.txt' files found to analyze.")
        return

    print(f"Found {len(generated_story_files)} reports to process.")
    
    # --- Load Original Stories ONCE ---
    original_stories_file = base_data_dir / "user-stories.txt"
    if not original_stories_file.exists():
        print(f"Error: Original user stories file not found at {original_stories_file}")
        return
        
    with open(original_stories_file, "r", encoding="utf-8") as f:
        original_stories_text = f.read()
    
    original_stories = parse_original_stories(original_stories_text)
    if not original_stories:
        print("Error: No stories parsed from original user-stories.txt file.")
        return
        
    print(f"Loaded {len(original_stories)} original stories.")
    original_embeddings = EMBEDDING_MODEL.encode(original_stories)
    midpoint_original = np.mean(original_embeddings, axis=0)
    
    processed_count = 0
    skipped_count = 0

    for gen_stories_path in generated_story_files:
        try:
            # --- 1. Determine paths ---
            run_dir = gen_stories_path.parent # e.g., "report_1"
            report_number = run_dir.name
            prompt_variant = run_dir.parent.name
            model_key = run_dir.parent.parent.name
            
            # Define output paths
            output_json_path = run_dir / "semantic_consistency.json"
            output_plot_path = run_dir / "semantic_consistency_plot.png"
            
            # --- 2. Check if already processed ---
            # if output_json_path.exists() and output_plot_path.exists():
            #     print(f"Skipping (exists): {run_dir.relative_to(base_results_dir)}")
            #     skipped_count += 1
            #     continue
                
            print(f"Processing: {run_dir.relative_to(base_results_dir)}")
            
            # --- 3. Load Model & Prompt Info from parallel run ---
            # Path to the corresponding stories-to-code run
            stories_to_code_run_dir = base_results_dir / "stories-to-code" / model_key / prompt_variant / report_number
            raw_json_path = stories_to_code_run_dir / "raw_response.json"

            model_name = model_key # Fallback
            prompt_variant_name = prompt_variant # Fallback

            if not raw_json_path.exists():
                print(f"  Warning: Could not find raw_response.json at {raw_json_path.relative_to(base_results_dir)}")
                print(f"  -> Using folder names '{model_key}' and '{prompt_variant}' as fallbacks.")
            else:
                try:
                    with open(raw_json_path, 'r', encoding='utf-8') as f:
                        raw_data = json.load(f)
                    # Use .get() for safety, falling back to folder names
                    model_name = raw_data.get("model", model_key)
                    prompt_variant_name = raw_data.get("prompt_variant", prompt_variant)
                except Exception as e:
                    print(f"  Warning: Error reading {raw_json_path.relative_to(base_results_dir)}: {e}")
                    print(f"  -> Using folder names '{model_key}' and '{prompt_variant}' as fallbacks.")

            # --- 4. Load Generated Stories ---
            with open(gen_stories_path, "r", encoding="utf-8") as f:
                generated_stories_text = f.read()
            
            generated_stories = parse_generated_stories(generated_stories_text)
            
            if not generated_stories:
                print(f"  Warning: No valid generated stories parsed from {gen_stories_path}")
                continue
                
            # --- 5. Create Embeddings ---
            # Original embeddings already calculated outside the loop
            generated_embeddings = EMBEDDING_MODEL.encode(generated_stories)

            # --- 6. Calculate Midpoints and Cosine Similarity ---
            # midpoint_original already calculated outside the loop
            midpoint_generated = np.mean(generated_embeddings, axis=0)

            similarity_score = cosine_similarity(
                midpoint_original.reshape(1, -1),
                midpoint_generated.reshape(1, -1)
            )[0][0]

            # --- 7. Save Analysis JSON ---
            analysis_data = {
                "model": model_name,
                "prompt_variant": prompt_variant_name,
                "semantic_consistency_score": float(similarity_score),
                "embedding_model": "all-MiniLM-L6-v2",
                "original_stories_count": len(original_stories),
                "generated_stories_count": len(generated_stories)
            }
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(analysis_data, f, indent=4)
            print(f"  ✅ Saved analysis: {output_json_path.name}")

            # --- 8. Generate and Save t-SNE Plot ---
            all_embeddings = np.vstack([
                original_embeddings, 
                generated_embeddings, 
                midpoint_original.reshape(1, -1), 
                midpoint_generated.reshape(1, -1)
            ])
            
            # Adjust perplexity if we have very few points
            perplexity = min(30, max(5, len(all_embeddings) - 2))

            tsne = TSNE(n_components=2, perplexity=perplexity, random_state=42, init='random', learning_rate=200)
            embeddings_2d = tsne.fit_transform(all_embeddings)

            # Separate points for plotting
            original_points = embeddings_2d[:len(original_stories)]
            generated_points = embeddings_2d[len(original_stories):-2]
            midpoint_original_2d = embeddings_2d[-2]
            midpoint_generated_2d = embeddings_2d[-1]

            plt.figure(figsize=(12, 8))
            plt.scatter(original_points[:, 0], original_points[:, 1], c='blue', alpha=0.5, label='Original Stories')
            plt.scatter(generated_points[:, 0], generated_points[:, 1], c='red', alpha=0.7, label='Generated Stories')
            plt.scatter(midpoint_original_2d[0], midpoint_original_2d[1], c='cyan', s=200, marker='X', edgecolors='black', label='Original Centroid')
            plt.scatter(midpoint_generated_2d[0], midpoint_generated_2d[1], c='magenta', s=200, marker='X', edgecolors='black', label='Generated Centroid')
            plt.plot(
                [midpoint_original_2d[0], midpoint_generated_2d[0]],
                [midpoint_original_2d[1], midpoint_generated_2d[1]],
                'g--', label=f'Centroid Connection (Score: {similarity_score:.2f})'
            )
            plt.title(f't-SNE Visualization: {model_key} / {prompt_variant} / {report_number}')
            plt.xlabel('t-SNE Dimension 1')
            plt.ylabel('t-SNE Dimension 2')
            plt.legend()
            plt.grid(True)
            
            plt.savefig(output_plot_path, bbox_inches='tight')
            plt.close() # Frees up memory

            print(f"  ✅ Saved plot: {output_plot_path.name}")
            processed_count += 1

        except Exception as e:
            print(f"Error processing {gen_stories_path}: {e}")

    print("\n--- Semantic Consistency Analysis Complete ---")
    print(f"Successfully processed: {processed_count} reports")
    print(f"Skipped (already exist): {skipped_count}")
    print(f"Total found: {len(generated_story_files)}")


if __name__ == "__main__":
    config = load_config()
    if config:
        analyze_all_reports(config)