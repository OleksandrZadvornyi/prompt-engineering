import numpy as np
import json
import yaml
import re
from pathlib import Path
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.manifold import TSNE
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

# --- Step 1: Initialize Models ---
print("Initializing embedding model (all-MiniLM-L6-v2)...")
try:
    EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
except Exception as e:
    print(f"Error initializing SentenceTransformer: {e}")
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
    Parses a block of text from an LLM into a list of user stories.
    """
    stories = []
    story_pattern = re.compile(r'(As a .*? I want to .*? so that .*)', re.IGNORECASE)
    for line in story_text_block.split('\n'):
        cleaned_line = line.strip().replace("**", "")
        match = story_pattern.search(cleaned_line)
        if match:
            story = match.group(1).strip().rstrip('.,')
            stories.append(story)
    return stories

def get_centroid(embeddings):
    """Calculates the mean centroid of a list of embeddings."""
    if embeddings.ndim == 1:
        return embeddings.reshape(1, -1)
    return np.mean(embeddings, axis=0).reshape(1, -1)

def safe_cosine_similarity(centroid1, centroid2):
    """Calculates cosine similarity, returning 0.0 on error (e.g., empty input)."""
    try:
        return cosine_similarity(centroid1, centroid2)[0][0]
    except Exception:
        return 0.0

def analyze_all_reports_iter2(config):
    """
    Scans for Iteration 2 generated stories, compares them to Iteration 1
    and Original stories, and saves consistency scores and plots.
    """
    if not config or 'project_paths' not in config:
        print("Error: 'project_paths' not found in config.")
        return

    base_results_dir = Path(config['project_paths']['results_dir'])
    base_data_dir = Path(config['project_paths']['data_dir'])
    
    # Path to Iteration 2 User Stories (US'')
    code_to_stories_iter2_path = base_results_dir / "code-to-stories-iter2"

    if not code_to_stories_iter2_path.exists():
        print(f"Error: Input directory does not exist: {code_to_stories_iter2_path}")
        return

    print(f"Scanning for 'generated_user_stories.txt' in: {code_to_stories_iter2_path}...")
    
    # These are the US'' files
    iter2_story_files = list(code_to_stories_iter2_path.rglob("generated_user_stories.txt"))
    
    if not iter2_story_files:
        print("No 'generated_user_stories.txt' files found in ...iter2 directory.")
        return

    print(f"Found {len(iter2_story_files)} Iteration 2 reports to process.")
    
    # --- Load Original Stories (US) ONCE ---
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
        
    print(f"Loaded {len(original_stories)} original stories (US).")
    original_embeddings = EMBEDDING_MODEL.encode(original_stories)
    midpoint_original = get_centroid(original_embeddings)
    
    processed_count = 0

    for iter2_stories_path in iter2_story_files:
        try:
            # --- 1. Determine paths ---
            run_dir_iter2 = iter2_stories_path.parent # e.g., ".../code-to-stories-iter2/.../report_1"
            report_number = run_dir_iter2.name
            prompt_variant = run_dir_iter2.parent.name
            model_key = run_dir_iter2.parent.parent.name
            
            print(f"Processing: {run_dir_iter2.relative_to(base_results_dir)}")
            
            # Path to Iteration 1 User Stories (US')
            iter1_stories_path = base_results_dir / "code-to-stories" / model_key / prompt_variant / report_number / "generated_user_stories.txt"
            
            # Output paths
            output_json_path = run_dir_iter2 / "semantic_consistency_report.json"
            output_plot_path = run_dir_iter2 / "semantic_consistency_plot.png"

            # --- 2. Load and Parse US'' (Iteration 2) ---
            with open(iter2_stories_path, "r", encoding="utf-8") as f:
                stories_iter2_text = f.read()
            stories_iter2 = parse_generated_stories(stories_iter2_text)
            
            if not stories_iter2:
                print(f"  Warning: No valid stories parsed from {iter2_stories_path.name}. Skipping.")
                continue
            
            embeddings_iter2 = EMBEDDING_MODEL.encode(stories_iter2)
            midpoint_iter2 = get_centroid(embeddings_iter2)

            # --- 3. Load and Parse US' (Iteration 1) ---
            if not iter1_stories_path.exists():
                print(f"  Warning: Corresponding Iteration 1 file not found at {iter1_stories_path}. Skipping.")
                continue
                
            with open(iter1_stories_path, "r", encoding="utf-8") as f:
                stories_iter1_text = f.read()
            stories_iter1 = parse_generated_stories(stories_iter1_text)
            
            if not stories_iter1:
                print(f"  Warning: No valid stories parsed from {iter1_stories_path.name}. Skipping.")
                continue

            embeddings_iter1 = EMBEDDING_MODEL.encode(stories_iter1)
            midpoint_iter1 = get_centroid(embeddings_iter1)

            # --- 4. Calculate All 3 Similarities ---
            score_us_vs_us1 = safe_cosine_similarity(midpoint_original, midpoint_iter1)
            score_us_vs_us2 = safe_cosine_similarity(midpoint_original, midpoint_iter2)
            score_us1_vs_us2 = safe_cosine_similarity(midpoint_iter1, midpoint_iter2)

            # --- 5. Save Analysis JSON ---
            analysis_data = {
                "model": model_key,
                "prompt_variant": prompt_variant,
                "embedding_model": "all-MiniLM-L6-v2",
                "scores": {
                    "original_vs_iter1": float(score_us_vs_us1),
                    "original_vs_iter2": float(score_us_vs_us2),
                    "iter1_vs_iter2": float(score_us1_vs_us2)
                },
                "counts": {
                    "original": len(original_stories),
                    "iter1": len(stories_iter1),
                    "iter2": len(stories_iter2)
                }
            }
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(analysis_data, f, indent=4)
            print(f"  ✅ Saved analysis: {output_json_path.name}")

            # --- 6. Generate and Save 3-Way t-SNE Plot ---
            all_embeddings = np.vstack([
                original_embeddings, 
                embeddings_iter1,
                embeddings_iter2,
                midpoint_original, 
                midpoint_iter1,
                midpoint_iter2
            ])
            
            perplexity = min(30, max(5, len(all_embeddings) - 2))
            tsne = TSNE(n_components=2, perplexity=perplexity, random_state=42, init='random', learning_rate=200)
            embeddings_2d = tsne.fit_transform(all_embeddings)

            # Separate points for plotting
            len_us = len(original_stories)
            len_us1 = len(stories_iter1)
            
            original_points = embeddings_2d[:len_us]
            iter1_points = embeddings_2d[len_us:len_us + len_us1]
            iter2_points = embeddings_2d[len_us + len_us1:-3]
            
            midpoint_us_2d = embeddings_2d[-3]
            midpoint_us1_2d = embeddings_2d[-2]
            midpoint_us2_2d = embeddings_2d[-1]

            plt.figure(figsize=(14, 10))
            # Plot individual points
            plt.scatter(original_points[:, 0], original_points[:, 1], c='blue', alpha=0.4, label=f'US (Original, n={len_us})')
            plt.scatter(iter1_points[:, 0], iter1_points[:, 1], c='red', alpha=0.4, label=f"US' (Iter 1, n={len_us1})")
            plt.scatter(iter2_points[:, 0], iter2_points[:, 1], c='green', alpha=0.4, label=f"US'' (Iter 2, n={len(stories_iter2)})")
            
            # Plot centroids
            plt.scatter(midpoint_us_2d[0], midpoint_us_2d[1], c='blue', s=250, marker='X', edgecolors='black', label='Centroid US')
            plt.scatter(midpoint_us1_2d[0], midpoint_us1_2d[1], c='red', s=250, marker='X', edgecolors='black', label="Centroid US'")
            plt.scatter(midpoint_us2_2d[0], midpoint_us2_2d[1], c='green', s=250, marker='X', edgecolors='black', label="Centroid US''")
            
            # Plot connection lines
            plt.plot([midpoint_us_2d[0], midpoint_us1_2d[0]], [midpoint_us_2d[1], midpoint_us1_2d[1]], 'r--', label=f'US vs US\' ({score_us_vs_us1:.3f})')
            plt.plot([midpoint_us_2d[0], midpoint_us2_2d[0]], [midpoint_us_2d[1], midpoint_us2_2d[1]], 'g--', label=f'US vs US\'\' ({score_us_vs_us2:.3f})')
            plt.plot([midpoint_us1_2d[0], midpoint_us2_2d[0]], [midpoint_us1_2d[1], midpoint_us2_2d[1]], 'k:', label=f"US' vs US'' ({score_us1_vs_us2:.3f})")
            
            plt.title(f't-SNE 3-Way Comparison: {model_key} / {prompt_variant} / {report_number}')
            plt.xlabel('t-SNE Dimension 1')
            plt.ylabel('t-SNE Dimension 2')
            plt.legend(loc='best')
            plt.grid(True, linestyle=':', alpha=0.6)
            
            plt.savefig(output_plot_path, bbox_inches='tight')
            plt.close() # Frees up memory

            print(f"  ✅ Saved plot: {output_plot_path.name}")
            processed_count += 1

        except Exception as e:
            print(f"Error processing {iter2_stories_path}: {e}")

    print("\n--- Iteration 2 Semantic Consistency Analysis Complete ---")
    print(f"Successfully processed: {processed_count} reports")
    print(f"Total found: {len(iter2_story_files)}")


if __name__ == "__main__":
    config = load_config()
    if config:
        analyze_all_reports_iter2(config)