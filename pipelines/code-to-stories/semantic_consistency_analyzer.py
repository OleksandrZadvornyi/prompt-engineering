import numpy as np
from dotenv import load_dotenv
from pathlib import Path
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.manifold import TSNE
from os import getenv
from langchain_openai import ChatOpenAI
import json
import matplotlib.pyplot as plt
import time

# --- ⚙️ CONFIGURATION ---
# ❗️ Change this path to point to the code file you want to analyze
MODEL_NAME = "openai/gpt-4o-mini"
MODEL_NAME_PATH = "gpt-4o-mini"
REPORT_NUMBER = 13
# for i in range(4, 5):
# print("Waiting 3 seconds between requests")
# time.sleep(3)

CODE_FILE_TO_ANALYZE = Path(f"../Reports/{MODEL_NAME_PATH}/report_{REPORT_NUMBER}/generated_code.py")
# -------------------------

# Load environment variables from .env file
load_dotenv()

# --- Step 1: Initialize Models ---
print("✅ Initializing models...")
# Initialize the model for creating embeddings
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# --- Step 2: Load Original Stories and Create Embeddings ---
print("✅ Loading original user stories and creating embeddings...")
original_stories_path = Path("data.txt")
if not original_stories_path.exists():
    raise FileNotFoundError(f"Error: Could not find '{original_stories_path}'. Make sure it's in the same directory.")

with open(original_stories_path, "r", encoding="utf-8") as f:
    original_stories = [line.strip() for line in f if line.strip()]

original_embeddings = embedding_model.encode(original_stories)
print(f"   - Found {len(original_stories)} original stories.")


# # --- Step 3: Load Generated Code ---
# print(f"✅ Loading generated code from '{CODE_FILE_TO_ANALYZE}'...")
# if not CODE_FILE_TO_ANALYZE.exists():
#     raise FileNotFoundError(f"Error: The file '{CODE_FILE_TO_ANALYZE}' does not exist.")

# with open(CODE_FILE_TO_ANALYZE, "r", encoding="utf-8") as f:
#     generated_code = f.read()


# --- Step 4: Reverse-Engineer User Stories from Code ---
# print("✅ Asking LLM to generate new user stories from the code...")
# reverse_prompt = (
#     "Analyze the following Python code and generate a list of user stories that "
#     "accurately describe its functionality. Each user story should be on a new line "
#     "and follow the format: 'As a [role], I want to [action], so that [benefit]'. "
#     "Only output the user stories.\n\n"
#     "Python Code:\n"
#     "\n```python\n"
#     f"{generated_code}"
#     "\n```"
# )

# # Initialize LLM
# llm = ChatOpenAI(
#     api_key=getenv("OPENROUTER_API_KEY"),
#     base_url=getenv("OPENROUTER_BASE_URL"),
#     model=MODEL_NAME
# ).bind(
#     logprobs=True,
#     extra_body={
#         "provider": {
#             "order": [
#                 "nebius"
#             ]
#         }
#     }
# )

# msg = llm.invoke(("human", reverse_prompt))
# bool(msg.response_metadata.get("logprobs"))

# generated_stories = [line.strip() for line in msg.content.split('\n') if "As a" in line]
generated_stories_path = Path(f"../Reports/{MODEL_NAME_PATH}/report_{REPORT_NUMBER}/User-stories/generated_user_stories.txt")
if not generated_stories_path.exists():
    raise FileNotFoundError(f"Error: Could not find '{generated_stories_path}'. Make sure it's in the same directory.")

with open(generated_stories_path, "r", encoding="utf-8") as f:
    generated_stories = [line.strip() for line in f if line.strip()]
print(f"   - LLM generated {len(generated_stories)} new stories.")

# --- Step 5: Create Embeddings for Generated Stories ---
if not generated_stories:
    raise ValueError("Error: The LLM did not return any valid user stories. Cannot continue.")

print("✅ Creating embeddings for the newly generated stories...")
generated_embeddings = embedding_model.encode(generated_stories)


# --- Step 6: Calculate Midpoints and Cosine Similarity ---
print("✅ Calculating midpoints and final cosine similarity score...")
# Calculate the midpoint (centroid) for each cluster of vectors
midpoint_original = np.mean(original_embeddings, axis=0)
midpoint_generated = np.mean(generated_embeddings, axis=0)

# Reshape vectors for scikit-learn's function
midpoint_original_reshaped = midpoint_original.reshape(1, -1)
midpoint_generated_reshaped = midpoint_generated.reshape(1, -1)

# Calculate the cosine similarity between the two midpoints
similarity_score = cosine_similarity(midpoint_original_reshaped, midpoint_generated_reshaped)[0][0]

print("\n" + "="*50)
print(f"📊 Semantic Consistency Score: {similarity_score:.4f}")
print("="*50 + "\n")

# --- Step 7: Visualize the Embeddings with t-SNE --- 
print("✅ Visualizing embedding clusters with t-SNE...")

# Combine all embeddings: original, generated, and their midpoints
all_embeddings = np.vstack([original_embeddings, generated_embeddings, midpoint_original, midpoint_generated])

# Create labels for each point to use in the plot
labels = ['Original'] * len(original_embeddings) + \
         ['Generated'] * len(generated_embeddings) + \
         ['Original Centroid', 'Generated Centroid']

# Perform t-SNE
tsne = TSNE(n_components=2, perplexity=15, random_state=42, init='random', learning_rate=200)
embeddings_2d = tsne.fit_transform(all_embeddings)

# Separate the 2D points for plotting
original_points = embeddings_2d[:len(original_embeddings)]
generated_points = embeddings_2d[len(original_embeddings):-2]
midpoint_original_2d = embeddings_2d[-2]
midpoint_generated_2d = embeddings_2d[-1]

# Create the plot
plt.figure(figsize=(12, 8))
# Plot original stories
plt.scatter(original_points[:, 0], original_points[:, 1], c='blue', alpha=0.5, label='Original Stories')
# Plot generated stories
plt.scatter(generated_points[:, 0], generated_points[:, 1], c='red', alpha=0.7, label='Generated Stories')
# Plot original centroid
plt.scatter(midpoint_original_2d[0], midpoint_original_2d[1], c='cyan', s=200, marker='X', edgecolors='black', label='Original Centroid')
# Plot generated centroid
plt.scatter(midpoint_generated_2d[0], midpoint_generated_2d[1], c='magenta', s=200, marker='X', edgecolors='black', label='Generated Centroid')

# Draw a line between centroids
plt.plot([midpoint_original_2d[0], midpoint_generated_2d[0]],
         [midpoint_original_2d[1], midpoint_generated_2d[1]],
         'g--', label=f'Centroid Connection (Score: {similarity_score:.2f})')

plt.title(f't-SNE Visualization of User Story Embeddings ({MODEL_NAME_PATH})')
plt.xlabel('t-SNE Dimension 1')
plt.ylabel('t-SNE Dimension 2')
plt.legend()
plt.grid(True)

# --- Step 7: Save Analysis Results ---
output_dir = CODE_FILE_TO_ANALYZE.parent / "User-stories"
output_dir.mkdir(parents=True, exist_ok=True)

stories_output_path = output_dir / "generated_user_stories.txt"
with open(stories_output_path, "w", encoding="utf-8") as f:
    f.write("\n".join(generated_stories))

print(f"✅ Generated user stories saved to '{stories_output_path}'")

results_output_path = output_dir / "analysis_results.json"

analysis_data = {
    "semantic_consistency_score": float(similarity_score),
    "analysis_model": MODEL_NAME,
    "original_stories_count": len(original_stories),
    "generated_stories_count": len(generated_stories)
}

with open(results_output_path, "w", encoding="utf-8") as f:
    json.dump(analysis_data, f, indent=4)
    
print(f"✅ Analysis score saved to '{results_output_path}'")


# Define the output path and save the plot
plot_path = output_dir / "embedding_visualization.png"
plt.savefig(plot_path, bbox_inches='tight')
plt.close() # Frees up memory

print(f"✅ Plot saved to '{plot_path}'")