import re
import json
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict
from gensim.models import Word2Vec
from sklearn.mixture import GaussianMixture
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score
)
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm

import os
os.environ["OMP_NUM_THREADS"] = "1"
# --- 1. Допоміжні функції (з попереднього скрипту) ---

def get_embedding(tokens: list[str], w2v_model: Word2Vec) -> np.ndarray:
    vectors = [w2v_model.wv[word] for word in tokens if word in w2v_model.wv]
    if vectors:
        return np.mean(vectors, axis=0)
    else:
        return np.zeros(w2v_model.vector_size)

def extract_stories_from_file(file_path: Path) -> list[str]:
    stories = []
    story_pattern = re.compile(r'(As a .*? I want to .*? so that .*)', re.IGNORECASE)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            story_text_block = f.read()
    except Exception as e:
        print(f"Помилка читання файлу {file_path}: {e}")
        return []

    for line in story_text_block.split('\n'):
        cleaned_line = line.strip().replace("**", "")
        match = story_pattern.search(cleaned_line)
        if match:
            story = match.group(1).strip().rstrip('.,')
            stories.append(story)
    return stories

def cluster_stories_and_find_k(story_list: list[str], w2v_model: Word2Vec) -> dict:
    """
    Повна (ПОВІЛЬНА) версія кластеризації з пошуком K.
    Повертає СЛОВНИК з об'єднаними текстами.
    """
    
    # 1. Токенізація та отримання ембедингів
    tokens_list = [s.lower().split() for s in story_list]
    embeddings_list = [get_embedding(tokens, w2v_model) for tokens in tokens_list]

    # 2. Фільтрація валідних ембедингів
    valid_stories = []
    valid_embeddings = []
    for story, emb in zip(story_list, embeddings_list):
        if np.any(emb):
            valid_stories.append(story)
            valid_embeddings.append(emb)

    # 3. Перевірка
    if len(valid_embeddings) < 2:
        return {"(0,)": " ".join(valid_stories)} # Повертаємо як один кластер
    
    embeddings_matrix = np.vstack(valid_embeddings)

    # 4. Динамічний пошук оптимального K
    max_k = min(10, len(valid_embeddings))
    if max_k <= 2:
         return {"(0,)": " ".join(valid_stories)}

    n_components_range = range(2, max_k)
    
    try:
        bic_scores = []
        silhouette_scores = []
        davies_bouldin_scores = []
        calinski_harabasz_scores = []

        for n in n_components_range:
            gmm = GaussianMixture(n_components=n, random_state=42)
            labels = gmm.fit_predict(embeddings_matrix)
            
            # Якщо всі мітки однакові, метрики не можна порахувати
            if len(np.unique(labels)) < 2:
                continue

            bic_scores.append(gmm.bic(embeddings_matrix))
            silhouette_scores.append(silhouette_score(embeddings_matrix, labels))
            davies_bouldin_scores.append(davies_bouldin_score(embeddings_matrix, labels))
            calinski_harabasz_scores.append(calinski_harabasz_score(embeddings_matrix, labels))

        # Перевірка, чи є хоч якісь результати
        if not bic_scores:
             return {"(0,)": " ".join(valid_stories)}

        # Визначення оптимального K
        optimal_bic = n_components_range[np.argmin(bic_scores)]
        optimal_silhouette = n_components_range[np.argmax(silhouette_scores)]
        optimal_davies_bouldin = n_components_range[np.argmin(davies_bouldin_scores)]
        optimal_calinski_harabasz = n_components_range[np.argmax(calinski_harabasz_scores)]

        optimal_n_clusters = int(np.mean([
            optimal_bic, optimal_silhouette, 
            optimal_davies_bouldin, optimal_calinski_harabasz
        ]))

        # 5. Фінальна кластеризація з оптимальним K
        gmm = GaussianMixture(n_components=optimal_n_clusters, random_state=42)
        gmm.fit(embeddings_matrix)
        probabilities = gmm.predict_proba(embeddings_matrix)

        # 6. Групування історій (з вашого коду)
        threshold = 0.1
        groups = defaultdict(list)
        for i, row in enumerate(probabilities):
            cols = tuple(np.where(row > threshold)[0])
            if cols:
                groups[cols].append(i)

        # 7. Формування СЛОВНИКА об'єднаних текстів кластерів
        valid_stories_np = np.array(valid_stories)
        d = {}
        for key, values in groups.items():
            # Формуємо ключ як рядок, щоб він був JSON-сумісний
            d[f'{key}'] = valid_stories_np[values]

        # Об'єднуємо тексти в межах кожного кластера
        combined_texts = {key: " ".join(story_list) for key, story_list in d.items()}

        return combined_texts

    except ValueError as e:
        print(f"Помилка кластеризації {e}")
        return {"(0,)": " ".join(valid_stories)} # Повертаємо як один кластер

# --- 2. Головний скрипт ---

def main_preprocess():
    print("🐢 Починаємо попередню обробку (кластеризацію) звітів...")

    # --- 2.1. Завантаження моделі Word2Vec ---
    W2V_MODEL_PATH = "data/original_w2v.model"
    print(f"Завантаження моделі Word2Vec з {W2V_MODEL_PATH}...")
    try:
        w2v_model = Word2Vec.load(W2V_MODEL_PATH)
    except FileNotFoundError:
        print(f"ПОМИЛКА: Файл моделі не знайдено: {W2V_MODEL_PATH}")
        return

    # --- 2.2. Пошук та обробка всіх згенерованих файлів ---
    SEARCH_PATH = Path("results/code-to-stories")
    all_story_files = list(SEARCH_PATH.rglob("generated_user_stories.txt"))
    
    if not all_story_files:
        print(f"ПОМИЛКА: Не знайдено 'generated_user_stories.txt' у {SEARCH_PATH}")
        return
        
    print(f"Знайдено {len(all_story_files)} звітів для кластеризації...")
    
    for file_path in tqdm(all_story_files, desc="Кластеризація звітів"):
        
        # 1. Витягнення згенерованих історій
        generated_stories = extract_stories_from_file(file_path)
        if not generated_stories:
            print(f"Немає історій у {file_path}. Створюємо порожній JSON.")
            clustered_data = {}
        else:
            # 2. Кластеризація
            clustered_data = cluster_stories_and_find_k(generated_stories, w2v_model)
        
        # 3. Збереження результату в JSON
        # Вихідний файл буде в тій же папці, що і .txt, але з іншою назвою
        output_json_path = file_path.parent / "generated_clusters.json"
        
        try:
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(clustered_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Не вдалося зберегти JSON для {file_path}: {e}")

    print("\n✅ Попередню обробку завершено.")
    print(f"У кожній папці 'report_X' тепер має бути файл 'generated_clusters.json'")

if __name__ == "__main__":
    main_preprocess()