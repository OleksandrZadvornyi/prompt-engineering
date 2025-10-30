import json
import numpy as np
import pandas as pd
from pathlib import Path
from gensim.models import Word2Vec
from sklearn.metrics import pairwise
from tqdm import tqdm

# --- 1. Допоміжні функції ---

def get_embedding(tokens: list[str], w2v_model: Word2Vec) -> np.ndarray:
    """Отримує усереднений вектор-ембединг."""
    vectors = [w2v_model.wv[word] for word in tokens if word in w2v_model.wv]
    if vectors:
        return np.mean(vectors, axis=0)
    else:
        return np.zeros(w2v_model.vector_size)

def calculate_structure_similarity(
    original_cluster_texts: list[str],
    generated_cluster_texts: list[str],
    w2v_model: Word2Vec
) -> float:
    """
    Обчислює "Оцінку Схожості Структур" (швидка операція).
    """
    if not generated_cluster_texts or not original_cluster_texts:
        return 0.0

    # 1. Ембединги для ОРИГІНАЛЬНИХ кластерів
    orig_tokens = [t.lower().split() for t in original_cluster_texts]
    v_orig_list = [get_embedding(t, w2v_model) for t in orig_tokens]
    # Фільтруємо нульові вектори, якщо раптом є
    v_orig = np.vstack([v for v in v_orig_list if np.any(v)])
    
    # 2. Ембединги для ЗГЕНЕРОВАНИХ кластерів
    gen_tokens = [t.lower().split() for t in generated_cluster_texts]
    v_gen_list = [get_embedding(t, w2v_model) for t in gen_tokens]
    v_gen = np.vstack([v for v in v_gen_list if np.any(v)])

    # Якщо після фільтрації не залишилось векторів
    if v_orig.shape[0] == 0 or v_gen.shape[0] == 0:
        return 0.0
    
    # 3. Обчислення матриці косинусної подібності
    sim_matrix = pairwise.cosine_similarity(v_orig, v_gen)
    
    # 4. Розрахунок фінальної оцінки
    max_sim_orig = np.max(sim_matrix, axis=1)
    max_sim_gen = np.max(sim_matrix, axis=0)
    
    score = (np.mean(max_sim_orig) + np.mean(max_sim_gen)) / 2
    
    return float(score)

# --- 2. Головний скрипт ---

def main_compare():
    print("🚀 Починаємо швидкий аналіз схожості (на основі JSON)...")

    # --- 2.1. Завантаження еталонних даних ---
    W2V_MODEL_PATH = "data/original_w2v.model"
    ORIGINAL_CLUSTERS_PATH = "data/clustered_stories.json"
    
    print(f"Завантаження моделі Word2Vec з {W2V_MODEL_PATH}...")
    try:
        w2v_model = Word2Vec.load(W2V_MODEL_PATH)
    except FileNotFoundError:
        print(f"ПОМИЛКА: Файл моделі не знайдено: {W2V_MODEL_PATH}")
        return

    print(f"Завантаження оригінальних кластерів з {ORIGINAL_CLUSTERS_PATH}...")
    try:
        with open(ORIGINAL_CLUSTERS_PATH, 'r', encoding='utf-8') as f:
            original_clusters_json = json.load(f)
        original_cluster_texts = list(original_clusters_json.values())
    except FileNotFoundError:
        print(f"ПОМИЛКА: Файл оригінальних кластерів не знайдено: {ORIGINAL_CLUSTERS_PATH}")
        return

    # --- 2.2. Пошук та обробка всіх згенерованих JSON-файлів ---
    SEARCH_PATH = Path("results/code-to-stories")
    # Шукаємо нові файли, які ми згенерували!
    all_cluster_files = list(SEARCH_PATH.rglob("generated_clusters.json"))
    
    if not all_cluster_files:
        print(f"ПОМИЛКА: Не знайдено жодного файлу 'generated_clusters.json' у {SEARCH_PATH}")
        print("Спочатку запустіть 'preprocess_clusters.py'.")
        return
        
    print(f"Знайдено {len(all_cluster_files)} готових файлів кластерів...")
    
    results = []

    for file_path in tqdm(all_cluster_files, desc="Порівняння структур"):
        
        # Визначення метаданих зі шляху
        try:
            parts = file_path.parts
            model_key = parts[-4]
            prompt_variant = parts[-3]
            report_number_str = parts[-2].replace('report_', '')
            
            # Завантаження повної назви моделі
            json_path = file_path.parent / "semantic_consistency.json"
            with open(json_path, 'r', encoding='utf-8') as f:
                meta_json = json.load(f)
                model_name = meta_json.get("model", model_key)
        
        except (IndexError, FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Не вдалося обробити метадані для {file_path}: {e}. Пропускаємо.")
            continue
            
        # 1. Завантаження ЗГЕНЕРОВАНИХ кластерів (вже готових!)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                generated_clusters_json = json.load(f)
            generated_cluster_texts = list(generated_clusters_json.values())
        except Exception as e:
            print(f"Не вдалося прочитати {file_path}: {e}. Пропускаємо.")
            continue

        # 2. Розрахунок схожості (швидко!)
        score = calculate_structure_similarity(
            original_cluster_texts,
            generated_cluster_texts,
            w2v_model
        )
        
        # 3. Збереження результату
        results.append({
            "model_key": model_key,
            "model_name": model_name,
            "prompt_variant": prompt_variant,
            "report_number": int(report_number_str),
            "similarity_score": score,
            "generated_clusters_count": len(generated_cluster_texts)
        })

    # --- 2.3. Збереження фінальних результатів ---
    if not results:
        print("Не було зібрано жодних результатів.")
        return

    print("\n✅ Обробку завершено. Створення фінального звіту...")
    
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values(
        by=["model_key", "prompt_variant", "report_number"]
    )
    
    OUTPUT_CSV_PATH = "results/cluster_similarity_results.csv"
    df_results.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
    
    print("\n--- Фінальний DataFrame ---")
    print(df_results.to_string())
    print(f"\n🎉 Результати збережено у файл: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main_compare()