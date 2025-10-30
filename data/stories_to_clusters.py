



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from gensim.models import Word2Vec
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.preprocessing import MinMaxScaler

df = pd.read_csv("data/user-stories.txt",
                 delimiter="\t",
                 header=None,
                 names=["text"],
                 engine="python")


# Токенізація тексту (просте розбиття на слова)
df['tokens'] = df['text'].apply(lambda x: x.lower().split())

# Навчання моделі Word2Vec (враховуючи версію gensim 4.3.2)
w2v_model = Word2Vec(sentences=df['tokens'], vector_size=500, window=5, min_count=3, workers=-1)

# Отримання векторів-ембедингів для кожного рядка (усереднення векторів слів)
def get_embedding(tokens):
    vectors = [w2v_model.wv[word] for word in tokens if word in w2v_model.wv]
    return np.mean(vectors, axis=0) if vectors else np.zeros(w2v_model.vector_size)

df['embedding'] = df['tokens'].apply(get_embedding)

# Формування матриці ембедингів
embeddings_matrix = np.vstack(df['embedding'].values)


#################################################################
#################################################################
#################################################################


#Візуалізація метрик якості кластеризації в залежності від кількості кластерів

# Діапазон кількості кластерів
n_components_range = range(2, 10)

# Збереження метрик кластеризації
bic_scores = []
silhouette_scores = []
davies_bouldin_scores = []
calinski_harabasz_scores = []
distortion_scores = []

for n in n_components_range:
    gmm = GaussianMixture(n_components=n, random_state=42)
    # Отримання кластерних міток
    labels = gmm.fit_predict(embeddings_matrix)
    
    bic_scores.append(gmm.bic(embeddings_matrix))  # BIC
    silhouette_scores.append(silhouette_score(embeddings_matrix, labels))  # Silhouette Score
    davies_bouldin_scores.append(davies_bouldin_score(embeddings_matrix, labels))  # Davies-Bouldin Index
    calinski_harabasz_scores.append(calinski_harabasz_score(embeddings_matrix, labels))  # Calinski-Harabasz Index
    distortion_scores.append(np.sum(np.min(np.linalg.norm(embeddings_matrix[:, np.newaxis] - gmm.means_, axis=2), axis=1)))  # Distortion (Inertia)

# Нормалізація метрик
scaler = MinMaxScaler()
bic_scores_norm = scaler.fit_transform(np.array(bic_scores).reshape(-1, 1)).flatten()
silhouette_scores_norm = scaler.fit_transform(np.array(silhouette_scores).reshape(-1, 1)).flatten()
davies_bouldin_scores_norm = scaler.fit_transform(np.array(davies_bouldin_scores).reshape(-1, 1)).flatten()
calinski_harabasz_scores_norm = scaler.fit_transform(np.array(calinski_harabasz_scores).reshape(-1, 1)).flatten()
distortion_scores_norm = scaler.fit_transform(np.array(distortion_scores).reshape(-1, 1)).flatten()

# Визначення оптимального числа кластерів для кожної метрики
optimal_bic = n_components_range[np.argmin(bic_scores)]  # Мінімальний BIC
optimal_silhouette = n_components_range[np.argmax(silhouette_scores)]  # Максимальний Silhouette Score
optimal_davies_bouldin = n_components_range[np.argmin(davies_bouldin_scores)]  # Мінімальний Davies-Bouldin Index
optimal_calinski_harabasz = n_components_range[np.argmax(calinski_harabasz_scores)]  # Максимальний Calinski-Harabasz Index
optimal_distortion = n_components_range[np.argmin(distortion_scores)]  # Мінімальна внутрішньокластерна інерція (Distortion)

# Агреговане визначення оптимального числа кластерів (усереднене)
optimal_n_clusters = int(np.mean([optimal_bic, optimal_silhouette, optimal_davies_bouldin, optimal_calinski_harabasz, optimal_distortion]))

# Виведення результатів
print(f"Оптимальна кількість кластерів за BIC: {optimal_bic}")
print(f"Оптимальна кількість кластерів за Silhouette Score: {optimal_silhouette}")
print(f"Оптимальна кількість кластерів за Davies-Bouldin Index: {optimal_davies_bouldin}")
print(f"Оптимальна кількість кластерів за Calinski-Harabasz Index: {optimal_calinski_harabasz}")
print(f"Оптимальна кількість кластерів за Distortion: {optimal_distortion}")
print(f"\nАгреговане оптимальне число кластерів: {optimal_n_clusters}")

# Візуалізація узагальнених результатів
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(n_components_range, silhouette_scores_norm, marker="o", color="red", label="Silhouette Score (норм.)")
ax.plot(n_components_range, davies_bouldin_scores_norm, marker="^", color="green", label="Davies-Bouldin Index (норм.)")
ax.plot(n_components_range, calinski_harabasz_scores_norm, marker="v", color="purple", label="Calinski-Harabasz Index (норм.)")
ax.plot(n_components_range, bic_scores_norm, marker="s", color="blue", label="BIC (норм.)")
ax.plot(n_components_range, distortion_scores_norm, marker="*", color="orange", label="Distortion (норм.)")

ax.axvline(optimal_n_clusters, color="black", linestyle="--", label=f"Оптимальна кількість кластерів: {optimal_n_clusters}")
ax.set_xlabel("Кількість кластерів")
ax.set_ylabel("Нормалізоване значення метрик")
ax.set_title("Оптимальне число кластерів за різними метриками")
ax.legend()
ax.grid()
plt.show()


#################################################################
#################################################################
#################################################################


# Отримання ймовірностей належності до оптимального числа кластерів
# на основі агрегації різних метрик якості кластеризації
gmm = GaussianMixture(n_components=optimal_n_clusters, random_state=42)
gmm.fit(embeddings_matrix)
probabilities = gmm.predict_proba(embeddings_matrix)


#################################################################
#################################################################
#################################################################


# Групуваня рядків по окремим кластерам, коли значення чисел хоча би в одній колонці більше 0.1
from collections import defaultdict

# Приклад масиву
data = np.array(probabilities)

threshold = 0.1
groups = defaultdict(list)

for i, row in enumerate(data):
    # Отримання індексів колонок, де значення перевищує threshold
    cols = tuple(np.where(row > threshold)[0])
    
    if cols:  # Додаємо рядок до відповідної групи
        groups[cols].append(i)

# Вивід згрупованих індексів
for key, value in groups.items():
    print(f"Група (колонки {key}): рядки {value}")


#################################################################
#################################################################
#################################################################


# Формування кластерів стрічок
d = {}
for key, values in groups.items():
    d[f'{key}'] = [df.loc[values, ['text']]][0]


#################################################################
#################################################################
#################################################################


# Об'єднання вимог в межах кластера - об'єднання рядків у колонці 'text' для кожного DataFrame
combined_texts = {key: df['text'].str.cat(sep=' ') for key, df in d.items()}


import json
with open("clustered_stories.json", "w", encoding="utf-8") as f:
    json.dump(combined_texts, f, ensure_ascii=False, indent=2)

w2v_model.save("data/original_w2v.model")










