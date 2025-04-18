import time
import logging
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import euclidean_distances

from models import ClusteringSession

logger = logging.getLogger(__name__)

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logging.warning("Faiss не найден. Алгоритм K-Means будет медленнее на больших данных.")

def perform_clustering(embeddings, algorithm, params):
    n_samples = embeddings.shape[0]
    start_time = time.time()
    logger.info(f"Запуск кластеризации: алгоритм={algorithm}, параметры={params}, размер данных={n_samples}")
    labels = None
    centroids = None

    if algorithm == 'kmeans':
        n_clusters = int(params.get('n_clusters', 5))
        if n_clusters <= 0:
            raise ValueError("n_clusters должен быть > 0 для K-means")
        logger.info(f"Использование K-means с n_clusters={n_clusters}")

        if FAISS_AVAILABLE and n_samples >= 10000:
            logger.info("Использование Faiss K-means")
            d = embeddings.shape[1]
            kmeans_faiss = faiss.Kmeans(d=d, k=n_clusters, niter=20, verbose=False, gpu=False)
            kmeans_faiss.train(embeddings.astype(np.float32))
            _, labels_faiss = kmeans_faiss.index.search(embeddings.astype(np.float32), 1)
            labels = labels_faiss.flatten()
            centroids = kmeans_faiss.centroids
        else:
            logger.info("Использование Scikit-learn K-means")
            kmeans_sklearn = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = kmeans_sklearn.fit_predict(embeddings)
            centroids = kmeans_sklearn.cluster_centers_

    elif algorithm == 'dbscan':
        eps = float(params.get('eps', 0.5))
        min_samples = int(params.get('min_samples', 5))
        if eps <= 0 or min_samples <= 0:
            raise ValueError("eps и min_samples должны быть > 0 для DBSCAN")
        logger.info(f"Использование DBSCAN с eps={eps}, min_samples={min_samples}")

        dbscan = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
        raw_labels = dbscan.fit_predict(embeddings)

        unique_labels = np.unique(raw_labels)
        cluster_labels = sorted([label for label in unique_labels if label != -1])
        label_map = {old_label: new_label for new_label, old_label in enumerate(cluster_labels)}

        labels = np.full_like(raw_labels, -1)
        centroids_list = []
        for old_label, new_label in label_map.items():
            mask = (raw_labels == old_label)
            labels[mask] = new_label
            cluster_points = embeddings[mask]
            if cluster_points.shape[0] > 0:
                centroids_list.append(np.mean(cluster_points, axis=0))
            else:
                logger.warning(f"DBSCAN: Кластер {old_label} (новый {new_label}) не содержит точек. Пропуск расчета центроида.")

        centroids = np.array(centroids_list) if centroids_list else np.empty((0, embeddings.shape[1]))

    else:
        raise ValueError(f"Неизвестный алгоритм: {algorithm}")

    processing_time = time.time() - start_time
    logger.info(f"Кластеризация алгоритмом {algorithm} завершена за {processing_time:.2f} сек.")

    return labels, centroids, processing_time


def get_cluster_labels_for_session(session: ClusteringSession, embeddings: np.ndarray) -> np.ndarray | None:
    if not session or embeddings is None:
        logger.warning("get_cluster_labels_for_session: Получена невалидная сессия или эмбеддинги.")
        return None

    algorithm = session.algorithm
    params = session.get_params()
    logger.info(f"Перерасчет финальных меток для сессии {session.id}, алгоритм: {algorithm}, параметры: {params}")

    try:
        if algorithm == 'kmeans':
            active_clusters = session.clusters.filter_by(is_deleted=False).all()
            stored_centroids = []
            internal_idx_to_label = {}

            for cluster_meta in active_clusters:
                 centroid_vec = cluster_meta.get_centroid()
                 if centroid_vec is not None and centroid_vec.shape[0] == embeddings.shape[1]:
                     current_internal_idx = len(stored_centroids)
                     stored_centroids.append(centroid_vec)
                     internal_idx_to_label[current_internal_idx] = cluster_meta.cluster_label
                 else:
                     logger.warning(f"Пропуск невалидного центроида для кластера {cluster_meta.cluster_label} в сессии {session.id}")

            if not stored_centroids:
                logger.warning(f"Не найдено активных центроидов для K-Means в сессии {session.id}. Все точки будут помечены как шум (-1).")
                return np.full(embeddings.shape[0], -1, dtype=int)

            logger.info(f"Присвоение точек {len(stored_centroids)} активным K-Means центроидам для сессии {session.id}")
            stored_centroids_np = np.array(stored_centroids)
            distances = euclidean_distances(embeddings, stored_centroids_np)
            assigned_internal_indices = np.argmin(distances, axis=1)

            labels = np.array([int(internal_idx_to_label.get(idx, -1)) for idx in assigned_internal_indices], dtype=int)

            if -1 in labels:
                 logger.error(f"Внутренняя ошибка: Не удалось сопоставить некоторые точки с финальной K-means меткой в сессии {session.id}")

        elif algorithm == 'dbscan':
            eps = float(params.get('eps', 0.5))
            min_samples = int(params.get('min_samples', 5))
            if eps <= 0 or min_samples <=0 :
                raise ValueError("eps и min_samples должны быть > 0 для DBSCAN")

            dbscan = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
            raw_labels = dbscan.fit_predict(embeddings)

            active_clusters = session.clusters.filter_by(is_deleted=False).all()

            raw_label_to_final_label = {}
            for c in active_clusters:
                if c.original_cluster_id is not None:
                    try:
                        raw_label_to_final_label[int(c.original_cluster_id)] = int(c.cluster_label)
                    except (ValueError, TypeError):
                         logger.warning(f"Не удалось преобразовать original_cluster_id '{c.original_cluster_id}' или cluster_label '{c.cluster_label}' в int для сессии {session.id}, кластер {c.id}")


            final_labels = np.full_like(raw_labels, -1)
            for idx, raw_label in enumerate(raw_labels):
                if raw_label != -1:
                    final_label = raw_label_to_final_label.get(raw_label)
                    if final_label is not None:
                        final_labels[idx] = final_label
                    else:
                        logger.debug(f"Точка {idx} принадлежит исходному DBSCAN кластеру {raw_label}, который удален/неактивен. Присвоение метки шума (-1).")
                        final_labels[idx] = -1
                else:
                    final_labels[idx] = -1

            labels = final_labels

        else:
            logger.error(f"Обнаружен неизвестный алгоритм '{algorithm}' при получении меток для сессии {session.id}")
            return None

        logger.info(f"Финальные метки для сессии {session.id} успешно пересчитаны.")
        return labels

    except Exception as e:
        logger.error(f"Ошибка перерасчета финальных меток для сессии {session.id}: {e}", exc_info=True)
        return None