import os
import time
import logging
import numpy as np
import pandas as pd
import json
import uuid
from flask import current_app
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import silhouette_score, euclidean_distances
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logging.warning("Faiss не найден. Поиск ближайших соседей будет медленным.")

from PIL import Image, ImageDraw, ImageFont
from models import db, ClusteringSession, ClusterMetadata, ManualAdjustmentLog
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

def load_embeddings(file_path):
    try:
        df = pd.read_parquet(file_path)
        if 'embedding' not in df.columns:
            embedding_cols = df.select_dtypes(include=np.number).columns
            if len(embedding_cols) > 1:
                 embeddings = df[embedding_cols].values
            else:
                 raise ValueError("Столбец 'embedding' не найден в Parquet файле")
        else:
             embeddings = np.array(df['embedding'].tolist(), dtype=np.float32)

        image_ids = df['id'].astype(str).tolist() if 'id' in df.columns else [str(i) for i in df.index.tolist()]
        image_paths = df['image_path'].tolist() if 'image_path' in df.columns else None

        if embeddings.ndim != 2 or embeddings.shape[1] != 768:
             raise ValueError(f"Некорректная размерность эмбеддингов: {embeddings.shape}. Ожидалось (?, 768)")

        logger.info(f"Загружено {embeddings.shape[0]} эмбеддингов из {file_path}")
        return embeddings, image_ids, image_paths

    except Exception as e:
        logger.error(f"Ошибка загрузки Parquet файла {file_path}: {e}", exc_info=True)
        raise

def perform_clustering(embeddings, algorithm, params):
    n_samples = embeddings.shape[0]
    start_time = time.time()
    logger.info(f"Запуск кластеризации: алгоритм={algorithm}, параметры={params}, размер данных={n_samples}")

    if algorithm == 'kmeans':
        n_clusters = int(params.get('n_clusters', 5))
        if n_clusters <= 0: raise ValueError("n_clusters должен быть > 0 для K-means")
        logger.info(f"Используем K-means с n_clusters={n_clusters}")
        if FAISS_AVAILABLE and n_samples > 10000:
            logger.info("Используем Faiss K-means")
            d = embeddings.shape[1]
            kmeans = faiss.Kmeans(d=d, k=n_clusters, niter=20, verbose=False, gpu=False)
            kmeans.train(embeddings)
            _, labels = kmeans.index.search(embeddings, 1)
            labels = labels.flatten()
            centroids = kmeans.centroids
        else:
             logger.info("Используем Scikit-learn K-means")
             kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
             labels = kmeans.fit_predict(embeddings)
             centroids = kmeans.cluster_centers_

    elif algorithm == 'dbscan':
        eps = float(params.get('eps', 0.5))
        min_samples = int(params.get('min_samples', 5))
        if eps <= 0 or min_samples <=0 : raise ValueError("eps и min_samples должны быть > 0 для DBSCAN")
        logger.info(f"Используем DBSCAN с eps={eps}, min_samples={min_samples}")
        dbscan = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
        labels = dbscan.fit_predict(embeddings)
        unique_labels = np.unique(labels)
        centroids = []
        actual_labels = []
        for label in unique_labels:
            if label == -1: continue
            cluster_points = embeddings[labels == label]
            if cluster_points.shape[0] > 0:
                 centroid = np.mean(cluster_points, axis=0)
                 centroids.append(centroid)
                 actual_labels.append(label)
            else:
                 logger.warning(f"DBSCAN: Кластер {label} не содержит точек, пропуск.")
        centroids = np.array(centroids) if centroids else np.empty((0, embeddings.shape[1]))
        new_labels = np.full_like(labels, -1)
        label_map = {old_label: i for i, old_label in enumerate(actual_labels)}
        for old_label, new_label_index in label_map.items():
             new_labels[labels == old_label] = new_label_index
        labels = new_labels

    else:
        raise ValueError(f"Неизвестный алгоритм: {algorithm}")

    processing_time = time.time() - start_time
    logger.info(f"Кластеризация завершена за {processing_time:.2f} сек.")
    metrics = {}

    return labels, centroids, metrics, processing_time

def find_nearest_images_to_centroids(embeddings, labels, centroids, image_ids, image_paths, n_images):
    nearest_neighbors = {}
    if not image_paths or centroids.shape[0] == 0: return nearest_neighbors

    unique_labels = np.unique(labels)
    num_clusters = centroids.shape[0]

    faiss_index = None
    if FAISS_AVAILABLE:
        try:
            d = embeddings.shape[1]
            faiss_index = faiss.IndexFlatL2(d)
            faiss_index.add(embeddings)
        except Exception as faiss_e:
            logger.error(f"Ошибка создания Faiss индекса: {faiss_e}")
            faiss_index = None

    for i in range(num_clusters):
        current_label = i

        cluster_mask = (labels == current_label)
        cluster_indices = np.where(cluster_mask)[0]

        if len(cluster_indices) == 0: continue

        cluster_embeddings = embeddings[cluster_indices]
        centroid = centroids[i]

        k_search = min(n_images, len(cluster_indices))
        if k_search == 0: continue

        distances = []
        indices_in_cluster = []

        if faiss_index:
            try:
                 D, I = faiss_index.search(np.array([centroid]), k=min(k_search * 5, embeddings.shape[0]))
                 indices_global = I[0]
                 distances_global = D[0]

                 filtered_indices = [idx for idx in indices_global if labels[idx] == current_label]
                 top_k_indices_global = filtered_indices[:k_search]

                 final_distances = []
                 for global_idx in top_k_indices_global:
                      dist = np.linalg.norm(embeddings[global_idx] - centroid)
                      final_distances.append(dist)
                 indices_local = [np.where(cluster_indices == glob_idx)[0][0] for glob_idx in top_k_indices_global]

                 distances = np.array(final_distances)
                 indices_in_cluster = np.array(indices_local)

            except Exception as faiss_search_e:
                logger.warning(f"Ошибка поиска Faiss для кластера {current_label}: {faiss_search_e}. Переключение на sklearn.")
                faiss_index = None

        if not faiss_index:
             dist_to_centroid = euclidean_distances(cluster_embeddings, np.array([centroid])).flatten()
             sorted_indices_local = np.argsort(dist_to_centroid)
             indices_in_cluster = sorted_indices_local[:k_search]
             distances = dist_to_centroid[indices_in_cluster]

        neighbors_for_cluster = []
        for j, local_idx in enumerate(indices_in_cluster):
            global_idx = cluster_indices[local_idx]
            img_id = image_ids[global_idx]
            img_path = image_paths[global_idx]
            dist = distances[j]
            neighbors_for_cluster.append((img_id, img_path, float(dist)))

        nearest_neighbors[current_label] = neighbors_for_cluster

    return nearest_neighbors

def create_contact_sheet(image_paths, output_path, grid_size, thumb_size, format='JPEG'):
    if not image_paths: return False

    cols, rows = grid_size
    thumb_w, thumb_h = thumb_size
    gap = 5
    total_width = cols * thumb_w + (cols + 1) * gap
    total_height = rows * thumb_h + (rows + 1) * gap

    contact_sheet = Image.new('RGB', (total_width, total_height), color='white')
    draw = ImageDraw.Draw(contact_sheet)
    try: font = ImageFont.truetype("arial.ttf", 10)
    except IOError: font = ImageFont.load_default()

    current_col, current_row = 0, 0
    for img_path in image_paths[:cols*rows]:
        try:
            img = Image.open(img_path)
            img.thumbnail(thumb_size, Image.Resampling.LANCZOS)
            x_pos = gap + current_col * (thumb_w + gap)
            y_pos = gap + current_row * (thumb_h + gap)
            contact_sheet.paste(img, (x_pos, y_pos))
        except FileNotFoundError:
            logger.warning(f"Файл не найден для отпечатка: {img_path}")
            x_pos = gap + current_col * (thumb_w + gap)
            y_pos = gap + current_row * (thumb_h + gap)
            draw.rectangle([x_pos, y_pos, x_pos + thumb_w, y_pos + thumb_h], fill="lightgray", outline="red")
            draw.text((x_pos + 5, y_pos + 5), "Not Found", fill="red", font=font)
        except Exception as e:
            logger.error(f"Ошибка обработки {img_path} для отпечатка: {e}")
            x_pos = gap + current_col * (thumb_w + gap)
            y_pos = gap + current_row * (thumb_h + gap)
            draw.rectangle([x_pos, y_pos, x_pos + thumb_w, y_pos + thumb_h], fill="lightgray", outline="orange")
            draw.text((x_pos + 5, y_pos + 5), "Error", fill="orange", font=font)

        current_col += 1
        if current_col >= cols:
            current_col = 0
            current_row += 1

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        contact_sheet.save(output_path, format=format, quality=85)
        logger.info(f"Контактный отпечаток сохранен: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения контактного отпечатка {output_path}: {e}", exc_info=True)
        return False

def save_clustering_results(session, labels, centroids, nearest_neighbors_map, image_paths_available):
    app_config = current_app.config
    contact_sheet_dir_base = app_config['CONTACT_SHEET_FOLDER']
    grid_size = app_config.get('CONTACT_SHEET_GRID_SIZE', (3, 3))
    thumb_size = app_config.get('CONTACT_SHEET_THUMBNAIL_SIZE', (100, 100))
    output_format = app_config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG')

    unique_labels = np.unique(labels)

    for i, label in enumerate(unique_labels):
        if label == -1: continue

        cluster_mask = (labels == label)
        cluster_size = int(np.sum(cluster_mask))
        centroid_index = int(label)
        if centroid_index < 0 or centroid_index >= len(centroids):
             logger.error(f"Некорректный индекс центроида {centroid_index} для метки {label} в сессии {session.id}")
             continue
        centroid_vector = centroids[centroid_index]

        cluster_meta = ClusterMetadata(
            session_id=session.id,
            cluster_label=str(label),
            original_cluster_id=str(label),
            size=cluster_size,
            is_deleted=False
        )
        cluster_meta.set_centroid(centroid_vector)
        cluster_meta.set_metrics({})

        contact_sheet_path = None
        if image_paths_available and label in nearest_neighbors_map and nearest_neighbors_map[label]:
            sheet_filename = f"cs_{session.id}_{label}.{output_format.lower()}"
            session_sheet_dir = os.path.join(contact_sheet_dir_base, session.id)
            sheet_full_path = os.path.join(session_sheet_dir, sheet_filename)
            neighbor_paths = [p for _, p, _ in nearest_neighbors_map[label]]
            if create_contact_sheet(neighbor_paths, sheet_full_path, grid_size, thumb_size, output_format):
                 contact_sheet_path = sheet_full_path
        cluster_meta.contact_sheet_path = contact_sheet_path

        db.session.add(cluster_meta)

def run_clustering_pipeline(user_id, file_path, algorithm, params):
    session_id = str(uuid.uuid4())
    logger.info(f"Запуск синхронной кластеризации {session_id} для пользователя {user_id}")

    session = ClusteringSession(
        id=session_id, user_id=user_id, status='STARTED',
        algorithm=algorithm, input_file_path=file_path
    )
    session.set_params(params)
    db.session.add(session)
    try:
        db.session.commit()
    except SQLAlchemyError as commit_err:
        db.session.rollback()
        logger.error(f"Ошибка создания сессии {session_id}: {commit_err}", exc_info=True)
        raise ValueError(f"Не удалось создать сессию в БД: {commit_err}") from commit_err

    try:
        embeddings, image_ids, image_paths = load_embeddings(file_path)
        image_paths_available = image_paths is not None

        labels, centroids, metrics, processing_time = perform_clustering(embeddings, algorithm, params)
        session.processing_time_sec = processing_time

        unique_labels = np.unique(labels)
        n_clusters_found = len(centroids)
        session.num_clusters = n_clusters_found
        logger.info(f"Session {session_id}: Найдено {n_clusters_found} кластеров.")

        nearest_neighbors_map = {}
        if image_paths_available:
            logger.info(f"Session {session_id}: Поиск изображений для отпечатков...")
            images_per_cluster = current_app.config.get('CONTACT_SHEET_IMAGES_PER_CLUSTER', 9)
            nearest_neighbors_map = find_nearest_images_to_centroids(
                embeddings, labels, centroids, image_ids, image_paths, images_per_cluster
            )

        logger.info(f"Session {session_id}: Сохранение результатов...")
        save_clustering_results(session, labels, centroids, nearest_neighbors_map, image_paths_available)

        session.status = 'SUCCESS'
        session.result_message = f'Кластеризация завершена. Найдено {n_clusters_found} кластеров.'
        db.session.commit()
        logger.info(f"Синхронная кластеризация {session_id} успешно завершена.")
        return session.id

    except Exception as e:
        db.session.rollback()
        logger.error(f"Ошибка в синхронной кластеризации {session_id}: {e}", exc_info=True)
        try:
             session.status = 'FAILURE'
             session.result_message = f"Ошибка: {str(e)[:500]}"
             db.session.commit()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус сессии {session_id} на FAILURE: {db_err}", exc_info=True)
        raise

def run_reclustering_pipeline(original_session_id, user_id):
    new_session_id = str(uuid.uuid4())
    logger.info(f"Запуск синхронной рекластеризации {new_session_id} для сессии {original_session_id}")

    original_session = db.session.get(ClusteringSession, original_session_id)
    if not original_session or original_session.user_id != user_id:
        raise ValueError("Исходная сессия не найдена или доступ запрещен")

    new_session = ClusteringSession(
        id=new_session_id, user_id=user_id, status='STARTED',
        algorithm=original_session.algorithm, input_file_path=original_session.input_file_path
    )
    new_session.set_params(original_session.get_params())
    db.session.add(new_session)
    original_session.status = 'RECLUSTERING_STARTED'
    try:
        db.session.commit()
    except SQLAlchemyError as commit_err:
        db.session.rollback()
        logger.error(f"Ошибка создания сессии рекластеризации {new_session_id}: {commit_err}", exc_info=True)
        raise ValueError(f"Не удалось создать сессию рекластеризации: {commit_err}") from commit_err

    try:
        embeddings, image_ids, image_paths = load_embeddings(original_session.input_file_path)
        image_paths_available = image_paths is not None

        labels, centroids, metrics, processing_time = perform_clustering(
            embeddings, new_session.algorithm, new_session.get_params()
        )
        new_session.processing_time_sec = processing_time

        unique_labels = np.unique(labels)
        n_clusters_found = len(centroids)
        new_session.num_clusters = n_clusters_found
        logger.info(f"Recluster {new_session_id}: Найдено {n_clusters_found} кластеров.")

        nearest_neighbors_map = {}
        if image_paths_available:
            logger.info(f"Recluster {new_session_id}: Поиск изображений...")
            images_per_cluster = current_app.config.get('CONTACT_SHEET_IMAGES_PER_CLUSTER', 9)
            nearest_neighbors_map = find_nearest_images_to_centroids(
                embeddings, labels, centroids, image_ids, image_paths, images_per_cluster
            )

        logger.info(f"Recluster {new_session_id}: Сохранение результатов...")
        save_clustering_results(new_session, labels, centroids, nearest_neighbors_map, image_paths_available)

        new_session.status = 'SUCCESS'
        new_session.result_message = f'Рекластеризация завершена. Найдено {n_clusters_found} кластеров.'
        original_session.status = 'RECLUSTERED'
        db.session.commit()
        logger.info(f"Синхронная рекластеризация {new_session_id} успешно завершена.")
        return new_session.id

    except Exception as e:
        db.session.rollback()
        logger.error(f"Ошибка в синхронной рекластеризации {new_session_id}: {e}", exc_info=True)
        try:
            new_session.status = 'FAILURE'
            new_session.result_message = f"Ошибка рекластеризации: {str(e)[:500]}"
            original_session.status = 'RECLUSTERING_FAILED'
            db.session.commit()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус сессии {new_session_id} на FAILURE: {db_err}", exc_info=True)
        raise

def log_manual_adjustment(session_id, user_id, action, details):
    try:
        log_entry = ManualAdjustmentLog(
            session_id=session_id,
            user_id=user_id,
            action_type=action
        )
        log_entry.set_details(details)
        db.session.add(log_entry)
        logger.info(f"Logged manual adjustment: Session={session_id}, User={user_id}, Action={action}")
        return True
    except Exception as e:
        logger.error(f"Failed to log manual adjustment for session {session_id}: {e}", exc_info=True)
        return False