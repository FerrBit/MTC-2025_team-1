import logging
import os
import uuid
import numpy as np
from flask import current_app
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.attributes import flag_modified
from algorithms import perform_clustering
from data_loader import load_embeddings
from models import db, ClusteringSession, ClusterMetadata
from visualization import (
    calculate_and_save_centroids_2d,
    create_contact_sheet,
    find_nearest_images_to_centroids,
)

logger = logging.getLogger(__name__)

def save_clustering_results(session, labels, centroids, nearest_neighbors_map, image_ids_available, archive_path=None):
    app_config = current_app.config
    contact_sheet_dir_base = app_config['CONTACT_SHEET_FOLDER']
    grid_size = app_config.get('CONTACT_SHEET_GRID_SIZE', (3, 3))
    thumb_size = app_config.get('CONTACT_SHEET_THUMBNAIL_SIZE', (100, 100))
    output_format = app_config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG')
    unique_cluster_labels = np.unique(labels[labels != -1])
    created_cluster_metadata = []

    logger.info(f"Сохранение результатов для сессии {session.id}. Найдено {len(unique_cluster_labels)} кластеров. Архив предоставлен: {'Да' if archive_path else 'Нет'}")

    if len(unique_cluster_labels) != centroids.shape[0]:
         logger.warning(f"Несоответствие между уникальными метками ({len(unique_cluster_labels)}) и количеством центроидов ({centroids.shape[0]}) для сессии {session.id}. Продолжаем с количеством центроидов.")

    for i in range(centroids.shape[0]):
        cluster_label = i
        cluster_mask = (labels == cluster_label)
        cluster_size = int(np.sum(cluster_mask))

        if cluster_size == 0:
            logger.warning(f"Метка кластера {cluster_label} имеет размер 0, пропуск создания метаданных для сессии {session.id}.")
            continue

        centroid_vector = centroids[i]
        cluster_meta = ClusterMetadata(
            session_id=session.id,
            cluster_label=str(cluster_label),
            original_cluster_id=str(cluster_label),
            size=cluster_size,
            is_deleted=False
        )
        cluster_meta.set_centroid(centroid_vector)
        cluster_meta.set_metrics({})
        cluster_meta.set_centroid_2d(None)

        contact_sheet_path = None
        if archive_path and image_ids_available and cluster_label in nearest_neighbors_map and nearest_neighbors_map[cluster_label]:
            logger.debug(f"Попытка создания контактного листа для кластера {cluster_label}. Архив: {os.path.basename(archive_path)}, Соседи: {len(nearest_neighbors_map[cluster_label])}")
            sheet_filename = f"cs_{session.id}_{cluster_label}.{output_format.lower()}"
            session_sheet_dir = os.path.join(contact_sheet_dir_base, str(session.id))
            sheet_full_path = os.path.join(session_sheet_dir, sheet_filename)

            parquet_paths_for_sheet = [img_id for img_id, _ in nearest_neighbors_map[cluster_label]]

            if create_contact_sheet(archive_path, parquet_paths_for_sheet, sheet_full_path, grid_size, thumb_size, output_format):
                contact_sheet_path = sheet_full_path
            else:
                logger.warning(f"Не удалось создать контактный лист для кластера {cluster_label} сессии {session.id}")
        elif not archive_path:
             if cluster_label in nearest_neighbors_map:
                 logger.info(f"Контактный лист для кластера {cluster_label} сессии {session.id} пропущен: Архив изображений не предоставлен.")
        elif not image_ids_available:
             if cluster_label in nearest_neighbors_map:
                 logger.info(f"Контактный лист для кластера {cluster_label} сессии {session.id} пропущен: Идентификаторы/пути изображений недоступны из Parquet.")

        cluster_meta.contact_sheet_path = contact_sheet_path
        db.session.add(cluster_meta)
        created_cluster_metadata.append(cluster_meta)

    try:
        db.session.commit()
        logger.info(f"Сохранены метаданные для {len(created_cluster_metadata)} кластеров для сессии {session.id}")
    except SQLAlchemyError as e:
         db.session.rollback()
         logger.error(f"Ошибка базы данных при сохранении метаданных кластера для сессии {session.id}: {e}", exc_info=True)
         raise RuntimeError(f"Не удалось сохранить метаданные кластера в БД для сессии {session.id}") from e

    return created_cluster_metadata


def run_clustering_pipeline(user_id, embedding_file_path, algorithm, params,
                            original_embedding_filename, archive_path=None,
                            original_archive_filename=None):
    session_id = str(uuid.uuid4())
    logger.info(f"Запуск конвейера кластеризации {session_id} для пользователя {user_id}")
    session = ClusteringSession(
        id=session_id, user_id=user_id, status='STARTED', algorithm=algorithm,
        input_file_path=embedding_file_path,
        original_input_filename=original_embedding_filename
    )
    session.set_params(params)
    db.session.add(session)
    try:
        db.session.commit()
    except SQLAlchemyError as commit_err:
        db.session.rollback()
        logger.error(f"Ошибка БД при создании сессии {session_id}: {commit_err}", exc_info=True)
        raise ValueError(f"Не удалось создать сессию в БД: {commit_err}") from commit_err

    try:
        session.status = 'LOADING_DATA'
        db.session.commit()
        embeddings, image_ids = load_embeddings(embedding_file_path)
        image_ids_available = image_ids is not None and len(image_ids) == embeddings.shape[0]
        if not image_ids_available:
             logger.warning(f"Сессия {session_id}: Идентификаторы изображений отсутствуют или их количество не совпадает. Это может повлиять на контактные листы/экспорт.")

        session.status = 'CLUSTERING'
        db.session.commit()
        labels, centroids, processing_time = perform_clustering(embeddings, algorithm, params)
        session.processing_time_sec = processing_time
        n_clusters_found = len(centroids)
        logger.info(f"Сессия {session_id}: Кластеризация нашла {n_clusters_found} кластеров (исключая шум, если есть).")

        nearest_neighbors_map = {}
        if image_ids_available and n_clusters_found > 0:
            logger.info(f"Сессия {session_id}: Поиск ближайших соседей...")
            session.status = 'FINDING_NEIGHBORS'
            db.session.commit()
            images_per_cluster = current_app.config.get('CONTACT_SHEET_IMAGES_PER_CLUSTER', 9)
            nearest_neighbors_map = find_nearest_images_to_centroids(
                embeddings, labels, centroids, image_ids, images_per_cluster
            )
            logger.info(f"Сессия {session_id}: Найдены соседи для {len(nearest_neighbors_map)} кластеров.")
        elif n_clusters_found == 0:
             logger.info(f"Сессия {session_id}: Алгоритм не нашел кластеров, пропуск поиска соседей.")
        else:
             logger.warning(f"Сессия {session_id}: Пропуск поиска соседей, так как идентификаторы/пути изображений не были загружены.")

        session.status = 'SAVING_RESULTS'
        db.session.commit()
        logger.info(f"Сессия {session_id}: Сохранение результатов кластеризации...")
        created_clusters = save_clustering_results(
             session, labels, centroids, nearest_neighbors_map, image_ids_available, archive_path
        )
        actual_saved_cluster_count = len(created_clusters)
        session.num_clusters = actual_saved_cluster_count

        session.status = 'PROCESSING'
        db.session.commit()
        logger.info(f"Сессия {session_id}: Вычисление 2D центроидов...")
        calculate_and_save_centroids_2d(session.id)

        session.status = 'SUCCESS'
        final_cluster_count = ClusterMetadata.query.with_session(db.session).filter_by(session_id=session.id, is_deleted=False).count()
        session.num_clusters = final_cluster_count
        session.result_message = f'Кластеризация завершена. Найдено {final_cluster_count} кластеров.'
        flag_modified(session, "num_clusters")
        flag_modified(session, "result_message")
        flag_modified(session, "status")
        db.session.commit()

        logger.info(f"Конвейер кластеризации {session_id} успешно завершен.")
        return session.id

    except Exception as e:
        db.session.rollback()
        logger.error(f"Ошибка в конвейере кластеризации {session_id}: {e}", exc_info=True)
        try:
            fail_session = db.session.merge(session)
            if fail_session:
                fail_session.status = 'FAILURE'
                fail_session.result_message = f"Ошибка: {str(e)[:500]}"
                db.session.commit()
            else:
                logger.error(f"Не удалось найти сессию {session_id} в БД, чтобы пометить ее как FAILURE.")
        except Exception as db_err:
            db.session.rollback()
            logger.error(f"Дополнительная ошибка при попытке пометить сессию {session_id} как FAILURE: {db_err}", exc_info=True)
        raise