import os
import json
import logging
import pandas as pd
from data_loader import load_embeddings
from algorithms import get_cluster_labels_for_session
from models import db, ClusteringSession, ClusterMetadata

logger = logging.getLogger(__name__)

def generate_assignments_data(session: ClusteringSession) -> pd.DataFrame | None:
    logger.info(f"Генерация данных назначений для экспорта для сессии {session.id}")
    if not session.input_file_path or not os.path.exists(session.input_file_path):
        logger.error(f"Путь к входному файлу отсутствует или файл не найден для сессии {session.id}")
        return None
    try:
        embeddings, image_ids = load_embeddings(session.input_file_path)
        if not image_ids:
            logger.error(f"Невозможно сгенерировать экспорт назначений для сессии {session.id}: Отсутствуют идентификаторы изображений.")
            return None
        if embeddings is None or embeddings.shape[0] != len(image_ids):
             logger.error(f"Невозможно сгенерировать экспорт назначений для сессии {session.id}: Вложения отсутствуют или не соответствуют идентификаторам изображений ({embeddings.shape[0] if embeddings is not None else 'None'} против {len(image_ids)}).")
             return None

        final_labels = get_cluster_labels_for_session(session, embeddings)
        if final_labels is None:
            logger.error(f"Невозможно сгенерировать экспорт назначений для сессии {session.id}: Не удалось получить финальные метки.")
            return None
        if final_labels.shape[0] != len(image_ids):
             logger.error(f"Невозможно сгенерировать экспорт назначений для сессии {session.id}: Несоответствие между финальными метками ({final_labels.shape[0]}) и идентификаторами изображений ({len(image_ids)}).")
             return None

        active_clusters = session.clusters.filter_by(is_deleted=False).all()
        cluster_label_to_name_map = {cluster.cluster_label: cluster.name for cluster in active_clusters}

        assignments = []
        for img_id, label_val in zip(image_ids, final_labels):
            label_str = str(label_val)
            cluster_name = cluster_label_to_name_map.get(label_str, None) if label_val != -1 else "Шум"
            assignments.append({
                "image_id": img_id,
                "cluster_label": label_str,
                "cluster_name": cluster_name
            })

        assignments_df = pd.DataFrame(assignments)
        logger.info(f"Успешно сгенерирован DataFrame назначений для экспорта (сессия {session.id}) с {len(assignments_df)} строками.")
        return assignments_df

    except FileNotFoundError:
        logger.error(f"Входной файл не найден для сессии {session.id} во время экспорта назначений.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Ошибка при генерации данных назначений для экспорта (сессия {session.id}): {e}", exc_info=True)
        return None

def generate_cluster_summary_json(session: ClusteringSession) -> list[dict]:
    logger.info(f"Генерация JSON-сводки кластеров для экспорта для сессии {session.id}")
    active_clusters = session.clusters.filter_by(is_deleted=False)\
                                     .order_by(ClusterMetadata.cluster_label).all()
    summary = []
    for cluster in active_clusters:
        sheet_url = None
        if cluster.contact_sheet_path:
             sheet_filename = os.path.basename(cluster.contact_sheet_path)
             sheet_url = f"/api/clustering/contact_sheet/{session.id}/{sheet_filename}"

        summary.append({
            "cluster_label": cluster.cluster_label,
            "cluster_name": cluster.name,
            "size": cluster.size,
            "original_cluster_id": cluster.original_cluster_id,
            "contact_sheet_url": sheet_url,
            "centroid_2d": cluster.get_centroid_2d(),
            "metrics": cluster.get_metrics()
        })
    logger.info(f"Сгенерирована JSON-сводка кластеров для экспорта (сессия {session.id}) с {len(summary)} кластерами.")
    return summary

def generate_session_summary_json(session: ClusteringSession) -> dict:
    logger.info(f"Генерация JSON-сводки сессии для экспорта для сессии {session.id}")
    active_cluster_count = session.clusters.filter_by(is_deleted=False).count()
    summary = {
        "session_id": session.id,
        "created_at": session.created_at.isoformat() + "Z",
        "status": session.status,
        "algorithm": session.algorithm,
        "params": session.get_params(),
        "original_input_filename": session.original_input_filename,
        "final_num_clusters": active_cluster_count,
        "processing_time_sec": session.processing_time_sec,
        "result_message": session.result_message,
        "scatter_pca_time_sec": None
    }

    if session.scatter_data_file_path and os.path.exists(session.scatter_data_file_path):
        try:
            with open(session.scatter_data_file_path, 'r') as f:
                scatter_cache = json.load(f)
                summary["scatter_pca_time_sec"] = scatter_cache.get("pca_time")
        except Exception as e:
             logger.warning(f"Не удалось прочитать время PCA из кэша диаграммы рассеяния {session.scatter_data_file_path} для экспорта сводки сессии {session.id}: {e}")

    logger.info(f"Сгенерирована JSON-сводка сессии для экспорта (сессия {session.id})")
    return summary