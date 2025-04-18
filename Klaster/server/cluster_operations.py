import os
import logging
import numpy as np
from sqlalchemy.exc import SQLAlchemyError
from sklearn.cluster import KMeans
from sklearn.metrics import euclidean_distances
from sqlalchemy.orm.attributes import flag_modified
from models import db, ClusteringSession, ClusterMetadata, ManualAdjustmentLog
from algorithms import get_cluster_labels_for_session
from data_loader import load_embeddings
from visualization import calculate_and_save_centroids_2d, generate_and_save_scatter_data

logger = logging.getLogger(__name__)

def log_manual_adjustment(session_id, user_id, action, details):
    try:
        log_entry = ManualAdjustmentLog(
            session_id=session_id,
            user_id=user_id,
            action_type=action
        )
        log_entry.set_details(details)
        db.session.add(log_entry)
        db.session.commit()
        logger.info(f"Залогировано ручное изменение: Сессия={session_id}, Пользователь={user_id}, Действие={action}")
        return True
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Не удалось залогировать ручное изменение для сессии {session_id}: {e}", exc_info=True)
        return False
    except Exception as e:
         db.session.rollback()
         logger.error(f"Неожиданная ошибка при логировании ручного изменения для сессии {session_id}: {e}", exc_info=True)
         return False

def redistribute_cluster_data(session_id, cluster_label_to_remove_str, user_id):
    logger.info(f"Начало перераспределения для кластера {cluster_label_to_remove_str} в сессии {session_id}")
    session = db.session.get(ClusteringSession, session_id)

    if not session:
        raise ValueError("Сессия не найдена")
    if session.user_id != user_id:
         raise ValueError("Доступ к сессии запрещен")
    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        raise ValueError(f"Невозможно изменить сессию со статусом {session.status}")

    cluster_to_remove = session.clusters.filter_by(
        cluster_label=cluster_label_to_remove_str,
        is_deleted=False
    ).first()

    if not cluster_to_remove:
        logger.warning(f"Кластер {cluster_label_to_remove_str} не найден или уже удален в сессии {session_id}.")
        raise ValueError(f"Кластер {cluster_label_to_remove_str} не найден или уже удален")

    sheet_path_to_delete = cluster_to_remove.contact_sheet_path
    removed_cluster_display_name = cluster_to_remove.name or f"Кластер {cluster_label_to_remove_str}"
    db_cluster_id_to_remove = cluster_to_remove.id

    remaining_clusters = session.clusters.filter(
        ClusterMetadata.id != db_cluster_id_to_remove,
        ClusterMetadata.is_deleted == False
    ).all()

    old_scatter_cache_path = session.scatter_data_file_path
    if old_scatter_cache_path and os.path.exists(old_scatter_cache_path):
        try:
            os.remove(old_scatter_cache_path)
            logger.info(f"Удален старый кэш scatter plot: {old_scatter_cache_path}")
        except OSError as e:
            logger.error(f"Ошибка удаления старого кэша scatter plot {old_scatter_cache_path}: {e}")
    session.scatter_data_file_path = None
    flag_modified(session, "scatter_data_file_path")
    db.session.commit()

    if not remaining_clusters:
        logger.warning(f"Нет активных кластеров для перераспределения из кластера {cluster_label_to_remove_str}. Кластер будет помечен как удаленный.")
        cluster_to_remove.is_deleted = True
        cluster_to_remove.contact_sheet_path = None
        flag_modified(cluster_to_remove, "is_deleted")
        flag_modified(cluster_to_remove, "contact_sheet_path")

        log_details = {"cluster_label": cluster_label_to_remove_str, "cluster_name": cluster_to_remove.name}
        log_manual_adjustment(session.id, user_id, "DELETE_CLUSTER_NO_TARGETS", log_details)

        session.result_message = f"'{removed_cluster_display_name}' удален. Других кластеров для перераспределения не найдено."
        session.num_clusters = 0
        flag_modified(session, "result_message")
        flag_modified(session, "num_clusters")

        try:
            db.session.commit()
            if sheet_path_to_delete and os.path.exists(sheet_path_to_delete):
                try: os.remove(sheet_path_to_delete)
                except OSError as e: logger.error(f"Ошибка удаления файла контактного листа {sheet_path_to_delete}: {e}")

            calculate_and_save_centroids_2d(session.id)
            try:
                 embeddings, image_ids = load_embeddings(session.input_file_path)
                 if embeddings is not None and image_ids is not None:
                      final_labels_after_delete = np.full(embeddings.shape[0], -1, dtype=int)
                      new_cache_path, _ = generate_and_save_scatter_data(session.id, embeddings, final_labels_after_delete)
                      if new_cache_path:
                           session_reloaded = db.session.get(ClusteringSession, session_id)
                           if session_reloaded:
                               session_reloaded.scatter_data_file_path = new_cache_path
                               flag_modified(session_reloaded, "scatter_data_file_path")
                               db.session.commit()
            except FileNotFoundError:
                 logger.error(f"Не найден файл эмбеддингов {session.input_file_path} для генерации scatter plot после удаления последнего кластера сессии {session.id}")
            except Exception as scatter_err:
                 logger.error(f"Ошибка генерации scatter plot после удаления последнего кластера {session_id}: {scatter_err}")

            return {"message": session.result_message}
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Ошибка БД при удалении последнего кластера {session_id}/{cluster_label_to_remove_str}: {e}", exc_info=True)
            raise RuntimeError("Ошибка БД при удалении последнего кластера") from e

    try:
         embeddings, image_ids = load_embeddings(session.input_file_path)
         if embeddings is None:
             raise ValueError("Не удалось загрузить эмбеддинги для перераспределения.")
         if image_ids is None or len(image_ids) != embeddings.shape[0]:
             logger.warning(f"Отсутствуют или не совпадают ID изображений при перераспределении для сессии {session_id}. Использование индексов.")
             image_ids = [str(i) for i in range(embeddings.shape[0])]

         initial_labels = get_cluster_labels_for_session(session, embeddings)
         if initial_labels is None:
             raise RuntimeError(f"Не удалось получить исходные метки для сессии {session_id}.")
         if initial_labels.shape[0] != embeddings.shape[0]:
             raise RuntimeError(f"Размер эмбеддингов ({embeddings.shape[0]}) не совпадает с размером исходных меток ({initial_labels.shape[0]})")

         try:
            cluster_label_to_remove_int = int(cluster_label_to_remove_str)
         except ValueError:
             raise ValueError(f"Некорректный ID кластера для удаления: {cluster_label_to_remove_str}")

         point_indices_to_move = np.where(initial_labels == cluster_label_to_remove_int)[0]

         if len(point_indices_to_move) == 0:
             logger.warning(f"Не найдено точек с меткой {cluster_label_to_remove_int} для перераспределения в сессии {session_id}. Кластер будет помечен как удаленный.")
             cluster_to_remove.is_deleted = True
             cluster_to_remove.contact_sheet_path = None
             flag_modified(cluster_to_remove, "is_deleted")
             flag_modified(cluster_to_remove, "contact_sheet_path")

             log_details = {"cluster_label": cluster_label_to_remove_str, "cluster_name": cluster_to_remove.name}
             log_manual_adjustment(session.id, user_id, "DELETE_CLUSTER_NO_POINTS", log_details)

             session.result_message = f"'{removed_cluster_display_name}' удален. Точек для перераспределения не найдено."
             flag_modified(session, "result_message")
             db.session.commit()

             if sheet_path_to_delete and os.path.exists(sheet_path_to_delete):
                 try: os.remove(sheet_path_to_delete)
                 except OSError as e: logger.error(f"Ошибка удаления файла контактного листа {sheet_path_to_delete}: {e}")

             calculate_and_save_centroids_2d(session.id)

             final_labels = np.copy(initial_labels)
             final_labels[final_labels == cluster_label_to_remove_int] = -1
             new_cache_path, _ = generate_and_save_scatter_data(session.id, embeddings, final_labels)
             if new_cache_path:
                  session_reloaded = db.session.get(ClusteringSession, session_id)
                  if session_reloaded:
                      session_reloaded.scatter_data_file_path = new_cache_path
                      flag_modified(session_reloaded, "scatter_data_file_path")
                      db.session.commit()

             return {"message": session.result_message}

         logger.info(f"Перераспределение {len(point_indices_to_move)} точек из кластера {cluster_label_to_remove_str}...")
         embeddings_to_move = embeddings[point_indices_to_move]

         target_centroids = []
         target_idx_to_cluster_meta = {}
         for i, cluster_meta in enumerate(remaining_clusters):
             centroid_vec = cluster_meta.get_centroid()
             if centroid_vec is not None and centroid_vec.shape[0] == embeddings.shape[1]:
                 target_centroids.append(centroid_vec)
                 target_idx_to_cluster_meta[len(target_centroids) - 1] = cluster_meta
             else:
                 logger.warning(f"Пропуск оставшегося кластера {cluster_meta.cluster_label} как целевого из-за невалидного центроида.")

         if not target_centroids:
              logger.error(f"Не найдено валидных целевых центроидов для перераспределения в сессии {session_id}.")
              raise ValueError("Нет валидных целевых кластеров для перераспределения.")

         target_centroids_np = np.array(target_centroids)
         distances = euclidean_distances(embeddings_to_move, target_centroids_np)
         nearest_target_indices = np.argmin(distances, axis=1)

         cluster_to_remove.is_deleted = True
         cluster_to_remove.contact_sheet_path = None
         flag_modified(cluster_to_remove, "is_deleted")
         flag_modified(cluster_to_remove, "contact_sheet_path")

         redistribution_counts = {}
         final_labels = np.copy(initial_labels)
         for i, point_global_idx in enumerate(point_indices_to_move):
             target_internal_idx = nearest_target_indices[i]
             target_cluster_meta = target_idx_to_cluster_meta[target_internal_idx]
             target_db_id = target_cluster_meta.id
             target_final_label = int(target_cluster_meta.cluster_label)

             final_labels[point_global_idx] = target_final_label
             redistribution_counts[target_db_id] = redistribution_counts.get(target_db_id, 0) + 1

         redistribution_log_details = {
             "cluster_label_removed": cluster_label_to_remove_str,
             "cluster_name_removed": cluster_to_remove.name,
             "points_moved": len(point_indices_to_move),
             "targets": []
         }
         for target_db_id, count in redistribution_counts.items():
             target_cluster = db.session.get(ClusterMetadata, target_db_id)
             if target_cluster and not target_cluster.is_deleted:
                 original_size = target_cluster.size if target_cluster.size else 0
                 target_cluster.size = original_size + count
                 flag_modified(target_cluster, "size")
                 redistribution_log_details["targets"].append({
                     "target_cluster_label": target_cluster.cluster_label,
                     "target_cluster_name": target_cluster.name,
                     "count": count,
                     "new_size": target_cluster.size
                 })
             else:
                  logger.error(f"Целевой кластер {target_db_id} не найден или удален при обновлении размера.")

         session.num_clusters = len(remaining_clusters)
         session.result_message = f"'{removed_cluster_display_name}' удален, {len(point_indices_to_move)} точек перераспределены."
         session.status = 'RECLUSTERED'
         flag_modified(session, "num_clusters")
         flag_modified(session, "result_message")
         flag_modified(session, "status")

         log_manual_adjustment(session.id, user_id, "REDISTRIBUTE_CLUSTER", redistribution_log_details)

         logger.info(f"Генерация scatter plot с перераспределенными метками для сессии {session_id}...")
         all_deleted_clusters = ClusterMetadata.query.with_session(db.session).filter_by(session_id=session_id, is_deleted=True).all()
         all_deleted_labels_int = set()
         for dc in all_deleted_clusters:
              try: all_deleted_labels_int.add(int(dc.cluster_label))
              except (ValueError, TypeError): pass

         for del_label_int in all_deleted_labels_int:
              mask = (final_labels == del_label_int)
              if np.any(mask):
                  logger.warning(f"Обнаружены точки с удаленной меткой {del_label_int} в final_labels. Установка в -1.")
                  final_labels[mask] = -1

         new_cache_path, _ = generate_and_save_scatter_data(session.id, embeddings, final_labels)
         if new_cache_path:
             session.scatter_data_file_path = new_cache_path
             flag_modified(session, "scatter_data_file_path")
         else:
              logger.error(f"Не удалось сгенерировать/сохранить новый кэш scatter plot для сессии {session_id} после перераспределения.")

         db.session.commit()

         logger.info(f"Перераспределение для кластера {cluster_label_to_remove_str} успешно. Обновление 2D координат центроидов...")
         calculate_and_save_centroids_2d(session.id)

         if sheet_path_to_delete and os.path.exists(sheet_path_to_delete):
             try:
                 os.remove(sheet_path_to_delete)
                 logger.info(f"Удален файл контактного листа для удаленного кластера: {sheet_path_to_delete}")
             except OSError as e:
                  logger.error(f"Ошибка удаления файла контактного листа {sheet_path_to_delete}: {e}")

         logger.info(f"Процесс перераспределения для кластера {cluster_label_to_remove_str} в сессии {session_id} завершен.")
         return {"message": session.result_message}

    except (ValueError, RuntimeError, FileNotFoundError) as e:
         db.session.rollback()
         logger.error(f"Ошибка подготовки/выполнения перераспределения {session_id}/{cluster_label_to_remove_str}: {e}", exc_info=True)
         raise RuntimeError(f"Ошибка подготовки/выполнения перераспределения: {e}") from e
    except SQLAlchemyError as e:
         db.session.rollback()
         logger.error(f"Ошибка БД при перераспределении {session_id}/{cluster_label_to_remove_str}: {e}", exc_info=True)
         raise RuntimeError("Ошибка БД при перераспределении кластера") from e
    except Exception as e:
        db.session.rollback()
        logger.error(f"Неожиданная ошибка при перераспределении {session_id}/{cluster_label_to_remove_str}: {e}", exc_info=True)
        raise RuntimeError(f"Непредвиденная ошибка сервера при перераспределении: {e}") from e


def _get_next_cluster_label(session_id):
    max_label_obj = db.session.query(db.func.max(db.cast(ClusterMetadata.cluster_label, db.Integer)))\
                              .filter(ClusterMetadata.session_id == session_id)\
                              .scalar()
    if max_label_obj is None:
        return 0
    try:
        max_label_int = int(max_label_obj)
        return max_label_int + 1
    except (ValueError, TypeError):
        logger.warning(f"Could not determine max integer label for session {session_id}, defaulting to 0.")
        all_labels = db.session.query(ClusterMetadata.cluster_label)\
                               .filter(ClusterMetadata.session_id == session_id).all()
        max_int = -1
        for label_tuple in all_labels:
            try:
                label_int = int(label_tuple[0])
                if label_int > max_int:
                    max_int = label_int
            except (ValueError, TypeError):
                continue
        return max_int + 1


def merge_clusters(session_id, cluster_labels_to_merge, user_id):
    logger.info(f"Начало слияния кластеров {cluster_labels_to_merge} в сессии {session_id}")
    session = db.session.get(ClusteringSession, session_id)

    if not session: raise ValueError("Сессия не найдена")
    if session.user_id != user_id: raise ValueError("Доступ к сессии запрещен")
    if session.status not in ['SUCCESS', 'RECLUSTERED']: raise ValueError(f"Невозможно изменить сессию со статусом {session.status}")
    if not isinstance(cluster_labels_to_merge, list) or len(cluster_labels_to_merge) < 2:
        raise ValueError("Необходимо указать список из как минимум 2 ID кластеров для слияния.")

    cluster_labels_str = [str(lbl) for lbl in cluster_labels_to_merge]

    clusters_to_merge = session.clusters.filter(
        ClusterMetadata.cluster_label.in_(cluster_labels_str),
        ClusterMetadata.is_deleted == False
    ).all()

    if len(clusters_to_merge) != len(cluster_labels_str):
         found_labels = {c.cluster_label for c in clusters_to_merge}
         missing = [lbl for lbl in cluster_labels_str if lbl not in found_labels]
         raise ValueError(f"Не найдены или уже удалены следующие кластеры для слияния: {', '.join(missing)}")

    old_scatter_cache_path = session.scatter_data_file_path
    if old_scatter_cache_path and os.path.exists(old_scatter_cache_path):
        try: os.remove(old_scatter_cache_path)
        except OSError as e: logger.error(f"Ошибка удаления кэша scatter plot {old_scatter_cache_path}: {e}")
    session.scatter_data_file_path = None
    flag_modified(session, "scatter_data_file_path")
    try:
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Ошибка БД при очистке пути к кэшу scatter перед слиянием: {e}")
        raise RuntimeError("Ошибка БД при подготовке к слиянию")

    try:
        embeddings, _ = load_embeddings(session.input_file_path)
        if embeddings is None: raise ValueError("Не удалось загрузить эмбеддинги.")

        initial_labels = get_cluster_labels_for_session(session, embeddings)
        if initial_labels is None: raise RuntimeError("Не удалось получить исходные метки.")

        cluster_labels_to_merge_int = set()
        for lbl_str in cluster_labels_str:
             try: cluster_labels_to_merge_int.add(int(lbl_str))
             except ValueError: raise ValueError(f"Некорректный ID кластера для слияния: {lbl_str}")

        point_indices_to_merge = np.where(np.isin(initial_labels, list(cluster_labels_to_merge_int)))[0]

        if len(point_indices_to_merge) == 0:
            logger.warning(f"Не найдено точек для слияния кластеров {cluster_labels_str} в сессии {session_id}. Операция отменена.")
            raise ValueError("Не найдено точек, принадлежащих указанным кластерам.")

        embeddings_to_merge = embeddings[point_indices_to_merge]
        new_centroid = np.mean(embeddings_to_merge, axis=0)
        new_size = len(point_indices_to_merge)

        next_label_int = _get_next_cluster_label(session.id)
        new_cluster_label_str = str(next_label_int)

        original_names = []
        paths_to_delete = []
        for cluster_meta in clusters_to_merge:
            cluster_meta.is_deleted = True
            if cluster_meta.contact_sheet_path:
                paths_to_delete.append(cluster_meta.contact_sheet_path)
            cluster_meta.contact_sheet_path = None
            flag_modified(cluster_meta, "is_deleted")
            flag_modified(cluster_meta, "contact_sheet_path")
            original_names.append(cluster_meta.name or f"Кластер {cluster_meta.cluster_label}")

        new_cluster = ClusterMetadata(
            session_id=session.id,
            cluster_label=new_cluster_label_str,
            original_cluster_id=None,
            size=new_size,
            is_deleted=False,
            name=f"Слито из [{', '.join(cluster_labels_str)}]"
        )
        new_cluster.set_centroid(new_centroid)
        db.session.add(new_cluster)

        active_cluster_count = session.clusters.filter_by(is_deleted=False).count() - len(clusters_to_merge) + 1
        session.num_clusters = active_cluster_count
        session.result_message = f"Кластеры {', '.join(cluster_labels_str)} слиты в новый кластер {new_cluster_label_str}."
        session.status = 'RECLUSTERED'
        flag_modified(session, "num_clusters")
        flag_modified(session, "result_message")
        flag_modified(session, "status")

        log_details = {
            "merged_cluster_labels": cluster_labels_str,
            "merged_cluster_names": original_names,
            "new_cluster_label": new_cluster_label_str,
            "new_size": new_size
        }
        log_manual_adjustment(session.id, user_id, "MERGE_CLUSTERS", log_details)

        final_labels = np.copy(initial_labels)
        final_labels[point_indices_to_merge] = next_label_int

        all_deleted_clusters = ClusterMetadata.query.with_session(db.session).filter_by(session_id=session_id, is_deleted=True).all()
        all_deleted_labels_int = set()
        for dc in all_deleted_clusters:
            try: all_deleted_labels_int.add(int(dc.cluster_label))
            except (ValueError, TypeError): pass
        for del_label_int in all_deleted_labels_int:
            mask = (final_labels == del_label_int)
            if np.any(mask):
                logger.warning(f"Merge: Обнаружены точки с удаленной меткой {del_label_int} в final_labels. Установка в -1.")
                final_labels[mask] = -1


        new_cache_path, _ = generate_and_save_scatter_data(session.id, embeddings, final_labels)
        if new_cache_path:
            session.scatter_data_file_path = new_cache_path
            flag_modified(session, "scatter_data_file_path")
        else:
            logger.error(f"Не удалось сгенерировать/сохранить кэш scatter plot для сессии {session_id} после слияния.")

        db.session.commit()

        logger.info(f"Слияние кластеров {cluster_labels_str} успешно. Обновление 2D координат...")
        calculate_and_save_centroids_2d(session.id)

        for sheet_path in paths_to_delete:
            if sheet_path and os.path.exists(sheet_path):
                try: os.remove(sheet_path)
                except OSError as e: logger.error(f"Ошибка удаления файла КС {sheet_path}: {e}")

        return {"message": session.result_message}

    except (ValueError, RuntimeError, FileNotFoundError) as e:
        db.session.rollback()
        logger.error(f"Ошибка подготовки/выполнения слияния {session_id}/{cluster_labels_str}: {e}", exc_info=True)
        raise RuntimeError(f"Ошибка подготовки/выполнения слияния: {e}") from e
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Ошибка БД при слиянии {session_id}/{cluster_labels_str}: {e}", exc_info=True)
        raise RuntimeError("Ошибка БД при слиянии кластеров") from e
    except Exception as e:
        db.session.rollback()
        logger.error(f"Неожиданная ошибка при слиянии {session_id}/{cluster_labels_str}: {e}", exc_info=True)
        raise RuntimeError(f"Непредвиденная ошибка сервера при слиянии: {e}") from e


def split_cluster(session_id, cluster_label_to_split_str, num_splits, user_id):
    logger.info(f"Начало разделения кластера {cluster_label_to_split_str} на {num_splits} части в сессии {session_id}")
    session = db.session.get(ClusteringSession, session_id)

    if not session: raise ValueError("Сессия не найдена")
    if session.user_id != user_id: raise ValueError("Доступ к сессии запрещен")
    if session.status not in ['SUCCESS', 'RECLUSTERED']: raise ValueError(f"Невозможно изменить сессию со статусом {session.status}")
    if not isinstance(num_splits, int) or num_splits < 2:
        raise ValueError("Количество частей для разделения должно быть целым числом >= 2.")

    cluster_to_split = session.clusters.filter_by(
        cluster_label=str(cluster_label_to_split_str),
        is_deleted=False
    ).first()

    if not cluster_to_split:
        raise ValueError(f"Кластер {cluster_label_to_split_str} не найден или уже удален.")

    old_scatter_cache_path = session.scatter_data_file_path
    if old_scatter_cache_path and os.path.exists(old_scatter_cache_path):
        try: os.remove(old_scatter_cache_path)
        except OSError as e: logger.error(f"Ошибка удаления кэша scatter plot {old_scatter_cache_path}: {e}")
    session.scatter_data_file_path = None
    flag_modified(session, "scatter_data_file_path")
    try:
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Ошибка БД при очистке пути к кэшу scatter перед разделением: {e}")
        raise RuntimeError("Ошибка БД при подготовке к разделению")

    try:
        embeddings, _ = load_embeddings(session.input_file_path)
        if embeddings is None: raise ValueError("Не удалось загрузить эмбеддинги.")

        initial_labels = get_cluster_labels_for_session(session, embeddings)
        if initial_labels is None: raise RuntimeError("Не удалось получить исходные метки.")

        try: cluster_label_to_split_int = int(cluster_label_to_split_str)
        except ValueError: raise ValueError(f"Некорректный ID кластера для разделения: {cluster_label_to_split_str}")

        point_indices_to_split = np.where(initial_labels == cluster_label_to_split_int)[0]
        num_points_in_cluster = len(point_indices_to_split)

        if num_points_in_cluster < num_splits:
            raise ValueError(f"В кластере {cluster_label_to_split_str} ({num_points_in_cluster} точек) недостаточно точек для разделения на {num_splits} части.")

        embeddings_to_split = embeddings[point_indices_to_split]

        logger.info(f"Запуск K-Means (k={num_splits}) для {num_points_in_cluster} точек кластера {cluster_label_to_split_str}")
        kmeans_split = KMeans(n_clusters=num_splits, random_state=42, n_init=10)
        try:
            local_split_labels = kmeans_split.fit_predict(embeddings_to_split)
        except Exception as kmeans_err:
            logger.error(f"Ошибка K-Means при разделении кластера {cluster_label_to_split_str}: {kmeans_err}", exc_info=True)
            raise RuntimeError(f"Внутренняя ошибка при разделении кластера (KMeans): {kmeans_err}")


        original_cluster_name = cluster_to_split.name or f"Кластер {cluster_label_to_split_str}"
        original_contact_sheet_path = cluster_to_split.contact_sheet_path

        cluster_to_split.is_deleted = True
        cluster_to_split.contact_sheet_path = None
        flag_modified(cluster_to_split, "is_deleted")
        flag_modified(cluster_to_split, "contact_sheet_path")

        final_labels = np.copy(initial_labels)
        new_cluster_details_log = []
        new_clusters_added = []

        current_max_label = _get_next_cluster_label(session.id) -1

        for i in range(num_splits):
            local_mask = (local_split_labels == i)
            local_point_indices = point_indices_to_split[local_mask]
            new_size = len(local_point_indices)

            if new_size == 0:
                logger.warning(f"Разделение кластера {cluster_label_to_split_str}: Часть {i+1} оказалась пустой, пропускаем.")
                continue

            new_centroid = np.mean(embeddings[local_point_indices], axis=0)
            new_label_int = current_max_label + 1 + i
            new_label_str = str(new_label_int)

            final_labels[local_point_indices] = new_label_int

            new_cluster = ClusterMetadata(
                session_id=session.id,
                cluster_label=new_label_str,
                original_cluster_id=cluster_label_to_split_str,
                size=new_size,
                is_deleted=False,
                name=f"Часть {i+1} из {cluster_label_to_split_str}"
            )
            new_cluster.set_centroid(new_centroid)
            db.session.add(new_cluster)
            new_clusters_added.append(new_cluster)
            new_cluster_details_log.append({
                "new_cluster_label": new_label_str,
                "new_size": new_size
            })

        active_cluster_count = session.clusters.filter_by(is_deleted=False).count() - 1 + len(new_clusters_added)
        session.num_clusters = active_cluster_count
        session.result_message = f"Кластер '{original_cluster_name}' ({cluster_label_to_split_str}) разделен на {len(new_clusters_added)} части."
        session.status = 'RECLUSTERED'
        flag_modified(session, "num_clusters")
        flag_modified(session, "result_message")
        flag_modified(session, "status")

        log_details = {
            "split_cluster_label": cluster_label_to_split_str,
            "split_cluster_name": original_cluster_name,
            "num_splits_requested": num_splits,
            "num_splits_created": len(new_clusters_added),
            "new_clusters": new_cluster_details_log
        }
        log_manual_adjustment(session.id, user_id, "SPLIT_CLUSTER", log_details)

        all_deleted_clusters = ClusterMetadata.query.with_session(db.session).filter_by(session_id=session_id, is_deleted=True).all()
        all_deleted_labels_int = set()
        for dc in all_deleted_clusters:
             try: all_deleted_labels_int.add(int(dc.cluster_label))
             except (ValueError, TypeError): pass
        for del_label_int in all_deleted_labels_int:
             mask = (final_labels == del_label_int)
             if np.any(mask):
                 logger.warning(f"Split: Обнаружены точки с удаленной меткой {del_label_int} в final_labels. Установка в -1.")
                 final_labels[mask] = -1


        new_cache_path, _ = generate_and_save_scatter_data(session.id, embeddings, final_labels)
        if new_cache_path:
            session.scatter_data_file_path = new_cache_path
            flag_modified(session, "scatter_data_file_path")
        else:
             logger.error(f"Не удалось сгенерировать/сохранить кэш scatter plot для сессии {session_id} после разделения.")


        db.session.commit()

        logger.info(f"Разделение кластера {cluster_label_to_split_str} успешно. Обновление 2D координат...")
        calculate_and_save_centroids_2d(session.id)

        if original_contact_sheet_path and os.path.exists(original_contact_sheet_path):
            try: os.remove(original_contact_sheet_path)
            except OSError as e: logger.error(f"Ошибка удаления файла КС {original_contact_sheet_path}: {e}")

        return {"message": session.result_message}

    except (ValueError, RuntimeError, FileNotFoundError) as e:
        db.session.rollback()
        logger.error(f"Ошибка подготовки/выполнения разделения {session_id}/{cluster_label_to_split_str}: {e}", exc_info=True)
        raise RuntimeError(f"Ошибка подготовки/выполнения разделения: {e}") from e
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Ошибка БД при разделении {session_id}/{cluster_label_to_split_str}: {e}", exc_info=True)
        raise RuntimeError("Ошибка БД при разделении кластера") from e
    except Exception as e:
        db.session.rollback()
        logger.error(f"Неожиданная ошибка при разделении {session_id}/{cluster_label_to_split_str}: {e}", exc_info=True)
        raise RuntimeError(f"Непредвиденная ошибка сервера при разделении: {e}") from e