import os
import io
import csv
import json
import uuid
import logging
import zipfile
from flask import Blueprint, jsonify, request, current_app, make_response, send_from_directory
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.attributes import flag_modified
from werkzeug.utils import secure_filename
from algorithms import get_cluster_labels_for_session
from cluster_operations import log_manual_adjustment, redistribute_cluster_data, merge_clusters, split_cluster
from data_loader import load_embeddings
from export import (
    generate_assignments_data,
    generate_cluster_summary_json,
    generate_session_summary_json,
)
from models import db, ClusteringSession, ClusterMetadata, ManualAdjustmentLog
from pipeline import run_clustering_pipeline
from visualization import generate_and_save_scatter_data

logger = logging.getLogger(__name__)

clustering_bp = Blueprint('clustering_api', __name__, url_prefix='/api/clustering')

ALLOWED_EXTENSIONS_EMBEDDINGS = {'parquet'}
ALLOWED_EXTENSIONS_ARCHIVE = {'zip'}

def allowed_embedding_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_EMBEDDINGS

def allowed_archive_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_ARCHIVE

@clustering_bp.route('/start', methods=['POST'])
@jwt_required()
def start_clustering():
    current_user_id = get_jwt_identity()
    if 'embeddingFile' not in request.files:
        return jsonify({"error": "Файл эмбеддингов ('embeddingFile') не найден"}), 400
    embedding_file = request.files['embeddingFile']
    algorithm = request.form.get('algorithm')
    params_str = request.form.get('params', '{}')

    if not algorithm or algorithm not in ['kmeans', 'dbscan']:
        return jsonify({"error": "Алгоритм не указан или не поддерживается (ожидается 'kmeans' или 'dbscan')"}), 400
    try:
        params = json.loads(params_str)
        if not isinstance(params, dict): raise ValueError("Params not a dict")
    except (json.JSONDecodeError, ValueError):
        logger.warning(f"Некорректные параметры JSON получены от пользователя {current_user_id}: {params_str}")
        return jsonify({"error": "Некорректный формат JSON для параметров ('params')"}), 400

    original_embedding_filename = embedding_file.filename
    if original_embedding_filename == '' or not allowed_embedding_file(original_embedding_filename):
         return jsonify({"error": "Некорректный файл эмбеддингов (пустое имя или не .parquet)"}), 400

    archive_file = request.files.get('imageArchive')
    original_archive_filename = None
    archive_path_for_storage = None
    archive_filename_for_storage = None

    if archive_file:
        original_archive_filename = archive_file.filename
        if original_archive_filename == '' or not allowed_archive_file(original_archive_filename):
            logger.warning(f"Пользователь {current_user_id} загрузил невалидный архив '{original_archive_filename}'. Игнорирование.")
            archive_file = None
        else:
            is_zip = False
            try:
                 archive_file.seek(0)
                 is_zip = zipfile.is_zipfile(archive_file)
                 archive_file.seek(0)
            except Exception as zip_err:
                 logger.warning(f"Ошибка валидации ZIP файла {original_archive_filename} от пользователя {current_user_id}: {zip_err}. Игнорирование файла.")
            if not is_zip:
                logger.warning(f"Пользователь {current_user_id} загрузил файл с именем .zip, который не является валидным ZIP: {original_archive_filename}. Игнорирование.")
                archive_file = None
            else:
                 logger.info(f"Пользователь {current_user_id} предоставил валидный архив изображений: {original_archive_filename}")
                 secure_archive_basename = secure_filename(original_archive_filename)
                 archive_filename_for_storage = f"{uuid.uuid4()}_{secure_archive_basename}"
                 archive_path_for_storage = os.path.join(current_app.config['UPLOAD_FOLDER'], archive_filename_for_storage)


    upload_folder = current_app.config['UPLOAD_FOLDER']
    embedding_filename_for_storage = f"{uuid.uuid4()}_{secure_filename(original_embedding_filename)}"
    embedding_file_path = os.path.join(upload_folder, embedding_filename_for_storage)


    files_to_cleanup = []
    try:
        embedding_file.save(embedding_file_path)
        files_to_cleanup.append(embedding_file_path)
        logger.info(f"Пользователь {current_user_id} загрузил файл эмбеддингов: {original_embedding_filename} -> {embedding_filename_for_storage}")

        if archive_file and archive_path_for_storage:
            archive_file.save(archive_path_for_storage)
            files_to_cleanup.append(archive_path_for_storage)
            logger.info(f"Пользователь {current_user_id} загрузил архив изображений: {original_archive_filename} -> {archive_filename_for_storage}")

    except Exception as e:
        logger.error(f"Не удалось сохранить загруженные файлы для пользователя {current_user_id}: {e}", exc_info=True)
        for f_path in files_to_cleanup:
             if os.path.exists(f_path):
                 try: os.remove(f_path)
                 except OSError as rem_e: logger.error(f"Ошибка удаления файла {f_path} после ошибки сохранения: {rem_e}")
        return jsonify({"error": "Не удалось сохранить файл(ы) на сервере"}), 500

    try:
        logger.info(f"Пользователь {current_user_id} запускает синхронную кластеризацию...")
        session_id = run_clustering_pipeline(
            user_id=int(current_user_id),
            embedding_file_path=embedding_file_path,
            archive_path=archive_path_for_storage,
            original_archive_filename=original_archive_filename,
            algorithm=algorithm,
            params=params,
            original_embedding_filename=original_embedding_filename
        )
        logger.info(f"Пользователь {current_user_id} завершил синхронную кластеризацию. ID сессии: {session_id}")
        return jsonify({"session_id": session_id}), 201

    except (ValueError, SQLAlchemyError) as ve:
        logger.error(f"Ошибка валидации или БД при старте кластеризации для пользователя {current_user_id}: {ve}", exc_info=False)
        for f_path in files_to_cleanup:
            if os.path.exists(f_path):
                 try: os.remove(f_path)
                 except OSError as rem_e: logger.error(f"Ошибка удаления файла {f_path} после ошибки: {rem_e}")
        return jsonify({"error": f"Ошибка входных данных или БД: {ve}"}), 400
    except Exception as e:
        logger.error(f"Синхронная кластеризация неожиданно завершилась с ошибкой для пользователя {current_user_id}: {e}", exc_info=True)
        for f_path in files_to_cleanup:
             if os.path.exists(f_path):
                 try: os.remove(f_path)
                 except OSError as rem_e: logger.error(f"Ошибка удаления файла {f_path} после ошибки: {rem_e}")
        return jsonify({"error": "Внутренняя ошибка сервера при кластеризации"}), 500


@clustering_bp.route('/sessions', methods=['GET'])
@jwt_required()
def list_clustering_sessions():
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    try:
        sessions = ClusteringSession.query.filter_by(user_id=user_id_int)\
                                         .order_by(ClusteringSession.created_at.desc()).all()
        output = []
        for session in sessions:
            output.append({
                "session_id": session.id,
                "created_at": session.created_at.isoformat() + "Z",
                "status": session.status,
                "algorithm": session.algorithm,
                "params": session.get_params(),
                "num_clusters": session.num_clusters,
                "result_message": session.result_message,
                "original_filename": session.original_input_filename if session.original_input_filename else "N/A"
            })
        return jsonify(output), 200
    except Exception as e:
        logger.error(f"Ошибка при получении списка сессий для пользователя {current_user_id}: {e}", exc_info=True)
        return jsonify({"error": "Внутренняя ошибка сервера при получении списка сессий"}), 500


@clustering_bp.route('/results/<session_id>', methods=['GET'])
@jwt_required()
def get_clustering_results(session_id):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    session = db.session.get(ClusteringSession, session_id)

    if not session or session.user_id != user_id_int:
        logger.warning(f"Пользователь {current_user_id} запросил недоступную сессию {session_id}")
        return jsonify({"error": "Сессия кластеризации не найдена или не принадлежит вам"}), 404

    base_response = {
        "session_id": session.id, "status": session.status, "algorithm": session.algorithm,
        "params": session.get_params(), "num_clusters": session.num_clusters,
        "processing_time_sec": session.processing_time_sec, "message": session.result_message,
        "original_filename": session.original_input_filename if session.original_input_filename else None,
        "clusters": [],
        "scatter_data": None,
        "scatter_pca_time_sec": None,
        "adjustments": []
    }

    is_final_status = session.status in ['SUCCESS', 'RECLUSTERED', 'FAILURE', 'RECLUSTERING_FAILED']
    can_show_partial = session.status == 'PROCESSING'

    cluster_metadatas = session.clusters.filter_by(is_deleted=False).order_by(ClusterMetadata.cluster_label).all()
    num_active_clusters = len(cluster_metadatas)
    base_response["num_clusters"] = num_active_clusters

    clusters_data = []
    for cluster_meta in cluster_metadatas:
        contact_sheet_url = None
        if cluster_meta.contact_sheet_path:
             sheet_filename = os.path.basename(cluster_meta.contact_sheet_path)
             contact_sheet_url = f"/api/clustering/contact_sheet/{session.id}/{sheet_filename}"
        clusters_data.append({
            "id": cluster_meta.cluster_label,
            "original_id": cluster_meta.original_cluster_id,
            "name": cluster_meta.name,
            "size": cluster_meta.size,
            "contactSheetUrl": contact_sheet_url,
            "metrics": cluster_meta.get_metrics(),
            "centroid_2d": cluster_meta.get_centroid_2d()
        })
    base_response["clusters"] = clusters_data

    try:
        adjustments = ManualAdjustmentLog.query.filter_by(session_id=session.id)\
                                                .order_by(ManualAdjustmentLog.timestamp.desc()).all()
        for adj in adjustments:
            base_response["adjustments"].append({
                "timestamp": adj.timestamp.isoformat() + "Z",
                "action_type": adj.action_type,
                "details": adj.get_details()
            })
        logger.info(f"Загружено {len(base_response['adjustments'])} записей истории для сессии {session_id}")
    except Exception as adj_err:
        logger.error(f"Ошибка при загрузке истории изменений для сессии {session_id}: {adj_err}", exc_info=True)

    if session.status == 'PROCESSING' and not base_response.get("message"):
         base_response["message"] = "Идет обработка..."
    elif session.status == 'PROCESSING':
         base_response["message"] = (base_response.get("message", "") + " Идет постобработка...").strip()

    scatter_data_generated = False
    if num_active_clusters == 0 and session.status in ['SUCCESS', 'RECLUSTERED']:
        logger.info(f"Нет активных кластеров для сессии {session_id}. Отображение заглушки для PCA.")
        base_response["scatter_data"] = {"message": "Нет активных кластеров для отображения PCA."}
        scatter_data_generated = True

    if not scatter_data_generated and (session.status in ['SUCCESS', 'RECLUSTERED'] or can_show_partial):
        scatter_cache_file_path = session.scatter_data_file_path
        scatter_data_loaded_from_cache = False

        if scatter_cache_file_path and os.path.exists(scatter_cache_file_path):
            try:
                with open(scatter_cache_file_path, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                if isinstance(cached_data, dict) and "scatter_plot_data" in cached_data and "pca_time" in cached_data:
                     base_response["scatter_data"] = cached_data["scatter_plot_data"]
                     base_response["scatter_pca_time_sec"] = cached_data["pca_time"]
                     scatter_data_loaded_from_cache = True
                     scatter_data_generated = True
                     logger.info(f"Загружены данные Scatter Plot из кэша для сессии {session_id}: {scatter_cache_file_path}")
                else:
                     logger.warning(f"Неверный формат кэша Scatter Plot для сессии {session_id}. Файл будет удален/перезаписан.")
                     session.scatter_data_file_path = None
                     try: os.remove(scatter_cache_file_path)
                     except OSError: pass
                     db.session.commit()
                     scatter_cache_file_path = None

            except (json.JSONDecodeError, OSError, IOError) as e:
                logger.error(f"Ошибка загрузки кэша Scatter Plot ({scatter_cache_file_path}) для сессии {session_id}: {e}. Будет перегенерация.", exc_info=True)
                session.scatter_data_file_path = None
                try:
                    if os.path.exists(scatter_cache_file_path): os.remove(scatter_cache_file_path)
                except OSError: pass
                db.session.commit()
                scatter_cache_file_path = None

        if not scatter_data_loaded_from_cache and session.status in ['SUCCESS', 'RECLUSTERED']:
            if session.input_file_path and os.path.exists(session.input_file_path):
                logger.info(f"Генерация данных Scatter Plot для сессии {session_id} (кэш не найден/невалиден)")
                try:
                    embeddings, _ = load_embeddings(session.input_file_path)
                    labels = get_cluster_labels_for_session(session, embeddings)

                    if embeddings is not None and labels is not None:
                        new_cache_path, pca_time = generate_and_save_scatter_data(session.id, embeddings, labels)

                        if new_cache_path:
                            try:
                                with open(new_cache_path, 'r', encoding='utf-8') as f:
                                    new_cache_content = json.load(f)
                                base_response["scatter_data"] = new_cache_content.get("scatter_plot_data")
                                base_response["scatter_pca_time_sec"] = new_cache_content.get("pca_time")
                                scatter_data_generated = True
                                session_reloaded = db.session.get(ClusteringSession, session_id)
                                if session_reloaded:
                                    session_reloaded.scatter_data_file_path = new_cache_path
                                    flag_modified(session_reloaded, "scatter_data_file_path")
                                    db.session.commit()
                                    logger.info(f"Scatter plot сгенерирован, сохранен и загружен для ответа (сессия {session_id})")
                                else:
                                    logger.error(f"Не удалось перезагрузить сессию {session_id} для сохранения пути к кэшу scatter plot.")

                            except (OSError, IOError, json.JSONDecodeError) as read_err:
                                logger.error(f"Не удалось прочитать свежесозданный кэш scatter plot {new_cache_path}: {read_err}")
                                base_response["scatter_data"] = {"error": "Ошибка чтения сгенерированных данных PCA."}
                                scatter_data_generated = True
                        else:
                             logger.error(f"Функция generate_and_save_scatter_data не смогла создать кэш для сессии {session_id}")
                             base_response["scatter_data"] = {"error": "Ошибка генерации или сохранения данных PCA."}
                             scatter_data_generated = True
                    else:
                        logger.warning(f"Не удалось получить эмбеддинги или метки для генерации scatter plot, сессия {session_id}")
                        base_response["scatter_data"] = {"error": "Не удалось загрузить данные для scatter plot."}
                        scatter_data_generated = True

                except Exception as e:
                    logger.error(f"Ошибка генерации данных scatter plot для сессии {session_id}: {e}", exc_info=True)
                    base_response["scatter_data"] = {"error": "Внутренняя ошибка при генерации данных scatter plot."}
                    scatter_data_generated = True
            else:
                 logger.warning(f"Путь к входному файлу не найден для сессии {session_id}, невозможно сгенерировать scatter plot.")
                 base_response["scatter_data"] = {"error": "Файл с входными данными не найден."}
                 scatter_data_generated = True

    if base_response["scatter_data"] is None and not scatter_data_generated:
         if not is_final_status and not can_show_partial:
            base_response["scatter_data"] = {"message": f"Визуализация PCA недоступна для статуса '{session.status}'."}
         elif can_show_partial:
             base_response["scatter_data"] = {"message": "Генерация данных PCA..."}
         elif is_final_status:
              base_response["scatter_data"] = {"error": "Данные PCA не были сгенерированы."}

    if session.status in ['FAILURE', 'RECLUSTERING_FAILED'] and not base_response.get("error"):
        base_response["error"] = f"Статус сессии: {session.status}. {session.result_message or ''}"

    return jsonify(base_response), 200

@clustering_bp.route('/contact_sheet/<session_id>/<filename>', methods=['GET'])
def get_contact_sheet_image(session_id, filename):
    expected_format = current_app.config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG').lower()
    if not filename.startswith(f"cs_{session_id}_") or not filename.lower().endswith(f".{expected_format}"):
        logger.warning(f"Попытка доступа к невалидному имени файла контактного листа: {filename} для сессии {session_id}")
        return jsonify({"error": "Некорректное имя файла отпечатка"}), 400

    contact_sheet_session_dir = os.path.join(current_app.config['CONTACT_SHEET_FOLDER'], session_id)
    safe_filename = secure_filename(filename)
    if safe_filename != filename:
         logger.warning(f"Обнаружено потенциально небезопасное имя файла для контактного листа: {filename} -> {safe_filename}")
         return jsonify({"error": "Некорректное имя файла"}), 400

    try:
        logger.debug(f"Отправка контактного листа: {safe_filename} из {contact_sheet_session_dir}")
        mimetype = f"image/{expected_format}"
        return send_from_directory(contact_sheet_session_dir, safe_filename, mimetype=mimetype)
    except FileNotFoundError:
         logger.warning(f"Файл контактного листа не найден: сессия={session_id}, файл={safe_filename}")
         return jsonify({"error": "Файл контактного отпечатка не найден"}), 404
    except Exception as e:
        logger.error(f"Ошибка отправки контактного листа {safe_filename} для сессии {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Ошибка сервера при доступе к файлу"}), 500

@clustering_bp.route('/results/<session_id>/cluster/<cluster_label>', methods=['DELETE'])
@jwt_required()
def delete_and_redistribute_cluster_route(session_id, cluster_label):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    logger.info(f"Пользователь {current_user_id} запросил УДАЛЕНИЕ/ПЕРЕРАСПРЕДЕЛЕНИЕ для кластера {cluster_label} в сессии {session_id}")

    try:
        result = redistribute_cluster_data(
            session_id=session_id,
            cluster_label_to_remove_str=cluster_label,
            user_id=user_id_int
        )
        logger.info(f"Пользователь {current_user_id} завершил УДАЛЕНИЕ/ПЕРЕРАСПРЕДЕЛЕНИЕ для {session_id}/{cluster_label}. Результат: {result}")
        return jsonify(result), 200

    except ValueError as ve:
         logger.warning(f"Ошибка валидации при перераспределении для {session_id}/{cluster_label} пользователем {current_user_id}: {ve}", exc_info=False)
         return jsonify({"error": f"Ошибка операции: {ve}"}), 400
    except RuntimeError as re:
         logger.error(f"Ошибка выполнения при перераспределении для {session_id}/{cluster_label} пользователем {current_user_id}: {re}", exc_info=True)
         return jsonify({"error": f"Внутренняя ошибка сервера при перераспределении: {re}"}), 500
    except SQLAlchemyError as e:
         db.session.rollback()
         logger.error(f"Ошибка БД при перераспределении {session_id}/{cluster_label} пользователем {current_user_id}: {e}", exc_info=True)
         return jsonify({"error": "Ошибка базы данных при выполнении операции"}), 500
    except Exception as e:
        db.session.rollback()
        logger.error(f"Неожиданная ошибка при перераспределении для {session_id}/{cluster_label} пользователем {current_user_id}: {e}", exc_info=True)
        return jsonify({"error": "Неизвестная внутренняя ошибка сервера"}), 500

@clustering_bp.route('/results/<session_id>/adjust', methods=['POST'])
@jwt_required()
def adjust_clusters(session_id):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    session = db.session.get(ClusteringSession, session_id)
    if not session or session.user_id != user_id_int:
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404
    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Невозможно редактировать сессию со статусом {session.status}"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "Отсутствует тело запроса JSON"}), 400

    action = data.get('action')

    try:
        if action == 'RENAME':
            cluster_id = data.get('cluster_id')
            new_name = data.get('new_name')

            if cluster_id is None or new_name is None:
                return jsonify({"error": "Для 'RENAME' необходимы 'cluster_id' и 'new_name'"}), 400

            cluster_label_str = str(cluster_id)
            cluster_to_rename = session.clusters.filter_by(cluster_label=cluster_label_str, is_deleted=False).first()

            if not cluster_to_rename:
                 return jsonify({"error": f"Активный кластер с ID {cluster_label_str} не найден"}), 404

            old_name = cluster_to_rename.name
            cluster_to_rename.name = new_name.strip() if new_name else None
            flag_modified(cluster_to_rename, "name")

            log_details = { "cluster_label": cluster_label_str, "old_name": old_name, "new_name": cluster_to_rename.name }
            log_success = log_manual_adjustment(session.id, user_id_int, "RENAME_CLUSTER", log_details)
            if not log_success:
                 db.session.rollback()
                 return jsonify({"error": "Ошибка логирования действия, переименование отменено"}), 500

            db.session.commit()
            logger.info(f"Пользователь {current_user_id} переименовал кластер {cluster_label_str} в сессии {session_id} с '{old_name}' на '{cluster_to_rename.name}'")
            updated_cluster = db.session.get(ClusterMetadata, cluster_to_rename.id)
            return jsonify({
                "message": "Кластер переименован",
                "cluster": {
                    "id": updated_cluster.cluster_label,
                    "name": updated_cluster.name,
                    "size": updated_cluster.size
                 }
             }), 200

        elif action == 'MERGE_CLUSTERS':
            cluster_ids_to_merge = data.get('cluster_ids_to_merge')
            if not isinstance(cluster_ids_to_merge, list) or len(cluster_ids_to_merge) < 2:
                return jsonify({"error": "Для 'MERGE_CLUSTERS' необходим массив 'cluster_ids_to_merge' (минимум 2 элемента)"}), 400

            logger.info(f"Пользователь {current_user_id} запросил MERGE для кластеров {cluster_ids_to_merge} в сессии {session_id}")
            result = merge_clusters(
                session_id=session_id,
                cluster_labels_to_merge=cluster_ids_to_merge,
                user_id=user_id_int
            )
            logger.info(f"Пользователь {current_user_id} завершил MERGE для {session_id}. Результат: {result}")
            return jsonify(result), 200

        elif action == 'SPLIT_CLUSTER':
            cluster_id_to_split = data.get('cluster_id_to_split')
            num_splits = data.get('num_splits', 2)

            if cluster_id_to_split is None:
                 return jsonify({"error": "Для 'SPLIT_CLUSTER' необходим 'cluster_id_to_split'"}), 400
            try:
                 num_splits_int = int(num_splits)
                 if num_splits_int < 2: raise ValueError()
            except (ValueError, TypeError):
                 return jsonify({"error": "'num_splits' должен быть целым числом >= 2"}), 400

            logger.info(f"Пользователь {current_user_id} запросил SPLIT для кластера {cluster_id_to_split} на {num_splits_int} части в сессии {session_id}")
            result = split_cluster(
                session_id=session_id,
                cluster_label_to_split_str=str(cluster_id_to_split),
                num_splits=num_splits_int,
                user_id=user_id_int
            )
            logger.info(f"Пользователь {current_user_id} завершил SPLIT для {session_id}/{cluster_id_to_split}. Результат: {result}")
            return jsonify(result), 200

        else:
             return jsonify({"error": f"Неизвестное действие: {action}"}), 400

    except ValueError as ve:
         db.session.rollback()
         logger.warning(f"Ошибка валидации при {action} для {session_id} пользователем {current_user_id}: {ve}", exc_info=False)
         return jsonify({"error": f"Ошибка операции: {ve}"}), 400
    except RuntimeError as re:
         db.session.rollback()
         logger.error(f"Ошибка выполнения при {action} для {session_id} пользователем {current_user_id}: {re}", exc_info=True)
         return jsonify({"error": f"Внутренняя ошибка сервера: {re}"}), 500
    except SQLAlchemyError as e:
         db.session.rollback()
         logger.error(f"Ошибка БД при {action} {session_id} пользователем {current_user_id}: {e}", exc_info=True)
         return jsonify({"error": "Ошибка базы данных при выполнении операции"}), 500
    except Exception as e:
        db.session.rollback()
        logger.error(f"Неожиданная ошибка при {action} для {session_id} пользователем {current_user_id}: {e}", exc_info=True)
        return jsonify({"error": "Неизвестная внутренняя ошибка сервера"}), 500


@clustering_bp.route('/export/<session_id>/assignments.csv', methods=['GET'])
@jwt_required()
def export_assignments_csv(session_id):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    session = db.session.get(ClusteringSession, session_id)

    if not session or session.user_id != user_id_int:
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404
    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Экспорт недоступен для сессии со статусом {session.status}"}), 400

    try:
        assignments_df = generate_assignments_data(session)
        if assignments_df is None:
             return jsonify({"error": "Не удалось сгенерировать данные о принадлежности (проверьте лог)"}), 500

        output = io.StringIO()
        assignments_df.to_csv(output, index=False, quoting=csv.QUOTE_NONNUMERIC)
        output.seek(0)

        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = f"attachment; filename=session_{session_id}_assignments.csv"
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        logger.info(f"Пользователь {current_user_id} успешно экспортировал assignments CSV для сессии {session_id}")
        return response

    except Exception as e:
        logger.error(f"Ошибка экспорта assignments CSV для сессии {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Внутренняя ошибка сервера при генерации CSV"}), 500

@clustering_bp.route('/export/<session_id>/cluster_summary.json', methods=['GET'])
@jwt_required()
def export_cluster_summary_json(session_id):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    session = db.session.get(ClusteringSession, session_id)

    if not session or session.user_id != user_id_int:
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404
    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Экспорт недоступен для сессии со статусом {session.status}"}), 400

    try:
        summary_data = generate_cluster_summary_json(session)
        response = make_response(jsonify(summary_data))
        response.headers["Content-Disposition"] = f"attachment; filename=session_{session_id}_cluster_summary.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        logger.info(f"Пользователь {current_user_id} успешно экспортировал cluster summary JSON для сессии {session_id}")
        return response

    except Exception as e:
        logger.error(f"Ошибка экспорта cluster summary JSON для сессии {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Внутренняя ошибка сервера при генерации JSON сводки по кластерам"}), 500

@clustering_bp.route('/export/<session_id>/session_summary.json', methods=['GET'])
@jwt_required()
def export_session_summary_json(session_id):
    current_user_id = get_jwt_identity()
    try:
        user_id_int = int(current_user_id)
    except ValueError:
         logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
         return jsonify({"error": "Невалидный идентификатор пользователя"}), 400

    session = db.session.get(ClusteringSession, session_id)

    if not session or session.user_id != user_id_int:
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404
    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Экспорт недоступен для сессии со статусом {session.status}"}), 400

    try:
        summary_data = generate_session_summary_json(session)
        response = make_response(jsonify(summary_data))
        response.headers["Content-Disposition"] = f"attachment; filename=session_{session_id}_session_summary.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        logger.info(f"Пользователь {current_user_id} успешно экспортировал session summary JSON для сессии {session_id}")
        return response

    except Exception as e:
        logger.error(f"Ошибка экспорта session summary JSON для сессии {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Внутренняя ошибка сервера при генерации JSON сводки по сессии"}), 500

def register_clustering_routes(app):
    app.register_blueprint(clustering_bp)