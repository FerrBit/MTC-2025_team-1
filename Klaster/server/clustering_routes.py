import os
import uuid
import json
import logging
from flask import Blueprint, request, jsonify, current_app, send_from_directory
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.utils import secure_filename
from models import db, ClusteringSession, ClusterMetadata, User, ManualAdjustmentLog
from clustering_logic import run_clustering_pipeline, run_reclustering_pipeline, log_manual_adjustment
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

clustering_bp = Blueprint('clustering_api', __name__, url_prefix='/api/clustering')

ALLOWED_EXTENSIONS = {'parquet'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@clustering_bp.route('/start', methods=['POST'])
@jwt_required()
def start_clustering():
    current_user_id = get_jwt_identity()

    if 'embeddingFile' not in request.files:
        return jsonify({"error": "Файл эмбеддингов ('embeddingFile') не найден"}), 400

    file = request.files['embeddingFile']
    algorithm = request.form.get('algorithm')
    params_str = request.form.get('params', '{}')

    if not algorithm or algorithm not in ['kmeans', 'dbscan']:
         return jsonify({"error": "Алгоритм не указан или не поддерживается (ожидается 'kmeans' или 'dbscan')"}), 400

    try:
        params = json.loads(params_str)
        if not isinstance(params, dict): raise ValueError("Params not a dict")
    except (json.JSONDecodeError, ValueError):
        logger.warning(f"Invalid params received for user {current_user_id}: {params_str}")
        return jsonify({"error": "Некорректный формат JSON для параметров ('params')"}), 400

    if file.filename == '':
        return jsonify({"error": "Имя файла не должно быть пустым"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Недопустимый тип файла. Разрешен только .parquet"}), 400

    file_path = None
    try:
        filename = secure_filename(f"{uuid.uuid4()}_{file.filename}")
        upload_folder = current_app.config['UPLOAD_FOLDER']
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        logger.info(f"User {current_user_id} uploaded file: {filename}")
    except Exception as e:
        logger.error(f"Failed to save uploaded file for user {current_user_id}: {e}", exc_info=True)
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except OSError: pass
        return jsonify({"error": "Не удалось сохранить файл на сервере"}), 500

    try:
        logger.info(f"User {current_user_id} starting SYNC clustering...")
        session_id = run_clustering_pipeline(
            user_id=current_user_id, file_path=file_path,
            algorithm=algorithm, params=params
        )
        logger.info(f"User {current_user_id} finished SYNC clustering. Session ID: {session_id}")
        return jsonify({"session_id": session_id}), 201

    except (ValueError, SQLAlchemyError) as ve:
        logger.error(f"Validation or DB error during clustering start for user {current_user_id}: {ve}", exc_info=False)
        if file_path and os.path.exists(file_path):
             try: os.remove(file_path)
             except OSError as rem_e: logger.error(f"Error removing file {file_path} after error: {rem_e}")
        return jsonify({"error": f"Ошибка входных данных или БД: {ve}"}), 400
    except Exception as e:
        logger.error(f"SYNC clustering failed unexpectedly for user {current_user_id}: {e}", exc_info=True)
        if file_path and os.path.exists(file_path):
             try: os.remove(file_path)
             except OSError as rem_e: logger.error(f"Error removing file {file_path} after error: {rem_e}")
        return jsonify({"error": "Внутренняя ошибка сервера при кластеризации"}), 500

@clustering_bp.route('/sessions', methods=['GET'])
@jwt_required()
def list_clustering_sessions():
    current_user_id = get_jwt_identity()
    sessions = ClusteringSession.query.filter_by(user_id=current_user_id)\
                                     .order_by(ClusteringSession.created_at.desc())\
                                     .all()
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
            "input_filename": os.path.basename(session.input_file_path) if session.input_file_path else "N/A"
        })
    return jsonify(output), 200


@clustering_bp.route('/results/<session_id>', methods=['GET'])
@jwt_required()
def get_clustering_results(session_id):
    current_user_id = get_jwt_identity()
    session = db.session.get(ClusteringSession, session_id)

    if not session or session.user_id != int(current_user_id):
        return jsonify({"error": "Сессия кластеризации не найдена или не принадлежит вам"}), 404

    base_response = {
        "session_id": session.id,
        "status": session.status,
        "algorithm": session.algorithm,
        "params": session.get_params(),
        "num_clusters": session.num_clusters,
        "processing_time_sec": session.processing_time_sec,
        "message": session.result_message,
        "input_filename": os.path.basename(session.input_file_path) if session.input_file_path else None,
        "clusters": []
    }

    if session.status != 'SUCCESS' and session.status != 'RECLUSTERED':
        if session.status in ['FAILURE', 'RECLUSTERING_FAILED']:
             base_response["error"] = f"Статус сессии: {session.status}. {session.result_message or ''}"
        else:
             base_response["error"] = f"Статус сессии: {session.status}. Результаты еще не готовы."
        return jsonify(base_response), 200

    clusters_data = []
    clusters = session.clusters.filter_by(is_deleted=False).order_by(ClusterMetadata.cluster_label).all()
    for cluster_meta in clusters:
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
            "metrics": cluster_meta.get_metrics()
        })

    base_response["clusters"] = clusters_data
    base_response["num_clusters"] = len(clusters_data)

    return jsonify(base_response), 200

@clustering_bp.route('/contact_sheet/<session_id>/<filename>', methods=['GET'])
@jwt_required()
def get_contact_sheet_image(session_id, filename):
    current_user_id = get_jwt_identity()
    session = db.session.get(ClusteringSession, session_id)
    if not session or session.user_id != int(current_user_id):
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404

    if not filename.startswith(f"cs_{session_id}_") or not filename.lower().endswith(f".{current_app.config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG').lower()}"):
        logger.warning(f"Attempt to access invalid contact sheet filename: {filename} for session {session_id}")
        return jsonify({"error": "Некорректное имя файла отпечатка"}), 400

    contact_sheet_session_dir = os.path.join(current_app.config['CONTACT_SHEET_FOLDER'], session_id)
    safe_filename = secure_filename(filename)

    try:
        mimetype = f"image/{current_app.config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG').lower()}"
        return send_from_directory(contact_sheet_session_dir, safe_filename, mimetype=mimetype)
    except FileNotFoundError:
         logger.warning(f"Contact sheet file not found: session={session_id}, filename={safe_filename}")
         return jsonify({"error": "Файл контактного отпечатка не найден"}), 404

@clustering_bp.route('/results/<session_id>/cluster/<cluster_label>', methods=['DELETE'])
@jwt_required()
def delete_cluster_and_recluster(session_id, cluster_label):
    current_user_id = get_jwt_identity()
    session = db.session.get(ClusteringSession, session_id)
    if not session or session.user_id != int(current_user_id):
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404

    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Невозможно удалить кластер из сессии со статусом {session.status}"}), 400

    cluster_to_delete = session.clusters.filter_by(cluster_label=cluster_label, is_deleted=False).first()
    if not cluster_to_delete:
         return jsonify({"error": "Кластер для удаления не найден или уже удален"}), 404

    cluster_to_delete.is_deleted = True
    sheet_path = cluster_to_delete.contact_sheet_path
    cluster_to_delete.contact_sheet_path = None

    log_manual_adjustment(session.id, current_user_id, "DELETE_CLUSTER", {"cluster_label": cluster_label})

    try:
        db.session.commit()
        logger.info(f"User {current_user_id} marked cluster {cluster_label} in session {session_id} for deletion.")
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"DB error marking cluster for deletion {session_id}/{cluster_label}: {e}", exc_info=True)
        return jsonify({"error": "Ошибка БД при удалении кластера"}), 500

    if sheet_path and os.path.exists(sheet_path):
        try:
            os.remove(sheet_path)
            logger.info(f"Deleted contact sheet file: {sheet_path}")
        except OSError as e:
             logger.error(f"Error deleting contact sheet file {sheet_path}: {e}")

    try:
        logger.info(f"User {current_user_id} starting SYNC re-clustering for session {session_id}...")
        new_session_id = run_reclustering_pipeline(
            original_session_id=session_id, user_id=current_user_id
        )
        logger.info(f"User {current_user_id} finished SYNC re-clustering. New session ID: {new_session_id}")
        return jsonify({
            "message": "Кластер удален, рекластеризация завершена.",
            "new_session_id": new_session_id
            }), 200

    except (ValueError, SQLAlchemyError) as ve:
         logger.error(f"Validation or DB error during re-clustering start for session {session_id}: {ve}", exc_info=False)
         return jsonify({"error": f"Ошибка входных данных или БД при рекластеризации: {ve}"}), 400
    except Exception as e:
        logger.error(f"SYNC re-clustering failed unexpectedly for session {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Внутренняя ошибка сервера при рекластеризации"}), 500


@clustering_bp.route('/results/<session_id>/adjust', methods=['POST'])
@jwt_required()
def adjust_clusters(session_id):
    current_user_id = get_jwt_identity()
    session = db.session.get(ClusteringSession, session_id)
    if not session or session.user_id != int(current_user_id):
        return jsonify({"error": "Сессия не найдена или доступ запрещен"}), 404

    if session.status not in ['SUCCESS', 'RECLUSTERED']:
        return jsonify({"error": f"Невозможно редактировать сессию со статусом {session.status}"}), 400

    data = request.get_json()
    action = data.get('action')
    cluster_id = data.get('cluster_id')
    new_name = data.get('new_name')

    if not action or action != 'RENAME' or not cluster_id or new_name is None:
        return jsonify({"error": "Некорректные параметры для 'RENAME' (нужны action='RENAME', cluster_id, new_name)"}), 400

    cluster_to_rename = session.clusters.filter_by(cluster_label=str(cluster_id), is_deleted=False).first()
    if not cluster_to_rename:
         return jsonify({"error": f"Кластер с ID {cluster_id} не найден или удален"}), 404

    old_name = cluster_to_rename.name
    cluster_to_rename.name = new_name if new_name else None

    log_manual_adjustment(session.id, current_user_id, "RENAME", {
        "cluster_label": cluster_id,
        "old_name": old_name,
        "new_name": cluster_to_rename.name
    })

    try:
        db.session.commit()
        logger.info(f"User {current_user_id} renamed cluster {cluster_id} in session {session_id} to '{cluster_to_rename.name}'")
        return jsonify({
            "message": "Кластер переименован",
            "cluster": {
                "id": cluster_to_rename.cluster_label,
                "name": cluster_to_rename.name,
            }
        }), 200
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"DB error renaming cluster {session_id}/{cluster_id}: {e}", exc_info=True)
        return jsonify({"error": "Ошибка БД при переименовании кластера"}), 500


def register_clustering_routes(app):
    app.register_blueprint(clustering_bp)