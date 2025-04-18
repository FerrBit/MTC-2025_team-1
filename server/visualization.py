import io
import json
import logging
import os
import time
import zipfile
import config
import numpy as np
from flask import current_app
from PIL import Image, ImageDraw, ImageFont, UnidentifiedImageError
from sklearn.decomposition import PCA
from sklearn.metrics import euclidean_distances
from sqlalchemy.orm.attributes import flag_modified
from models import ClusterMetadata, db

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False

logger = logging.getLogger(__name__)

def find_nearest_images_to_centroids(embeddings, labels, centroids, image_ids, n_images):
    nearest_neighbors = {}
    if centroids.shape[0] == 0 or not image_ids:
        logger.warning("Центроиды или идентификаторы изображений не предоставлены для поиска ближайших соседей.")
        return nearest_neighbors
    if embeddings.shape[0] != len(image_ids):
         logger.error(f"Несоответствие между вложениями ({embeddings.shape[0]}) и image_ids ({len(image_ids)}) при поиске ближайших изображений.")
         return nearest_neighbors

    num_clusters = centroids.shape[0]

    faiss_index = None
    if FAISS_AVAILABLE and embeddings.shape[0] > 1000:
        try:
            d = embeddings.shape[1]
            faiss_index = faiss.IndexFlatL2(d)
            faiss_index.add(embeddings.astype(np.float32))
            logger.info("Использование Faiss для поиска ближайших соседей.")
        except Exception as faiss_e:
            logger.error(f"Ошибка создания Faiss индекса для поиска соседей: {faiss_e}")
            faiss_index = None

    for i in range(num_clusters):
        current_label = i
        cluster_mask = (labels == current_label)
        cluster_indices = np.where(cluster_mask)[0]

        if len(cluster_indices) == 0:
            continue

        centroid = centroids[i]
        k_search = min(n_images, len(cluster_indices))
        if k_search == 0: continue

        neighbors_for_cluster = []
        if faiss_index:
            try:
                search_k_faiss = min(k_search * 5 + 10, embeddings.shape[0])
                distances_faiss, indices_faiss = faiss_index.search(np.array([centroid], dtype=np.float32), k=search_k_faiss)

                filtered_neighbors = []
                for dist, global_idx in zip(distances_faiss[0], indices_faiss[0]):
                    if labels[global_idx] == current_label:
                        img_id = image_ids[global_idx]
                        filtered_neighbors.append((img_id, float(np.sqrt(dist))))
                    if len(filtered_neighbors) == k_search:
                        break

                filtered_neighbors.sort(key=lambda x: x[1])
                neighbors_for_cluster = filtered_neighbors

                if len(neighbors_for_cluster) < k_search:
                     logger.warning(f"Faiss нашел только {len(neighbors_for_cluster)} соседей в кластере {current_label} (запрошено {k_search}).")

            except Exception as faiss_search_e:
                logger.warning(f"Ошибка поиска Faiss для кластера {current_label}: {faiss_search_e}. Переключение на sklearn.")
                faiss_index = None

        if not faiss_index or not neighbors_for_cluster:
            cluster_embeddings = embeddings[cluster_indices]
            dist_to_centroid = euclidean_distances(cluster_embeddings, np.array([centroid])).flatten()
            sorted_indices_local = np.argsort(dist_to_centroid)[:k_search]
            neighbors_for_cluster = []
            for local_idx in sorted_indices_local:
                global_idx = cluster_indices[local_idx]
                img_id = image_ids[global_idx]
                dist = dist_to_centroid[local_idx]
                neighbors_for_cluster.append((img_id, float(dist)))

        nearest_neighbors[current_label] = neighbors_for_cluster

    return nearest_neighbors


def _try_open_image_from_nested_zip(outer_zip_ref, path_to_nested_zip, path_within_nested_zip, thumb_size):
    try:
        nested_zip_bytes = outer_zip_ref.read(path_to_nested_zip)
        with zipfile.ZipFile(io.BytesIO(nested_zip_bytes), 'r') as inner_zip:
            with inner_zip.open(path_within_nested_zip) as image_file_handle:
                image_data = io.BytesIO(image_file_handle.read())
                img = Image.open(image_data)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                resample_method = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
                img.thumbnail(thumb_size, resample_method)
                return img, None
    except KeyError as e:
        if str(e).strip("'") == path_to_nested_zip:
             logger.warning(f"Вложенный zip не найден во внешнем архиве: '{path_to_nested_zip}'")
             return None, "Вложенный Zip НН"
        else:
             logger.warning(f"Изображение не найдено во вложенном zip '{path_to_nested_zip}': '{path_within_nested_zip}' (KeyError: {e})")
             return None, "Изобр. НН"
    except zipfile.BadZipFile:
        logger.warning(f"Неверный или поврежденный вложенный zip-файл: '{path_to_nested_zip}'")
        return None, "Плохой Влож. Zip"
    except UnidentifiedImageError:
        logger.warning(f"Файл во вложенном zip не является допустимым изображением: '{path_within_nested_zip}' в '{path_to_nested_zip}'")
        return None, "Не Изображение"
    except Exception as e:
        logger.error(f"Ошибка обработки изображения '{path_within_nested_zip}' из вложенного zip '{path_to_nested_zip}': {e}", exc_info=False)
        return None, "Ошибка чтения"


def create_contact_sheet(archive_path, internal_image_paths, output_path, grid_size, thumb_size, format='JPEG'):
    if not internal_image_paths:
        logger.warning("Внутренние пути к изображениям не предоставлены для создания контактного листа.")
        return False
    if not archive_path or not os.path.exists(archive_path):
        logger.warning(f"Путь к архиву не указан или не существует: {archive_path}. Невозможно создать контактный лист.")
        return False
    if not zipfile.is_zipfile(archive_path):
        logger.error(f"Файл не является действительным ZIP-архивом: {archive_path}")
        return False

    cols, rows = grid_size
    thumb_w, thumb_h = thumb_size
    gap = 5
    total_width = cols * thumb_w + (cols + 1) * gap
    total_height = rows * thumb_h + (rows + 1) * gap
    contact_sheet = Image.new('RGB', (total_width, total_height), color='white')
    draw = ImageDraw.Draw(contact_sheet)

    try:
        font = ImageFont.truetype("arial.ttf", 10)
    except IOError:
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 10)
        except IOError:
            logger.warning("Шрифты truetype по умолчанию не найдены, используется растровый шрифт Pillow по умолчанию.")
            font = ImageFont.load_default()

    current_col, current_row = 0, 0
    files_drawn = 0

    outer_zip_basename = os.path.basename(archive_path)
    potential_top_folder = None
    if '_' in outer_zip_basename:
        potential_top_folder = outer_zip_basename.split('_', 1)[1].rsplit('.', 1)[0]
        logger.info(f"Определена возможная папка верхнего уровня в zip: '{potential_top_folder}' из '{outer_zip_basename}'")

    try:
        with zipfile.ZipFile(archive_path, 'r') as main_zip_ref:
            logger.debug(f"Открыт основной архив: {archive_path} для контактного листа {os.path.basename(output_path)}")

            for parquet_image_path in internal_image_paths:
                if files_drawn >= cols * rows:
                    logger.info(f"Достигнут лимит сетки контактного листа ({cols*rows}) для {os.path.basename(output_path)}.")
                    break

                img = None
                error_message = None
                found_image = False

                normalized_parquet_path = parquet_image_path.replace('\\', '/')
                path_parts = normalized_parquet_path.split('/', 1)

                if len(path_parts) == 2:
                    nested_zip_key = path_parts[0]
                    path_within_nested_zip = normalized_parquet_path

                    nested_zip_filename = f"{nested_zip_key}.zip"
                    path_to_nested_zip_in_outer = nested_zip_filename
                    if potential_top_folder:
                        path_to_nested_zip_in_outer = f"{potential_top_folder}/{nested_zip_filename}"

                    img, error_message = _try_open_image_from_nested_zip(
                        main_zip_ref,
                        path_to_nested_zip_in_outer,
                        path_within_nested_zip,
                        thumb_size
                    )
                    if img:
                        found_image = True

                else:
                    logger.warning(f"Путь '{normalized_parquet_path}' не похож на формат 'вложенный_zip/путь'. Попытка прямого доступа.")
                    try:
                        with main_zip_ref.open(normalized_parquet_path) as image_file_handle:
                             image_data = io.BytesIO(image_file_handle.read())
                             img = Image.open(image_data)
                             if img.mode != 'RGB': img = img.convert('RGB')
                             resample_method = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
                             img.thumbnail(thumb_size, resample_method)
                             found_image = True
                             error_message = None
                             logger.info(f"Успешно открыто изображение напрямую из внешнего zip: {normalized_parquet_path}")
                    except KeyError:
                        error_message = "Изобр. НН Напрямую"
                        logger.warning(f"Изображение не найдено напрямую во внешнем zip: {normalized_parquet_path}")
                    except Exception as direct_e:
                         error_message = "Ошибка чтения Напрямую"
                         logger.error(f"Ошибка чтения изображения напрямую {normalized_parquet_path}: {direct_e}")

                x_pos = gap + current_col * (thumb_w + gap)
                y_pos = gap + current_row * (thumb_h + gap)

                if found_image and img:
                    try:
                        contact_sheet.paste(img, (x_pos, y_pos))
                    except Exception as paste_e:
                         logger.error(f"Ошибка вставки изображения для {normalized_parquet_path}: {paste_e}")
                         error_message = "Ошибка Вставки"
                         draw.rectangle([x_pos, y_pos, x_pos + thumb_w, y_pos + thumb_h], fill="lightcoral", outline="red")
                         draw.text((x_pos + 5, y_pos + 5), error_message or "Ош.Вставки", fill="red", font=font)
                else:
                    draw.rectangle([x_pos, y_pos, x_pos + thumb_w, y_pos + thumb_h], fill="lightgray", outline="darkred")
                    display_path = normalized_parquet_path
                    if len(display_path) > 40: display_path = "..." + display_path[-37:]
                    draw.text((x_pos + 5, y_pos + 5), error_message or "Не Найдено", fill="darkred", font=font)

                current_col += 1
                if current_col >= cols:
                    current_col = 0
                    current_row += 1
                files_drawn += 1

    except zipfile.BadZipFile:
        logger.error(f"Не удалось открыть ОСНОВНОЙ ZIP-архив (BadZipFile): {archive_path}")
        return False
    except Exception as e:
         logger.error(f"Неожиданная ошибка при создании контактного листа из ОСНОВНОГО ZIP {archive_path}: {e}", exc_info=True)
         return False

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        contact_sheet.save(output_path, format=format.upper(), quality=85, optimize=True)
        logger.info(f"Контактный лист сохранен: {output_path} (обработано {len(internal_image_paths)} путей, нарисовано {files_drawn} ячеек)")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения контактного листа {output_path}: {e}", exc_info=True)
        return False


def calculate_and_save_centroids_2d(session_id):
    logger.info(f"Расчет 2D центроидов для сессии {session_id}")
    try:
        clusters = ClusterMetadata.query.filter_by(session_id=session_id, is_deleted=False).all()
        if not clusters:
            logger.info(f"Активные кластеры для сессии {session_id} не найдены для расчета 2D центроидов.")
            ClusterMetadata.query.filter_by(session_id=session_id)\
                           .update({ClusterMetadata.centroid_2d_json: None})
            db.session.commit()
            return

        original_centroids = []
        cluster_map = {}

        for cluster_meta in clusters:
            centroid_vec = cluster_meta.get_centroid()
            if centroid_vec is not None:
                original_centroids.append(centroid_vec)
                cluster_map[len(original_centroids) - 1] = cluster_meta.id
            else:
                 logger.warning(f"Вектор центроида равен None для активного кластера {cluster_meta.id} в сессии {session_id}")

        if len(original_centroids) < 2:
            logger.warning(f"Требуется как минимум 2 действительных центроида для PCA, найдено {len(original_centroids)} для сессии {session_id}. Очистка 2D центроидов.")
            for cluster_meta in clusters:
                if cluster_meta.centroid_2d_json is not None:
                     cluster_meta.set_centroid_2d(None)
                     flag_modified(cluster_meta, "centroid_2d_json")
            db.session.commit()
            return

        original_centroids_np = np.array(original_centroids)
        pca = PCA(n_components=2, svd_solver='full', random_state=42)
        try:
            centroids_2d = pca.fit_transform(original_centroids_np)
        except ValueError as pca_err:
             logger.error(f"PCA не удался для центроидов сессии {session_id}: {pca_err}", exc_info=True)
             for cluster_meta in clusters:
                 if cluster_meta.centroid_2d_json is not None:
                     cluster_meta.set_centroid_2d(None)
                     flag_modified(cluster_meta, "centroid_2d_json")
             db.session.commit()
             return

        updated_ids = set()
        for idx_in_pca, cluster_db_id in cluster_map.items():
            cluster_to_update = db.session.get(ClusterMetadata, cluster_db_id)
            if cluster_to_update and not cluster_to_update.is_deleted:
                coords_2d = centroids_2d[idx_in_pca].tolist()
                cluster_to_update.set_centroid_2d(coords_2d)
                flag_modified(cluster_to_update, "centroid_2d_json")
                updated_ids.add(cluster_db_id)

        for cluster_meta in clusters:
            if cluster_meta.id not in updated_ids and cluster_meta.centroid_2d_json is not None:
                 cluster_meta.set_centroid_2d(None)
                 flag_modified(cluster_meta, "centroid_2d_json")
                 logger.warning(f"Очищен устаревший 2D центроид для кластера {cluster_meta.id}, так как он не был включен в PCA.")

        db.session.commit()
        logger.info(f"Успешно рассчитаны и сохранены 2D центроиды для {len(updated_ids)} активных кластеров в сессии {session_id}")

    except Exception as e:
        db.session.rollback()
        logger.error(f"Ошибка при расчете/сохранении 2D центроидов для сессии {session_id}: {e}", exc_info=True)


def generate_and_save_scatter_data(session_id, embeddings, labels):
    logger.info(f"Генерация и сохранение данных Scatter Plot для сессии {session_id}")
    scatter_plot_data = None
    pca_time_sec = None
    scatter_cache_file_path = None

    try:
        if embeddings is None or labels is None or embeddings.shape[0] == 0 or labels.shape[0] == 0:
            logger.warning(f"Невозможно сгенерировать диаграмму рассеяния для сессии {session_id}: Вложения или метки отсутствуют или пусты.")
            return None, None
        if embeddings.shape[0] != labels.shape[0]:
            logger.warning(f"Генерация диаграммы рассеяния не удалась для сессии {session_id}: Несоответствие между вложениями ({embeddings.shape[0]}) и метками ({labels.shape[0]})")
            return None, None

        num_points = embeddings.shape[0]
        max_points = config.Config.MAX_SCATTER_PLOT_POINTS
        indices = np.arange(num_points)

        if num_points > max_points:
            logger.warning(f"Сессия {session_id}: Выборка {num_points} точек до {max_points} для диаграммы рассеяния")
            indices = np.random.choice(indices, max_points, replace=False)
            embeddings_sampled = embeddings[indices]
            labels_sampled = labels[indices]
        else:
            embeddings_sampled = embeddings
            labels_sampled = labels

        if embeddings_sampled.shape[0] < 2:
            logger.warning(f"Недостаточно точек ({embeddings_sampled.shape[0]}) после выборки для PCA в сессии {session_id}")
            return None, None

        pca_start_time = time.time()
        pca = PCA(n_components=2, svd_solver='full', random_state=42)
        try:
            embeddings_2d = pca.fit_transform(embeddings_sampled)
            pca_end_time = time.time()
            pca_time_sec = pca_end_time - pca_start_time

            formatted_scatter_data = []
            for i in range(embeddings_2d.shape[0]):
                formatted_scatter_data.append({
                    'x': float(embeddings_2d[i, 0]),
                    'y': float(embeddings_2d[i, 1]),
                    'cluster': str(labels_sampled[i])
                })
            scatter_plot_data = formatted_scatter_data
            logger.info(f"Успешно сгенерированы данные Scatter Plot для сессии {session_id} ({len(scatter_plot_data)} точек). PCA занял {pca_time_sec:.2f} сек.")

            scatter_filename = f"scatter_{session_id}.json"
            scatter_folder = current_app.config['SCATTER_DATA_FOLDER']
            os.makedirs(scatter_folder, exist_ok=True)
            scatter_cache_file_path = os.path.join(scatter_folder, scatter_filename)
            cache_content = {
                "scatter_plot_data": scatter_plot_data,
                "pca_time": pca_time_sec,
                "points_sampled": embeddings_sampled.shape[0],
                "total_points": num_points
            }
            try:
                with open(scatter_cache_file_path, 'w') as f:
                    json.dump(cache_content, f)
                logger.info(f"Данные Scatter Plot сохранены в кэш для сессии {session_id}: {scatter_cache_file_path}")
            except (OSError, IOError) as write_err:
                logger.error(f"Не удалось записать файл кэша диаграммы рассеяния {scatter_cache_file_path} для сессии {session_id}: {write_err}", exc_info=True)
                scatter_cache_file_path = None

        except ValueError as pca_err:
            logger.error(f"PCA не удался для диаграммы рассеяния в сессии {session_id}: {pca_err}", exc_info=True)
            scatter_cache_file_path = None
            pca_time_sec = None

    except Exception as e:
        logger.error(f"Неожиданная ошибка во время генерации/сохранения диаграммы рассеяния для сессии {session_id}: {e}", exc_info=True)
        scatter_cache_file_path = None
        pca_time_sec = None

    return scatter_cache_file_path, pca_time_sec


def regenerate_contact_sheet_for_cluster(cluster_meta: ClusterMetadata, session, all_embeddings, all_image_ids, final_labels):
    app_config = current_app.config
    cluster_label_str = cluster_meta.cluster_label
    cluster_label_int = -999
    try:
        cluster_label_int = int(cluster_label_str)
    except (ValueError, TypeError):
        logger.error(f"Не удалось преобразовать метку кластера '{cluster_label_str}' в int для регенерации КС сессии {session.id}")
        return None

    archive_path = session.image_archive_path
    if not archive_path or not os.path.exists(archive_path):
        logger.info(f"Регенерация КС для {session.id}/{cluster_label_str} пропущена: архив не найден ({archive_path})")
        return None
    if not all_image_ids:
        logger.info(f"Регенерация КС для {session.id}/{cluster_label_str} пропущена: отсутствуют ID изображений.")
        return None
    if all_embeddings is None or all_embeddings.shape[0] != len(all_image_ids):
        logger.error(f"Регенерация КС для {session.id}/{cluster_label_str}: несоответствие вложений и ID.")
        return None
    if final_labels is None or final_labels.shape[0] != len(all_image_ids):
        logger.error(f"Регенерация КС для {session.id}/{cluster_label_str}: несоответствие меток и ID.")
        return None

    cluster_point_indices = np.where(final_labels == cluster_label_int)[0]
    num_points_in_cluster = len(cluster_point_indices)

    if num_points_in_cluster == 0:
        logger.info(f"Регенерация КС для {session.id}/{cluster_label_str} пропущена: нет точек в кластере.")
        return None

    cluster_embeddings = all_embeddings[cluster_point_indices]
    centroid_vector = cluster_meta.get_centroid()
    if centroid_vector is None:
        logger.warning(f"Регенерация КС для {session.id}/{cluster_label_str}: центроид не найден в метаданных.")
        if centroid_vector is None:
            return None

    images_per_cluster = app_config.get('CONTACT_SHEET_IMAGES_PER_CLUSTER', 9)
    k_search = min(images_per_cluster, num_points_in_cluster)

    try:
        dist_to_centroid = euclidean_distances(cluster_embeddings, np.array([centroid_vector])).flatten()
        sorted_indices_local = np.argsort(dist_to_centroid)[:k_search]

        nearest_image_ids_for_sheet = []
        for local_idx in sorted_indices_local:
            global_idx = cluster_point_indices[local_idx]
            nearest_image_ids_for_sheet.append(all_image_ids[global_idx])

    except Exception as e:
        logger.error(f"Ошибка поиска ближайших соседей для регенерации КС {session.id}/{cluster_label_str}: {e}", exc_info=True)
        return None

    if not nearest_image_ids_for_sheet:
        logger.warning(f"Не найдено ближайших соседей для регенерации КС {session.id}/{cluster_label_str}.")
        return None

    contact_sheet_dir_base = app_config['CONTACT_SHEET_FOLDER']
    grid_size = app_config.get('CONTACT_SHEET_GRID_SIZE', (3, 3))
    thumb_size = app_config.get('CONTACT_SHEET_THUMBNAIL_SIZE', (100, 100))
    output_format = app_config.get('CONTACT_SHEET_OUTPUT_FORMAT', 'JPEG')

    sheet_filename = f"cs_{session.id}_{cluster_label_str}.{output_format.lower()}"
    session_sheet_dir = os.path.join(contact_sheet_dir_base, str(session.id))
    sheet_full_path = os.path.join(session_sheet_dir, sheet_filename)

    logger.info(f"Попытка регенерации контактного листа для {session.id}/{cluster_label_str} -> {sheet_full_path}")

    if cluster_meta.contact_sheet_path and os.path.exists(cluster_meta.contact_sheet_path):
         if cluster_meta.contact_sheet_path != sheet_full_path:
            try:
                os.remove(cluster_meta.contact_sheet_path)
                logger.info(f"Удален старый файл КС: {cluster_meta.contact_sheet_path}")
            except OSError as e:
                logger.error(f"Не удалось удалить старый файл КС {cluster_meta.contact_sheet_path}: {e}")

    success = create_contact_sheet(
        archive_path=archive_path,
        internal_image_paths=nearest_image_ids_for_sheet,
        output_path=sheet_full_path,
        grid_size=grid_size,
        thumb_size=thumb_size,
        format=output_format
    )

    if success:
        cluster_meta.contact_sheet_path = sheet_full_path
        flag_modified(cluster_meta, "contact_sheet_path")
        logger.info(f"Контактный лист успешно регенерирован для {session.id}/{cluster_label_str}")
        return sheet_full_path
    else:
        logger.error(f"Не удалось регенерировать контактный лист для {session.id}/{cluster_label_str}")
        if cluster_meta.contact_sheet_path == sheet_full_path:
            cluster_meta.contact_sheet_path = None
            flag_modified(cluster_meta, "contact_sheet_path")
        return None