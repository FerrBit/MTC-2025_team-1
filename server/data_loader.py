import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def load_embeddings(file_path):
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Столбцы Parquet: {df.columns.tolist()}")
        logger.info(f"Тип индекса Parquet: {type(df.index)}")
        logger.info(f"Имя индекса Parquet: {df.index.name}")
        logger.info(f"Первые 5 значений индекса Parquet: {df.index[:5].tolist()}")

        if 'embedding' not in df.columns:
            embedding_cols = df.select_dtypes(include=np.number).columns
            if len(embedding_cols) > 1:
                embeddings = df[embedding_cols].values.astype(np.float32)
                logger.info(f"Использование {len(embedding_cols)} числовых столбцов в качестве эмбеддингов.")
            else:
                raise ValueError("Столбец 'embedding' или несколько числовых столбцов не найдены в Parquet файле")
        else:
            embeddings = np.array(df['embedding'].tolist(), dtype=np.float32)
            logger.info("Использование столбца 'embedding'.")

        if isinstance(df.index, pd.Index) and df.index.dtype == 'object':
             image_ids = df.index.astype(str).tolist()
             logger.info("Использование индекса DataFrame для идентификаторов изображений.")
        elif 'id' in df.columns:
            image_ids = df['id'].astype(str).tolist()
            logger.info("Использование столбца 'id' для идентификаторов изображений.")
        elif 'image_path' in df.columns:
             image_ids = df['image_path'].astype(str).tolist()
             logger.info("Использование столбца 'image_path' для идентификаторов изображений.")
        else:
             image_ids = [str(i) for i in range(embeddings.shape[0])]
             logger.warning("Не найден подходящий индекс или столбец-идентификатор ('id', 'image_path'). Используется простой индекс диапазона. Контактные листы из ZIP могут работать некорректно.")

        logger.info(f"Загружено {embeddings.shape[0]} эмбеддингов и {len(image_ids)} идентификаторов из {file_path}")
        return embeddings, image_ids

    except FileNotFoundError:
        logger.error(f"Parquet файл не найден: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Ошибка загрузки Parquet файла {file_path}: {e}", exc_info=True)
        raise