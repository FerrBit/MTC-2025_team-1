import React from 'react';
import { AdjustmentLogEntry } from '../services/api';
import '../styles/AdjustmentHistoryDisplay.css';

interface AdjustmentHistoryDisplayProps {
    adjustments: AdjustmentLogEntry[];
    sessionId: string | null;
}

const formatActionType = (actionType: string): string => {
    switch (actionType) {
        case 'RENAME_CLUSTER': return 'Переименование';
        case 'MERGE_CLUSTERS': return 'Слияние';
        case 'SPLIT_CLUSTER': return 'Разделение';
        case 'REDISTRIBUTE_CLUSTER': return 'Удаление/Перераспределение';
        case 'DELETE_CLUSTER_NO_TARGETS': return 'Удаление (нет целей)';
        case 'DELETE_CLUSTER_NO_POINTS': return 'Удаление (нет точек)';
        default: return actionType;
    }
};

const formatAdjustmentDetails = (log: AdjustmentLogEntry): React.ReactNode => {
    const { action_type, details } = log;

    if (!details || typeof details !== 'object') {
        return <p className="details-fallback">Подробности недоступны.</p>;
    }

    try {
        switch (action_type) {
            case 'RENAME_CLUSTER': {
                const oldName = details.old_name || 'Кластер без имени';
                const newName = details.new_name ? `'${details.new_name}'` : 'имя удалено';
                return (
                    <p>
                        Кластер <strong>'{oldName}' (ID: {details.cluster_label})</strong> переименован в {newName}.
                    </p>
                );
            }
            case 'MERGE_CLUSTERS': {
                const mergedLabels = details.merged_cluster_labels?.join(', ') || 'N/A';
                const mergedDisplay = details.merged_cluster_names?.length > 0
                    ? details.merged_cluster_names.map((name: string, index: number) => `'${name || 'ID ' + details.merged_cluster_labels[index]}'`).join(', ')
                    : `ID: ${mergedLabels}`;

                return (
                    <p>
                        Кластеры {mergedDisplay} слиты в новый кластер <strong>ID: {details.new_cluster_label}</strong> (размер: {details.new_size}).
                    </p>
                );
            }
            case 'SPLIT_CLUSTER': {
                const splitName = details.split_cluster_name || `Кластер ID: ${details.split_cluster_label}`;
                const newClustersInfo = details.new_clusters?.map((nc: any) =>
                    `ID: ${nc.new_cluster_label} (размер: ${nc.new_size})`
                ).join('; ') || 'нет';
                return (
                    <p>
                        Кластер <strong>'{splitName}'</strong> разделен на {details.num_splits_created || 'N/A'} части: {newClustersInfo}.
                        {details.num_splits_created !== details.num_splits_requested && ` (Запрошено ${details.num_splits_requested})`}
                    </p>
                );
            }
            case 'REDISTRIBUTE_CLUSTER': {
                const removedName = details.cluster_name_removed || `Кластер ID: ${details.cluster_label_removed}`;
                const targetsInfo = details.targets?.map((t: any) =>
                    `'${t.target_cluster_name || 'ID ' + t.target_cluster_label}' (${t.count} шт.)`
                ).join(', ') || 'нет';
                return (
                    <p>
                        Кластер <strong>'{removedName}'</strong> удален. {details.points_moved || 0} точек перераспределено в: {targetsInfo}.
                    </p>
                );
            }
            case 'DELETE_CLUSTER_NO_TARGETS': {
                const clusterName = details.cluster_name || `Кластер ID: ${details.cluster_label}`;
                return (
                    <p>
                        Кластер <strong>'{clusterName}'</strong> удален. Целевых кластеров для перераспределения не было.
                    </p>
                );
            }
             case 'DELETE_CLUSTER_NO_POINTS': {
                 const clusterName = details.cluster_name || `Кластер ID: ${details.cluster_label}`;
                 return (
                     <p>
                         Кластер <strong>'{clusterName}'</strong> удален. Точек для перераспределения не найдено.
                     </p>
                 );
             }
            default:
                return (
                    <div className="details-fallback">
                        <p>Неизвестное действие или формат деталей:</p>
                        <pre>{JSON.stringify(details, null, 2)}</pre>
                    </div>
                );
        }
    } catch (error) {
        console.error("Error formatting adjustment details:", error, log);
         return (
             <div className="details-fallback error">
                 <p>Ошибка отображения деталей:</p>
                 <pre>{JSON.stringify(details, null, 2)}</pre>
             </div>
         );
    }
};

const AdjustmentHistoryDisplay: React.FC<AdjustmentHistoryDisplayProps> = ({ adjustments, sessionId }) => {
    if (!sessionId) return null;

    if (!adjustments || adjustments.length === 0) {
        return (
            <div className="card adjustment-history-card">
                <h3>История ручных изменений</h3>
                <p className="no-history-message">История ручных изменений для этой сессии отсутствует.</p>
            </div>
        );
    }

    return (
        <div className="card adjustment-history-card">
            <h3>История ручных изменений</h3>
            <div className="history-list-container">
                <ul className="history-list">
                    {adjustments.map((log, index) => (
                        <li key={`${log.timestamp}-${index}`} className="history-item">
                            <div className="history-item-header">
                                <span className="history-timestamp">
                                    {new Date(log.timestamp).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'medium' })}
                                </span>
                                <span className="history-action">
                                    {formatActionType(log.action_type)}
                                </span>
                            </div>
                            <div className="history-details">
                                {formatAdjustmentDetails(log)}
                            </div>
                        </li>
                    ))}
                </ul>
            </div>
        </div>
    );
};

export default AdjustmentHistoryDisplay;