import React from 'react';
import { SessionResultResponse } from '../services/api';
import '../styles/SessionDetailsDisplay.css';

interface SessionDetailsDisplayProps {
    details: SessionResultResponse | null;
}

const PARAM_LABELS: { [key: string]: string } = {
    n_clusters: 'n_clusters',
    eps: 'eps',
    min_samples: 'min_samples'
};


const SessionDetailsDisplay: React.FC<SessionDetailsDisplayProps> = ({ details }) => {
    if (!details) {
        return null;
    }

    return (
        <div className="card session-details-card">
            <h3>Результаты сессии: {details.session_id}</h3>
            <div className="details-grid">
                <span>Алгоритм:</span> <strong>{details.algorithm?.toUpperCase()}</strong>
                <span>Статус:</span> <strong>{details.status}</strong>
                <span>Параметры:</span>
                <strong>
                    {Object.entries(details.params || {})
                        .map(([key, value]) => `${PARAM_LABELS[key] || key}: ${value}`)
                        .join('; ') || 'N/A'
                    }
                </strong>
                {details.original_filename && <><span>Входной файл:</span> <strong>{details.original_filename}</strong></>}
                {details.num_clusters !== null && <><span>Найдено кластеров:</span> <strong>{details.num_clusters}</strong></>}
                {details.clusters && details.clusters.length > 0 && <><span>Средний размер:</span> <strong>{(details.clusters.reduce((sum, c) => sum + c.size, 0) / details.clusters.length).toFixed(0)}</strong></>}
                {details.processing_time_sec !== null && <><span>Время обработки:</span> <strong>{details.processing_time_sec.toFixed(2)} сек.</strong></>}
            </div>
            {details.message && <p className="session-message">Сообщение: {details.message}</p>}
            {details.error && <p className="error-message session-error">Ошибка сессии: {details.error}</p>}
        </div>
    );
};

export default SessionDetailsDisplay;