import React, { useState } from 'react';
import { toast } from 'react-toastify';
import {
    exportAssignmentsCsv,
    exportClusterSummaryJson,
    exportSessionSummaryJson
} from '../services/api';
import '../styles/ExportControls.css';

type FetchWithAuth = (url: string, options?: RequestInit) => Promise<Response>;

interface ExportControlsProps {
    sessionId: string | null;
    fetchWithAuth: FetchWithAuth;
    disabled: boolean;
    sessionStatus: string | null;
}

const ExportControls: React.FC<ExportControlsProps> = ({
    sessionId,
    fetchWithAuth,
    disabled,
    sessionStatus,
}) => {
    const [isAssignmentsLoading, setIsAssignmentsLoading] = useState(false);
    const [isClusterSummaryLoading, setIsClusterSummaryLoading] = useState(false);
    const [isSessionSummaryLoading, setIsSessionSummaryLoading] = useState(false);

    const handleExport = async (
        exportFunc: (fetch: FetchWithAuth, id: string) => Promise<void>,
        setLoading: React.Dispatch<React.SetStateAction<boolean>>,
        successMessage: string,
        errorMessagePrefix: string
    ) => {
        if (!sessionId) return;
        setLoading(true);
        try {
            await exportFunc(fetchWithAuth, sessionId);
            toast.success(successMessage);
        } catch (err: any) {
            console.error(`${errorMessagePrefix} error:`, err);
            toast.error(`${errorMessagePrefix}: ${err.message || 'Неизвестная ошибка'}`);
        } finally {
            setLoading(false);
        }
    };

    const canExport = sessionId && (sessionStatus === 'SUCCESS' || sessionStatus === 'RECLUSTERED');
    const commonDisabled = disabled || !canExport;

    return (
        <div className="card export-card">
            <h3>Экспорт результатов</h3>
            {!canExport && sessionId && (
                <p className="export-disabled-reason">
                    Экспорт доступен только для завершенных сессий (статус SUCCESS или RECLUSTERED). Текущий статус: {sessionStatus || 'Неизвестен'}.
                </p>
            )}
             {!sessionId && (
                <p className="export-disabled-reason">
                    Выберите сессию для экспорта результатов.
                </p>
            )}
            <div className="export-actions">
                <button
                    className="secondary-btn"
                    onClick={() => handleExport(
                        exportAssignmentsCsv,
                        setIsAssignmentsLoading,
                        'CSV с привязками изображений к кластерам загружается...',
                        'Ошибка экспорта CSV привязок'
                    )}
                    disabled={commonDisabled || isAssignmentsLoading}
                    title={canExport ? "Экспорт CSV: image_id, cluster_label, cluster_name" : "Экспорт недоступен"}
                >
                    {isAssignmentsLoading ? 'Загрузка CSV...' : 'Экспорт привязок (CSV)'}
                </button>
                <button
                    className="secondary-btn"
                    onClick={() => handleExport(
                        exportClusterSummaryJson,
                        setIsClusterSummaryLoading,
                        'JSON со сводкой по кластерам загружается...',
                        'Ошибка экспорта JSON кластеров'
                    )}
                    disabled={commonDisabled || isClusterSummaryLoading}
                     title={canExport ? "Экспорт JSON: сводка по активным кластерам" : "Экспорт недоступен"}
               >
                    {isClusterSummaryLoading ? 'Загрузка JSON...' : 'Сводка по кластерам (JSON)'}
                </button>
                <button
                    className="secondary-btn"
                    onClick={() => handleExport(
                        exportSessionSummaryJson,
                        setIsSessionSummaryLoading,
                        'JSON со сводкой по сессии загружается...',
                        'Ошибка экспорта JSON сессии'
                    )}
                    disabled={commonDisabled || isSessionSummaryLoading}
                    title={canExport ? "Экспорт JSON: сводка по параметрам и результатам сессии" : "Экспорт недоступен"}
                >
                    {isSessionSummaryLoading ? 'Загрузка JSON...' : 'Сводка по сессии (JSON)'}
                </button>
            </div>
        </div>
    );
};

export default ExportControls;