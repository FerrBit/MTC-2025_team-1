import React from 'react';
import { SessionListItem } from '../services/api';
import '../styles/SessionSelector.css';

interface SessionSelectorProps {
    sessions: SessionListItem[];
    currentSessionId: string | null;
    onSelectSession: (sessionId: string) => void;
    disabled: boolean;
    isLoading: boolean;
    error: string | null;
}

const SessionSelector: React.FC<SessionSelectorProps> = ({
    sessions,
    currentSessionId,
    onSelectSession,
    disabled,
    isLoading,
    error
}) => {
    return (
        <div className="card sessions-card">
            <h3>Сессии кластеризации</h3>
            {isLoading && <p>Загрузка списка сессий...</p>}
            {!isLoading && sessions.length === 0 && !error && (
                <p>Нет доступных сессий. Запустите новую кластеризацию ниже.</p>
            )}
            {error && !isLoading && <p className="error-message" style={{ marginTop: '0.5rem' }}>Ошибка загрузки сессий: {error}</p>}

            {!isLoading && sessions.length > 0 && (
                <div className="sessions-list-container">
                    {sessions.map(session => (
                        <div
                            key={session.session_id}
                            className={`session-item ${currentSessionId === session.session_id ? 'active' : ''} ${disabled ? 'disabled' : ''}`}
                            onClick={() => !disabled && onSelectSession(session.session_id)}
                            title={`Выбрать сессию ${session.session_id}`}
                        >
                            <span className="session-info">
                                ID: {session.session_id}
                                <br />
                                <span className="session-meta">
                                    ({session.algorithm?.toUpperCase()}) |
                                    Файл: {session.original_filename ?? 'N/A'} |
                                    {new Date(session.created_at).toLocaleString()}
                                </span>
                            </span>
                            <span className="session-status-badge">
                                {session.status}
                            </span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default SessionSelector;