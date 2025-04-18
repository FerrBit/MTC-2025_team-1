import React, { useState, useEffect } from 'react';
import '../styles/ConfirmationModal.css';

interface SplitClusterModalProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: (numSplits: number) => void;
    clusterId: string | number | null;
    clusterDisplayName: string;
    isLoading: boolean;
}

const SplitClusterModal: React.FC<SplitClusterModalProps> = ({
    isOpen,
    onClose,
    onConfirm,
    clusterId,
    clusterDisplayName,
    isLoading,
}) => {
    const [numSplits, setNumSplits] = useState<string>('2');
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (isOpen) {
            setNumSplits('2');
            setError(null);
        }
    }, [isOpen, clusterId]);

    if (!isOpen || clusterId === null) {
        return null;
    }

    const handleNumSplitsChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const val = e.target.value;
        if (val === '' || /^[0-9]+$/.test(val)) {
            setNumSplits(val);
            setError(null);
        }
    };

    const handleConfirm = () => {
        const splits = parseInt(numSplits, 10);
        if (isNaN(splits) || splits < 2) {
            setError('Введите целое число не менее 2.');
            return;
        }
        setError(null);
        onConfirm(splits);
    };

    const handleClose = () => {
         if (!isLoading) {
             onClose();
         }
     };

    const isConfirmDisabled = isLoading || !numSplits || parseInt(numSplits, 10) < 2 || isNaN(parseInt(numSplits, 10));


    return (
        <div className="confirmation-modal-overlay" onClick={handleClose}>
            <div className="confirmation-modal-content" onClick={(e) => e.stopPropagation()}>
                <button
                    className="confirmation-modal-close-btn"
                    onClick={handleClose}
                    disabled={isLoading}
                    aria-label="Закрыть"
                >
                    ×
                </button>
                <h2>Разделить кластер</h2>
                <div className="confirmation-modal-message" style={{ marginBottom: '1.5rem' }}>
                    Вы собираетесь разделить кластер <br/>
                    <strong>'{clusterDisplayName}' (ID: {clusterId})</strong>.
                    <br />
                    Укажите, на сколько новых кластеров его разделить (минимум 2).
                </div>

                <div className="form-group" style={{ marginBottom: '1.5rem', textAlign: 'left' }}>
                    <label htmlFor="numSplitsInput" style={{ fontWeight: '500', marginBottom: '0.5rem', display: 'block' }}>
                        Количество новых кластеров:
                    </label>
                    <input
                        type="number"
                        id="numSplitsInput"
                        value={numSplits}
                        onChange={handleNumSplitsChange}
                        min="2"
                        step="1"
                        disabled={isLoading}
                        className={`param-input ${error ? 'input-error' : ''}`}
                        style={{ width: '100px', textAlign: 'center', margin: '0 auto', display: 'block' }}
                        autoFocus
                    />
                    {error && <span className="error-message" style={{ textAlign: 'center', marginTop: '0.5rem' }}>{error}</span>}
                </div>


                <div className="confirmation-modal-actions">
                    <button
                        className={`secondary-btn ${isLoading ? 'disabled' : ''}`}
                        onClick={handleClose}
                        disabled={isLoading}
                    >
                        Отмена
                    </button>
                    <button
                        className={`primary-btn ${isConfirmDisabled ? 'disabled' : ''}`}
                        onClick={handleConfirm}
                        disabled={isConfirmDisabled}
                    >
                        {isLoading ? 'Разделение...' : 'Разделить'}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default SplitClusterModal;