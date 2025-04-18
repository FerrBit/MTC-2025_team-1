import React from 'react';
import '../styles/ConfirmationModal.css';

interface ConfirmationModalProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: () => void;
    title: string;
    message: React.ReactNode;
    confirmText?: string;
    cancelText?: string;
    confirmButtonClass?: string;
    isLoading?: boolean;
}

const ConfirmationModal: React.FC<ConfirmationModalProps> = ({
    isOpen,
    onClose,
    onConfirm,
    title,
    message,
    confirmText = 'Подтвердить',
    cancelText = 'Отмена',
    confirmButtonClass = 'primary-btn',
    isLoading = false,
}) => {
    if (!isOpen) {
        return null;
    }

    const handleConfirm = () => {
        if (!isLoading) {
            onConfirm();
        }
    };

    const handleClose = () => {
         if (!isLoading) {
             onClose();
         }
     };

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
                <h2>{title}</h2>
                <div className="confirmation-modal-message">{message}</div>
                <div className="confirmation-modal-actions">
                    <button
                        className={`secondary-btn ${isLoading ? 'disabled' : ''}`}
                        onClick={handleClose}
                        disabled={isLoading}
                    >
                        {cancelText}
                    </button>
                    <button
                        className={`${confirmButtonClass} ${isLoading ? 'disabled' : ''}`}
                        onClick={handleConfirm}
                        disabled={isLoading}
                    >
                        {isLoading ? 'Обработка...' : confirmText}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default ConfirmationModal;