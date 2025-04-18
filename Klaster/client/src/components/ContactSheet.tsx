import React, { useState, useEffect } from 'react';
import '../styles/ContactSheet.css';

interface ContactSheetProps {
  clusterId: string | number;
  imageUrl: string | null | undefined;
  clusterSize: number;
  initialName: string | null;
  onRedistribute: (clusterId: string | number) => void;
  onRename: (clusterId: string | number, newName: string) => Promise<boolean>;
  isProcessing: boolean;
  isSelected: boolean;
  onToggleSelection: (clusterId: string | number) => void;
  onInitiateSplit: (clusterId: string | number) => void;
}

const ImagePlaceholder: React.FC = () => (
    <div className="contact-sheet-image placeholder-image-container">
        <p>Нет<br/>отпечатка</p>
    </div>
);

const ContactSheet: React.FC<ContactSheetProps> = ({
  clusterId,
  imageUrl,
  clusterSize,
  initialName,
  onRedistribute,
  onRename,
  isProcessing,
  isSelected,
  onToggleSelection,
  onInitiateSplit,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [currentName, setCurrentName] = useState(initialName || '');
  const [isSavingName, setIsSavingName] = useState(false);

  useEffect(() => {
    setCurrentName(initialName || '');
    if (!isSavingName) {
        setIsEditing(false);
    }
  }, [initialName, isSavingName]);

  const handleRenameClick = () => {
    setIsEditing(true);
  };

  const handleCancelClick = () => {
    setIsEditing(false);
    setCurrentName(initialName || '');
  };

  const handleSaveClick = async () => {
    if (currentName === initialName) {
        setIsEditing(false);
        return;
    }
    setIsSavingName(true);
    try {
        const success = await onRename(clusterId, currentName.trim());
        if (success) {
            setIsEditing(false);
        }
    } catch (error) {
         console.error("Error saving cluster name (ContactSheet):", error);
    } finally {
        setIsSavingName(false);
    }
  };

  const handleNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setCurrentName(event.target.value);
  };

  const handleSelectionChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onToggleSelection(clusterId);
  };

  const displayClusterName = initialName || `Кластер ${clusterId}`;
  const commonDisabled = isProcessing || isEditing || isSavingName;

  return (
    <div className={`contact-sheet-card ${isProcessing ? 'is-deleting' : ''} ${isEditing ? 'is-editing-name' : ''} ${isSelected ? 'selected' : ''}`}>

       <div style={{ position: 'absolute', top: '10px', right: '10px', zIndex: 2 }}>
           <input
               type="checkbox"
               checked={isSelected}
               onChange={handleSelectionChange}
               disabled={commonDisabled}
               title={commonDisabled ? "Действие недоступно" : "Выбрать для слияния"}
               style={{ cursor: commonDisabled ? 'not-allowed' : 'pointer', transform: 'scale(1.3)' }}
           />
       </div>

      {isEditing ? (
        <div className="cluster-name-edit">
            <input
                type="text"
                value={currentName}
                onChange={handleNameChange}
                placeholder={`Имя для кластера ${clusterId}`}
                disabled={isSavingName}
                className="cluster-name-input"
                maxLength={100}
                autoFocus
            />
        </div>
      ) : (
        <h4 title={displayClusterName}>{displayClusterName}</h4>
      )}

      {imageUrl ? (
        <img
          src={imageUrl}
          alt={`Контактный отпечаток для кластера ${clusterId}`}
          className="contact-sheet-image"
          loading="lazy"
          onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
         />
      ) : (
         <ImagePlaceholder />
      )}

      <p>Размер: {clusterSize} изображений</p>

      <div className="cluster-actions">
        {isEditing ? (
            <>
                <button
                    className="primary-btn save-name-btn"
                    onClick={handleSaveClick}
                    disabled={isSavingName}
                >
                    {isSavingName ? 'Сохр...' : 'Сохранить'}
                </button>
                <button
                    className="secondary-btn cancel-name-btn"
                    onClick={handleCancelClick}
                    disabled={isSavingName}
                >
                    Отмена
                </button>
            </>
        ) : (
             <>
                <button
                    className="secondary-btn rename-cluster-btn"
                    onClick={handleRenameClick}
                    disabled={commonDisabled}
                    title={commonDisabled ? "Действие недоступно" : "Переименовать кластер"}
                >
                    Переименовать
                </button>
                <button
                     className="secondary-btn split-cluster-btn"
                     onClick={() => onInitiateSplit(clusterId)}
                     disabled={commonDisabled || clusterSize < 2}
                     title={commonDisabled ? "Действие недоступно" : (clusterSize < 2 ? "Кластер слишком мал для разделения" : `Разделить кластер ${clusterId}`)}
                >
                    Разделить
                </button>
                <button
                    className="secondary-btn delete-sheet-btn"
                    onClick={() => onRedistribute(clusterId)}
                    disabled={commonDisabled}
                    title={commonDisabled ? "Действие недоступно" : `Удалить кластер ${clusterId} и перераспределить его точки`}
                >
                    Удалить и перераспределить
                </button>
             </>
        )}
      </div>
    </div>
  );
};

export default ContactSheet;