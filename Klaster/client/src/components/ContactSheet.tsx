import React from 'react';
import '../styles/ContactSheet.css'

interface ContactSheetProps {
  clusterId: string | number;
  imageUrl: string;
  clusterSize: number;
  onDelete: (clusterId: string | number) => void;
  isDeleting: boolean;
}

const ContactSheet: React.FC<ContactSheetProps> = ({
  clusterId,
  imageUrl,
  clusterSize,
  onDelete,
  isDeleting
}) => {
  return (
    <div className={`contact-sheet-card ${isDeleting ? 'is-deleting' : ''}`}>
      <h4>Кластер {clusterId}</h4>
      <img
        src={imageUrl}
        alt={`Контактный отпечаток для кластера ${clusterId}`}
        className="contact-sheet-image"
        onError={(e) => {
            const target = e.target as HTMLImageElement;
            target.onerror = null;
            target.src = 'https://placehold.co/300x200/EEE/31343C?text=Error+Loading';
            console.warn(`Failed to load contact sheet image: ${imageUrl}`);
        }}
       />
      <p>Размер: {clusterSize} изображений</p>
      <button
        className="secondary-btn delete-sheet-btn"
        onClick={() => onDelete(clusterId)}
        disabled={isDeleting}
        title={isDeleting ? "Выполняется операция..." : "Удалить контактный отпечаток и рекластеризовать"}
        aria-label={`Удалить контактный отпечаток кластера ${clusterId}`}
      >
        {isDeleting ? 'Удаление...' : 'Удалить отпечаток'}
      </button>
    </div>
  );
};

export default ContactSheet;