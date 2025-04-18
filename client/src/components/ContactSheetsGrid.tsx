import React from 'react';
import ContactSheet from './ContactSheet';
import { ClusterResult } from '../services/api';
import '../styles/ContactSheetsGrid.css';
import '../styles/ContactSheet.css';

interface ContactSheetsGridProps {
    clusters: ClusterResult[];
    sessionId: string | null;
    onRedistribute: (clusterId: string | number) => void;
    onRename: (clusterId: string | number, newName: string) => Promise<boolean>;
    isDeletingId: string | number | null;
    disabled: boolean;
    status: string;
    selectedIds: Set<string | number>;
    onToggleSelection: (clusterId: string | number) => void;
    onInitiateSplit: (clusterId: string | number) => void;
}

const ContactSheetsGrid: React.FC<ContactSheetsGridProps> = ({
    clusters,
    sessionId,
    onRedistribute,
    onRename,
    isDeletingId,
    disabled,
    status,
    selectedIds,
    onToggleSelection,
    onInitiateSplit,
}) => {

    if ((status !== 'SUCCESS' && status !== 'RECLUSTERED')) {
        return null;
    }
    if (!clusters || clusters.length === 0) {
        if (status === 'SUCCESS' || status === 'RECLUSTERED') {
            return (
                <div className="card status-card">
                    <p>Кластеризация завершена, но не найдено кластеров для отображения контактных отпечатков.</p>
                </div>
            );
        }
        return null;
    }

    const sortedClusters = [...clusters].sort((a, b) => {
        const numA = parseFloat(String(a.id));
        const numB = parseFloat(String(b.id));

        if (isNaN(numA) && isNaN(numB)) {
             return String(a.id).localeCompare(String(b.id));
        }
        if (isNaN(numA)) return 1;
        if (isNaN(numB)) return -1;

        return numA - numB;
    });

    return (
        <div className="card contact-sheets-card">
            <h3>Контактные отпечатки ({sortedClusters.length} шт.)</h3>
            <div className="contact-sheets-scroll-container">
                <div className="contact-sheets-grid-layout">
                    {sortedClusters.map(cluster => (
                        <ContactSheet
                            key={cluster.id}
                            clusterId={cluster.id}
                            imageUrl={cluster.contactSheetUrl}
                            clusterSize={cluster.size}
                            initialName={cluster.name}
                            onRedistribute={onRedistribute}
                            onRename={onRename}
                            isProcessing={isDeletingId === cluster.id || disabled}
                            isSelected={selectedIds.has(cluster.id)}
                            onToggleSelection={onToggleSelection}
                            onInitiateSplit={onInitiateSplit}
                        />
                    ))}
                </div>
            </div>
        </div>
    );
};

export default ContactSheetsGrid;