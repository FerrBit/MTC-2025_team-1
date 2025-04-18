import React, { useState, useRef, useCallback, ChangeEvent } from 'react';
import { toast } from 'react-toastify';
import { StartClusteringPayload } from '../services/api';
import '../styles/ClusteringControls.css';

interface ClusteringControlsProps {
    onStartClustering: (payload: StartClusteringPayload) => Promise<void>;
    disabled: boolean;
}

type Algorithm = 'kmeans' | 'dbscan';
const ALGORITHMS: { key: Algorithm; name: string; params: string[] }[] = [
  { key: 'kmeans', name: 'K-means', params: ['n_clusters'] },
  { key: 'dbscan', name: 'DBSCAN', params: ['eps', 'min_samples'] },
];

interface AlgorithmParamsState {
  n_clusters?: string;
  eps?: string;
  min_samples?: string;
}

const PARAM_LABELS: { [key: string]: string } = {
  n_clusters: 'Задайте количество поиска кластеров (n_clusters)',
  eps: 'Задайте радиус окрестности для поиска соседних точек (eps)',
  min_samples: 'Установите минимальное количество точек для формирования кластера (min_samples)'
};

const ClusteringControls: React.FC<ClusteringControlsProps> = ({ onStartClustering, disabled }) => {
    const [selectedFile, setSelectedFile] = useState<File | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [selectedArchive, setSelectedArchive] = useState<File | null>(null);
    const archiveInputRef = useRef<HTMLInputElement>(null);
    const [selectedAlgorithm, setSelectedAlgorithm] = useState<Algorithm | ''>('');
    const [algorithmParams, setAlgorithmParams] = useState<AlgorithmParamsState>({});
    const [formError, setFormError] = useState<string | null>(null);
    const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

    const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setFormError(null);
        if (event.target.files && event.target.files.length > 0) {
            const file = event.target.files[0];
            if (file.name.endsWith('.parquet')) {
                setSelectedFile(file);
            } else {
                setSelectedFile(null);
                if (fileInputRef.current) { fileInputRef.current.value = ""; }
                toast.error("Пожалуйста, выберите файл формата .parquet");
                setFormError("Неверный формат файла эмбеддингов. Требуется .parquet");
            }
        } else {
            setSelectedFile(null);
        }
    };

    const handleArchiveChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setFormError(null);
        if (event.target.files && event.target.files.length > 0) {
            const file = event.target.files[0];
            if (file.name.endsWith('.zip')) {
                setSelectedArchive(file);
            } else {
                setSelectedArchive(null);
                if (archiveInputRef.current) { archiveInputRef.current.value = ""; }
                toast.error("Пожалуйста, выберите файл формата .zip для архива изображений.");
                setFormError("Неверный формат файла архива. Требуется .zip");
            }
        } else {
            setSelectedArchive(null);
        }
    };

    const handleAlgorithmChange = (event: ChangeEvent<HTMLSelectElement>) => {
        setFormError(null);
        setSelectedAlgorithm(event.target.value as Algorithm | '');
        setAlgorithmParams({});
    };

    const handleParamChange = (event: ChangeEvent<HTMLInputElement>) => {
        setFormError(null);
        const { name, value } = event.target;
        setAlgorithmParams(prevParams => ({ ...prevParams, [name]: value }));
    };

    const getRequiredParams = useCallback((): string[] => {
        const algoConfig = ALGORITHMS.find(a => a.key === selectedAlgorithm);
        return algoConfig ? algoConfig.params : [];
    }, [selectedAlgorithm]);

    const validateAndParseParams = useCallback((): { [key: string]: number } | null => {
        setFormError(null);
        if (!selectedAlgorithm) {
            setFormError("Пожалуйста, выберите алгоритм кластеризации.");
            toast.error("Пожалуйста, выберите алгоритм кластеризации.");
            return null;
        }
        const requiredParams = getRequiredParams();
        const parsedParams: { [key: string]: number } = {};
        for (const paramName of requiredParams) {
            const valueStr = algorithmParams[paramName as keyof AlgorithmParamsState];
            const paramLabel = PARAM_LABELS[paramName] || paramName;
            if (valueStr === undefined || valueStr === '' || valueStr === null) {
                setFormError(`Параметр "${paramLabel}" обязателен.`);
                toast.error(`Параметр "${paramLabel}" обязателен.`);
                return null;
            }
            const numValue = Number(valueStr);
            if (isNaN(numValue)) {
                setFormError(`Параметр "${paramLabel}" должен быть числом.`);
                toast.error(`Параметр "${paramLabel}" должен быть числом.`);
                return null;
            }
            if ((paramName === 'n_clusters' || paramName === 'min_samples') && (!Number.isInteger(numValue) || numValue <= 0)) {
                setFormError(`Параметр "${paramLabel}" должен быть целым числом больше 0.`);
                toast.error(`Параметр "${paramLabel}" должен быть целым числом больше 0.`);
                return null;
            }
            if (paramName === 'eps' && numValue <= 0) {
                setFormError(`Параметр "${paramLabel}" должен быть положительным числом.`);
                toast.error(`Параметр "${paramLabel}" должен быть положительным числом.`);
                return null;
            }
            parsedParams[paramName] = numValue;
        }
        return parsedParams;
    }, [selectedAlgorithm, algorithmParams, getRequiredParams]);

    const handleSubmit = async () => {
        setFormError(null);
        if (!selectedFile) {
            setFormError("Пожалуйста, выберите файл эмбеддингов (.parquet).");
            toast.error("Пожалуйста, выберите файл эмбеддингов (.parquet).");
            return;
        }
        const parsedParams = validateAndParseParams();
        if (!parsedParams || !selectedAlgorithm) {
            return;
        }

        setIsSubmitting(true);
        try {
            const payload: StartClusteringPayload = {
                embeddingFile: selectedFile,
                algorithm: selectedAlgorithm,
                params: parsedParams,
                imageArchive: selectedArchive
            };
            await onStartClustering(payload);
            setSelectedFile(null);
            if (fileInputRef.current) fileInputRef.current.value = "";
            setSelectedArchive(null);
            if (archiveInputRef.current) archiveInputRef.current.value = "";
            setSelectedAlgorithm('');
            setAlgorithmParams({});
            setFormError(null);
        } catch (error) {
            console.error("Error during onStartClustering callback:", error);
        } finally {
            setIsSubmitting(false);
        }
    };

    const requiredParams = getRequiredParams();
    const isSubmitDisabled = disabled || isSubmitting || !selectedFile || !selectedAlgorithm || requiredParams.some(p => !algorithmParams[p as keyof AlgorithmParamsState]);
    const startButtonText = `Запустить ${selectedAlgorithm ? selectedAlgorithm.toUpperCase() : 'кластеризацию'}`;
    const startButtonTitle = !selectedFile ? "Сначала выберите файл .parquet" : !selectedAlgorithm ? "Выберите алгоритм" : requiredParams.some(p => !algorithmParams[p as keyof AlgorithmParamsState]) ? "Заполните все параметры алгоритма" : startButtonText;

    return (
        <div className="card controls-card">
            <h3>Запуск новой кластеризации</h3>
            <div className="clustering-controls-form">
                <div className="file-upload-wrapper">
                    <label htmlFor="parquet-upload" className="file-upload-label">Файл эмбеддингов (.parquet):</label>
                    <input
                        type="file"
                        id="parquet-upload"
                        className="file-input"
                        accept=".parquet"
                        onChange={handleFileChange}
                        ref={fileInputRef}
                        disabled={disabled || isSubmitting}
                        aria-describedby="file-status-info"
                        required
                    />
                </div>

                <div className="file-upload-wrapper">
                    <label htmlFor="archive-upload" className="file-upload-label">Архив изображений (.zip):</label>
                    <input
                        type="file"
                        id="archive-upload"
                        className="file-input"
                        accept=".zip"
                        onChange={handleArchiveChange}
                        ref={archiveInputRef}
                        disabled={disabled || isSubmitting}
                        aria-describedby="archive-status-info"
                    />
                     <small style={{fontSize: '0.8em', color: '#666', marginTop: '3px'}}>Опционально, для контактных отпечатков.</small>
                </div>

                <div className="form-group algo-select-group">
                    <label htmlFor="algorithm-select">Алгоритм:</label>
                    <select
                        id="algorithm-select"
                        value={selectedAlgorithm}
                        onChange={handleAlgorithmChange}
                        disabled={disabled || isSubmitting}
                        className="algo-select"
                        required={selectedFile !== null}
                    >
                        <option value="" disabled>-- Выберите --</option>
                        {ALGORITHMS.map(algo => (<option key={algo.key} value={algo.key}>{algo.name}</option>))}
                    </select>
                </div>

                {selectedAlgorithm && (
                    <div className="algorithm-params">
                         <label>Параметры для {ALGORITHMS.find(a => a.key === selectedAlgorithm)?.name}:</label>
                        {requiredParams.map(paramName => (
                            <div className="form-group param-group" key={paramName}>
                                <label htmlFor={`param-${paramName}`}>{PARAM_LABELS[paramName] || paramName}:</label>
                                <input
                                    type="number"
                                    id={`param-${paramName}`}
                                    name={paramName}
                                    value={algorithmParams[paramName as keyof AlgorithmParamsState] ?? ''}
                                    onChange={handleParamChange}
                                    disabled={disabled || isSubmitting}
                                    step={paramName === 'eps' ? '0.01' : '1'}
                                    min={paramName === 'eps' ? '0.01' : '1'}
                                    required
                                    className={`param-input ${(!algorithmParams[paramName as keyof AlgorithmParamsState] && selectedFile && selectedAlgorithm) ? 'input-error' : ''}`}
                                />
                            </div>
                        ))}
                    </div>
                )}

                <div className="start-button-container">
                    <button
                        className="primary-btn start-clustering-btn"
                        onClick={handleSubmit}
                        disabled={isSubmitDisabled}
                        title={startButtonTitle}
                    >
                        {isSubmitting ? 'Запуск...' : `${startButtonText}`}
                    </button>
                </div>
            </div>

            <div id="file-status-info" className="status-messages">
                {selectedFile && !isSubmitting && (<p className="file-status-info">Выбран файл эмбеддингов: {selectedFile.name}</p>)}
                {!selectedFile && !isSubmitting && (<p className="file-status-info error">Файл эмбеддингов (.parquet) не выбран.</p>)}
                <div id="archive-status-info">
                    {selectedArchive && !isSubmitting && (<p className="file-status-info">Выбран архив изображений: {selectedArchive.name}</p>)}
                    {!selectedArchive && !isSubmitting && (<p className="file-status-info">Архив изображений (.zip) не выбран (контактные отпечатки не будут созданы).</p>)}
                </div>
                {!selectedAlgorithm && selectedFile && !isSubmitting && (<p className="file-status-info error">Алгоритм не выбран.</p>)}
                {selectedAlgorithm && requiredParams.some(p => !algorithmParams[p as keyof AlgorithmParamsState]) && !isSubmitting && (<p className="file-status-info error">Заполните все параметры для {selectedAlgorithm.toUpperCase()}.</p>)}
                {formError && <p className="error-message">{formError}</p>}
            </div>
        </div>
    );
};

export default ClusteringControls;