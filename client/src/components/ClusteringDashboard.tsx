import React, { useState, useCallback, useRef, ChangeEvent, useEffect } from 'react';
import { toast } from 'react-toastify';
import ContactSheet from './ContactSheet';
import '../styles/ClusteringDashboard.css';
import '../styles/ContactSheet.css';

interface ClusterData {
  id: string | number;
  contactSheetUrl: string;
  size: number;
}

type Algorithm = 'kmeans' | 'dbscan';
const ALGORITHMS: { key: Algorithm; name: string; params: string[] }[] = [
  { key: 'kmeans', name: 'K-means', params: ['n_clusters'] },
  { key: 'dbscan', name: 'DBSCAN', params: ['eps', 'min_samples'] },
];

interface AlgorithmParams {
  n_clusters?: number | string;
  eps?: number | string;
  min_samples?: number | string;
}

const generateMockClusters = (count: number, algorithm: Algorithm, params: AlgorithmParams): ClusterData[] => {
    let finalCount = count;
    if (algorithm === 'kmeans' && params.n_clusters && typeof params.n_clusters === 'number' && params.n_clusters > 0) {
        finalCount = params.n_clusters;
    } else if (algorithm === 'dbscan') {
        const epsFactor = (typeof params.eps === 'number' && params.eps > 0) ? (1 / params.eps) : 1;
        const samplesFactor = (typeof params.min_samples === 'number' && params.min_samples > 0) ? params.min_samples : 5;
        finalCount = Math.max(2, Math.min(15, Math.floor(count * epsFactor * 0.1 + samplesFactor * 0.5)));
    }

    console.log(`Simulating generation of ${finalCount} clusters using ${algorithm}`);

    return Array.from({ length: finalCount }, (_, i) => ({
        id: `${algorithm.substring(0,1).toUpperCase()}${i + 1}`,
        contactSheetUrl: `https://placehold.co/300x200/EEE/31343C?text=${algorithm}+${i + 1}\\nContact+Sheet`,
        size: Math.floor(Math.random() * (algorithm === 'kmeans' ? 300 : 400)) + (algorithm === 'dbscan' ? 20 : 50),
    }));
};


const ClusteringDashboard: React.FC = () => {
  const [clusters, setClusters] = useState<ClusterData[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isDeletingId, setIsDeletingId] = useState<string | number | null>(null);
  const [clusteringInitiated, setClusteringInitiated] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [selectedAlgorithm, setSelectedAlgorithm] = useState<Algorithm | ''>('');
  const [algorithmParams, setAlgorithmParams] = useState<AlgorithmParams>({});

   useEffect(() => {
        setAlgorithmParams({});
   }, [selectedAlgorithm]);


  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files.length > 0) {
      const file = event.target.files[0];
      if (file.name.endsWith('.parquet')) {
          setSelectedFile(file);
          setError(null);
      } else {
          setSelectedFile(null);
          if (fileInputRef.current) {
              fileInputRef.current.value = "";
          }
          toast.error("Пожалуйста, выберите файл формата .parquet");
          setError("Неверный формат файла. Требуется .parquet");
      }
    } else {
        setSelectedFile(null);
    }
  };

  const handleAlgorithmChange = (event: ChangeEvent<HTMLSelectElement>) => {
    setSelectedAlgorithm(event.target.value as Algorithm | '');
  };

  const handleParamChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { name, value } = event.target;
    setAlgorithmParams(prevParams => ({
      ...prevParams,
      [name]: value
    }));
  };

  const getRequiredParams = useCallback((): string[] => {
      const algoConfig = ALGORITHMS.find(a => a.key === selectedAlgorithm);
      return algoConfig ? algoConfig.params : [];
  }, [selectedAlgorithm]);

  const validateParams = useCallback((): boolean => {
      if (!selectedAlgorithm) {
          toast.error("Пожалуйста, выберите алгоритм кластеризации.");
          return false;
      }
      const requiredParams = getRequiredParams();
      for (const paramName of requiredParams) {
           const value = algorithmParams[paramName as keyof AlgorithmParams];
           if (value === undefined || value === '' || value === null) {
               toast.error(`Параметр "${paramName}" для алгоритма ${selectedAlgorithm.toUpperCase()} обязателен.`);
               return false;
           }
           const numValue = Number(value);
           if (isNaN(numValue)) {
               toast.error(`Параметр "${paramName}" должен быть числом.`);
               return false;
           }
           if ((paramName === 'n_clusters' || paramName === 'min_samples') && numValue <= 0) {
                toast.error(`Параметр "${paramName}" должен быть больше 0.`);
                return false;
           }
           if (paramName === 'eps' && numValue <= 0) {
               toast.error(`Параметр "${paramName}" должен быть положительным числом.`);
               return false;
           }
      }
      return true;
  }, [selectedAlgorithm, algorithmParams, getRequiredParams]);

   const getParsedParams = useCallback((): AlgorithmParams => {
      const parsed: AlgorithmParams = {};
      const requiredParams = getRequiredParams();
      for (const paramName of requiredParams) {
          const value = algorithmParams[paramName as keyof AlgorithmParams];
          if (value !== undefined && value !== '' && value !== null) {
              parsed[paramName as keyof AlgorithmParams] = Number(value);
          }
      }
      return parsed;
   // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [selectedAlgorithm, algorithmParams, getRequiredParams]);

  const handleStartClustering = useCallback(() => {
    if (!validateParams()) {
        return;
    }

    if (!selectedAlgorithm) {
        console.error("handleStartClustering called with empty algorithm despite validation.");
        toast.error("Внутренняя ошибка: Алгоритм не выбран.");
        return;
    }

    setIsLoading(true);
    setError(null);
    setClusteringInitiated(true);
    setClusters([]);
    const parsedParams = getParsedParams();

    let clusteringMode = `симуляцию (${selectedAlgorithm.toUpperCase()})`;
    if (selectedFile) {
        clusteringMode = `кластеризацию по файлу "${selectedFile.name}" (Алгоритм: ${selectedAlgorithm.toUpperCase()})`;
        console.log(`Simulating: Starting clustering with ${selectedAlgorithm} (Params: ${JSON.stringify(parsedParams)}) using file: ${selectedFile.name}`);
    } else {
        clusteringMode = `автоматическую кластеризацию (Алгоритм: ${selectedAlgorithm.toUpperCase()})`;
        console.log(`Simulating: Starting default clustering with ${selectedAlgorithm} (Params: ${JSON.stringify(parsedParams)})...`);
    }

    toast.info(`Начинаем ${clusteringMode}...`);

    setTimeout(() => {
      try {
        const mockData = generateMockClusters(selectedFile ? 7 : 5, selectedAlgorithm, parsedParams);
        setClusters(mockData);
        setIsLoading(false);
        console.log(`Simulating: Clustering with ${selectedAlgorithm} completed.`, mockData);
        toast.success(`Кластеризация (${selectedAlgorithm.toUpperCase()}) успешно завершена!`);

      } catch (err) {
          const message = err instanceof Error ? err.message : 'Неизвестная ошибка симуляции';
          setError(`Ошибка симуляции кластеризации: ${message}`);
          setIsLoading(false);
          setClusters([]);
          console.error(`Simulating: Clustering with ${selectedAlgorithm?.toUpperCase()} failed.`, err);
          toast.error(`Ошибка кластеризации (${selectedAlgorithm?.toUpperCase()}): ${message}`);
      }
    }, 2500);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedFile, selectedAlgorithm, algorithmParams, validateParams, getParsedParams]);

  const handleDeleteContactSheet = useCallback((clusterId: string | number) => {
    setIsDeletingId(clusterId);
    setError(null);
    console.log(`Simulating: Deleting contact sheet for cluster ${clusterId} and re-clustering...`);
    toast.info(`Удаляем отпечаток кластера ${clusterId} и запускаем рекластеризацию...`);

    const reclusterAlgo = selectedAlgorithm || 'kmeans';
    const reclusterParams = Object.keys(algorithmParams).length > 0 ? getParsedParams() : {n_clusters: 4};

    setTimeout(() => {
        try {
            const remainingClusters = clusters.filter(c => c.id !== clusterId);
            const updatedClusters = generateMockClusters(Math.max(1, remainingClusters.length), reclusterAlgo, reclusterParams);

            setClusters(updatedClusters);
            setIsDeletingId(null);
            console.log(`Simulating: Re-clustering complete after deleting ${clusterId}.`, updatedClusters);
            toast.success(`Кластер ${clusterId} удален, рекластеризация (${reclusterAlgo.toUpperCase()}) завершена!`);

            if (updatedClusters.length === 0) {
                 toast.info("Все кластеры были удалены или рекластеризация не дала результатов.");
                 setClusteringInitiated(false);
            }

        } catch (err) {
            const message = err instanceof Error ? err.message : 'Неизвестная ошибка симуляции';
            setError(`Ошибка симуляции удаления/рекластеризации: ${message}`);
            setIsDeletingId(null);
            console.error(`Simulating: Failed to delete/re-cluster for ${clusterId}.`, err);
            toast.error(`Ошибка удаления/рекластеризации: ${message}`);
        }
    }, 1500);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusters, selectedAlgorithm, algorithmParams, getParsedParams]);

  const startButtonText = selectedFile
        ? `Запустить по файлу (${selectedFile.name.substring(0, 20)}${selectedFile.name.length > 20 ? '...' : ''})`
        : `Запустить ${selectedAlgorithm ? selectedAlgorithm.toUpperCase() : 'кластеризацию'} (симуляция)`;

    const isControlsDisabled = isLoading || isDeletingId !== null;
    const requiredParams = getRequiredParams();

  return (
    <div className="clustering-dashboard">
      <h2>Панель управления кластеризацией</h2>

      <div className="card controls-card">
        <h3>Управление</h3>
        <div className="clustering-controls">
            <div className="file-upload-wrapper">
                 <label htmlFor="parquet-upload" className="file-upload-label">
                     1. Загрузить файл эмбеддингов (.parquet):
                 </label>
                <input
                    type="file"
                    id="parquet-upload"
                    className="file-input"
                    accept=".parquet"
                    onChange={handleFileChange}
                    ref={fileInputRef}
                    disabled={isControlsDisabled}
                    aria-describedby="file-status-info"
                />
            </div>

             <div className="form-group algo-select-group">
                <label htmlFor="algorithm-select">2. Выбрать алгоритм:</label>
                <select
                    id="algorithm-select"
                    value={selectedAlgorithm}
                    onChange={handleAlgorithmChange}
                    disabled={isControlsDisabled}
                    className="algo-select"
                    required
                >
                    <option value="" disabled>-- Выберите алгоритм --</option>
                    {ALGORITHMS.map(algo => (
                        <option key={algo.key} value={algo.key}>{algo.name}</option>
                    ))}
                </select>
            </div>
            <button
              className="primary-btn start-clustering-btn"
              onClick={handleStartClustering}
              disabled={isControlsDisabled || !selectedAlgorithm}
              title={startButtonText}
            >
              {isLoading ? 'Кластеризация...' : `3. Запустить ${selectedAlgorithm ? selectedAlgorithm.toUpperCase() : ''}`}
            </button>

            {selectedAlgorithm && (
                 <div className="algorithm-params">
                    <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: '500' }}>
                       Параметры для {ALGORITHMS.find(a => a.key === selectedAlgorithm)?.name}:
                    </label>
                    {requiredParams.map(paramName => (
                        <div className="form-group param-group" key={paramName}>
                           <label htmlFor={`param-${paramName}`}>{paramName}:</label>
                            <input
                                type="number"
                                id={`param-${paramName}`}
                                name={paramName}
                                value={algorithmParams[paramName as keyof AlgorithmParams] ?? ''}
                                onChange={handleParamChange}
                                disabled={isControlsDisabled}
                                step={paramName === 'eps' ? '0.01' : '1'}
                                min={paramName === 'eps' ? '0.01' : '1'}
                                required
                                className="param-input"
                            />
                        </div>
                    ))}
                 </div>
            )}
        </div>

        <div id="file-status-info" className="status-messages">
            {selectedFile && !isLoading && (
                <p className="file-status-info">Выбран файл: {selectedFile.name}</p>
            )}
            {!selectedFile && !isLoading && (
                 <p className="file-status-info">Файл не выбран, будет запущена симуляция на основе случайных данных.</p>
            )}
             {!selectedAlgorithm && !isLoading && (
                 <p className="file-status-info" style={{ color: '#dc3545' }}>Алгоритм кластеризации не выбран.</p>
             )}
        </div>
        {error && <p className="error-message" style={{marginTop: '1rem'}}>{error}</p>}
      </div>

      {isLoading && (
        <div className="card status-card">
          <p>Идет процесс кластеризации ({selectedAlgorithm?.toUpperCase()}), пожалуйста, подождите...</p>
        </div>
      )}

      {!isLoading && clusteringInitiated && clusters.length === 0 && !error && (
          <div className="card status-card">
             <p>Нет данных для отображения. Возможно, кластеризация ({selectedAlgorithm?.toUpperCase()}) не дала результатов или все кластеры были удалены.</p>
          </div>
      )}

      {!isLoading && clusters.length > 0 && (
        <>
          <div className="card metrics-card">
            <h3>Метрики и Графики ({selectedAlgorithm?.toUpperCase()}, Симуляция)</h3>
            <p>Всего кластеров: {clusters.length}</p>
            <p>Средний размер кластера: {(clusters.reduce((sum, c) => sum + c.size, 0) / clusters.length).toFixed(0)}</p>
            <div className="graph-placeholder">
                <img src={`https://placehold.co/600x300/E8E8E8/A9A9A9?text=График+распределения+кластеров+(${selectedAlgorithm?.toUpperCase()})`} alt="Placeholder Graph" />
            </div>
             <h4>Ручная корректировка (Placeholder)</h4>
             <p>Здесь будут элементы для объединения, разделения кластеров.</p>
              <button className="secondary-btn" disabled>Объединить выбранные</button>
              <button className="secondary-btn" disabled style={{marginLeft: '10px'}}>Разделить выбранный</button>
          </div>

          <div className="card contact-sheets-card">
            <h3>Контактные отпечатки ({selectedAlgorithm?.toUpperCase()})</h3>
            <div className="contact-sheets-grid">
              {clusters.map(cluster => (
                <ContactSheet
                  key={cluster.id}
                  clusterId={cluster.id}
                  imageUrl={cluster.contactSheetUrl}
                  clusterSize={cluster.size}
                  onDelete={handleDeleteContactSheet}
                  isDeleting={isDeletingId === cluster.id}
                />
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default ClusteringDashboard;