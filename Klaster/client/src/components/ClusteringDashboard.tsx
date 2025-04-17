import React, { useState, useCallback, useRef, ChangeEvent, useEffect } from 'react';
import { toast } from 'react-toastify';
import ContactSheet from './ContactSheet';
import '../styles/ClusteringDashboard.css';
import '../styles/ContactSheet.css';
import {
    startClustering,
    getClusteringSessions,
    getClusteringResults,
    deleteClusterAndRecluster,
    ClusterResult,
    SessionResultResponse,
    SessionListItem,
    StartClusteringPayload
} from '../services/api';

type FetchWithAuth = (url: string, options?: RequestInit) => Promise<Response>;

interface ClusteringDashboardProps {
  fetchWithAuth: FetchWithAuth;
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

const ClusteringDashboard: React.FC<ClusteringDashboardProps> = ({ fetchWithAuth }) => {
  const [clusters, setClusters] = useState<ClusterResult[]>([]);
  const [sessions, setSessions] = useState<SessionListItem[]>([]);
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null);
  const [currentSessionDetails, setCurrentSessionDetails] = useState<SessionResultResponse | null>(null);

  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isFetchingSessions, setIsFetchingSessions] = useState<boolean>(true);
  const [isFetchingResults, setIsFetchingResults] = useState<boolean>(false);
  const [isDeletingId, setIsDeletingId] = useState<string | number | null>(null);

  const [error, setError] = useState<string | null>(null);

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [selectedAlgorithm, setSelectedAlgorithm] = useState<Algorithm | ''>('');
  const [algorithmParams, setAlgorithmParams] = useState<AlgorithmParamsState>({});

  useEffect(() => {
    const fetchSessions = async () => {
      setIsFetchingSessions(true);
      setError(null);
      try {
        const fetchedSessions = await getClusteringSessions(fetchWithAuth);
        setSessions(fetchedSessions);
      } catch (err) {
        console.error("Error fetching sessions:", err);
        const errorMsg = err instanceof Error ? err.message : 'Не удалось загрузить список сессий.';
        setError(errorMsg);
        toast.error(errorMsg);
      } finally {
        setIsFetchingSessions(false);
      }
    };
    fetchSessions();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchWithAuth]);

  useEffect(() => {
    if (!currentSessionId) {
      setClusters([]);
      setCurrentSessionDetails(null);
      return;
    }

    const fetchResults = async () => {
      setIsFetchingResults(true);
      setError(null);
      setClusters([]);
      setCurrentSessionDetails(null);
      console.log(`Fetching results for session: ${currentSessionId}`);
      try {
        const resultsData = await getClusteringResults(fetchWithAuth, currentSessionId);
        console.log("Results data received:", resultsData);

        setCurrentSessionDetails(resultsData);
        setClusters(resultsData.clusters || []);

        if (resultsData.status !== 'SUCCESS' && resultsData.status !== 'RECLUSTERED') {
            toast.info(`Статус сессии ${currentSessionId.substring(0,8)}...: ${resultsData.status}. ${resultsData.message || resultsData.error || ''}`);
        } else if (!resultsData.clusters || resultsData.clusters.length === 0) {
             toast.info(`Сессия ${currentSessionId.substring(0,8)}... (${resultsData.algorithm}) завершена, но кластеры не найдены.`);
        }

      } catch (err) {
        console.error(`Error fetching results for session ${currentSessionId}:`, err);
        const errorMsg = err instanceof Error ? err.message : `Не удалось загрузить результаты для сессии ${currentSessionId.substring(0,8)}...`;
        setError(errorMsg);
        toast.error(errorMsg);
      } finally {
        setIsFetchingResults(false);
      }
    };

    fetchResults();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentSessionId, fetchWithAuth]);

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
    setAlgorithmParams({});
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

  const validateAndParseParams = useCallback((): { [key: string]: number } | null => {
      if (!selectedAlgorithm) {
          toast.error("Пожалуйста, выберите алгоритм кластеризации.");
          return null;
      }
      const requiredParams = getRequiredParams();
      const parsedParams: { [key: string]: number } = {};

      for (const paramName of requiredParams) {
           const valueStr = algorithmParams[paramName as keyof AlgorithmParamsState];
           if (valueStr === undefined || valueStr === '' || valueStr === null) {
               toast.error(`Параметр "${paramName}" для ${selectedAlgorithm.toUpperCase()} обязателен.`);
               return null;
           }
           const numValue = Number(valueStr);
           if (isNaN(numValue)) {
               toast.error(`Параметр "${paramName}" должен быть числом.`);
               return null;
           }
           if ((paramName === 'n_clusters' || paramName === 'min_samples') && numValue <= 0) {
                toast.error(`Параметр "${paramName}" должен быть целым числом больше 0.`);
                return null;
           }
           if (paramName === 'eps' && numValue <= 0) {
               toast.error(`Параметр "${paramName}" должен быть положительным числом.`);
               return null;
           }
           if ((paramName === 'n_clusters' || paramName === 'min_samples') && !Number.isInteger(numValue)){
                toast.error(`Параметр "${paramName}" должен быть целым числом.`);
                return null;
           }

           parsedParams[paramName] = numValue;
      }
      return parsedParams;
  }, [selectedAlgorithm, algorithmParams, getRequiredParams]);


  const handleStartClustering = useCallback(async () => {
    if (!selectedFile) {
        toast.error("Пожалуйста, выберите файл эмбеддингов (.parquet).");
        return;
    }
    const parsedParams = validateAndParseParams();
    if (!parsedParams) {
        return;
    }
    const currentAlgorithm = selectedAlgorithm as Algorithm;

    setIsLoading(true);
    setError(null);

    toast.info(`Запускаем кластеризацию (${currentAlgorithm.toUpperCase()})...`);

    try {
        const payload: StartClusteringPayload = {
            embeddingFile: selectedFile,
            algorithm: currentAlgorithm,
            params: parsedParams
        };
        const response = await startClustering(fetchWithAuth, payload);
        toast.success(`Кластеризация запущена! ID сессии: ${response.session_id.substring(0,8)}...`);

        const newSessionItem: SessionListItem = {
            session_id: response.session_id,
            created_at: new Date().toISOString(),
            status: 'STARTED',
            algorithm: currentAlgorithm,
            params: parsedParams,
            num_clusters: null,
            result_message: "Запущено...",
            input_filename: selectedFile.name
        };

        setSessions(prev => [newSessionItem, ...prev]);
        setCurrentSessionId(response.session_id);

    } catch (err) {
        console.error("Clustering start error:", err);
        const errorMsg = err instanceof Error ? err.message : 'Не удалось запустить кластеризацию.';
        setError(errorMsg);
        toast.error(`Ошибка запуска кластеризации: ${errorMsg}`);
    } finally {
        setIsLoading(false);
    }
  }, [selectedFile, selectedAlgorithm, fetchWithAuth, validateAndParseParams]);


   const handleDeleteClusterAndRecluster = useCallback(async (clusterLabel: string | number) => {
    if (!currentSessionId) {
        toast.error("Нет активной сессии для выполнения операции.");
        return;
    }
    const labelToDelete = String(clusterLabel);

    setIsDeletingId(clusterLabel);
    setIsLoading(true);
    setError(null);
    console.log(`Requesting delete/recluster for cluster ${labelToDelete} in session ${currentSessionId}`);
    toast.info(`Удаляем кластер ${labelToDelete} и запускаем рекластеризацию...`);

    try {
        const response = await deleteClusterAndRecluster(fetchWithAuth, currentSessionId, labelToDelete);
        toast.success(`Кластер ${labelToDelete} удален. Создана новая сессия: ${response.new_session_id.substring(0,8)}...`);
        console.log("Recluster response:", response);

        const originalSession = sessions.find(s => s.session_id === currentSessionId);

        const newSessionItem: SessionListItem = {
            session_id: response.new_session_id,
            created_at: new Date().toISOString(),
            status: 'STARTED',
            algorithm: originalSession?.algorithm || '',
            params: originalSession?.params || {},
            num_clusters: null,
            result_message: "Рекластеризация завершена, загрузка...",
            input_filename: originalSession?.input_filename || "N/A"
        };

        setSessions(prev => [
            newSessionItem,
            ...prev.map(s =>
                s.session_id === currentSessionId ? { ...s, status: 'RECLUSTERED' } : s
            )
        ]);
        setCurrentSessionId(response.new_session_id);

    } catch (err) {
        console.error(`Error deleting/re-clustering cluster ${labelToDelete}:`, err);
        const errorMsg = err instanceof Error ? err.message : `Не удалось удалить кластер ${labelToDelete}.`;
        setError(errorMsg);
        toast.error(errorMsg);
    } finally {
        setIsDeletingId(null);
        setIsLoading(false);
    }
  }, [currentSessionId, fetchWithAuth, sessions]);

  const startButtonText = `Запустить ${selectedAlgorithm ? selectedAlgorithm.toUpperCase() : 'кластеризацию'}`;
  const isProcessing = isLoading || isFetchingSessions || isFetchingResults || isDeletingId !== null;
  const requiredParams = getRequiredParams();

  return (
    <div className="clustering-dashboard">
      <h2>Панель управления кластеризацией</h2>

      <div className="card sessions-card" style={{marginBottom: '2rem'}}>
          <h3>Сессии кластеризации</h3>
          {isFetchingSessions && <p>Загрузка списка сессий...</p>}
          {!isFetchingSessions && sessions.length === 0 && (
              <p>Нет доступных сессий. Запустите новую кластеризацию ниже.</p>
           )}
          {!isFetchingSessions && sessions.length > 0 && (
              <div style={{ maxHeight: '200px', overflowY: 'auto', border: '1px solid #eee', padding: '0.5rem', borderRadius: '8px' }}>
                  {sessions.map(session => (
                      <div key={session.session_id} style={{
                          padding: '8px 12px',
                          marginBottom: '5px',
                          border: `1px solid ${currentSessionId === session.session_id ? '#007bff' : '#ddd'}`,
                          borderRadius: '6px',
                          cursor: isProcessing ? 'not-allowed' : 'pointer',
                          backgroundColor: currentSessionId === session.session_id ? '#e7f3ff' : '#fff',
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          opacity: isProcessing && currentSessionId !== session.session_id ? 0.6 : 1,
                          transition: 'background-color 0.2s ease, border-color 0.2s ease'
                      }}
                           onClick={() => !isProcessing && setCurrentSessionId(session.session_id)}
                           title={`Выбрать сессию ${session.session_id.substring(0,8)}...`}
                           >
                           <span style={{overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginRight: '10px'}}>
                              ID: {session.session_id.substring(0, 8)}... ({session.algorithm.toUpperCase()}) | {session.input_filename} | {new Date(session.created_at).toLocaleString()}
                           </span>
                          <span style={{fontWeight: 500, fontSize: '0.85em', padding: '3px 8px', borderRadius: '4px', backgroundColor: '#f0f0f0', whiteSpace: 'nowrap'}}>
                            {session.status}
                          </span>
                      </div>
                  ))}
              </div>
          )}
           {error && !isFetchingSessions && sessions.length === 0 && <p className="error-message" style={{ marginTop: '0.5rem' }}>{error}</p>}
      </div>


      <div className="card controls-card">
        <h3>Запуск новой кластеризации</h3>
        <div className="clustering-controls">
            <div className="file-upload-wrapper">
                 <label htmlFor="parquet-upload" className="file-upload-label">
                     1. Загрузить файл .parquet:
                 </label>
                <input
                    type="file" id="parquet-upload" className="file-input"
                    accept=".parquet" onChange={handleFileChange} ref={fileInputRef}
                    disabled={isProcessing} aria-describedby="file-status-info"
                />
            </div>

             <div className="form-group algo-select-group">
                <label htmlFor="algorithm-select">2. Выбрать алгоритм:</label>
                <select
                    id="algorithm-select" value={selectedAlgorithm}
                    onChange={handleAlgorithmChange} disabled={isProcessing}
                    className="algo-select" required={selectedFile !== null}
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
              disabled={isProcessing || !selectedFile || !selectedAlgorithm}
              title={!selectedFile ? "Сначала выберите файл" : !selectedAlgorithm ? "Выберите алгоритм" : startButtonText}
            >
              {isLoading ? 'Запуск...' : `3. ${startButtonText}`}
            </button>

            {selectedAlgorithm && (
                 <div className="algorithm-params">
                     <label>
                       Параметры для {ALGORITHMS.find(a => a.key === selectedAlgorithm)?.name}:
                    </label>
                    {requiredParams.map(paramName => (
                        <div className="form-group param-group" key={paramName}>
                           <label htmlFor={`param-${paramName}`}>{paramName}:</label>
                            <input
                                type="number" id={`param-${paramName}`} name={paramName}
                                value={algorithmParams[paramName as keyof AlgorithmParamsState] ?? ''}
                                onChange={handleParamChange} disabled={isProcessing}
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
                 <p className="file-status-info">Файл не выбран.</p>
            )}
             {!selectedAlgorithm && selectedFile && !isLoading && (
                 <p className="file-status-info" style={{ color: '#dc3545' }}>Алгоритм не выбран.</p>
             )}
             {error && isLoading && <p className="error-message">{error}</p>}
        </div>
      </div>

      {isFetchingResults && (
        <div className="card status-card">
          <p>Загрузка результатов для сессии {currentSessionId?.substring(0,8)}...</p>
        </div>
      )}

       {currentSessionId && !isFetchingResults && (
           <>
               {currentSessionDetails ? (
                   <>
                       <div className="card metrics-card">
                           <h3>Результаты сессии: {currentSessionId.substring(0, 8)}...</h3>
                           <p>Алгоритм: {currentSessionDetails.algorithm.toUpperCase()}</p>
                           <p>Статус: {currentSessionDetails.status}</p>
                           <p>Параметры: {JSON.stringify(currentSessionDetails.params)}</p>
                           {currentSessionDetails.input_filename && <p>Входной файл: {currentSessionDetails.input_filename}</p> }
                           {currentSessionDetails.num_clusters !== null && <p>Найдено кластеров: {currentSessionDetails.num_clusters}</p>}
                           {clusters.length > 0 && <p>Средний размер кластера: {(clusters.reduce((sum, c) => sum + c.size, 0) / clusters.length).toFixed(0)}</p>}
                           {currentSessionDetails.processing_time_sec !== null && <p>Время обработки: {currentSessionDetails.processing_time_sec.toFixed(2)} сек.</p>}
                            {currentSessionDetails.message && <p>Сообщение: {currentSessionDetails.message}</p>}
                            {currentSessionDetails.error && <p className="error-message">Ошибка сессии: {currentSessionDetails.error}</p>}


                            <div className="graph-placeholder" style={{marginTop: '1rem'}}>
                               <img src={`https://placehold.co/600x300/E8E8E8/A9A9A9?text=График+распределения+(${currentSessionDetails.algorithm?.toUpperCase()})`} alt="Placeholder Graph" />
                           </div>
                           <h4>Ручная корректировка (Placeholder)</h4>
                           <p>Здесь будут элементы для объединения, разделения, переименования кластеров.</p>
                           <button className="secondary-btn" disabled>Объединить</button>
                           <button className="secondary-btn" disabled style={{ marginLeft: '10px' }}>Разделить</button>
                           <button className="secondary-btn" disabled style={{ marginLeft: '10px' }}>Переименовать</button>
                       </div>

                       {(currentSessionDetails.status === 'SUCCESS' || currentSessionDetails.status === 'RECLUSTERED') && clusters.length > 0 && (
                          <div className="card contact-sheets-card">
                            <h3>Контактные отпечатки ({clusters.length} шт.)</h3>
                            <div className="contact-sheets-grid">
                              {clusters.map(cluster => cluster.contactSheetUrl ? (
                                <ContactSheet
                                  key={cluster.id}
                                  clusterId={cluster.id}
                                  imageUrl={cluster.contactSheetUrl}
                                  clusterSize={cluster.size}
                                  onDelete={handleDeleteClusterAndRecluster}
                                  isDeleting={isDeletingId === cluster.id || currentSessionDetails.status === 'RECLUSTERED'}
                                />
                              ) : (
                                <div key={cluster.id} className="contact-sheet-card" style={{opacity: 0.7, background: '#f8f8f8'}}>
                                   <h4>Кластер {cluster.id}</h4>
                                   <div style={{aspectRatio: '3/2', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#eee', borderRadius: '6px', marginBottom: '1rem'}}>
                                       <p style={{margin: 0, color: '#888', textAlign: 'center', fontSize: '0.9em'}}>Нет<br/>отпечатка</p>
                                    </div>
                                    <p>Размер: {cluster.size} изображений</p>
                                     <button className="secondary-btn delete-sheet-btn" disabled>Удаление недоступно</button>
                                </div>
                              ))}
                            </div>
                          </div>
                       )}

                       {(currentSessionDetails.status === 'SUCCESS' || currentSessionDetails.status === 'RECLUSTERED') && clusters.length === 0 && (
                            <div className="card status-card">
                                 <p>Кластеризация для сессии {currentSessionId.substring(0,8)}... ({currentSessionDetails.algorithm}) завершена, но не найдено кластеров для отображения.</p>
                            </div>
                        )}

                         {currentSessionDetails.status !== 'SUCCESS' && currentSessionDetails.status !== 'RECLUSTERED' && currentSessionDetails.status !== 'STARTED' && (
                             <div className="card status-card">
                                 <p>Статус сессии {currentSessionId.substring(0,8)}...: {currentSessionDetails.status}.</p>
                                 {currentSessionDetails.message && <p>{currentSessionDetails.message}</p>}
                                 {currentSessionDetails.error && <p className="error-message">{currentSessionDetails.error}</p>}
                                 <p>Результаты не могут быть отображены.</p>
                             </div>
                         )}

                   </>
               ) : (
                   error && <div className="card status-card error-message"><p>{error}</p></div>
               )}
           </>
       )}

        {!currentSessionId && !isProcessing && sessions.length > 0 && (
            <div className="card status-card">
                 <p>Выберите сессию из списка выше для просмотра результатов или запустите новую кластеризацию.</p>
            </div>
        )}
         {!currentSessionId && !isProcessing && sessions.length === 0 && !isFetchingSessions && (
             <div className="card status-card">
                  <p>Нет доступных сессий. Запустите новую кластеризацию, используя форму выше.</p>
             </div>
         )}

    </div>
  );
};

export default ClusteringDashboard;