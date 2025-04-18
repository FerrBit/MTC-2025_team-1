import React, { useMemo } from 'react';
import { SessionResultResponse, ClusterResult, ScatterPoint } from '../services/api';
import { Bar, Scatter } from 'react-chartjs-2';
import { ChartOptions, ChartData } from 'chart.js';
import '../styles/ChartsDisplay.css';

interface ChartsDisplayProps {
    details: SessionResultResponse | null;
    sessionId: string | null;
}

type ScatterDataType = ScatterPoint[] | { error: string } | { message: string } | null | undefined;

const generateColor = (index: number, total: number, saturation = 70, lightness = 60): string => {
  const validIndex = Math.max(0, isNaN(index) ? 0 : index);
  const validTotal = Math.max(1, isNaN(total) ? 1 : total);
  const hue = (validIndex * (360 / validTotal)) % 360;
  return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
};


const OUTLIER_COLOR = 'rgba(150, 150, 150, 0.5)';

const sortClustersNumerically = <T extends { id: string | number }>(clusters: T[]): T[] => {
    return [...clusters].sort((a, b) => {
        const numA = parseFloat(String(a.id));
        const numB = parseFloat(String(b.id));
        if (isNaN(numA) && isNaN(numB)) return String(a.id).localeCompare(String(b.id));
        if (isNaN(numA)) return 1;
        if (isNaN(numB)) return -1;
        return numA - numB;
    });
};

const ChartsDisplay: React.FC<ChartsDisplayProps> = ({ details, sessionId }) => {

    const { barChartData, centroidChartData, scatterChartData, scatterMessage, scatterError } = useMemo(() => {
        const result: {
            barChartData: ChartData<'bar'> | null;
            centroidChartData: ChartData<'scatter'> | null;
            scatterChartData: ChartData<'scatter'> | null;
            scatterMessage: string | null;
            scatterError: string | null;
        } = { barChartData: null, centroidChartData: null, scatterChartData: null, scatterMessage: null, scatterError: null };

        if (!details || (details.status !== 'SUCCESS' && details.status !== 'RECLUSTERED' && details.status !== 'PROCESSING')) {
             if (details?.status !== 'PROCESSING') { return result; }
        }

        const clustersMap = new Map<string | number, ClusterResult>();
        details.clusters?.forEach(c => clustersMap.set(c.id, c));

        if (details.clusters && details.clusters.length > 0) {
            const sortedClustersForBar = sortClustersNumerically<ClusterResult>(details.clusters);
            const barLabels = sortedClustersForBar.map(c => c.name || `Кластер ${c.id}`);
            const barDataPoints = sortedClustersForBar.map(c => c.size);
            result.barChartData = {
                labels: barLabels,
                datasets: [{
                    label: 'Размер кластера (кол-во изображений)',
                    data: barDataPoints,
                    backgroundColor: 'rgba(0, 123, 255, 0.6)',
                    borderColor: 'rgba(0, 123, 255, 1)',
                    borderWidth: 1,
                }],
             };
        }

        const clustersWith2dCentroids = details.clusters?.filter(c => c.centroid_2d && c.centroid_2d.length === 2) || [];
        if (clustersWith2dCentroids.length > 0) {
            const sortedCentroids = sortClustersNumerically<ClusterResult>(clustersWith2dCentroids);

            let maxCentroidIdNum = -1;
            sortedCentroids.forEach(c => {
                const numId = parseInt(String(c.id), 10);
                if (!isNaN(numId)) {
                    maxCentroidIdNum = Math.max(maxCentroidIdNum, numId);
                }
            });
            const numCentroidColors = Math.max(1, maxCentroidIdNum + 1, sortedCentroids.length);

            result.centroidChartData = {
                datasets: sortedCentroids.map((cluster, index) => {
                    let clusterIndexForColor = -1;
                    const numId = parseInt(String(cluster.id), 10);
                    if (!isNaN(numId)) {
                        clusterIndexForColor = numId;
                    } else {
                        clusterIndexForColor = index;
                    }

                    return {
                        label: cluster.name || `Центр ${cluster.id}`,
                        data: [{ x: cluster.centroid_2d![0], y: cluster.centroid_2d![1] }],
                        backgroundColor: generateColor(clusterIndexForColor, numCentroidColors),
                        pointRadius: 6,
                        pointHoverRadius: 8,
                    };
                }),
            };
        }

        const scatterDataRaw: ScatterDataType = details.scatter_data;

        if (scatterDataRaw) {
            if (Array.isArray(scatterDataRaw)) {
                const points = scatterDataRaw;
                if (points.length === 0) {
                     result.scatterMessage = "Нет данных для отображения на графике рассеяния.";
                } else {
                    const clustersPresentInScatter = Array.from(new Set(points.map(p => p.cluster))).sort((a, b) => {
                         const numA = parseInt(a, 10); const numB = parseInt(b, 10);
                         if (a === '-1') return -1;
                         if (b === '-1') return 1;
                         if (!isNaN(numA) && !isNaN(numB)) return numA - numB;
                         return a.localeCompare(b);
                    });

                    let maxScatterIdNum = -1;
                    clustersPresentInScatter.forEach(id => {
                         if (id === '-1') return;
                         const numId = parseInt(id, 10);
                         if (!isNaN(numId)) {
                             maxScatterIdNum = Math.max(maxScatterIdNum, numId);
                         }
                     });
                     const numScatterColors = Math.max(1, maxScatterIdNum + 1, clustersPresentInScatter.filter(id => id !== '-1').length);

                    result.scatterChartData = {
                        datasets: clustersPresentInScatter.map((clusterIdStr, index) => {
                            const clusterPoints = points
                                .filter(p => p.cluster === clusterIdStr)
                                .map(p => ({ x: p.x, y: p.y }));

                            const isOutlier = clusterIdStr === '-1';
                            let clusterMeta: ClusterResult | undefined;
                            if (!isOutlier) {
                                const numId = parseInt(clusterIdStr, 10);
                                if (!isNaN(numId)) {
                                    clusterMeta = clustersMap.get(numId);
                                }
                                if (!clusterMeta) {
                                    clusterMeta = clustersMap.get(clusterIdStr);
                                }
                            } else {
                                clusterMeta = clustersMap.get("-1") ?? clustersMap.get(-1);
                            }
                            const clusterName = clusterMeta?.name;

                            let clusterIndexForColor = -1;
                            if (!isOutlier) {
                                const numId = parseInt(clusterIdStr, 10);
                                if (!isNaN(numId)) {
                                    clusterIndexForColor = numId;
                                } else {
                                     clusterIndexForColor = index;
                                }
                            }

                            return {
                                label: isOutlier ? 'Шум (-1)' : (clusterName || `Кластер ${clusterIdStr}`),
                                data: clusterPoints,
                                backgroundColor: isOutlier ? OUTLIER_COLOR : generateColor(clusterIndexForColor, numScatterColors, 60, 70),
                                pointRadius: 2.5,
                                pointHoverRadius: 4,
                                showLine: false,
                                borderColor: 'transparent'
                            };
                        }),
                    };
                }
            } else if (typeof scatterDataRaw === 'object' && scatterDataRaw !== null && 'error' in scatterDataRaw && typeof scatterDataRaw.error === 'string' && scatterDataRaw.error.length > 0) {
                result.scatterError = scatterDataRaw.error;
            } else if (typeof scatterDataRaw === 'object' && scatterDataRaw !== null && 'message' in scatterDataRaw && typeof scatterDataRaw.message === 'string' && scatterDataRaw.message.length > 0) {
                result.scatterMessage = scatterDataRaw.message;
            } else if (typeof scatterDataRaw === 'object' && scatterDataRaw !== null && Object.keys(scatterDataRaw).length === 0) {
                 console.warn("Получены пустые объектные данные для scatter_data:", scatterDataRaw);
                 result.scatterError = "Получены некорректные пустые данные для графика.";
            } else {
                 console.warn("Получен неожиданный формат для scatter_data:", scatterDataRaw);
                 result.scatterError = "Получен неизвестный формат данных для графика.";
            }
        } else if (details.status === 'SUCCESS' || details.status === 'RECLUSTERED') {
             result.scatterError = "Данные для графика рассеяния не были сгенерированы или отсутствуют.";
        } else if (details.status === 'PROCESSING'){
             result.scatterMessage = "Генерация данных...";
        }

        return result;
    }, [details]);

    const barChartOptions = useMemo((): ChartOptions<'bar'> => ({
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' as const },
            title: { display: false },
            tooltip: {
                callbacks: {
                    label: function(context) {
                        let label = context.dataset.label || '';
                        if (label) { label += ': '; }
                        if (context.parsed.y !== null) {
                             label += `${context.parsed.y} изображений`;
                        }
                        return label;
                    },
                    title: function(context) {
                        return context[0]?.label || '';
                    }
                }
            }
        },
        scales: {
            y: { beginAtZero: true, title: { display: true, text: 'Количество изображений' } },
            x: { title: { display: true, text: 'Кластер' } }
        },
    }), []);

    const centroidChartOptions = useMemo((): ChartOptions<'scatter'> => ({
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'right' as const, display: (details?.clusters?.length ?? 0) <= 20 },
            title: { display: false },
            tooltip: {
                callbacks: {
                    label: function(context) {
                        const label = context.dataset.label || '';
                        const point = context.parsed;
                        return `${label}: (x: ${point.x.toFixed(2)}, y: ${point.y.toFixed(2)})`;
                    }
                }
            }
        },
        scales: {
            x: { type: 'linear', position: 'bottom', title: { display: true, text: 'Компонента PCA 1' } },
            y: { type: 'linear', title: { display: true, text: 'Компонента PCA 2' } }
        },
    }), [details?.clusters]);

    const scatterChartOptions = useMemo((): ChartOptions<'scatter'> => ({
        responsive: true,
        maintainAspectRatio: false,
         plugins: {
            legend: {
                position: 'right' as const,
                display: (scatterChartData?.datasets?.length ?? 0) <= 15,
            },
            title: { display: false },
            tooltip: {
                 callbacks: {
                    label: function(context) {
                        return context.dataset.label || '';
                    }
                }
            },
            zoom: {
               zoom: {
                 wheel: { enabled: true },
                 pinch: { enabled: true },
                 mode: 'xy',
               },
                pan: {
                    enabled: true,
                    mode: 'xy',
                },
            }
        },
        scales: {
             x: { type: 'linear', position: 'bottom', title: { display: true, text: 'Компонента PCA 1' } },
             y: { type: 'linear', title: { display: true, text: 'Компонента PCA 2' } }
        },
    }), [scatterChartData]);

    const scatterDescription = useMemo(() => {
        if (!details) {
            return "Описание графика недоступно.";
        }
        if (scatterMessage) return scatterMessage;
        if (scatterError) return `Ошибка: ${scatterError}`;

        const dataArray = Array.isArray(details?.scatter_data) ? details.scatter_data : null;
        const pointCount = dataArray ? dataArray.length : 'N/A';
        let desc = `Визуализация сэмпла изображений (${pointCount} точек) в 2D пространстве после PCA, окрашенных по кластерам.`;
        desc += " Используйте колесо мыши/жесты для зума/перемещения.";
        if (details?.scatter_pca_time_sec !== null && details?.scatter_pca_time_sec !== undefined) {
            desc += ` Время расчета PCA: ${details.scatter_pca_time_sec.toFixed(2)} сек.`;
        }
        return desc;
    }, [details, scatterMessage, scatterError]);

    if (!details || (details.status !== 'SUCCESS' && details.status !== 'RECLUSTERED' && details.status !== 'PROCESSING')) {
        return null;
    }

    const chartKeyBase = sessionId || 'no-session';

    return (
        <div className="card charts-card">
            <h3 className="charts-main-title">Визуализации кластеризации</h3>
            <div className="charts-column-layout">

                <div className='chart-wrapper' style={{marginBottom: '2rem'}}>
                    <h4>Распределение размеров кластеров</h4>
                    {barChartData ? ( <div className="chart-container" style={{ height: '350px' }}> <Bar key={`${chartKeyBase}-bar`} options={barChartOptions} data={barChartData} /> </div> )
                    : (<p className='chart-placeholder-text'>Нет данных для графика размеров.</p>)
                    }
                    <p className="chart-description">Визуализация распределения изображений по кластерам.</p>
                </div>

                <div className='chart-wrapper' style={{marginBottom: '2rem'}}>
                    <h4>График центроидов (PCA)</h4>
                    {centroidChartData ? ( <div className="chart-container" style={{ height: '400px' }}> <Scatter key={`${chartKeyBase}-centroid-scatter`} options={centroidChartOptions} data={centroidChartData} /> </div> )
                    : ( details.status === 'PROCESSING' ? <p className='chart-placeholder-text'>Расчет 2D координат...</p> : <p className='chart-placeholder-text'>Нет активных кластеров для графика центроидов.</p> )
                    }
                    <p className="chart-description">Визуализация центров кластеров в 2D пространстве после PCA.</p>
                </div>

                 <div className='chart-wrapper' style={{marginBottom: '2rem'}}>
                    <h4>График рассеяния изображений (PCA)</h4>
                    {scatterChartData ? (
                        <div className="chart-container" style={{ height: '500px' }}>
                            <Scatter key={`${chartKeyBase}-embedding-scatter`} options={scatterChartOptions} data={scatterChartData} />
                        </div>
                    ) : scatterError ? (
                         <p className='chart-placeholder-text error'>{scatterError}</p>
                    ) : scatterMessage ? (
                         <p className='chart-placeholder-text'>{scatterMessage}</p>
                    ) : (
                         <p className='chart-placeholder-text'>Данные для графика рассеяния недоступны.</p>
                    )}
                    <p className="chart-description">{scatterDescription}</p>
                </div>
            </div>
        </div>
    );
};

export default ChartsDisplay;