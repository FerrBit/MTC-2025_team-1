type FetchWithAuth = (url: string, options?: RequestInit) => Promise<Response>;

export interface StartClusteringPayload {
    algorithm: 'kmeans' | 'dbscan';
    params: { [key: string]: number };
    embeddingFile: File;
    imageArchive?: File | null;
}

export interface ClusterResult {
    id: string | number;
    original_id: string | null;
    name: string | null;
    size: number;
    contactSheetUrl: string | null;
    metrics?: any;
    centroid_2d?: [number, number] | null;
}

export interface AdjustmentLogEntry {
    timestamp: string;
    action_type: string;
    details: any;
}

export interface SessionResultResponse {
    session_id: string;
    status: string;
    original_filename: string | null;
    algorithm: string;
    params: { [key: string]: number | string };
    num_clusters: number | null;
    processing_time_sec: number | null;
    clusters: ClusterResult[];
    scatter_data?: ScatterPoint[] | { error: string } | { message: string } | null | undefined;
    scatter_pca_time_sec?: number | null;
    message?: string;
    error?: string;
    adjustments?: AdjustmentLogEntry[];
}

export interface SessionListItem {
     session_id: string;
     created_at: string;
     status: string;
     algorithm: string;
     params: { [key: string]: number | string };
     num_clusters: number | null;
     result_message: string | null;
     original_filename: string | null;
}

export interface ScatterPoint {
    x: number;
    y: number;
    cluster: string;
}

export interface MergeClustersPayload {
    action: 'MERGE_CLUSTERS';
    cluster_ids_to_merge: (string | number)[];
}

export interface SplitClusterPayload {
    action: 'SPLIT_CLUSTER';
    cluster_id_to_split: string | number;
    num_splits?: number;
}

export interface RenameClusterPayload {
    action: 'RENAME';
    cluster_id: string | number;
    new_name: string;
}

export interface AdjustResult {
    message: string;
    cluster?: { id: string | number; name: string | null; size: number };
}


const handleResponse = async (response: Response) => {
    if (response.status === 204) {
        return null;
    }
    let data;
    try {
        data = await response.json();
    } catch (e) {
        if (!response.ok) {
             throw new Error(`HTTP error! Status: ${response.status}, Body is not JSON.`);
        }
        console.warn("Response was OK but body is not JSON.");
        return null;
    }
    if (!response.ok) {
        throw new Error(data?.error || data?.message || `HTTP error! Status: ${response.status}`);
    }
    return data;
};

const triggerDownload = async (response: Response, defaultFilename: string) => {
    const header = response.headers.get('Content-Disposition');
    let filename = defaultFilename;
    if (header) {
        const parts = header.split(';');
        const filenamePart = parts.find(part => part.trim().startsWith('filename='));
        if (filenamePart) {
            filename = filenamePart.split('=')[1].trim().replace(/"/g, '');
        }
    }
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    a.remove();
};

export const startClustering = async (fetchWithAuth: FetchWithAuth, data: StartClusteringPayload): Promise<{ session_id: string }> => {
    const formData = new FormData();
    formData.append('embeddingFile', data.embeddingFile);
    formData.append('algorithm', data.algorithm);
    formData.append('params', JSON.stringify(data.params));
    if (data.imageArchive) {
        formData.append('imageArchive', data.imageArchive);
    }
    const response = await fetchWithAuth('/api/clustering/start', { method: 'POST', body: formData });
    return handleResponse(response);
};

export const getClusteringSessions = async (fetchWithAuth: FetchWithAuth): Promise<SessionListItem[]> => {
    const response = await fetchWithAuth('/api/clustering/sessions');
    return handleResponse(response);
};

export const getClusteringResults = async (fetchWithAuth: FetchWithAuth, sessionId: string): Promise<SessionResultResponse> => {
    const response = await fetchWithAuth(`/api/clustering/results/${sessionId}`);
    const results: SessionResultResponse = await handleResponse(response);
    return results;
};

export const deleteAndRedistributeCluster = async (fetchWithAuth: FetchWithAuth, sessionId: string, clusterLabel: string | number): Promise<{ message: string }> => {
    const response = await fetchWithAuth(`/api/clustering/results/${sessionId}/cluster/${clusterLabel}`, { method: 'DELETE' });
    return handleResponse(response);
};

export const renameCluster = async (fetchWithAuth: FetchWithAuth, sessionId: string, clusterId: string | number, newName: string): Promise<AdjustResult> => {
    const payload: RenameClusterPayload = { action: 'RENAME', cluster_id: clusterId, new_name: newName };
    const response = await fetchWithAuth(`/api/clustering/results/${sessionId}/adjust`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    return handleResponse(response);
};

export const mergeSelectedClusters = async (fetchWithAuth: FetchWithAuth, sessionId: string, clusterIds: (string | number)[]): Promise<AdjustResult> => {
    const payload: MergeClustersPayload = { action: 'MERGE_CLUSTERS', cluster_ids_to_merge: clusterIds };
    const response = await fetchWithAuth(`/api/clustering/results/${sessionId}/adjust`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    return handleResponse(response);
};

export const splitSelectedCluster = async (fetchWithAuth: FetchWithAuth, sessionId: string, clusterId: string | number, numSplits: number = 2): Promise<AdjustResult> => {
    const payload: SplitClusterPayload = { action: 'SPLIT_CLUSTER', cluster_id_to_split: clusterId, num_splits: numSplits };
    const response = await fetchWithAuth(`/api/clustering/results/${sessionId}/adjust`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    return handleResponse(response);
};

export const exportAssignmentsCsv = async (fetchWithAuth: FetchWithAuth, sessionId: string): Promise<void> => {
    const response = await fetchWithAuth(`/api/clustering/export/${sessionId}/assignments.csv`);
    if (!response.ok) {
         const errorData = await response.json().catch(() => ({ error: `HTTP error ${response.status}`}));
         throw new Error(errorData.error || `Failed to export assignments CSV: ${response.status}`);
    }
    await triggerDownload(response, `session_${sessionId}_assignments.csv`);
};

export const exportClusterSummaryJson = async (fetchWithAuth: FetchWithAuth, sessionId: string): Promise<void> => {
    const response = await fetchWithAuth(`/api/clustering/export/${sessionId}/cluster_summary.json`);
     if (!response.ok) {
         const errorData = await response.json().catch(() => ({ error: `HTTP error ${response.status}`}));
         throw new Error(errorData.error || `Failed to export cluster summary JSON: ${response.status}`);
    }
    await triggerDownload(response, `session_${sessionId}_cluster_summary.json`);
};

export const exportSessionSummaryJson = async (fetchWithAuth: FetchWithAuth, sessionId: string): Promise<void> => {
    const response = await fetchWithAuth(`/api/clustering/export/${sessionId}/session_summary.json`);
     if (!response.ok) {
         const errorData = await response.json().catch(() => ({ error: `HTTP error ${response.status}`}));
         throw new Error(errorData.error || `Failed to export session summary JSON: ${response.status}`);
    }
    await triggerDownload(response, `session_${sessionId}_session_summary.json`);
};