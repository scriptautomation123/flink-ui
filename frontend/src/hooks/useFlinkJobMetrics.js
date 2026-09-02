import { useEffect, useState } from 'react';

/**
 * Polls a Flink job's status from the REST API until unmounted.
 */
export function useFlinkJobMetrics(jobId, flinkRestUrl, pollIntervalMs = 2000) {
  const [metrics, setMetrics] = useState(null);

  useEffect(() => {
    let cancelled = false;

    const fetchMetrics = async () => {
      try {
        const res = await fetch(`${flinkRestUrl}/jobs/${jobId}`);
        if (!res.ok) return;
        const data = await res.json();
        if (!cancelled) setMetrics({ name: data.name, status: data.status });
      } catch (e) {
        console.error('Metrics loop offline', e);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, pollIntervalMs);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [jobId, flinkRestUrl, pollIntervalMs]);

  return metrics;
}
