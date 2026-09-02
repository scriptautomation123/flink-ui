import React from 'react';
import { useFlinkJobMetrics } from '../hooks/useFlinkJobMetrics';

export default function FlinkLiveMetricsDashboard({ jobId, flinkRestUrl }) {
  const metrics = useFlinkJobMetrics(jobId, flinkRestUrl);

  if (!metrics) {
    return <div className="text-xs text-slate-500">Connecting to Flink metrics engine...</div>;
  }

  return (
    <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl flex justify-between items-center">
      <div>
        <h4 className="font-bold text-white text-sm">{metrics.name}</h4>
        <p className="text-[11px] text-slate-500 font-mono">{jobId}</p>
      </div>
      <span className="px-3 py-1 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-full text-xs font-semibold">
        {metrics.status}
      </span>
    </div>
  );
}
