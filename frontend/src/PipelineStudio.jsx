

import React, { useState, useMemo, useEffect, useCallback } from 'react';
import ReactFlow, { Background, Controls, MiniMap, addEdge, useNodesState, useEdgesState } from 'reactflow';
import { validateFlinkConnectionTopology, compileWindowTransformToSQL } from './flinkCompiler';
import { triggerFlinkSavepoint, pollSavepointStatus } from './flinkApiUtils';
import 'reactflow/dist/style.css';
import './ReactFlowFlink.css';

// Form Component Definition
function SlidingWindowForm({ nodeConfig, onConfigChange }) {
  const updateField = (key, value) => onConfigChange({ ...nodeConfig, [key]: value });
  return (
    <div className="space-y-4 bg-slate-900/40 p-4 border border-slate-800 rounded-xl">
      <div>
        <label className="text-xs font-semibold text-slate-400 block mb-1">Slide Interval</label>
        <div className="grid grid-cols-3 gap-2">
          <input type="number" value={nodeConfig.slideValue || 15} onChange={(e) => updateField('slideValue', parseInt(e.target.value) || 0)} className="col-span-2 px-3 py-2 bg-slate-950 border border-slate-800 rounded-lg text-sm text-slate-200" />
          <select value={nodeConfig.slideUnit || 'MINUTES'} onChange={(e) => updateField('slideUnit', e.target.value)} className="px-2 py-2 bg-slate-950 border border-slate-800 rounded-lg text-xs text-slate-300">
            <option value="MINUTES">MIN</option>
            <option value="HOURS">HRS</option>
          </select>
        </div>
      </div>
      <div>
        <label className="text-xs font-semibold text-slate-400 block mb-1">Window Size</label>
        <div className="grid grid-cols-3 gap-2">
          <input type="number" value={nodeConfig.windowValue || 1} onChange={(e) => updateField('windowValue', parseInt(e.target.value) || 0)} className="col-span-2 px-3 py-2 bg-slate-950 border border-slate-800 rounded-lg text-sm text-slate-200" />
          <select value={nodeConfig.windowUnit || 'HOURS'} onChange={(e) => updateField('windowUnit', e.target.value)} className="px-2 py-2 bg-slate-950 border border-slate-800 rounded-lg text-xs text-slate-300">
            <option value="MINUTES">MIN</option>
            <option value="HOURS">HRS</option>
          </select>
        </div>
      </div>
    </div>
  );
}

// Runtime Metrics Aggregator Widget
function FlinkLiveMetricsDashboard({ jobId, flinkRestUrl }) {
  const [metrics, setMetrics] = useState(null);
  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${flinkRestUrl}/jobs/${jobId}`);
        if (res.ok) {
          const data = await res.json();
          setMetrics({ name: data.name, status: data.status });
        }
      } catch (e) { console.error("Metrics loop offline", e); }
    }, 2000);
    return () => clearInterval(interval);
  }, [jobId, flinkRestUrl]);

  if (!metrics) return <div className="text-xs text-slate-500">Connecting to Flink metrics engine...</div>;
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

// Master Studio Blueprint Controller
export default function PipelineStudioController() {
  const [nodes, setNodes, onNodesChange] = useNodesState([
    { id: 'source-1', type: 'input', data: { label: 'Kafka Source', config: { tableName: 'kafka_orders' } }, position: { x: 50, y: 100 } },
    { id: 'transform-1', data: { label: 'Sliding aggregation', config: { windowType: 'SLIDING', slideValue: 15, slideUnit: 'MINUTES', windowValue: 1, windowUnit: 'HOURS', timeAttribute: 'order_time' } }, position: { x: 300, y: 100 } },
    { id: 'sink-1', type: 'output', data: { label: 'JDBC Postgres Sink', config: { tableName: 'postgres_metrics' } }, position: { x: 550, y: 100 } }
  ]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [activeNodeId] = useState('transform-1');

  const onConnect = useCallback((params) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)), [setEdges]);

  const handleConfigUpdate = (updatedConfig) => {
    setNodes((prev) => prev.map((n) => n.id === activeNodeId ? { ...n, data: { ...n.data, config: updatedConfig } } : n));
  };

  const activeNode = nodes.find((n) => n.id === activeNodeId);

  const liveGeneratedSQL = useMemo(() => {
    const tNode = nodes.find(n => n.id === 'transform-1');
    const normNode = {
      ...tNode,
      data: {
        config: {
          ...tNode.data.config,
          slideInterval: `${tNode.data.config.slideValue} ${tNode.data.config.slideUnit}`,
          windowInterval: `${tNode.data.config.windowValue} ${tNode.data.config.windowUnit}`
        }
      }
    };
    return compileWindowTransformToSQL(normNode, 'kafka_orders', 'postgres_metrics');
  }, [nodes]);

  return (
    <div className="grid grid-cols-4 h-screen bg-slate-950 text-slate-100 font-sans">
      <div className="col-span-3 flex flex-col p-6 gap-4">
        <header className="flex justify-between items-center">
          <h2 className="text-xl font-black text-white tracking-tight">Flink Studio Workspace</h2>
        </header>
        
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden h-[450px]">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            isValidConnection={(conn) => validateFlinkConnectionTopology(conn, nodes)}
            fitView
          >
            <Background color="#334155" gap={16} />
            <Controls />
            <MiniMap maskColor="rgba(15, 23, 42, 0.7)" />
          </ReactFlow>
        </div>

        <FlinkLiveMetricsDashboard jobId="sample-flink-job-hash-123" flinkRestUrl="http://localhost:8081" />
      </div>

      <div className="col-span-1 border-l border-slate-800 bg-slate-900/30 p-6 flex flex-col gap-6">
        <div>
          <h3 className="font-bold text-slate-200 mb-4">Node Configuration</h3>
          {activeNode && activeNode.data?.config?.windowType === 'SLIDING' && (
            <SlidingWindowForm nodeConfig={activeNode.data.config} onConfigChange={handleConfigUpdate} />
          )}
        </div>

        <div className="flex-1 flex flex-col min-h-0">
          <span className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Live SQL Output</span>
          <pre className="bg-slate-950 border border-slate-800 rounded-xl p-4 font-mono text-xs text-emerald-400 overflow-y-auto flex-1 whitespace-pre-wrap">
            {liveGeneratedSQL}
          </pre>
        </div>
      </div>
    </div>
  );
}

