import React, { useState, useMemo, useCallback } from 'react';
import { useNodesState, useEdgesState, addEdge } from '@xyflow/react';
import FlowCanvas from './components/FlowCanvas';
import NodeConfigPanel from './components/NodeConfigPanel';
import SqlPreviewPanel from './components/SqlPreviewPanel';
import FlinkLiveMetricsDashboard from './components/FlinkLiveMetricsDashboard';
import { validateFlinkConnectionTopology, compileWindowTransformToSQL } from './utils/flinkCompiler';
import { INITIAL_NODES, INITIAL_EDGES } from './constants/initialPipeline';

const SAMPLE_JOB_ID = 'sample-flink-job-hash-123';
const FLINK_REST_URL = 'http://localhost:8081';

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(INITIAL_NODES);
  const [edges, setEdges, onEdgesChange] = useEdgesState(INITIAL_EDGES);
  const [activeNodeId] = useState('transform-1');

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)),
    [setEdges]
  );

  const isValidConnection = useCallback((conn) => validateFlinkConnectionTopology(conn, nodes), [nodes]);

  const handleConfigUpdate = useCallback(
    (updatedConfig) => {
      setNodes((prev) =>
        prev.map((n) => (n.id === activeNodeId ? { ...n, data: { ...n.data, config: updatedConfig } } : n))
      );
    },
    [activeNodeId, setNodes]
  );

  const activeNode = nodes.find((n) => n.id === activeNodeId);

  const liveGeneratedSQL = useMemo(() => {
    const transformNode = nodes.find((n) => n.id === 'transform-1');
    const { config } = transformNode.data;
    const normalizedNode = {
      ...transformNode,
      data: {
        config: {
          ...config,
          slideInterval: `${config.slideValue} ${config.slideUnit}`,
          windowInterval: `${config.windowValue} ${config.windowUnit}`
        }
      }
    };
    return compileWindowTransformToSQL(normalizedNode, 'kafka_orders', 'postgres_metrics');
  }, [nodes]);

  return (
    <div className="grid grid-cols-4 h-screen bg-slate-950 text-slate-100 font-sans">
      <div className="col-span-3 flex flex-col p-6 gap-4">
        <header className="flex justify-between items-center">
          <h2 className="text-xl font-black text-white tracking-tight">Flink Studio Workspace</h2>
        </header>

        <FlowCanvas
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          isValidConnection={isValidConnection}
        />

        <FlinkLiveMetricsDashboard jobId={SAMPLE_JOB_ID} flinkRestUrl={FLINK_REST_URL} />
      </div>

      <div className="col-span-1 border-l border-slate-800 bg-slate-900/30 p-6 flex flex-col gap-6">
        <NodeConfigPanel node={activeNode} onConfigChange={handleConfigUpdate} />
        <SqlPreviewPanel sql={liveGeneratedSQL} />
      </div>
    </div>
  );
}
