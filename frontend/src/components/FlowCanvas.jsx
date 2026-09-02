import React from 'react';
import { ReactFlow, Background, Controls, MiniMap } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import './ReactFlowFlink.css';

export default function FlowCanvas({ nodes, edges, onNodesChange, onEdgesChange, onConnect, isValidConnection }) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden h-[450px]">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        isValidConnection={isValidConnection}
        fitView
      >
        <Background color="#334155" gap={16} />
        <Controls />
        <MiniMap maskColor="rgba(15, 23, 42, 0.7)" />
      </ReactFlow>
    </div>
  );
}
