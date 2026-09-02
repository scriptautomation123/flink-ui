import React from 'react';
import SlidingWindowForm from './SlidingWindowForm';

export default function NodeConfigPanel({ node, onConfigChange }) {
  return (
    <div>
      <h3 className="font-bold text-slate-200 mb-4">Node Configuration</h3>
      {node?.data?.config?.windowType === 'SLIDING' && (
        <SlidingWindowForm nodeConfig={node.data.config} onConfigChange={onConfigChange} />
      )}
    </div>
  );
}
