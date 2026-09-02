import React from 'react';

export default function SqlPreviewPanel({ sql }) {
  return (
    <div className="flex-1 flex flex-col min-h-0">
      <span className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Live SQL Output</span>
      <pre className="bg-slate-950 border border-slate-800 rounded-xl p-4 font-mono text-xs text-emerald-400 overflow-y-auto flex-1 whitespace-pre-wrap">
        {sql}
      </pre>
    </div>
  );
}
