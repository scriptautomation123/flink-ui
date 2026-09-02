import React from 'react';

export default function SlidingWindowForm({ nodeConfig, onConfigChange }) {
  const updateField = (key, value) => onConfigChange({ ...nodeConfig, [key]: value });

  return (
    <div className="space-y-4 bg-slate-900/40 p-4 border border-slate-800 rounded-xl">
      <div>
        <label className="text-xs font-semibold text-slate-400 block mb-1">Slide Interval</label>
        <div className="grid grid-cols-3 gap-2">
          <input
            type="number"
            value={nodeConfig.slideValue || 15}
            onChange={(e) => updateField('slideValue', parseInt(e.target.value) || 0)}
            className="col-span-2 px-3 py-2 bg-slate-950 border border-slate-800 rounded-lg text-sm text-slate-200"
          />
          <select
            value={nodeConfig.slideUnit || 'MINUTES'}
            onChange={(e) => updateField('slideUnit', e.target.value)}
            className="px-2 py-2 bg-slate-950 border border-slate-800 rounded-lg text-xs text-slate-300"
          >
            <option value="MINUTES">MIN</option>
            <option value="HOURS">HRS</option>
          </select>
        </div>
      </div>
      <div>
        <label className="text-xs font-semibold text-slate-400 block mb-1">Window Size</label>
        <div className="grid grid-cols-3 gap-2">
          <input
            type="number"
            value={nodeConfig.windowValue || 1}
            onChange={(e) => updateField('windowValue', parseInt(e.target.value) || 0)}
            className="col-span-2 px-3 py-2 bg-slate-950 border border-slate-800 rounded-lg text-sm text-slate-200"
          />
          <select
            value={nodeConfig.windowUnit || 'HOURS'}
            onChange={(e) => updateField('windowUnit', e.target.value)}
            className="px-2 py-2 bg-slate-950 border border-slate-800 rounded-lg text-xs text-slate-300"
          >
            <option value="MINUTES">MIN</option>
            <option value="HOURS">HRS</option>
          </select>
        </div>
      </div>
    </div>
  );
}
