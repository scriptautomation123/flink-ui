
/**
 * Custom React Flow interface callback connection validation function.
 */
export const validateFlinkConnectionTopology = (connection, nodes) => {
  const sourceNode = nodes.find((n) => n.id === connection.source);
  const targetNode = nodes.find((n) => n.id === connection.target);

  if (!sourceNode || !targetNode) return false;

  if (connection.source === connection.target) return false;
  if (sourceNode.type === 'input' && targetNode.type === 'input') return false;
  if (sourceNode.type === 'output' && targetNode.type === 'output') return false;
  if (sourceNode.type === 'output' && targetNode.type === 'input') return false;

  return true;
};

/**
 * Processes intermediate window transformation metadata into structured append data queries.
 */
export function compileWindowTransformToSQL(transformNode, sourceTable, sinkTable) {
  const config = transformNode.data?.config || {};
  const windowType = config.windowType;
  const timeField = config.timeAttribute || 'order_time';
  
  let query = `INSERT INTO ${sinkTable}\nSELECT \n  product_id,\n`;

  if (windowType === 'SLIDING') {
    const slideSize = config.slideInterval || "15 MINUTES";
    const windowSize = config.windowInterval || "1 HOUR";
    
    query += `  SUM(amount) AS total_revenue,\n`;
    query += `  HOP_START(${timeField}, INTERVAL '${slideSize}', INTERVAL '${windowSize}') AS window_start,\n`;
    query += `  HOP_END(${timeField}, INTERVAL '${slideSize}', INTERVAL '${windowSize}') AS window_end\n`;
    query += `FROM ${sourceTable}\n`;
    query += `GROUP BY \n  product_id, \n  HOP(${timeField}, INTERVAL '${slideSize}', INTERVAL '${windowSize}');\n`;
    
  } else if (windowType === 'SESSION') {
    const gapSize = config.gapInterval || "30 MINUTES";
    
    query += `  SUM(amount) AS total_revenue,\n`;
    query += `  SESSION_START(${timeField}, INTERVAL '${gapSize}') AS window_start,\n`;
    query += `  SESSION_END(${timeField}, INTERVAL '${gapSize}') AS window_end\n`;
    query += `FROM ${sourceTable}\n`;
    query += `GROUP BY \n  product_id, \n  SESSION(${timeField}, INTERVAL '${gapSize}');\n`;
    
  } else {
    query += `  SUM(amount) AS total_revenue\nFROM ${sourceTable}\nGROUP BY product_id;\n`;
  }

  return query;
}

