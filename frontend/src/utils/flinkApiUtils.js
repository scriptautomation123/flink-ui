
/**
 * Triggers an asynchronous savepoint checkpoint event.
 */
export async function triggerFlinkSavepoint(flinkRestUrl, jobId, targetDirectory = "/tmp/flink-savepoints") {
  const endpoint = `${flinkRestUrl}/jobs/${jobId}/savepoints`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ "target-directory": targetDirectory, "cancel-job": false })
  });

  if (!response.ok) throw new Error(`Flink cluster error: ${response.status}`);
  const result = await response.json();
  return result["request-id"];
}

/**
 * Automatically polls the Flink REST endpoint to monitor the progress of a triggered savepoint.
 */
export async function pollSavepointStatus(flinkRestUrl, jobId, requestId, maxRetries = 30, delayMs = 2000) {
  const statusEndpoint = `${flinkRestUrl}/jobs/${jobId}/savepoints/${requestId}`;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const response = await fetch(statusEndpoint);
    if (!response.ok) throw new Error(`Status path failure: ${response.status}`);

    const report = await response.json();
    const state = report.status?.id;

    if (state === 'COMPLETED') return report.operation;
    if (state === 'FAILED') throw new Error(`Flink interior failure message: ${report.failureCause?.['class']}`);
    
    await new Promise(resolve => setTimeout(resolve, delayMs));
  }

  throw new Error(`The snapshot confirmation lifecycle timed out.`);
}

