export const INITIAL_NODES = [
  {
    id: 'source-1',
    type: 'input',
    data: { label: 'Kafka Source', config: { tableName: 'kafka_orders' } },
    position: { x: 50, y: 100 }
  },
  {
    id: 'transform-1',
    data: {
      label: 'Sliding aggregation',
      config: {
        windowType: 'SLIDING',
        slideValue: 15,
        slideUnit: 'MINUTES',
        windowValue: 1,
        windowUnit: 'HOURS',
        timeAttribute: 'order_time'
      }
    },
    position: { x: 300, y: 100 }
  },
  {
    id: 'sink-1',
    type: 'output',
    data: { label: 'JDBC Postgres Sink', config: { tableName: 'postgres_metrics' } },
    position: { x: 550, y: 100 }
  }
];

export const INITIAL_EDGES = [];
