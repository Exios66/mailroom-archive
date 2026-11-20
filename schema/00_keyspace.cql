CREATE KEYSPACE IF NOT EXISTS {ks}
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': {rf}};
