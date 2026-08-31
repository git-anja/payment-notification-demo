
CREATE TABLE IF NOT EXISTS clients (
  client_id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  webhook_url TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO clients(client_id, name, webhook_url) VALUES
('amazon', 'Amazon', 'http://amazon-mock:8082/webhook'),
('udemy', 'Udemy', 'http://udemy-mock:8083/webhook')
ON CONFLICT (client_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS payments (
  payment_id VARCHAR(100) PRIMARY KEY,
  order_id VARCHAR(100) NOT NULL,
  client_id VARCHAR(50) NOT NULL,
  amount BIGINT NOT NULL,
  currency VARCHAR(10) NOT NULL,
  status VARCHAR(30) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS notification_delivery (
  event_id VARCHAR(100) PRIMARY KEY,
  payment_id VARCHAR(100) NOT NULL,
  client_id VARCHAR(50) NOT NULL,
  webhook_url TEXT NOT NULL,
  status VARCHAR(30) NOT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMP NULL,
  last_error TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
