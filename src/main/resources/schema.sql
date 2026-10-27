CREATE TABLE IF NOT EXISTS orders (
                                      order_id VARCHAR(64) PRIMARY KEY,
                                      customer_id VARCHAR(64),
                                      amount DECIMAL(12,2)
);
CREATE TABLE IF NOT EXISTS processed_messages (
                                                  message_id VARCHAR(64) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS outbox (
                                      id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                      event_key VARCHAR(64),
                                      topic VARCHAR(64),
                                      payload VARCHAR(4000),
                                      published BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS placed_orders (
                                             order_id VARCHAR(64) PRIMARY KEY,
                                             customer_id VARCHAR(64),
                                             amount DECIMAL(12,2)
);