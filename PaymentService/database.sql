CREATE DATABASE IF NOT EXISTS payment_db;
USE payment_db;

CREATE TABLE IF NOT EXISTS payments (
    payment_id      CHAR(70) PRIMARY KEY,
    order_id        CHAR(70) NOT NULL UNIQUE,
    amount          DECIMAL(10,2) NOT NULL,
    status          ENUM('COMPLETED','FAILED') NOT NULL,
    transaction_ref VARCHAR(100),
    reason          VARCHAR(255),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
