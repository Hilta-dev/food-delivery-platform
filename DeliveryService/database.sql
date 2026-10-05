CREATE DATABASE IF NOT EXISTS delivery_db;
USE delivery_db;

CREATE TABLE IF NOT EXISTS drivers (
    driver_id     CHAR(70) PRIMARY KEY,
    name          VARCHAR(150) NOT NULL,
    phone         VARCHAR(50) NOT NULL,
    vehicle       VARCHAR(100) NOT NULL,
    is_available  BOOLEAN NOT NULL DEFAULT TRUE,
    current_zone  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id   CHAR(70) PRIMARY KEY,
    order_id      CHAR(70) NOT NULL UNIQUE,
    driver_id     CHAR(70) NOT NULL,
    status        ENUM('ASSIGNED','DELIVERED') NOT NULL DEFAULT 'ASSIGNED',
    assigned_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at  TIMESTAMP NULL,
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE IF NOT EXISTS order_views (
    order_id      CHAR(36) PRIMARY KEY,
    customer_id   CHAR(36) NOT NULL,
    restaurant_id CHAR(36) NOT NULL,
    status        VARCHAR(30) NOT NULL,
    total_amount  DECIMAL(10,2) NOT NULL,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
