CREATE DATABASE IF NOT EXISTS admin_db;
USE admin_db;

CREATE TABLE IF NOT EXISTS restaurant_stats (
    restaurant_id     CHAR(36) PRIMARY KEY,
    total_orders      INT NOT NULL DEFAULT 0,
    cancelled_orders  INT NOT NULL DEFAULT 0,
    total_revenue     DECIMAL(12,2) NOT NULL DEFAULT 0,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS driver_stats (
    driver_id                CHAR(36) PRIMARY KEY,
    deliveries_completed     INT NOT NULL DEFAULT 0,
    total_delivery_seconds   BIGINT NOT NULL DEFAULT 0,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_timers (
    delivery_id  CHAR(36) PRIMARY KEY,
    driver_id    CHAR(36) NOT NULL,
    assigned_at  TIMESTAMP NOT NULL
);
