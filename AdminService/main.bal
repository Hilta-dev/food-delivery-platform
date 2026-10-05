import ballerina/io;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string USER = ?;
configurable string PASSWORD = ?;
configurable string HOST = ?;
configurable int PORT = ?;
configurable string DATABASE = ?;

final mysql:Client dbClient = check new (
    host = HOST,
    user = USER,
    password = PASSWORD,
    port = PORT,
    database = DATABASE
);

configurable string KAFKA_BROKER = "localhost:9092";

function adminServiceMenu() {
    io:println("\nAdmin Service Menu");
    io:println("1. View All Restaurant Stats");
    io:println("2. View Restaurant Stats by ID");
    io:println("3. View All Driver Stats");
    io:println("4. View Driver Stats by ID");
    io:println("5. Exit");
}

function adminServiceSelection() returns error? {
    while true {
        adminServiceMenu();
        io:println("Enter your Choice: ");
        int choice = check int:fromString(io:readln().trim());

        match choice {
            1 => {
                check viewAllRestaurantStats();
            }
            2 => {
                check viewRestaurantStatsById();
            }
            3 => {
                check viewAllDriverStats();
            }
            4 => {
                check viewDriverStatsById();
            }
            5 => {
                io:println("Exiting Admin Service...");
                break;
            }
            _ => {
                io:println("Invalid choice, try again.");
            }
        }
    }
}

function viewAllRestaurantStats() returns error? {
    io:println("\n--- Restaurant Statistics ---");

    stream<record {|string restaurant_id; int total_orders; int cancelled_orders; decimal total_revenue;|}, sql:Error?> res =
        dbClient->query(`SELECT restaurant_id, total_orders, cancelled_orders, total_revenue FROM restaurant_stats`);

    var row = check res.next();
    if row is () {
        io:println("No restaurant activity recorded yet.");
        return;
    }
    while row is record {|record {|string restaurant_id; int total_orders; int cancelled_orders; decimal total_revenue;|} value;|} {
        io:println("Restaurant " + row.value.restaurant_id + " | Orders: " + row.value.total_orders.toString() +
                " | Cancelled: " + row.value.cancelled_orders.toString() +
                " | Revenue: N$ " + row.value.total_revenue.toString());
        io:println("───────────────────────────────");
        row = check res.next();
    }
}

function viewRestaurantStatsById() returns error? {
    io:print("Restaurant ID: ");
    string restaurantId = io:readln().trim();

    stream<record {|int total_orders; int cancelled_orders; decimal total_revenue;|}, sql:Error?> res =
        dbClient->query(`SELECT total_orders, cancelled_orders, total_revenue FROM restaurant_stats WHERE restaurant_id = ${restaurantId}`);

    var row = check res.next();
    check res.close();

    if row is () {
        io:println("No stats for this restaurant yet.");
        return;
    }
    var r = row.value;
    io:println("Total Orders:     " + r.total_orders.toString());
    io:println("Cancelled Orders: " + r.cancelled_orders.toString());
    io:println("Total Revenue:    N$ " + r.total_revenue.toString());
}

function viewAllDriverStats() returns error? {
    io:println("\n--- Driver / Delivery Performance ---");

    stream<record {|string driver_id; int deliveries_completed; decimal total_delivery_seconds;|}, sql:Error?> res =
        dbClient->query(`SELECT driver_id, deliveries_completed, total_delivery_seconds FROM driver_stats`);

    var row = check res.next();
    if row is () {
        io:println("No delivery activity recorded yet.");
        return;
    }
    while row is record {|record {|string driver_id; int deliveries_completed; decimal total_delivery_seconds;|} value;|} {
        decimal avgSeconds = row.value.deliveries_completed > 0
            ? row.value.total_delivery_seconds / <decimal>row.value.deliveries_completed
            : 0d;
        io:println("Driver " + row.value.driver_id + " | Completed: " + row.value.deliveries_completed.toString() +
                " | Avg Delivery Time: " + avgSeconds.toString() + "s");
        io:println("───────────────────────────────");
        row = check res.next();
    }
}

function viewDriverStatsById() returns error? {
    io:print("Driver ID: ");
    string driverId = io:readln().trim();

    stream<record {|int deliveries_completed; decimal total_delivery_seconds;|}, sql:Error?> res =
        dbClient->query(`SELECT deliveries_completed, total_delivery_seconds FROM driver_stats WHERE driver_id = ${driverId}`);

    var row = check res.next();
    check res.close();

    if row is () {
        io:println("No stats for this driver yet.");
        return;
    }
    var r = row.value;
    decimal avgSeconds = r.deliveries_completed > 0 ? r.total_delivery_seconds / <decimal>r.deliveries_completed : 0d;
    io:println("Deliveries Completed: " + r.deliveries_completed.toString());
    io:println("Avg Delivery Time:    " + avgSeconds.toString() + "s");
}

// CREATE TABLE restaurant_stats (
//     restaurant_id     CHAR(36) PRIMARY KEY,
//     total_orders      INT NOT NULL DEFAULT 0,
//     cancelled_orders  INT NOT NULL DEFAULT 0,
//     total_revenue     DECIMAL(12,2) NOT NULL DEFAULT 0,
//     updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
// );
//
// CREATE TABLE driver_stats (
//     driver_id                CHAR(36) PRIMARY KEY,
//     deliveries_completed     INT NOT NULL DEFAULT 0,
//     total_delivery_seconds   BIGINT NOT NULL DEFAULT 0,
//     updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
// );
//
// -- Tracks in-flight deliveries so driver_stats can compute a duration
// -- once delivery.completed arrives, without calling Delivery Service.
// CREATE TABLE delivery_timers (
//     delivery_id  CHAR(36) PRIMARY KEY,
//     driver_id    CHAR(36) NOT NULL,
//     assigned_at  TIMESTAMP NOT NULL
// );

function recordOrderCreated(string restaurantId) returns error? {
    var result = dbClient->execute(
        `INSERT INTO restaurant_stats (restaurant_id, total_orders, cancelled_orders, total_revenue)
         VALUES (${restaurantId}, 1, 0, 0)
         ON DUPLICATE KEY UPDATE total_orders = total_orders + 1`
    );
    if result is sql:Error {
        log:printError("Failed to record order-created stat", result);
    }
}

function recordOrderStatusChange(string restaurantId, string newStatus, decimal totalAmount) returns error? {
    if newStatus == "CANCELLED" {
        var result = dbClient->execute(
            `INSERT INTO restaurant_stats (restaurant_id, total_orders, cancelled_orders, total_revenue)
             VALUES (${restaurantId}, 0, 1, 0)
             ON DUPLICATE KEY UPDATE cancelled_orders = cancelled_orders + 1`
        );
        if result is sql:Error {
            log:printError("Failed to record cancellation stat", result);
        }
    } else if newStatus == "DELIVERED" {
        var result = dbClient->execute(
            `INSERT INTO restaurant_stats (restaurant_id, total_orders, cancelled_orders, total_revenue)
             VALUES (${restaurantId}, 0, 0, ${totalAmount})
             ON DUPLICATE KEY UPDATE total_revenue = total_revenue + ${totalAmount}`
        );
        if result is sql:Error {
            log:printError("Failed to record revenue stat", result);
        }
    }
}

function recordDeliveryAssigned(string deliveryId, string driverId) returns error? {
    var result = dbClient->execute(
        `INSERT INTO delivery_timers (delivery_id, driver_id, assigned_at) VALUES (${deliveryId}, ${driverId}, NOW())`
    );
    if result is sql:Error {
        log:printError("Failed to record delivery-assigned timer", result);
    }
}

function recordDeliveryCompleted(string deliveryId, string driverId) returns error? {
    stream<record {|string assigned_at;|}, sql:Error?> timerResult =
        dbClient->query(`SELECT assigned_at FROM delivery_timers WHERE delivery_id = ${deliveryId}`);
    var timerRow = check timerResult.next();
    check timerResult.close();

    int elapsedSeconds = 0;
    if timerRow is record {|record {|string assigned_at;|} value;|} {
        stream<record {|int elapsed;|}, sql:Error?> diffResult =
            dbClient->query(`SELECT TIMESTAMPDIFF(SECOND, ${timerRow.value.assigned_at}, NOW()) AS elapsed`);
        var diffRow = check diffResult.next();
        check diffResult.close();
        if diffRow is record {|record {|int elapsed;|} value;|} {
            elapsedSeconds = diffRow.value.elapsed;
        }
    }

    var result = dbClient->execute(
        `INSERT INTO driver_stats (driver_id, deliveries_completed, total_delivery_seconds)
         VALUES (${driverId}, 1, ${elapsedSeconds})
         ON DUPLICATE KEY UPDATE deliveries_completed = deliveries_completed + 1,
                                  total_delivery_seconds = total_delivery_seconds + ${elapsedSeconds}`
    );
    if result is sql:Error {
        log:printError("Failed to record driver delivery stat", result);
    }

    var cleanupResult = dbClient->execute(`DELETE FROM delivery_timers WHERE delivery_id = ${deliveryId}`);
    if cleanupResult is sql:Error {
        log:printError("Failed to clean up delivery timer", cleanupResult);
    }
}

// Entry point
public function main() returns error? {
    io:println("Admin Service started.\n");
    check adminServiceSelection();
}

// Admin Service has no producer - every number it reports is built purely
// by consuming the event stream, so reporting load can never compete with
// or slow down the transactional path (orders, payments, dispatch).
//
// offsetReset is "latest" - stats only accumulate from events that happen
// AFTER this service starts. Start it early in your demo if you want the
// numbers to build up live as you go, rather than all appearing at once.
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "adminService",
    topics: ["orders.created", "orders.status.updated", "delivery.assigned", "delivery.completed"],
    pollingInterval: 1,
    offsetReset: "latest"
};

listener kafka:Listener consumerListener = new (KAFKA_BROKER, consumerConfig);

service on consumerListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord item in records {
            string msg = check string:fromBytes(item.value);
            log:printInfo("Received message from kafka [" + item.offset.partition.topic + "]: " + msg);

            json event = check msg.fromJsonString();

            match item.offset.partition.topic {
                "orders.created" => {
                    string restaurantId = check event.restaurantId.ensureType(string);
                    check recordOrderCreated(restaurantId);
                }
                "orders.status.updated" => {
                    string restaurantId = check event.restaurantId.ensureType(string);
                    string newStatus = check event.newStatus.ensureType(string);
                    decimal totalAmount = check event.totalAmount.ensureType(decimal);
                    check recordOrderStatusChange(restaurantId, newStatus, totalAmount);
                }
                "delivery.assigned" => {
                    string deliveryId = check event.deliveryId.ensureType(string);
                    string driverId = check event.driverId.ensureType(string);
                    check recordDeliveryAssigned(deliveryId, driverId);
                }
                "delivery.completed" => {
                    string deliveryId = check event.deliveryId.ensureType(string);
                    string driverId = check event.driverId.ensureType(string);
                    check recordDeliveryCompleted(deliveryId, driverId);
                }
                _ => {
                    log:printWarn("Unhandled topic in Admin Service: " + item.offset.partition.topic);
                }
            }
        }
    }
}
