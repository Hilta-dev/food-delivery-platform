import ballerina/io;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

public type Driver record {|
    string driverId;
    string name;
    string phone;
    string vehicle;
    boolean isAvailable = true;
    string currentZone;
|};

public type Delivery record {|
    string deliveryId;
    string orderId;
    string driverId;
    string status;
|};

// Local cache of order details, kept current by consuming
// orders.status.updated for every status (not just READY). This service
// needs customer_id/restaurant_id/total_amount on hand to re-publish a
// complete orders.status.updated event itself once a delivery is
// assigned or completed.
type OrderView record {|
    string status;
    string customer_id;
    string restaurant_id;
    decimal total_amount;
|};

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

function deliveryServiceMenu() {
    io:println("\nDelivery Service Menu");
    io:println("1. Register Driver");
    io:println("2. Set Driver Availability");
    io:println("3. View Deliveries In Progress");
    io:println("4. Mark Delivery Complete");
    io:println("5. Exit");
}

function deliveryServiceSelection() returns error? {
    while true {
        deliveryServiceMenu();
        io:println("Enter your Choice: ");
        int choice = check int:fromString(io:readln().trim());

        match choice {
            1 => {
                check registerDriver();
            }
            2 => {
                check setDriverAvailability();
            }
            3 => {
                check viewDeliveriesInProgress();
            }
            4 => {
                check markDeliveryComplete();
            }
            5 => {
                io:println("Exiting Delivery Service...");
                break;
            }
            _ => {
                io:println("Invalid choice, try again.");
            }
        }
    }
}

// Producer configuration - used both to announce a new dispatch
// (delivery.assigned / delivery.completed) and to re-publish the
// corresponding orders.status.updated transition, since Delivery Service
// owns OUT_FOR_DELIVERY and DELIVERED the same way Restaurant Service owns
// PREPARING/READY.
kafka:ProducerConfiguration producerConfig = {
    clientId: "deliveryService",
    acks: "all"
};

kafka:Producer producer = check new (KAFKA_BROKER, producerConfig);

function registerDriver() returns error? {
    io:println("\n--- Register Driver ---");
    io:print("Name: ");
    string name = io:readln().trim();

    if name == "exit" {
        io:println("Cancelled.");
        return;
    }

    io:print("Phone: ");
    string phone = io:readln().trim();

    io:print("Vehicle: ");
    string vehicle = io:readln().trim();

    io:print("Current Zone: ");
    string currentZone = io:readln().trim();

    string driverId = uuid:createType1AsString();

    sql:ExecutionResult _ = check dbClient->execute(
        `INSERT INTO drivers (driver_id, name, phone, vehicle, is_available, current_zone)
         VALUES (${driverId}, ${name}, ${phone}, ${vehicle}, TRUE, ${currentZone})`
    );

    io:println("Driver registered with ID: " + driverId);
}

// CREATE TABLE drivers (
//     driver_id     CHAR(36) PRIMARY KEY,
//     name          VARCHAR(150) NOT NULL,
//     phone         VARCHAR(50) NOT NULL,
//     vehicle       VARCHAR(100) NOT NULL,
//     is_available  BOOLEAN NOT NULL DEFAULT TRUE,
//     current_zone  VARCHAR(100)
// );

function setDriverAvailability() returns error? {
    io:println("\n--- Set Driver Availability ---");
    io:print("Driver ID: ");
    string driverId = io:readln().trim();

    io:print("Available? (y/n): ");
    boolean isAvailable = io:readln().trim().toLowerAscii() == "y";

    sql:ExecutionResult _ = check dbClient->execute(
        `UPDATE drivers SET is_available = ${isAvailable} WHERE driver_id = ${driverId}`
    );

    io:println("Driver availability updated!");
}

function findAvailableDriver() returns record {|string driver_id; string name;|}?|error {
    stream<record {|string driver_id; string name;|}, sql:Error?> result =
        dbClient->query(`SELECT driver_id, name FROM drivers WHERE is_available = TRUE LIMIT 1`);

    var row = check result.next();
    check result.close();

    if row is record {|record {|string driver_id; string name;|} value;|} {
        return row.value;
    }
    return ();
}

function viewDeliveriesInProgress() returns error? {
    io:println("\n--- Deliveries In Progress ---");

    stream<record {|string delivery_id; string order_id; string driver_id; string status;|}, sql:Error?> res =
        dbClient->query(`SELECT delivery_id, order_id, driver_id, status
                         FROM deliveries WHERE status = 'ASSIGNED'
                         ORDER BY assigned_at DESC LIMIT 20`);

    var row = check res.next();
    if row is () {
        io:println("No deliveries in progress.");
        return;
    }
    while row is record {|record {|string delivery_id; string order_id; string driver_id; string status;|} value;|} {
        io:println("Delivery: " + row.value.delivery_id + " | Order: " + row.value.order_id +
                " | Driver: " + row.value.driver_id + " | Status: " + row.value.status);
        io:println("───────────────────────────────");
        row = check res.next();
    }
}

// CREATE TABLE deliveries (
//     delivery_id   CHAR(36) PRIMARY KEY,
//     order_id      CHAR(36) NOT NULL UNIQUE,
//     driver_id     CHAR(36) NOT NULL,
//     status        ENUM('ASSIGNED','DELIVERED') NOT NULL DEFAULT 'ASSIGNED',
//     assigned_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//     completed_at  TIMESTAMP NULL,
//     FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
// );

function markDeliveryComplete() returns error? {
    io:println("\n--- Mark Delivery Complete ---");
    io:print("Delivery ID: ");
    string deliveryId = io:readln().trim();

    stream<record {|string order_id; string driver_id; string status;|}, sql:Error?> res =
        dbClient->query(`SELECT order_id, driver_id, status FROM deliveries WHERE delivery_id = ${deliveryId}`);

    var row = check res.next();
    check res.close();

    if row is () {
        io:println("No delivery found with that ID.");
        return;
    }
    var delivery = row.value;

    if delivery.status == "DELIVERED" {
        io:println("This delivery is already marked complete.");
        return;
    }

    sql:ExecutionResult _ = check dbClient->execute(
        `UPDATE deliveries SET status = 'DELIVERED', completed_at = NOW() WHERE delivery_id = ${deliveryId}`
    );
    sql:ExecutionResult _ = check dbClient->execute(
        `UPDATE drivers SET is_available = TRUE WHERE driver_id = ${delivery.driver_id}`
    );

    json completedEvent = {
        deliveryId: deliveryId,
        orderId: delivery.order_id,
        driverId: delivery.driver_id,
        completedAt: time:utcToString(time:utcNow())
    };
    check producer->send({topic: "delivery.completed", value: completedEvent.toJsonString()});
    io:println("Kafka Event Published: " + completedEvent.toJsonString());

    OrderView? orderView = check getOrderView(delivery.order_id);
    if orderView is OrderView {
        check publishOrderStatusUpdated(delivery.order_id, orderView, "DELIVERED");
    } else {
        log:printWarn("No cached order view for " + delivery.order_id + " - orders.status.updated not re-published.");
    }

    io:println("Delivery marked complete!");
}

function getOrderView(string orderId) returns OrderView?|error {
    stream<OrderView, sql:Error?> result =
        dbClient->query(`SELECT status, customer_id, restaurant_id, total_amount FROM order_views WHERE order_id = ${orderId}`, OrderView);

    var row = check result.next();
    check result.close();

    if row is record {|OrderView value;|} {
        return row.value;
    }
    return ();
}

function publishOrderStatusUpdated(string orderId, OrderView 'order, string newStatus) returns error? {
    json event = {
        orderId: orderId,
        customerId: 'order.customer_id,
        restaurantId: 'order.restaurant_id,
        previousStatus: 'order.status,
        newStatus: newStatus,
        totalAmount: 'order.total_amount,
        updatedAt: time:utcToString(time:utcNow())
    };

    check producer->send({
        topic: "orders.status.updated",
        value: event.toJsonString()
    });

    io:println("Kafka Event Published: " + event.toJsonString());
}

// Entry point
public function main() returns error? {
    io:println("Delivery Service started.\n");
    check deliveryServiceSelection();
}

// CREATE TABLE order_views (
//     order_id      CHAR(36) PRIMARY KEY,
//     customer_id   CHAR(36) NOT NULL,
//     restaurant_id CHAR(36) NOT NULL,
//     status        VARCHAR(30) NOT NULL,
//     total_amount  DECIMAL(10,2) NOT NULL,
//     updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
// );
//
// Watches orders.status.updated for every status so order_views stays
// current, and auto-dispatches a driver the instant it sees READY.
//
// offsetReset is "latest" - this service only reacts to status changes
// that happen AFTER it starts. Start Delivery Service before an order
// reaches READY if you want to see it dispatch a driver live.
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "deliveryService",
    topics: ["orders.status.updated"],
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
            string orderId = check event.orderId.ensureType(string);
            string customerId = check event.customerId.ensureType(string);
            string restaurantId = check event.restaurantId.ensureType(string);
            string newStatus = check event.newStatus.ensureType(string);
            decimal totalAmount = check event.totalAmount.ensureType(decimal);

            var upsertResult = dbClient->execute(
                `INSERT INTO order_views (order_id, customer_id, restaurant_id, status, total_amount)
                 VALUES (${orderId}, ${customerId}, ${restaurantId}, ${newStatus}, ${totalAmount})
                 ON DUPLICATE KEY UPDATE status = ${newStatus}`
            );
            if upsertResult is sql:Error {
                log:printError("Failed to update order_views cache", upsertResult);
            }

            if newStatus != "READY" {
                continue;
            }

            record {|string driver_id; string name;|}? driver = check findAvailableDriver();
            if driver is () {
                log:printWarn("No available driver for order " + orderId + "; manual dispatch or retry needed.");
                continue;
            }

            string deliveryId = uuid:createType1AsString();
            sql:ExecutionResult _ = check dbClient->execute(
                `INSERT INTO deliveries (delivery_id, order_id, driver_id, status) VALUES (${deliveryId}, ${orderId}, ${driver.driver_id}, 'ASSIGNED')`
            );
            sql:ExecutionResult _ = check dbClient->execute(
                `UPDATE drivers SET is_available = FALSE WHERE driver_id = ${driver.driver_id}`
            );

            json assignedEvent = {
                deliveryId: deliveryId,
                orderId: orderId,
                driverId: driver.driver_id,
                driverName: driver.name,
                assignedAt: time:utcToString(time:utcNow())
            };
            check producer->send({topic: "delivery.assigned", value: assignedEvent.toJsonString()});
            io:println("Kafka Event Published: " + assignedEvent.toJsonString());

            OrderView orderView = {status: newStatus, customer_id: customerId, restaurant_id: restaurantId, total_amount: totalAmount};
            check publishOrderStatusUpdated(orderId, orderView, "OUT_FOR_DELIVERY");
        }
    }
}
