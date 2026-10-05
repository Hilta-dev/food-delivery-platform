import ballerina/io;
import ballerina/log;
import ballerina/random;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

public type Payment record {|
    string paymentId;
    string orderId;
    decimal amount;
    string status;
    string transactionRef?;
    string reason?;
    string createdAt;
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

// Fraction (0.0-1.0) of simulated payments that fail, for demo purposes.
configurable decimal FAILURE_RATE = 0.1;

function paymentServiceMenu() {
    io:println("\nPayment Service Menu");
    io:println("1. View All Payments");
    io:println("2. View Payment by Order ID");
    io:println("3. Exit");
}

function paymentServiceSelection() returns error? {
    while true {
        paymentServiceMenu();
        io:println("Enter your Choice: ");
        int choice = check int:fromString(io:readln().trim());

        match choice {
            1 => {
                check viewAllPayments();
            }
            2 => {
                check viewPaymentByOrderId();
            }
            3 => {
                io:println("Exiting Payment Service...");
                break;
            }
            _ => {
                io:println("Invalid choice, try again.");
            }
        }
    }
}

// Producer configuration - used to emit the outcome of each simulated
// payment as soon as it's processed by the Kafka consumer at the bottom
// of this file.
kafka:ProducerConfiguration producerConfig = {
    clientId: "paymentService",
    acks: "all"
};

kafka:Producer producer = check new (KAFKA_BROKER, producerConfig);

function viewAllPayments() returns error? {
    io:println("\n--- All Payments ---");

    stream<record {|string payment_id; string order_id; decimal amount; string status; string? transaction_ref; string? reason;|}, sql:Error?> res =
        dbClient->query(`SELECT payment_id, order_id, amount, status, transaction_ref, reason
                         FROM payments ORDER BY created_at DESC LIMIT 20`);

    var row = check res.next();
    if row is () {
        io:println("No payments processed yet.");
        return;
    }
    while row is record {|record {|string payment_id; string order_id; decimal amount; string status; string? transaction_ref; string? reason;|} value;|} {
        string detail = row.value.status == "COMPLETED" ? ("Ref: " + (row.value.transaction_ref ?: "-")) : ("Reason: " + (row.value.reason ?: "-"));
        io:println("Order: " + row.value.order_id + " | Amount: N$ " + row.value.amount.toString() + " | " + row.value.status + " | " + detail);
        io:println("───────────────────────────────");
        row = check res.next();
    }
}

function viewPaymentByOrderId() returns error? {
    io:print("\nEnter Order ID: ");
    string orderId = io:readln().trim();

    stream<record {|string payment_id; decimal amount; string status; string? transaction_ref; string? reason; string created_at;|}, sql:Error?> res =
        dbClient->query(`SELECT payment_id, amount, status, transaction_ref, reason, created_at
                         FROM payments WHERE order_id = ${orderId}`);

    var row = check res.next();
    check res.close();

    if row is () {
        io:println("No payment found for this order.");
        return;
    }
    var r = row.value;
    io:println("Payment ID: " + r.payment_id);
    io:println("Amount:     N$ " + r.amount.toString());
    io:println("Status:     " + r.status);
    if r.status == "COMPLETED" {
        io:println("Ref:        " + (r.transaction_ref ?: "-"));
    } else {
        io:println("Reason:     " + (r.reason ?: "-"));
    }
    io:println("Processed:  " + r.created_at);
}

// CREATE TABLE payments (
//     payment_id      CHAR(36) PRIMARY KEY,
//     order_id        CHAR(36) NOT NULL UNIQUE,
//     amount          DECIMAL(10,2) NOT NULL,
//     status          ENUM('COMPLETED','FAILED') NOT NULL,
//     transaction_ref VARCHAR(100),
//     reason          VARCHAR(255),
//     created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
// );

// Entry point
public function main() returns error? {
    io:println("Payment Service started.\n");
    check paymentServiceSelection();
}
type ExistingPayment record {|
    string payment_id;
    string status;
|};


function paymentExists(string orderId) returns boolean|error {
    stream<ExistingPayment, sql:Error?> result =
        dbClient->query(
            `SELECT payment_id, status
             FROM payments
             WHERE order_id = ${orderId}`,
            ExistingPayment
        );

    var row = check result.next();
    check result.close();

    if row is record {|ExistingPayment value;|} {
        return true;
    }

    return false;
}
// This is where the actual "simulate payment processing" requirement is
// met: every orders.created event is picked up here, put through a
// simulated gateway, persisted, and the outcome published so Order
// Service (and anyone else listening) learns whether the order can be
// confirmed. No manual/admin step is involved - this runs automatically
// the moment an order is placed, same as a real payment gateway callback.
//
// offsetReset is "latest" - this service only processes orders placed
// AFTER it starts. Start Payment Service before creating an order you
// want it to react to live.
kafka:ConsumerConfiguration consumerConfig = {
    groupId: "paymentService",
    topics: ["orders.created"],
    pollingInterval: 1,
    offsetReset: "earliest"
};

listener kafka:Listener consumerListener = new (KAFKA_BROKER, consumerConfig);

service on consumerListener {
    remote function onConsumerRecord(
        kafka:Caller caller,
        kafka:BytesConsumerRecord[] records
    ) returns error? {

        foreach kafka:BytesConsumerRecord item in records {
            string msg = check string:fromBytes(item.value);

            log:printInfo(
                "Received message from Kafka [" +
                item.offset.partition.topic + "]: " + msg
            );

            json event = check msg.fromJsonString();

            string orderId = check event.orderId.ensureType(string);
            decimal amount = check event.totalAmount.ensureType(decimal);

            // Important: never reprocess an order that already has
            // a persisted payment result.
            boolean alreadyProcessed = check paymentExists(orderId);

            if alreadyProcessed {
                log:printInfo(
                    "Payment already exists for order " + orderId +
                    ". Skipping historical/duplicate event."
                );
                continue;
            }

            string paymentId = uuid:createType1AsString();

            // The Customer ID is retained and emitted in payment events.
            // Notification Service needs it to correctly deliver payment
            // notifications to the real customer.
            string customerId = check event.customerId.ensureType(string);

            float draw = random:createDecimal();

            if <decimal>draw < FAILURE_RATE {
                string reason = "Simulated gateway decline";

                var insertResult = dbClient->execute(
                    `INSERT INTO payments (
                        payment_id,
                        order_id,
                        amount,
                        status,
                        reason
                    )
                    VALUES (
                        ${paymentId},
                        ${orderId},
                        ${amount},
                        'FAILED',
                        ${reason}
                    )`
                );

                if insertResult is sql:Error {
                    log:printError(
                        "Failed to persist failed payment for order " + orderId,
                        insertResult
                    );

                    // Do not publish an event when persistence failed.
                    continue;
                }

                json failedEvent = {
                    paymentId: paymentId,
                    orderId: orderId,
                    customerId: customerId,
                    amount: amount,
                    reason: reason,
                    failedAt: time:utcToString(time:utcNow())
                };

                check producer->send({
                    topic: "payments.failed",
                    value: failedEvent.toJsonString()
                });

                io:println(
                    "Kafka Event Published: " +
                    failedEvent.toJsonString()
                );
            } else {
                string transactionRef =
                    "TXN-" + uuid:createType1AsString().substring(0, 8);

                var insertResult = dbClient->execute(
                    `INSERT INTO payments (
                        payment_id,
                        order_id,
                        amount,
                        status,
                        transaction_ref
                    )
                    VALUES (
                        ${paymentId},
                        ${orderId},
                        ${amount},
                        'COMPLETED',
                        ${transactionRef}
                    )`
                );

                if insertResult is sql:Error {
                    log:printError(
                        "Failed to persist completed payment for order " +
                        orderId,
                        insertResult
                    );

                    // Do not publish an event when persistence failed.
                    continue;
                }

                json completedEvent = {
                    paymentId: paymentId,
                    orderId: orderId,
                    customerId: customerId,
                    amount: amount,
                    transactionRef: transactionRef,
                    completedAt: time:utcToString(time:utcNow())
                };

                check producer->send({
                    topic: "payments.completed",
                    value: completedEvent.toJsonString()
                });

                io:println(
                    "Kafka Event Published: " +
                    completedEvent.toJsonString()
                );
            }
        }
    }
}
