import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9096;

final mongodb:Client mongo = check new ({connection: mongoUri});

function notifications() returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("notification_db");
    return db->getCollection("notifications");
}

function notify(string recipientType, string recipientId, string channel, string message) returns error? {
    mongodb:Collection c = check notifications();
    check c->insertOne({
        "recipientType": recipientType,
        "recipientId": recipientId,
        "channel": channel,
        "message": message,
        "sentAt": time:utcToString(time:utcNow())
    });
    log:printInfo(string `[${channel}] -> ${recipientType} ${recipientId}: ${message}`);
}


type Notification record {
    string recipientType;
    string recipientId;
    string channel;
    string message;
    string sentAt;
};

service /notifications on new http:Listener(port) {
    resource function get [string recipientId]() returns Notification[]|error {
        mongodb:Collection c = check notifications();
        stream<Notification, error?> s = check c->find({recipientId: recipientId}, targetType = Notification);
        return from Notification n in s
            select n;
    }
}

listener kafka:Listener allEvents = new (kafkaBroker, {
    groupId: "notification-service",
    topics: ["orders.created", "payments.completed", "payments.failed", "order.status", "delivery.assigned", "delivery.completed"]
});

service on allEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            string topic = r.offset.partition.topic;
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            string orderId = m.get("orderId").toString();
            string customerId = m.get("customerId").toString();
            match topic {
                "orders.created" => {
                    check notify("restaurant", m.get("restaurantId").toString(), "push", string `New order ${orderId}`);
                    check notify("customer", customerId, "email", string `Order ${orderId} received`);
                }
                "payments.completed" => {
                    check notify("customer", customerId, "sms", string `Payment confirmed for order ${orderId}`);
                }
                "payments.failed" => {
                    check notify("customer", customerId, "sms", string `Payment failed, order ${orderId} cancelled`);
                }
                "order.status" => {
                    check notify("customer", customerId, "push", string `Order ${orderId} is ${m.get("status").toString()}`);
                }
                "delivery.assigned" => {
                    check notify("driver", m.get("driverId").toString(), "push", string `New delivery: order ${orderId}`);
                    check notify("customer", customerId, "push", string `A driver is on the way with order ${orderId}`);
                }
                "delivery.completed" => {
                    check notify("customer", customerId, "email", string `Order ${orderId} delivered. Enjoy!`);
                }
            }
        }
    }
}



