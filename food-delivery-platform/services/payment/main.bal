import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9094;
configurable decimal maxAmount = 5000;   // simulated card limit

type Payment record {
    string orderId;
    string customerId;
    decimal amount;
    string status;     // PAID | FAILED
    string processedAt;
};

final kafka:Producer producer = check new (kafkaBroker);
final mongodb:Client mongo = check new ({connection: mongoUri});

function payments() returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("payment_db");
    return db->getCollection("payments");
}

service /payments on new http:Listener(port) {
    resource function get [string orderId]() returns Payment|http:NotFound|error {
        mongodb:Collection c = check payments();
        Payment? p = check c->findOne({orderId: orderId}, targetType = Payment);
        if p is () {
            return http:NOT_FOUND;
        }
        return p;
    }
}

listener kafka:Listener orderEvents = new (kafkaBroker, {
    groupId: "payment-service",
    topics: ["orders.created"]
});

service on orderEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            string orderId = m.get("orderId").toString();
            string customerId = m.get("customerId").toString();
            decimal amount = check decimal:fromString(m.get("total").toString());

            boolean ok = amount <= maxAmount;   // simulated processing
            Payment p = {
                orderId: orderId,
                customerId: customerId,
                amount: amount,
                status: ok ? "PAID" : "FAILED",
                processedAt: time:utcToString(time:utcNow())
            };
            mongodb:Collection c = check payments();
            check c->insertOne(p);

            string topic = ok ? "payments.completed" : "payments.failed";
            json ev = {orderId: orderId, customerId: customerId, amount: amount, status: p.status};
            check producer->send({topic: topic, key: orderId.toBytes(), value: ev.toJsonString().toBytes()});
            log:printInfo(string `payment ${p.status} for order ${orderId}`);
        }
    }
}
