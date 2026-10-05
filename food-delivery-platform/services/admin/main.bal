import ballerina/http;
import ballerina/time;
import ballerinax/kafka;

configurable string kafkaBroker = "localhost:29092";
configurable int port = 9097;


map<int> ordersPerRestaurant = {};
map<decimal> revenuePerRestaurant = {};
map<int> cancelledPerRestaurant = {};
map<string> restaurantOfOrder = {};
map<time:Utc> assignedAt = {};
int deliveriesCompleted = 0;
decimal totalDeliverySeconds = 0;

service /admin on new http:Listener(port) {

    resource function get reports/restaurants() returns json {
        json[] rows = [];
        foreach string rid in ordersPerRestaurant.keys() {
            rows.push({
                restaurantId: rid,
                orders: ordersPerRestaurant.get(rid),
                revenue: revenuePerRestaurant[rid] ?: 0,
                cancelled: cancelledPerRestaurant[rid] ?: 0
            });
        }
        return rows;
    }

    resource function get reports/delivery() returns json {
        decimal avg = deliveriesCompleted > 0 ? totalDeliverySeconds / <decimal>deliveriesCompleted : 0;
        return {deliveriesCompleted: deliveriesCompleted, avgDeliverySeconds: avg, inProgress: assignedAt.length()};
    }
}

listener kafka:Listener allEvents = new (kafkaBroker, {
    groupId: "admin-service",
    topics: ["orders.created", "payments.failed", "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on allEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            string topic = r.offset.partition.topic;
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            string orderId = m.get("orderId").toString();
            match topic {
                "orders.created" => {
                    string rid = m.get("restaurantId").toString();
                    restaurantOfOrder[orderId] = rid;
                    ordersPerRestaurant[rid] = (ordersPerRestaurant[rid] ?: 0) + 1;
                    revenuePerRestaurant[rid] = (revenuePerRestaurant[rid] ?: 0d) + check decimal:fromString(m.get("total").toString());
                }
                "payments.failed" => {
                    string rid = restaurantOfOrder[orderId] ?: "unknown";
                    cancelledPerRestaurant[rid] = (cancelledPerRestaurant[rid] ?: 0) + 1;
                }
                "delivery.assigned" => {
                    assignedAt[orderId] = time:utcNow();
                }
                "delivery.completed" => {
                    time:Utc? assignedAtValue = assignedAt[orderId];
                    if assignedAtValue is time:Utc {
                        totalDeliverySeconds += time:utcDiffSeconds(time:utcNow(), assignedAtValue);
                        deliveriesCompleted += 1;
                        _ = assignedAt.remove(orderId);
                    }
                }
            }
        }
    }
}

configurable string mongoUri = "mongodb://localhost:27017";
