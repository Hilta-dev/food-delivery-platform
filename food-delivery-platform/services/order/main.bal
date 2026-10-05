import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9093;

type OrderItem record {|
    string itemId;
    int qty;
    decimal price;
|};

type NewOrder record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
|};

type Ord record {
    string id;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal total;
    string status;
    string createdAt;
};

type StatusUpdate record {|
    string status;
|};


final map<string[]> transitions = {
    "CREATED": ["CONFIRMED", "CANCELLED"],
    "CONFIRMED": ["PREPARING", "CANCELLED"],
    "PREPARING": ["READY"],
    "READY": ["OUT_FOR_DELIVERY"],
    "OUT_FOR_DELIVERY": ["DELIVERED"]
};

final kafka:Producer producer = check new (kafkaBroker);
final mongodb:Client mongo = check new ({connection: mongoUri});

function orders() returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("order_db");
    return db->getCollection("orders");
}

function publish(string topic, string key, json payload) returns error? {
    check producer->send({topic: topic, key: key.toBytes(), value: payload.toJsonString().toBytes()});
}

function setStatus(string id, string newStatus) returns error? {
    mongodb:Collection c = check orders();
    Ord? cur = check c->findOne({id: id}, targetType = Ord);
    if cur is () {
        return error("order not found");
    }
    string[] allowed = transitions[cur.status] ?: [];
    if allowed.indexOf(newStatus) is () {
        return error(string `illegal transition ${cur.status} -> ${newStatus}`);
    }
    _ = check c->updateOne({id: id}, {set: {status: newStatus}});
    check publish("order.status", id, {
        orderId: id,
        customerId: cur.customerId,
        restaurantId: cur.restaurantId,
        status: newStatus
    });
    log:printInfo(string `order ${id}: ${cur.status} -> ${newStatus}`);
}

service /orders on new http:Listener(port) {

    resource function post .(NewOrder req) returns http:Created|http:BadRequest|error {
        if req.items.length() == 0 {
            return http:BAD_REQUEST;
        }
        decimal total = 0;
        foreach OrderItem i in req.items {
            total += i.price * <decimal>i.qty;
        }
        Ord ord = {
            id: uuid:createType4AsString(),
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            total: total,
            status: "CREATED",
            createdAt: time:utcToString(time:utcNow())
        };
        mongodb:Collection c = check orders();
        check c->insertOne(ord);
        check publish("orders.created", ord.id, {
            orderId: ord.id,
            customerId: ord.customerId,
            restaurantId: ord.restaurantId,
            items: ord.items.toJson(),
            total: ord.total
        });
        return <http:Created>{body: ord};
    }

    resource function get [string id]() returns Ord|http:NotFound|error {
        mongodb:Collection c = check orders();
        Ord? o = check c->findOne({id: id}, targetType = Ord);
        if o is () {
            return http:NOT_FOUND;
        }
        return o;
    }

    resource function get .(string? customerId = (), string? restaurantId = ()) returns Ord[]|error {
        mongodb:Collection c = check orders();
        map<json> filter = {};
        if customerId is string {
            filter["customerId"] = customerId;
        }
        if restaurantId is string {
            filter["restaurantId"] = restaurantId;
        }
        stream<Ord, error?> s = check c->find(filter, targetType = Ord);
        return from Ord o in s
            select o;
    }

    // restaurant drives PREPARING / READY; customer or restaurant may CANCEL
    resource function put [string id]/status(StatusUpdate upd) returns http:Ok|http:BadRequest {
        error? e = setStatus(id, upd.status);
        if e is error {
            log:printWarn("status update rejected", 'error = e);
            return http:BAD_REQUEST;
        }
        return http:OK;
    }
}

listener kafka:Listener orderEvents = new (kafkaBroker, {
    groupId: "order-service",
    topics: ["payments.completed", "payments.failed", "delivery.assigned", "delivery.completed"]
});

service on orderEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            string topic = r.offset.partition.topic;
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            string orderId = m.get("orderId").toString();
            string? next = ();
            match topic {
                "payments.completed" => {
                    next = "CONFIRMED";
                }
                "payments.failed" => {
                    next = "CANCELLED";
                }
                "delivery.assigned" => {
                    next = "OUT_FOR_DELIVERY";
                }
                "delivery.completed" => {
                    next = "DELIVERED";
                }
            }
            if next is string {
                error? e = setStatus(orderId, next);
                if e is error {
                    log:printError("event handling failed", 'error = e, topic = topic);
                }
            }
        }
    }
}
