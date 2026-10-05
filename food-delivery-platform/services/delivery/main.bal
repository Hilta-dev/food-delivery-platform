import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9095;

type NewDriver record {|
    string name;
    string vehicle;
|};

type Driver record {
    string id;
    string name;
    string vehicle;
    boolean available;
};

type Location record {|
    decimal lat;
    decimal lng;
|};

type Delivery record {
    string id;
    string orderId;
    string customerId;
    string driverId;
    string status; // "ASSIGNED" or "DELIVERED"
    decimal lat;
    decimal lng;
    string assignedAt;
};

final kafka:Producer producer = check new (kafkaBroker);
final mongodb:Client mongo = check new ({connection: mongoUri});

function coll(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("delivery_db");
    return db->getCollection(name);
}

function publish(string topic, string key, json payload) returns error? {
    check producer->send({topic: topic, key: key.toBytes(), value: payload.toJsonString().toBytes()});
}

service /delivery on new http:Listener(port) {

    resource function post drivers(NewDriver d) returns http:Created|error {
        Driver drv = {id: uuid:createType4AsString(), name: d.name, vehicle: d.vehicle, available: true};
        mongodb:Collection c = check coll("drivers");
        check c->insertOne(drv);
        return {body: drv};
    }

    resource function get drivers() returns Driver[]|error {
        mongodb:Collection c = check coll("drivers");
        stream<Driver, error?> s = check c->find({}, targetType = Driver);
        return from Driver d in s
            select d;
    }

    resource function put drivers/[string id]/availability(boolean available) returns http:Ok|error {
        mongodb:Collection c = check coll("drivers");
        _ = check c->updateOne({id: id}, {set: {available: available}});
        return http:OK;
    }

    resource function get deliveries/[string orderId]() returns Delivery|http:NotFound|error {
        mongodb:Collection c = check coll("deliveries");
        Delivery? d = check c->findOne({orderId: orderId}, targetType = Delivery);
        if d is () {
            return http:NOT_FOUND;
        }
        return d;
    }


    resource function put deliveries/[string orderId]/location(Location loc) returns http:Ok|error {
        mongodb:Collection c = check coll("deliveries");
        _ = check c->updateOne({orderId: orderId}, {set: {lat: loc.lat, lng: loc.lng}});
        return http:OK;
    }

    resource function post deliveries/[string orderId]/complete() returns http:Ok|http:NotFound|error {
        mongodb:Collection dc = check coll("deliveries");
        Delivery? d = check dc->findOne({orderId: orderId}, targetType = Delivery);
        if d is () {
            return http:NOT_FOUND;
        }
        _ = check dc->updateOne({orderId: orderId}, {set: {status: "DELIVERED"}});
        mongodb:Collection drc = check coll("drivers");
        _ = check drc->updateOne({id: d.driverId}, {set: {available: true}});
        check publish("delivery.completed", orderId, {
            orderId: orderId,
            deliveryId: d.id,
            driverId: d.driverId,
            customerId: d.customerId
        });
        return http:OK;
    }
}

listener kafka:Listener statusEvents = new (kafkaBroker, {
    groupId: "delivery-service",
    topics: ["order.status"]
});

service on statusEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            if m.get("status").toString() != "READY" {
                continue;
            }
            string orderId = m.get("orderId").toString();
            string customerId = m.get("customerId").toString();


            mongodb:Collection drc = check coll("drivers");
            Driver? drv = check drc->findOne({available: true}, targetType = Driver);
            if drv is () {
                log:printWarn(string `no driver available for order ${orderId}`);
                continue;
            }
            _ = check drc->updateOne({id: drv.id}, {set: {available: false}});

            Delivery d = {
                id: uuid:createType4AsString(),
                orderId: orderId,
                customerId: customerId,
                driverId: drv.id,
                status: "ASSIGNED",
                lat: 0,
                lng: 0,
                assignedAt: time:utcToString(time:utcNow())
            };
            mongodb:Collection dc = check coll("deliveries");
            check dc->insertOne(d);
            check publish("delivery.assigned", orderId, {
                orderId: orderId,
                deliveryId: d.id,
                driverId: drv.id,
                customerId: customerId
            });
            log:printInfo(string `order ${orderId} assigned to driver ${drv.name}`);
        }
    }
}
