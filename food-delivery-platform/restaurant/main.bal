import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9092;

type NewRestaurant record {|
    string name;
    string openTime;    // "08:00"
    string closeTime;   // "21:00"
|};

type Restaurant record {
    string id;
    string name;
    string openTime;
    string closeTime;
};

type MenuItem record {
    string id;
    string restaurantId;
    string name;
    decimal price;
    int stock;
};

type NewMenuItem record {|
    string name;
    decimal price;
    int stock;
|};

final mongodb:Client mongo = check new ({connection: mongoUri});

function coll(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("restaurant_db");
    return db->getCollection(name);
}

service /restaurants on new http:Listener(port) {

    resource function post .(NewRestaurant nr) returns http:Created|error {
        Restaurant r = {id: uuid:createType4AsString(), name: nr.name, openTime: nr.openTime, closeTime: nr.closeTime};
        mongodb:Collection c = check coll("restaurants");
        check c->insertOne(r);
        return {body: r};
    }

    resource function get .() returns Restaurant[]|error {
        mongodb:Collection c = check coll("restaurants");
        stream<Restaurant, error?> s = check c->find({}, targetType = Restaurant);
        return from Restaurant r in s
            select r;
    }

    resource function get [string id]() returns Restaurant|http:NotFound|error {
        mongodb:Collection c = check coll("restaurants");
        Restaurant? r = check c->findOne({id: id}, targetType = Restaurant);
        if r is () {
            return http:NOT_FOUND;
        }
        return r;
    }

    resource function post [string id]/menu(NewMenuItem mi) returns http:Created|error {
        MenuItem item = {id: uuid:createType4AsString(), restaurantId: id, name: mi.name, price: mi.price, stock: mi.stock};
        mongodb:Collection c = check coll("menu");
        check c->insertOne(item);
        return {body: item};
    }

    resource function get [string id]/menu() returns MenuItem[]|error {
        mongodb:Collection c = check coll("menu");
        stream<MenuItem, error?> s = check c->find({restaurantId: id}, targetType = MenuItem);
        return from MenuItem m in s
            select m;
    }

    resource function put [string id]/menu/[string itemId]/stock(int stock) returns http:Ok|error {
        mongodb:Collection c = check coll("menu");
        _ = check c->updateOne({id: itemId, restaurantId: id}, {set: {stock: stock}});
        return http:OK;
    }
}


listener kafka:Listener orderEvents = new (kafkaBroker, {
    groupId: "restaurant-service",
    topics: ["orders.created"]
});

service on orderEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            json[] items = check m.get("items").ensureType();
            mongodb:Collection c = check coll("menu");
            foreach json it in items {
                map<json> im = check it.ensureType();
                string itemId = im.get("itemId").toString();
                int qty = check int:fromString(im.get("qty").toString());
                _ = check c->updateOne({id: itemId}, {inc: {stock: -qty}});
            }
            log:printInfo(string `inventory updated for order ${m.get("orderId").toString()}`);
        }
    }
}
