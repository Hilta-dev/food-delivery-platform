import ballerina/http;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:29092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable int port = 9091;

type NewCustomer record {|
    string name;
    string email;
    string phone;
|};

type Customer record {
    string id;
    string name;
    string email;
    string phone;
};

type Address record {|
    string label;
    string street;
    string city;
|};

type AddressDoc record {
    string customerId;
    string label;
    string street;
    string city;
};

type OrderDoc record {
    string orderId;
    string customerId;
    string restaurantId;
    string total;
};

final mongodb:Client mongo = check new ({connection: mongoUri});

function coll(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("customer_db");
    return db->getCollection(name);
}

service /customers on new http:Listener(port) {

    resource function post .(NewCustomer nc) returns http:Created|error {
        Customer c = {id: uuid:createType4AsString(), name: nc.name, email: nc.email, phone: nc.phone};
        mongodb:Collection col = check coll("customers");
        check col->insertOne(c);
        return {body: c};
    }

    resource function get [string id]() returns Customer|http:NotFound|error {
        mongodb:Collection col = check coll("customers");
        Customer? c = check col->findOne({id: id}, targetType = Customer);
        if c is () {
            return http:NOT_FOUND;
        }
        return c;
    }

    resource function post [string id]/addresses(Address a) returns http:Created|error {
        mongodb:Collection col = check coll("addresses");
        check col->insertOne({"customerId": id, "label": a.label, "street": a.street, "city": a.city});
        return {body: a};
    }

    resource function get [string id]/addresses() returns AddressDoc[]|error {
        mongodb:Collection col = check coll("addresses");
        stream<AddressDoc, error?> s = check col->find({customerId: id}, targetType = AddressDoc);
        return from AddressDoc a in s
            select a;
    }


    resource function get [string id]/orders() returns OrderDoc[]|error {
        mongodb:Collection col = check coll("order_history");
        stream<OrderDoc, error?> s = check col->find({customerId: id}, targetType = OrderDoc);
        return from OrderDoc o in s
            select o;
    }
}

listener kafka:Listener orderEvents = new (kafkaBroker, {
    groupId: "customer-service",
    topics: ["orders.created"]
});

service on orderEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            json j = check (check string:fromBytes(r.value)).fromJsonString();
            map<json> m = check j.ensureType();
            mongodb:Collection col = check coll("order_history");
                        check col->insertOne({
                "orderId": m.get("orderId").toString(),
                "customerId": m.get("customerId").toString(),
                "restaurantId": m.get("restaurantId").toString(),
                "total": m.get("total").toString()
            });
        }
    }
}



