import ballerina/http;
import ballerina/io;

configurable string customerUrl = "http://localhost:9091";
configurable string restaurantUrl = "http://localhost:9092";
configurable string orderUrl = "http://localhost:9093";
configurable string deliveryUrl = "http://localhost:9095";
configurable string notificationUrl = "http://localhost:9096";
configurable string adminUrl = "http://localhost:9097";

function show(json|error r) {
    if r is error {
        io:println("Error: " + r.message());
    } else {
        io:println(r.toJsonString());
    }
}

function showStatus(http:Response|error r) {
    if r is error {
        io:println("Error: " + r.message());
    } else {
        io:println("Response status: ", r.statusCode);
    }
}

function readInt(string prompt) returns int {
    while true {
        int|error n = int:fromString(io:readln(prompt).trim());
        if n is int {
            return n;
        }
        io:println("Please enter a whole number.");
    }
}

function readDecimal(string prompt) returns decimal {
    while true {
        decimal|error n = decimal:fromString(io:readln(prompt).trim());
        if n is decimal {
            return n;
        }
        io:println("Please enter a number.");
    }
}

function findPrice(json menu, string itemId) returns decimal? {
    if menu is json[] {
        foreach json m in menu {
            if m is map<json> && m.get("id").toString() == itemId {
                decimal|error d = decimal:fromString(m.get("price").toString());
                if d is decimal {
                    return d;
                }
            }
        }
    }
    return ();
}

public function main() returns error? {
    http:Client customerApi = check new (customerUrl);
    http:Client restaurantApi = check new (restaurantUrl);
    http:Client orderApi = check new (orderUrl);
    http:Client deliveryApi = check new (deliveryUrl);
    http:Client notificationApi = check new (notificationUrl);
    http:Client adminApi = check new (adminUrl);

    while true {
        io:println("\nFOOD DELIVERY PLATFORM");
        io:println("-- Customers --");
        io:println("1.  Register a customer");
        io:println("2.  View a customer");
        io:println("3.  Add a delivery address");
        io:println("4.  View delivery addresses");
        io:println("5.  View order history of a customer");
        io:println("-- Restaurants --");
        io:println("6.  Add a restaurant");
        io:println("7.  View all restaurants");
        io:println("8.  Add a menu item");
        io:println("9.  View a restaurant's menu");
        io:println("10. Update stock of a menu item");
        io:println("-- Orders --");
        io:println("11. Place an order");
        io:println("12. View an order");
        io:println("13. View orders of a customer");
        io:println("14. Update order status (PREPARING, READY, CANCELLED)");
        io:println("-- Delivery --");
        io:println("15. Register a driver");
        io:println("16. View all drivers");
        io:println("17. Set driver availability");
        io:println("18. View delivery of an order");
        io:println("19. Update delivery location");
        io:println("20. Complete a delivery");
        io:println("-- Notifications & Admin --");
        io:println("21. View notifications of a user");
        io:println("22. Restaurant report");
        io:println("23. Delivery report");
        io:println("24. Exit");
        string choice = io:readln("Enter your choice (1-24): ").trim();

        if choice == "1" {
            string name = io:readln("Enter the customer name: ");
            string email = io:readln("Enter the email: ");
            string phone = io:readln("Enter the phone number: ");
            json|error r = customerApi->post("/customers", {name: name, email: email, phone: phone});
            show(r);

        } else if choice == "2" {
            string id = io:readln("Enter the customer ID: ");
            json|error r = customerApi->get("/customers/" + id);
            show(r);

        } else if choice == "3" {
            string id = io:readln("Enter the customer ID: ");
            string label = io:readln("Enter a label (Home, Work...): ");
            string street = io:readln("Enter the street: ");
            string city = io:readln("Enter the city: ");
            json|error r = customerApi->post("/customers/" + id + "/addresses", {label: label, street: street, city: city});
            show(r);

        } else if choice == "4" {
            string id = io:readln("Enter the customer ID: ");
            json|error r = customerApi->get("/customers/" + id + "/addresses");
            show(r);

        } else if choice == "5" {
            string id = io:readln("Enter the customer ID: ");
            json|error r = customerApi->get("/customers/" + id + "/orders");
            show(r);

        } else if choice == "6" {
            string name = io:readln("Enter the restaurant name: ");
            string openTime = io:readln("Enter the opening time (HH:MM): ");
            string closeTime = io:readln("Enter the closing time (HH:MM): ");
            json|error r = restaurantApi->post("/restaurants", {name: name, openTime: openTime, closeTime: closeTime});
            show(r);

        } else if choice == "7" {
            io:println("Fetching all restaurants...");
            json|error r = restaurantApi->get("/restaurants");
            show(r);

        } else if choice == "8" {
            string rid = io:readln("Enter the restaurant ID: ");
            string name = io:readln("Enter the item name: ");
            decimal price = readDecimal("Enter the price: ");
            int stock = readInt("Enter the stock quantity: ");
            json|error r = restaurantApi->post("/restaurants/" + rid + "/menu", {name: name, price: price, stock: stock});
            show(r);

        } else if choice == "9" {
            string rid = io:readln("Enter the restaurant ID: ");
            json|error r = restaurantApi->get("/restaurants/" + rid + "/menu");
            show(r);

        } else if choice == "10" {
            string rid = io:readln("Enter the restaurant ID: ");
            string itemId = io:readln("Enter the menu item ID: ");
            int stock = readInt("Enter the new stock quantity: ");
            http:Response|error r = restaurantApi->put(string `/restaurants/${rid}/menu/${itemId}/stock?stock=${stock}`, {});
            showStatus(r);

        } else if choice == "11" {
            string cid = io:readln("Enter your customer ID: ");
            string rid = io:readln("Enter the restaurant ID: ");
            json|error menu = restaurantApi->get("/restaurants/" + rid + "/menu");
            if menu is error {
                io:println("Could not load menu: " + menu.message());
                continue;
            }
            io:println("Menu: ", menu.toJsonString());

            json[] items = [];
            while true {
                string itemId = io:readln("Enter an item ID to add (or press Enter to finish): ").trim();
                if itemId == "" {
                    break;
                }
                decimal? price = findPrice(menu, itemId);
                if price is () {
                    io:println("That item is not on this restaurant's menu.");
                    continue;
                }
                int qty = readInt("Enter the quantity: ");
                items.push({itemId: itemId, qty: qty, price: price});
            }
            if items.length() == 0 {
                io:println("No items selected, order cancelled.");
                continue;
            }
            io:println("Placing order...");
            json|error r = orderApi->post("/orders", {customerId: cid, restaurantId: rid, items: items});
            show(r);

        } else if choice == "12" {
            string oid = io:readln("Enter the order ID: ");
            json|error r = orderApi->get("/orders/" + oid);
            show(r);

        } else if choice == "13" {
            string cid = io:readln("Enter the customer ID: ");
            json|error r = orderApi->get("/orders?customerId=" + cid);
            show(r);

        } else if choice == "14" {
            string oid = io:readln("Enter the order ID: ");
            string status = io:readln("Enter the new status (PREPARING, READY, CANCELLED): ").trim().toUpperAscii();
            http:Response|error r = orderApi->put("/orders/" + oid + "/status", {status: status});
            showStatus(r);

        } else if choice == "15" {
            string name = io:readln("Enter the driver name: ");
            string vehicle = io:readln("Enter the vehicle (bike, car...): ");
            json|error r = deliveryApi->post("/delivery/drivers", {name: name, vehicle: vehicle});
            show(r);

        } else if choice == "16" {
            json|error r = deliveryApi->get("/delivery/drivers");
            show(r);

        } else if choice == "17" {
            string did = io:readln("Enter the driver ID: ");
            string avail = io:readln("Is the driver available? (yes/no): ").trim().toLowerAscii();
            boolean available = avail == "yes" || avail == "y";
            http:Response|error r = deliveryApi->put(string `/delivery/drivers/${did}/availability?available=${available}`, {});
            showStatus(r);

        } else if choice == "18" {
            string oid = io:readln("Enter the order ID: ");
            json|error r = deliveryApi->get("/delivery/deliveries/" + oid);
            show(r);

        } else if choice == "19" {
            string oid = io:readln("Enter the order ID: ");
            decimal lat = readDecimal("Enter the latitude: ");
            decimal lng = readDecimal("Enter the longitude: ");
            http:Response|error r = deliveryApi->put("/delivery/deliveries/" + oid + "/location", {lat: lat, lng: lng});
            showStatus(r);

        } else if choice == "20" {
            string oid = io:readln("Enter the order ID to mark as delivered: ");
            http:Response|error r = deliveryApi->post("/delivery/deliveries/" + oid + "/complete", {});
            showStatus(r);

        } else if choice == "21" {
            string uid = io:readln("Enter the customer / restaurant / driver ID: ");
            json|error r = notificationApi->get("/notifications/" + uid);
            show(r);

        } else if choice == "22" {
            json|error r = adminApi->get("/admin/reports/restaurants");
            show(r);

        } else if choice == "23" {
            json|error r = adminApi->get("/admin/reports/delivery");
            show(r);

        } else if choice == "24" {
            io:println("Exiting...");
            break;
        } else {
            io:println("Invalid choice. Please try again.");
        }
    }
}
