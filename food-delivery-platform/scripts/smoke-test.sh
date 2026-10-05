#!/bin/bash
# End-to-end happy path. Run after `docker compose up --build`.
set -e
j() { python3 -c "import sys,json;print(json.load(sys.stdin)['$1'])"; }

R=$(curl -s -X POST localhost:9092/restaurants -H 'Content-Type: application/json' -d '{"name":"Joe Beerhouse","openTime":"08:00","closeTime":"22:00"}' | j id)
I=$(curl -s -X POST localhost:9092/restaurants/$R/menu -H 'Content-Type: application/json' -d '{"name":"Kapana","price":45.5,"stock":50}' | j id)
C=$(curl -s -X POST localhost:9091/customers -H 'Content-Type: application/json' -d '{"name":"Matty","email":"m@nust.na","phone":"0811234567"}' | j id)
curl -s -X POST localhost:9095/delivery/drivers -H 'Content-Type: application/json' -d '{"name":"Driver One","vehicle":"bike"}' > /dev/null

O=$(curl -s -X POST localhost:9093/orders -H 'Content-Type: application/json' \
  -d "{\"customerId\":\"$C\",\"restaurantId\":\"$R\",\"items\":[{\"itemId\":\"$I\",\"qty\":2,\"price\":45.5}]}" | j id)
echo "order $O created"; sleep 3
for s in PREPARING READY; do
  curl -s -X PUT localhost:9093/orders/$O/status -H 'Content-Type: application/json' -d "{\"status\":\"$s\"}" -o /dev/null -w "$s -> %{http_code}\n"
  sleep 2
done
curl -s -X POST localhost:9095/delivery/deliveries/$O/complete -o /dev/null -w "complete -> %{http_code}\n"
sleep 2
echo; curl -s localhost:9093/orders/$O; echo
curl -s localhost:9097/admin/reports/restaurants; echo
curl -s localhost:9097/admin/reports/delivery; echo
curl -s localhost:9096/notifications/$C; echo
