# ERP inventory

Pyro ERP-style inventory uses the same universal `records` table with ERP entity types.

## Entity types
- `inventory_item` — stock on hand (available_quantity, allocated_quantity, total_quantity, part_number_or_sku / name).
- `inventory_request` — requests / receiving flow (quantity, part_number_or_sku, status such as IN_SHIPPING).

## Receiving flow
- When a shipment is received, the `receive_add_to_inventory` rule action adds quantity to a matching inventory_item (by SKU or name), or creates a new item.
- Problematic shipments can be rolled back to NEW_REQUEST for the inventory manager.

## Asking the assistant
- How-to: "How does receiving add to inventory?"
- Live data: "Summarize inventory stock" or "How many inventory_request records are open?"
