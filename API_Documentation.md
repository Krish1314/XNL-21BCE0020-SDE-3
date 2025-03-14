Order Format: JSON schema for orders published to the Redis channel. Example:

{
  "order_id": "unique_id",
  "price": 100.0,
  "quantity": 5,
  "order_type": "limit" | "market" | "stop_limit",
  "user_id": "user123",
  "stop_loss_price": 95.0,
  "stop_price": 102.0,
  "limit_price": 103.0
}


# API Documentation

This document describes the format of messages published to the Redis `orders` channel.

## Order Format (JSON)

All orders must be published as JSON objects with the following fields:

| Field          | Type     | Description                               | Required |
|----------------|----------|-------------------------------------------|----------|
| `order_id`     | String   | Unique identifier for the order           | Yes      |
| `price`        | Float    | Price of the order                        | Yes      |
| `quantity`     | Integer  | Quantity of the asset to trade            | Yes      |
| `order_type`   | String   | Type of order (limit, market, stop_limit) | Yes      |
| `user_id`      | String   | Identifier of the user placing the order  | Yes      |
| `stop_loss_price` | Float    | Stop-loss price for the order (optional) | No       |
| `stop_price`   | Float    | Stop price for a stop-limit order (optional) | No       |
| `limit_price`  | Float    | Limit price for a stop-limit order (optional) | No       |

**Example Order (Limit Order):**

```json
{
  "order_id": "12345",
  "price": 100.0,
  "quantity": 5,
  "order_type": "limit",
  "user_id": "user123"
}
