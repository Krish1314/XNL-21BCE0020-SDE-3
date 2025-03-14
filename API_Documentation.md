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
Use code with caution.
Json
Response/Logging: Describe the logging output and any potential responses from the system.
