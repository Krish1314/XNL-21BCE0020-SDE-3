import heapq
import redis
import json
import logging
import time
import threading
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Order:
    def __init__(self, order_id, price, quantity, order_type, user_id, stop_loss_price=None, stop_price=None, limit_price=None):
        self.order_id = order_id
        self.price = price
        self.quantity = quantity
        self.order_type = order_type
        self.user_id = user_id
        self.stop_loss_price = stop_loss_price
        self.stop_price = stop_price
        self.limit_price = limit_price

    def get_best_ask(self):
        """Returns the best ask price."""
        if self.asks:
            return self.asks[0][0]
        return None

    def get_best_bid(self):
        """Returns the best bid price."""
        if self.bids:
            return -self.bids[0][0]  # Remember bids are stored as negative prices
        return None

    def __repr__(self):
        return f"Order(ID={self.order_id}, Price={self.price}, Quantity={self.quantity}, Type={self.order_type}, User ID={self.user_id}, Stop Loss={self.stop_loss_price}, Stop Price={self.stop_price}, Limit Price={self.limit_price})"

class PositionManager:
    def __init__(self, redis_client, position_limits):
        self.redis_client = redis_client
        self.position_limits = position_limits

    def get_user_position(self, user_id, asset_symbol):
        """Retrieves the user's position for a given asset from Redis."""
        position_key = f"user:{user_id}:asset:{asset_symbol}"
        position = self.redis_client.get(position_key)
        return int(position) if position else 0

    def update_user_position(self, user_id, asset_symbol, quantity_change):
        """Updates the user's position in Redis."""
        position_key = f"user:{user_id}:asset:{asset_symbol}"
        current_position = self.get_user_position(user_id, asset_symbol)
        new_position = current_position + quantity_change
        self.redis_client.set(position_key, new_position)
        logging.info(f"User {user_id} position for {asset_symbol} updated to {new_position}")

    def check_position_limit(self, user_id, asset_symbol, quantity_change):
        """Checks if a trade would exceed the position limit."""
        current_position = self.get_user_position(user_id, asset_symbol)
        new_position = current_position + quantity_change
        limit = self.position_limits.get(asset_symbol, float('inf'))
        return new_position <= limit

class OrderBook:
    def __init__(self, redis_client, position_manager):
        self.bids = []  # Max heap (price is negative)
        self.asks = []  # Min heap
        self.redis_client = redis_client
        self.position_manager = position_manager
        self.last_price = None
        self.asset_symbol = "asset_1"
        self.stop_limit_orders = []  # List to store stop-limit orders

    def add_order(self, order):
        """Adds an order to the order book."""
        user_id = order.user_id

        # Check position limit before adding the order
        if not self.position_manager.check_position_limit(user_id, self.asset_symbol, order.quantity if order.order_type == "buy" else -order.quantity):
            logging.warning(f"Position limit exceeded for user {user_id} on asset {self.asset_symbol}")
            return False  # Reject the order

        if order.order_type == "limit":
            if order.order_type == "buy":
                heapq.heappush(self.bids, (-order.price, order.order_id, order))  # Negative price for max heap
            else:
                heapq.heappush(self.asks, (order.price, order.order_id, order))
        elif order.order_type == "market":
            self.match_market_order(order)  # Call the new function
        elif order.order_type == "stop_limit":
            self.add_stop_limit_order(order)

        self.match_orders()
        return True

    def match_market_order(self, order):
        """Matches a market order against existing limit orders."""
        while order.quantity > 0:
            if not self.bids and not self.asks:
                logging.warning(f"Market order {order.order_id} cannot be filled - no orders in the book.")
                break

            if self.asks:
                if len(self.asks) > 0:  # Check if asks list is not empty
                    best_ask_price = self.asks[0][0]
                    ask_order = heapq.heappop(self.asks)[2]

                    fill_quantity = min(order.quantity, ask_order.quantity)
                    logging.info(f"Market order filled at price {best_ask_price}, quantity {fill_quantity}")

                    order.quantity -= fill_quantity
                    ask_order.quantity -= fill_quantity

                    # Update user positions
                    self.position_manager.update_user_position(order.user_id, self.asset_symbol, -fill_quantity)
                    self.position_manager.update_user_position(ask_order.user_id, self.asset_symbol, fill_quantity)

                    if ask_order.quantity > 0:
                        heapq.heappush(self.asks, (ask_order.price, ask_order.order_id, ask_order))
                else:
                    logging.warning("No asks available to match market order.")
                    break
            elif self.bids:  # If no asks, try to fill against bids (shouldn't happen often)
                best_bid_price = -self.bids[0][0]
                bid_order = heapq.heappop(self.bids)[2]

                fill_quantity = min(order.quantity, bid_order.quantity)
                logging.info(f"Market order filled at price {best_bid_price}, quantity {fill_quantity}")

                order.quantity -= fill_quantity
                bid_order.quantity -= fill_quantity

                # Update user positions
                self.position_manager.update_user_position(order.user_id, self.asset_symbol, fill_quantity)
                self.position_manager.update_user_position(bid_order.user_id, self.asset_symbol, -fill_quantity)

                if bid_order.quantity > 0:
                    heapq.heappush(self.bids, (-bid_order.price, bid_order.order_id, bid_order))

    def add_stop_limit_order(self, order):
        """Adds a stop-limit order to the order book."""
        self.stop_limit_orders.append(order)
        logging.info(f"Stop-limit order added: {order}")

    def check_stop_limit_orders(self, current_price):
        """Checks if any stop-limit orders should be triggered."""
        orders_to_trigger = []
        for order in self.stop_limit_orders:
            if current_price >= order.stop_price:
                # Convert stop-limit order to a limit order
                limit_order = Order(
                    order_id=order.order_id + 1000,
                    price=order.limit_price,
                    quantity=order.quantity,
                    order_type="limit",
                    user_id=order.user_id
                )
                logging.info(f"Stop-limit order {order.order_id} triggered, converting to limit order {limit_order.order_id}")
                orders_to_trigger.append(order)
                self.add_order(limit_order)  # Add the limit order to the book

        # Remove triggered orders
        for order in orders_to_trigger:
            self.stop_limit_orders.remove(order)

    def match_orders(self):
        """Matches buy and sell orders in the order book."""
        current_price = self.get_best_ask() if self.asks else self.get_best_bid()
        if current_price is not None:
            self.check_stop_limit_orders(current_price)

        while self.bids and self.asks and -self.bids[0][0] >= self.asks[0][0]:
            best_bid_price = -self.bids[0][0]
            best_ask_price = self.asks[0][0]

            bids_at_price = []
            asks_at_price = []

            while self.bids and -self.bids[0][0] == best_bid_price:
                order = heapq.heappop(self.bids)[2]
                bids_at_price.append(order)
            while self.asks and self.asks[0][0] == best_ask_price:
                order = heapq.heappop(self.asks)[2]
                asks_at_price.append(order)

            total_bid_quantity = sum(order.quantity for order in bids_at_price)
            total_ask_quantity = sum(order.quantity for order in asks_at_price)

            trade_quantity = min(total_bid_quantity, total_ask_quantity)

            bid_portion = trade_quantity * (total_bid_quantity / (total_bid_quantity + total_ask_quantity))
            ask_portion = trade_quantity * (total_ask_quantity / (total_bid_quantity + total_ask_quantity))

            bid_index = 0
            ask_index = 0
            while trade_quantity > 0 and bid_index < len(bids_at_price) and ask_index < len(asks_at_price):
                bid_order = bids_at_price[bid_index]
                ask_order = asks_at_price[ask_index]

                fill_quantity = min(trade_quantity, bid_order.quantity, ask_order.quantity)

                logging.info(f"Trade: Price={best_bid_price}, Quantity={fill_quantity}, Bid ID={bid_order.order_id}, Ask ID={ask_order.order_id}")

                bid_order.quantity -= fill_quantity
                ask_order.quantity -= fill_quantity
                trade_quantity -= fill_quantity

                # Update user positions
                self.position_manager.update_user_position(bid_order.user_id, self.asset_symbol, fill_quantity)
                self.position_manager.update_user_position(ask_order.user_id, self.asset_symbol, -fill_quantity)

                if bid_order.quantity == 0:
                    bid_index += 1
                if ask_order.quantity == 0:
                    ask_index += 1

            for order in bids_at_price:
                if order.quantity > 0:
                    heapq.heappush(self.bids, (-order.price, order.order_id, order))
            for order in asks_at_price:
                if order.quantity > 0:
                    heapq.heappush(self.asks, (order.price, order.order_id, order))

    def save_state_to_redis(self):
        """Saves the order book state to Redis."""
        # Serialize bids and asks to JSON
        bids_data = []
        for _, _, order in self.bids:
            bids_data.append({
                "order_id": order.order_id,
                "price": order.price,
                "quantity": order.quantity,
                "order_type": order.order_type,
                "user_id": order.user_id,
                "stop_loss_price": order.stop_loss_price,
                "stop_price": order.stop_price,
                "limit_price": order.limit_price
            })

        asks_data = []
        for _, _, order in self.asks:
            asks_data.append({
                "order_id": order.order_id,
                "price": order.price,
                "quantity": order.quantity,
                "order_type": order.order_type,
                "user_id": order.user_id,
                "stop_loss_price": order.stop_loss_price,
                "stop_price": order.stop_price,
                "limit_price": order.limit_price
            })

        bids_json = json.dumps(bids_data)
        asks_json = json.dumps(asks_data)

        self.redis_client.set("order_book:bids", bids_json)
        self.redis_client.set("order_book:asks", asks_json)
        logging.info("Order book state saved to Redis.")

    def load_state_from_redis(self):
        """Loads the order book state from Redis."""
        bids_json = self.redis_client.get("order_book:bids")
        asks_json = self.redis_client.get("order_book:asks")

        if bids_json:
            bids_data = json.loads(bids_json.decode('utf-8'))
            self.bids = []
            for bid_data in bids_data:
                order = Order(
                    order_id=bid_data["order_id"],
                    price=bid_data["price"],
                    quantity=bid_data["quantity"],
                    order_type=bid_data["order_type"],
                    user_id=bid_data["user_id"],
                    stop_loss_price=bid_data.get("stop_loss_price"),
                    stop_price=bid_data.get("stop_price", None),  # Use get with default value
                    limit_price=bid_data.get("limit_price", None)  # Use get with default value
                )
                heapq.heappush(self.bids, (-order.price, order.order_id, order))

        if asks_json:
            asks_data = json.loads(asks_json.decode('utf-8'))
            self.asks = []
            for ask_data in asks_data:
                order = Order(
                    order_id=ask_data["order_id"],
                    price=ask_data["price"],
                    quantity=ask_data["quantity"],
                    order_type=ask_data["order_type"],
                    user_id=ask_data["user_id"],
                    stop_loss_price=ask_data.get("stop_loss_price"),
                    stop_price=ask_data.get("stop_price", None),  # Use get with default value
                    limit_price=ask_data.get("limit_price", None)  # Use get with default value
                )
                heapq.heappush(self.asks, (order.price, order.order_id, order))

            logging.info("Order book state loaded from Redis.")

# Redis connection details
redis_host = "localhost"
redis_port = 6379
redis_channel = "orders"

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port)

position_limits = {
    "asset_1": 100,
}

# Initialize user positions in Redis
user_positions = {}
user_positions["user1"] = {"asset_1": 0}

# Initialize PositionManager
position_manager = PositionManager(r, position_limits)

# Initialize OrderBook
order_book = OrderBook(r, position_manager)
order_book.load_state_from_redis()
order_book.match_orders()

# Function to initialize the order book and load state
def initialize_order_book():
    try:
        # Attempt to ping the Redis server to check if it's running
        r.ping()
        order_book.load_state_from_redis()
        logging.info("Order book initialized and state loaded from Redis.")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Could not connect to Redis: {e}")
        # Handle the case where Redis is not running (e.g., retry, exit)

# Call the initialization function
initialize_order_book()

def process_message(message):
    """Processes incoming order messages from Redis."""
    try:
        if isinstance(message, bytes):
            message = message.decode('utf-8')

        order_data = json.loads(message)
        logging.info(f"Order data: {order_data}")

        order = Order(
            order_id=order_data["order_id"],
            price=order_data["price"],
            quantity=order_data["quantity"],
            order_type=order_data["order_type"],
            user_id=order_data["user_id"],
            stop_loss_price=order_data.get("stop_loss_price"),
            stop_price=order_data.get("stop_price"),
            limit_price=order_data.get("limit_price")
        )

        logging.info(f"Order created: {order}")
        if order_book.add_order(order):
            order_book.save_state_to_redis()  # Save state to Redis after adding an order

    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON message: {message}")
    except KeyError as e:
        logging.error(f"Missing key in message: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# Subscribe to the Redis channel
p = r.pubsub()
p.subscribe(redis_channel)

# Main loop to listen for messages
print("Listening for orders...")
for message in p.listen():
    if message['type'] == 'message':
        process_message(message['data'])

# Basic Unit Test
class TestOrderBook(unittest.TestCase):
    def setUp(self):
        """Set up for test methods."""
        # No need to load state here, it's already loaded globally
        pass

    def test_add_order_and_match(self):
        # Create a simple buy and sell order
        buy_order = Order(order_id=1, price=100, quantity=5, order_type="limit", user_id="user1")
        sell_order = Order(order_id=2, price=100, quantity=5, order_type="limit", user_id="user2")

        # Add the orders to the order book
        order_book.add_order(buy_order)
        order_book.add_order(sell_order)

        # Assert that the orders were matched
        self.assertEqual(buy_order.quantity, 0)
        self.assertEqual(sell_order.quantity, 0)

if __name__ == '__main__':
    unittest.main()