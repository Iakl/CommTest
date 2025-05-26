#!/usr/bin/env python3
"""
Node script - connects only to node2
"""

import requests
import threading
import time
import sys
import json
from flask import Flask, request, jsonify


class CommNode:
    def __init__(self, node_id, port, peers=[], self_consumption=0.0):
        self.node_id = node_id
        self.port = port
        self.running = True
        self.peers_addresses = {}
        self.app = Flask(__name__)
        self.callback = None
        self.self_consumption = self_consumption
        self.peers_data = {}

        # Initialize counters
        for peer in peers:
            self.peers_data[peer] = self.self_consumption

        # Extract peer addresses from adresses.json
        for peer in peers:
            try:
                with open('addresses.json', 'r') as f:
                    addresses = json.load(f)
                    self.peers_addresses[peer] = addresses[peer]
            except KeyError:
                raise Exception(
                    f"❌ Peer address for {peer} not found in addresses.json. Please ensure it is defined.")
            except FileNotFoundError:
                raise Exception(
                    "❌ addresses.json not found. Please create it with peer addresses.")
            except json.JSONDecodeError:
                raise Exception(
                    "❌ Error decoding addresses.json. Please ensure it is valid JSON.")

        self._setup_routes()

    def _setup_routes(self):
        """Setup Flask routes"""
        @self.app.route('/message', methods=['POST'])
        def handle_message():
            return self._handle_message_route()

        @self.app.route('/status', methods=['GET'])
        def get_status():
            return self._get_status_route()

    def _handle_message_route(self):
        """Handle incoming messages"""
        data = request.json
        from_node = data.get('from_node', 'unknown')
        message_type = data.get('type', 'unknown')
        message_data = data.get('data', {})

        if message_type == 'chat_message':
            message = message_data.get('message', '')
            self.handle_chat_message(from_node, message)

        return jsonify({'status': 'received'})

    def _get_status_route(self):
        """Get node status"""
        return jsonify({
            'node_id': self.node_id,
            'peers': self.peers,
            'running': self.running,
            'message_counters': self.message_counter
        })

    def send_message_with_retry(self, peer_address, message):
        """Send message to a peer with persistent retry in separate thread"""
        def retry_send():
            payload = {
                'from_node': self.node_id,
                'type': 'chat_message',
                'data': {
                    'message': message,
                    'timestamp': time.time()
                }
            }

            retry_count = 0
            while self.running:
                try:
                    response = requests.post(
                        f"{peer_address}/message", json=payload, timeout=5)
                    if response.status_code == 200:
                        print(
                            f"✅ Sent to {peer_address}: {message} (after {retry_count} retries)")
                        return True
                    else:
                        retry_count += 1
                        print(
                            f"⚠️ Failed to send to {peer_address}, retrying... ({retry_count})")
                except Exception as e:
                    retry_count += 1
                    print(
                        f"⚠️ Error sending to {peer_address}: {e}, retrying... ({retry_count})")

                # Wait before retry (exponential backoff capped at 10 seconds)
                wait_time = min(2 ** min(retry_count - 1, 3), 10)
                time.sleep(wait_time)

            return False

        # Start retry in separate thread to allow parallel communications
        threading.Thread(target=retry_send, daemon=True).start()

    def send_message(self, peer_address, message):
        """Send message to a peer (legacy method for immediate calls)"""
        self.send_message_with_retry(peer_address, message)

    def send_initial_messages(self):
        """Send initial online message"""
        # time.sleep(3)  # Wait for all nodes to be ready
        for peer_address in self.peers_addresses.values():
            self.send_message(peer_address, "online")

    def handle_chat_message(self, from_node, message):
        """Handle incoming chat messages"""
        print(f"📨 Received from {from_node}: {message}")

        from_address = self.peers_addresses.get(from_node)

        if isinstance(message, str) and "online" in message:
            if not from_address:
                print(f"❌ Unknown peer: {from_node}")
                return
            self.self_consumption = self.callback(self.self_consumption,
                                                  self.peers_data.values())
            self.send_message(from_address, self.self_consumption)

        elif isinstance(message, float):
            self.peers_data[from_node] = message
            self.self_consumption = self.callback(self.self_consumption,
                                                  self.peers_data.values())
            threading.Timer(2.0, lambda: self.send_message(
                from_address, self.self_consumption)).start()

    def set_callback(self, callback):
        """Set a callback function for incoming messages"""
        self.callback = callback

    def start(self):
        """Start the node"""
        print(f"🚀 Starting {self.node_id} on port {self.port}")

        # Send initial messages after a delay
        threading.Timer(5.0, self.send_initial_messages).start()

        # Start Flask server
        self.app.run(host='0.0.0.0', port=self.port,
                     debug=False, threaded=True)

    def shutdown(self):
        """Graceful shutdown"""
        print(f"\n🛑 Stopping {self.node_id}...")
        self.running = False
        time.sleep(2)  # Give retry threads time to stop
        print(f"🛑 {self.node_id} stopped")


# if __name__ == "__main__":
#     try:
#         node.start()
#     except KeyboardInterrupt:
#         print(f"\n🛑 Stopping {node.node_id}...")
#         node.running = False
#         time.sleep(2)  # Give retry threads time to stop
#         print(f"🛑 {node.node_id} stopped")
#         sys.exit(0)
