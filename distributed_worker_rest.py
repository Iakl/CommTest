#!/usr/bin/env python3
"""
Improved Distributed Worker using REST API
Run on different computers with different node_ids and peer addresses
"""

import flask
from flask import Flask, request, jsonify
import requests
import threading
import time
import queue
import sys
import logging
import signal
import socket
from contextlib import closing

app = Flask(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class DistributedWorker:
    def __init__(self, node_id, host='0.0.0.0', port=5000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = []  # List of peer addresses: ['http://192.168.1.100:5000']
        self.task_queue = queue.Queue()
        self.responses = {}  # Store responses from other nodes
        self.response_lock = threading.Lock()  # Thread safety for responses
        self.running = True
        self.worker_thread = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Node {self.node_id}: Received shutdown signal")
        self.shutdown()
        sys.exit(0)

    def _is_port_available(self, host, port):
        """Check if a port is available"""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            try:
                sock.bind((host, port))
                return True
            except socket.error:
                return False

    def add_peer(self, peer_address):
        """Add a peer node address with validation"""
        if not peer_address.startswith('http'):
            peer_address = f"http://{peer_address}"

        if peer_address not in self.peers:
            # Test connection to peer
            try:
                response = requests.get(f"{peer_address}/status", timeout=5)
                if response.status_code == 200:
                    self.peers.append(peer_address)
                    logging.info(f"Added peer: {peer_address}")
                    return True
                else:
                    logging.warning(
                        f"Peer {peer_address} not responding properly")
                    return False
            except Exception as e:
                logging.error(f"Failed to connect to peer {peer_address}: {e}")
                return False
        return True

    def remove_peer(self, peer_address):
        """Remove a peer node"""
        if peer_address in self.peers:
            self.peers.remove(peer_address)
            logging.info(f"Removed peer: {peer_address}")

    def send_message(self, peer_address, message_type, data, timeout=10):
        """Send a message to a peer node with improved error handling"""
        try:
            response = requests.post(
                f"{peer_address}/message",
                json={
                    'from_node': self.node_id,
                    'type': message_type,
                    'data': data,
                    'timestamp': time.time()
                },
                timeout=timeout
            )
            if response.status_code == 200:
                return response.json()
            else:
                logging.warning(
                    f"Peer {peer_address} returned status {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            logging.error(f"Timeout sending message to {peer_address}")
            # Consider removing unresponsive peer
            self.remove_peer(peer_address)
            return None
        except Exception as e:
            logging.error(f"Failed to send message to {peer_address}: {e}")
            return None

    def broadcast_message(self, message_type, data):
        """Send a message to all peer nodes"""
        responses = []
        dead_peers = []

        for peer in self.peers[:]:  # Copy list to avoid modification during iteration
            response = self.send_message(peer, message_type, data)
            if response:
                responses.append(response)
            else:
                dead_peers.append(peer)

        # Remove dead peers
        for peer in dead_peers:
            if peer in self.peers:
                self.remove_peer(peer)

        return responses

    def wait_for_responses(self, message_id, expected_count, timeout=30):
        """Wait for responses from other nodes with improved logic"""
        start_time = time.time()
        while time.time() - start_time < timeout and self.running:
            with self.response_lock:
                if message_id in self.responses:
                    if len(self.responses[message_id]) >= expected_count:
                        return self.responses[message_id]
            time.sleep(0.1)

        with self.response_lock:
            return self.responses.get(message_id, [])

    def process_task(self, task_data):
        """Process a task - this is where your actual work happens"""
        task_id = task_data.get('task_id', 'unknown')
        logging.info(f"Node {self.node_id}: Processing task {task_id}")

        # Simulate some work
        work_time = task_data.get('work_time', 2)
        time.sleep(work_time)
        result = f"Task {task_id} completed by node {self.node_id}"

        # If we need input from other nodes
        if task_data.get('needs_collaboration'):
            message_id = f"collab_{task_id}_{int(time.time())}"
            logging.info(
                f"Node {self.node_id}: Requesting collaboration for task {task_id}")

            # Ask other nodes for their input
            responses = self.broadcast_message('collaboration_request', {
                'message_id': message_id,
                'task_id': task_id,
                'question': f"What should node {self.node_id} do for task {task_id}?",
                'requesting_node': self.node_id
            })

            # Wait for responses
            if self.peers:
                peer_responses = self.wait_for_responses(
                    message_id, len(self.peers))
                if peer_responses:
                    result += f" with input from: {[r.get('data', 'No response') for r in peer_responses]}"
                else:
                    result += " (no peer responses received)"

        return result

    def worker_loop(self):
        """Main worker loop with improved error handling"""
        logging.info(f"Node {self.node_id}: Worker loop started")
        while self.running:
            try:
                # Get task from queue (with timeout)
                task = self.task_queue.get(timeout=1)
                result = self.process_task(task)
                logging.info(f"Node {self.node_id}: {result}")
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error processing task: {e}")
                continue
        logging.info(f"Node {self.node_id}: Worker loop stopped")

    def start(self):
        """Start the worker with improved initialization"""
        # Check if port is available
        if not self._is_port_available(self.host if self.host != '0.0.0.0' else 'localhost', self.port):
            logging.error(f"Port {self.port} is already in use")
            return False

        # Start the worker thread
        self.worker_thread = threading.Thread(
            target=self.worker_loop, daemon=True)
        self.worker_thread.start()

        # Start Flask server
        logging.info(
            f"Starting node {self.node_id} on {self.host}:{self.port}")
        try:
            # Disable Flask's reloader and set threaded=True
            app.run(host=self.host, port=self.port, debug=False,
                    threaded=True, use_reloader=False)
        except Exception as e:
            logging.error(f"Failed to start Flask server: {e}")
            return False
        return True

    def shutdown(self):
        """Graceful shutdown"""
        logging.info(f"Node {self.node_id}: Shutting down...")
        self.running = False

        # Notify peers about shutdown
        self.broadcast_message('node_shutdown', {'node_id': self.node_id})

        # Wait for worker thread to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)

        logging.info(f"Node {self.node_id}: Shutdown complete")


# Global worker instance
worker = None


@app.route('/message', methods=['POST'])
def handle_message():
    """Handle incoming messages from other nodes"""
    if not worker:
        return jsonify({'status': 'error', 'message': 'Worker not initialized'}), 500

    data = request.json
    from_node = data.get('from_node', 'unknown')
    message_type = data.get('type', 'unknown')
    message_data = data.get('data', {})
    timestamp = data.get('timestamp', time.time())

    logging.info(
        f"Node {worker.node_id}: Received {message_type} from {from_node}")

    if message_type == 'collaboration_request':
        # Respond to collaboration request
        message_id = message_data.get('message_id')
        task_id = message_data.get('task_id')
        requesting_node = message_data.get('requesting_node')

        if not message_id or not task_id:
            return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400

        response_data = f"Node {worker.node_id} suggests: Use algorithm X for task {task_id}"

        # Send response back to requesting node
        requesting_node_address = None
        for peer in worker.peers:
            try:
                status_response = requests.get(f"{peer}/status", timeout=5)
                if status_response.status_code == 200:
                    peer_status = status_response.json()
                    if peer_status.get('node_id') == requesting_node:
                        requesting_node_address = peer
                        break
            except:
                continue

        if requesting_node_address:
            worker.send_message(requesting_node_address, 'collaboration_response', {
                'message_id': message_id,
                'data': response_data,
                'responding_node': worker.node_id
            })

        return jsonify({
            'status': 'success',
            'message_id': message_id,
            'data': response_data
        })

    elif message_type == 'collaboration_response':
        # Store response from another node
        message_id = message_data.get('message_id')
        if message_id:
            with worker.response_lock:
                if message_id not in worker.responses:
                    worker.responses[message_id] = []
                worker.responses[message_id].append({
                    'from_node': from_node,
                    'data': message_data.get('data'),
                    'timestamp': timestamp
                })

        return jsonify({'status': 'received'})

    elif message_type == 'node_shutdown':
        # Handle peer shutdown notification
        shutting_down_node = message_data.get('node_id')
        # Remove from peers list
        peers_to_remove = [
            peer for peer in worker.peers if shutting_down_node in peer]
        for peer in peers_to_remove:
            worker.remove_peer(peer)

        return jsonify({'status': 'acknowledged'})

    elif message_type == 'ping':
        # Simple ping/pong for health checks
        return jsonify({
            'status': 'pong',
            'node_id': worker.node_id,
            'timestamp': time.time()
        })

    elif message_type == 'custom_message':
        # Handle custom messages
        message_content = message_data.get('message', '')
        target_node = message_data.get('target_node', '')

        # Print to console
        print(f"\n🔔 CUSTOM MESSAGE RECEIVED 🔔")
        print(f"From: {from_node}")
        print(f"To: {target_node}")
        print(f"Message: {message_content}")
        print(f"Timestamp: {timestamp}")
        print("-" * 40)

        # Log it too
        logging.info(f"Custom message from {from_node}: {message_content}")

        return jsonify({
            'status': 'message_received',
            'from_node': from_node,
            'to_node': worker.node_id,
            'message': message_content
        })

    return jsonify({'status': 'unknown_message_type'}), 400


@app.route('/add_task', methods=['POST'])
def add_task():
    """Add a task to this node's queue"""
    if not worker:
        return jsonify({'status': 'error', 'message': 'Worker not initialized'}), 500

    task_data = request.json
    if not task_data or 'task_id' not in task_data:
        return jsonify({'status': 'error', 'message': 'Invalid task data'}), 400

    worker.task_queue.put(task_data)
    return jsonify({'status': 'task_added', 'queue_size': worker.task_queue.qsize()})


@app.route('/status', methods=['GET'])
def get_status():
    """Get node status"""
    if not worker:
        return jsonify({'status': 'error', 'message': 'Worker not initialized'}), 500

    return jsonify({
        'node_id': worker.node_id,
        'peers': worker.peers,
        'queue_size': worker.task_queue.qsize(),
        'active_responses': len(worker.responses),
        'running': worker.running,
        'timestamp': time.time()
    })


@app.route('/add_peer', methods=['POST'])
def add_peer():
    """Add a peer node"""
    if not worker:
        return jsonify({'status': 'error', 'message': 'Worker not initialized'}), 500

    data = request.json
    if not data or 'peer_address' not in data:
        return jsonify({'status': 'error', 'message': 'Missing peer_address'}), 400

    peer_address = data['peer_address']
    success = worker.add_peer(peer_address)

    if success:
        return jsonify({'status': 'peer_added', 'peers': worker.peers})
    else:
        return jsonify({'status': 'failed_to_add_peer'}), 400


@app.route('/remove_peer', methods=['POST'])
def remove_peer():
    """Remove a peer node"""
    if not worker:
        return jsonify({'status': 'error', 'message': 'Worker not initialized'}), 500

    data = request.json
    if not data or 'peer_address' not in data:
        return jsonify({'status': 'error', 'message': 'Missing peer_address'}), 400

    peer_address = data['peer_address']
    worker.remove_peer(peer_address)
    return jsonify({'status': 'peer_removed', 'peers': worker.peers})


@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'node_id': worker.node_id if worker else 'uninitialized',
        'timestamp': time.time()
    })


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python distributed_worker_rest.py <node_id> [port]")
        print("Example: python distributed_worker_rest.py node1 5000")
        sys.exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    worker = DistributedWorker(node_id, port=port)

    # Example: Add some initial tasks
    worker.task_queue.put({'task_id': 'init_1', 'needs_collaboration': False})
    worker.task_queue.put({'task_id': 'collab_1', 'needs_collaboration': True})

    try:
        worker.start()
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
        worker.shutdown()
        print("✅ Node stopped successfully")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        if worker:
            worker.shutdown()
        sys.exit(1)
