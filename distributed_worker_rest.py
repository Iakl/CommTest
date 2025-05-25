#!/usr/bin/env python3
"""
Distributed Worker using REST API
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

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

class DistributedWorker:
    def __init__(self, node_id, host='0.0.0.0', port=5000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = []  # List of peer addresses: ['http://192.168.1.100:5000']
        self.task_queue = queue.Queue()
        self.responses = {}  # Store responses from other nodes
        self.running = True
        
    def add_peer(self, peer_address):
        """Add a peer node address"""
        if peer_address not in self.peers:
            self.peers.append(peer_address)
            print(f"Added peer: {peer_address}")
    
    def send_message(self, peer_address, message_type, data):
        """Send a message to a peer node"""
        try:
            response = requests.post(
                f"{peer_address}/message",
                json={
                    'from_node': self.node_id,
                    'type': message_type,
                    'data': data
                },
                timeout=10
            )
            return response.json()
        except Exception as e:
            print(f"Failed to send message to {peer_address}: {e}")
            return None
    
    def broadcast_message(self, message_type, data):
        """Send a message to all peer nodes"""
        responses = []
        for peer in self.peers:
            response = self.send_message(peer, message_type, data)
            if response:
                responses.append(response)
        return responses
    
    def wait_for_responses(self, message_id, expected_count, timeout=30):
        """Wait for responses from other nodes"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if message_id in self.responses:
                if len(self.responses[message_id]) >= expected_count:
                    return self.responses[message_id]
            time.sleep(0.1)
        return self.responses.get(message_id, [])
    
    def process_task(self, task_data):
        """Process a task - this is where your actual work happens"""
        task_id = task_data.get('task_id', 'unknown')
        print(f"Node {self.node_id}: Processing task {task_id}")
        
        # Simulate some work
        time.sleep(2)
        result = f"Task {task_id} completed by node {self.node_id}"
        
        # If we need input from other nodes
        if task_data.get('needs_collaboration'):
            message_id = f"collab_{task_id}_{int(time.time())}"
            print(f"Node {self.node_id}: Requesting collaboration for task {task_id}")
            
            # Ask other nodes for their input
            responses = self.broadcast_message('collaboration_request', {
                'message_id': message_id,
                'task_id': task_id,
                'question': f"What should node {self.node_id} do for task {task_id}?"
            })
            
            # Wait for responses
            if self.peers:
                peer_responses = self.wait_for_responses(message_id, len(self.peers))
                result += f" with input from: {[r['data'] for r in peer_responses]}"
        
        return result
    
    def worker_loop(self):
        """Main worker loop"""
        while self.running:
            try:
                # Get task from queue (with timeout)
                task = self.task_queue.get(timeout=1)
                result = self.process_task(task)
                print(f"Node {self.node_id}: {result}")
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except KeyboardInterrupt:
                break
    
    def start(self):
        """Start the worker"""
        # Start the worker thread
        worker_thread = threading.Thread(target=self.worker_loop, daemon=True)
        worker_thread.start()
        
        # Start Flask server
        print(f"Starting node {self.node_id} on {self.host}:{self.port}")
        app.run(host=self.host, port=self.port, debug=False)

# Global worker instance
worker = None

@app.route('/message', methods=['POST'])
def handle_message():
    """Handle incoming messages from other nodes"""
    data = request.json
    from_node = data['from_node']
    message_type = data['type']
    message_data = data['data']
    
    print(f"Node {worker.node_id}: Received {message_type} from {from_node}")
    
    if message_type == 'collaboration_request':
        # Respond to collaboration request
        message_id = message_data['message_id']
        task_id = message_data['task_id']
        
        response_data = f"Node {worker.node_id} suggests: Use algorithm X for task {task_id}"
        
        return jsonify({
            'status': 'success',
            'message_id': message_id,
            'data': response_data
        })
    
    elif message_type == 'collaboration_response':
        # Store response from another node
        message_id = message_data['message_id']
        if message_id not in worker.responses:
            worker.responses[message_id] = []
        worker.responses[message_id].append(data)
        
        return jsonify({'status': 'received'})
    
    return jsonify({'status': 'unknown_message_type'})

@app.route('/add_task', methods=['POST'])
def add_task():
    """Add a task to this node's queue"""
    task_data = request.json
    worker.task_queue.put(task_data)
    return jsonify({'status': 'task_added', 'queue_size': worker.task_queue.qsize()})

@app.route('/status', methods=['GET'])
def get_status():
    """Get node status"""
    return jsonify({
        'node_id': worker.node_id,
        'peers': worker.peers,
        'queue_size': worker.task_queue.qsize(),
        'active_responses': len(worker.responses)
    })

@app.route('/add_peer', methods=['POST'])
def add_peer():
    """Add a peer node"""
    peer_address = request.json['peer_address']
    worker.add_peer(peer_address)
    return jsonify({'status': 'peer_added'})

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python distributed_worker.py <node_id> [port]")
        print("Example: python distributed_worker.py node1 5000")
        sys.exit(1)
    
    node_id = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    
    worker = DistributedWorker(node_id, port=port)
    
    # Example: Add some initial tasks
    worker.task_queue.put({'task_id': 'init_1', 'needs_collaboration': False})
    worker.task_queue.put({'task_id': 'collab_1', 'needs_collaboration': True})
    
    worker.start()