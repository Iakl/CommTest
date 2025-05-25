#!/usr/bin/env python3
"""
Python script to establish selective network topology
Usage: python connect_nodes.py <node_id> [local_port]
"""

import requests
import sys
import time
import json

# Configuration
NODE_CONFIG = {
    'node1': {
        'peers': ['http://192.168.1.137:5002'],  # Only connect to node2
        'description': 'Node1 setup: connecting only to node2'
    },
    'node2': {
        'peers': [
            'http://192.168.1.137:5001',  # Connect to node1
            'http://192.168.1.193:5003'   # Connect to node3
        ],
        'description': 'Node2 setup: connecting to node1 and node3'
    },
    'node3': {
        'peers': ['http://192.168.1.137:5002'],  # Only connect to node2
        'description': 'Node3 setup: connecting only to node2'
    }
}

def check_local_node(local_url):
    """Check if local node is running"""
    try:
        response = requests.get(f"{local_url}/status", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Local node {data.get('node_id', 'unknown')} is running")
            return True
        else:
            print(f"❌ Local node returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Local node not running on {local_url}")
        print(f"Error: {e}")
        return False

def add_peer(local_url, peer_address):
    """Add a peer to the local node"""
    print(f"Connecting to: {peer_address}")
    
    try:
        response = requests.post(
            f"{local_url}/add_peer",
            json={'peer_address': peer_address},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'peer_added':
                print(f"✅ Successfully connected to {peer_address}")
                return True
            else:
                print(f"❌ Failed to connect to {peer_address}")
                print(f"Response: {result}")
                return False
        else:
            print(f"❌ Failed to connect to {peer_address} - HTTP {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to {peer_address}: {e}")
        return False

def get_node_status(local_url):
    """Get current node status"""
    try:
        response = requests.get(f"{local_url}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.exceptions.RequestException:
        return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python connect_nodes.py <node_id> [local_port]")
        print("Example: python connect_nodes.py node1 5001")
        print("Valid node IDs: node1, node2, node3")
        sys.exit(1)
    
    node_id = sys.argv[1]
    local_port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    local_url = f"http://localhost:{local_port}"
    
    print(f"Setting up connections for {node_id} on port {local_port}...")
    
    # Validate node ID
    if node_id not in NODE_CONFIG:
        print(f"❌ Unknown node ID: {node_id}")
        print(f"Valid node IDs: {', '.join(NODE_CONFIG.keys())}")
        sys.exit(1)
    
    # Check if local node is running
    print("Checking if local node is running...")
    if not check_local_node(local_url):
        print(f"Start it with: python distributed_worker_rest.py {node_id} {local_port}")
        sys.exit(1)
    
    # Wait a bit for node to be fully ready
    time.sleep(2)
    
    # Get configuration for this node
    config = NODE_CONFIG[node_id]
    print(config['description'])
    
    # Connect to peers
    success_count = 0
    total_peers = len(config['peers'])
    
    for peer_address in config['peers']:
        if add_peer(local_url, peer_address):
            success_count += 1
    
    print(f"\nConnection setup complete!")
    print(f"Successfully connected to {success_count}/{total_peers} peers")
    
    # Show final status
    print("\nChecking final status...")
    status = get_node_status(local_url)
    
    if status:
        peer_count = len(status.get('peers', []))
        print(f"✅ {node_id} now has {peer_count} peer(s) connected")
        
        if status.get('peers'):
            print("Peers:")
            for peer in status['peers']:
                print(f"  - {peer}")
        
        print(f"Queue size: {status.get('queue_size', 0)}")
        print(f"Running: {status.get('running', False)}")
    else:
        print("❌ Could not retrieve final status")

if __name__ == "__main__":
    main()