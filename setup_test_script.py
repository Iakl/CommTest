#!/usr/bin/env python3
"""
Setup script to test the distributed worker system
This can be used to test locally or configure remote nodes
"""

import requests
import json
import time
import threading

def setup_nodes():
    """Configure nodes to know about each other"""
    # Assuming you have two nodes running:
    # Node 1: localhost:5000
    # Node 2: localhost:5001 (or another computer's IP)
    
    nodes = [
        {'id': 'node1', 'address': 'http://localhost:5000'},
        {'id': 'node2', 'address': 'http://localhost:5001'}
    ]
    
    print("Setting up peer connections...")
    
    # Make each node aware of the others
    for i, node in enumerate(nodes):
        for j, peer in enumerate(nodes):
            if i != j:  # Don't add self as peer
                try:
                    response = requests.post(
                        f"{node['address']}/add_peer",
                        json={'peer_address': peer['address']},
                        timeout=5
                    )
                    print(f"{node['id']} -> {peer['id']}: {response.json()}")
                except Exception as e:
                    print(f"Failed to connect {node['id']} to {peer['id']}: {e}")

def add_test_tasks():
    """Add some test tasks to the nodes"""
    tasks = [
        {'task_id': 'task_1', 'needs_collaboration': False, 'description': 'Simple task'},
        {'task_id': 'task_2', 'needs_collaboration': True, 'description': 'Collaborative task'},
        {'task_id': 'task_3', 'needs_collaboration': True, 'description': 'Another collaborative task'}
    ]
    
    nodes = ['http://localhost:5000', 'http://localhost:5001']
    
    print("\nAdding test tasks...")
    for i, task in enumerate(tasks):
        node_address = nodes[i % len(nodes)]  # Distribute tasks
        try:
            response = requests.post(f"{node_address}/add_task", json=task, timeout=5)
            print(f"Added {task['task_id']} to {node_address}: {response.json()}")
        except Exception as e:
            print(f"Failed to add task to {node_address}: {e}")

def monitor_status():
    """Monitor the status of all nodes"""
    nodes = ['http://localhost:5000', 'http://localhost:5001']
    
    print("\n" + "="*50)
    print("NODE STATUS MONITORING")
    print("="*50)
    
    for node_address in nodes:
        try:
            response = requests.get(f"{node_address}/status", timeout=5)
            status = response.json()
            print(f"\n{node_address}:")
            print(f"  Node ID: {status['node_id']}")
            print(f"  Peers: {status['peers']}")
            print(f"  Queue Size: {status['queue_size']}")
            print(f"  Active Responses: {status['active_responses']}")
        except Exception as e:
            print(f"\n{node_address}: OFFLINE ({e})")

def continuous_monitoring():
    """Continuously monitor node status"""
    while True:
        time.sleep(10)
        monitor_status()

if __name__ == "__main__":
    print("Distributed Worker Setup Script")
    print("="*40)
    
    # Wait a bit for nodes to start
    print("Waiting for nodes to start...")
    time.sleep(3)
    
    # Setup peer connections
    setup_nodes()
    
    # Add test tasks
    add_test_tasks()
    
    # Initial status check
    time.sleep(2)
    monitor_status()
    
    # Start continuous monitoring in a separate thread
    monitor_thread = threading.Thread(target=continuous_monitoring, daemon=True)
    monitor_thread.start()
    
    print("\nSetup complete! Monitoring status every 10 seconds...")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping monitor...")
