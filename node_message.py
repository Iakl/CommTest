#!/usr/bin/env python3
"""
Send messages through the distributed node network
This script instructs a node to send a message to its peers
Usage: python node_message.py <local_node_port> <target_node_id> <message>
"""

import requests
import sys
import time

# Node configuration mapping
NODE_ADDRESSES = {
    'node1': 'http://192.168.1.137:5001',
    'node2': 'http://192.168.1.137:5002', 
    'node3': 'http://192.168.1.193:5003'
}

def get_node_status(local_port):
    """Get the status of the local node"""
    try:
        response = requests.get(f"http://localhost:{local_port}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def send_message_via_node(local_port, target_node_id, message):
    """Send a message via the local node to a target node"""
    
    # Get local node info
    local_status = get_node_status(local_port)
    if not local_status:
        print(f"❌ Could not connect to local node on port {local_port}")
        return False
    
    local_node_id = local_status.get('node_id', f'localhost:{local_port}')
    peers = local_status.get('peers', [])
    
    print(f"Local node: {local_node_id}")
    print(f"Available peers: {len(peers)}")
    for peer in peers:
        print(f"  - {peer}")
    
    # Find target node address
    target_address = NODE_ADDRESSES.get(target_node_id)
    if not target_address:
        print(f"❌ Unknown target node: {target_node_id}")
        print(f"Available nodes: {list(NODE_ADDRESSES.keys())}")
        return False
    
    # Check if target is in our peer list
    if target_address not in peers:
        print(f"❌ Target node {target_node_id} ({target_address}) is not in peer list")
        print("Available peers:")
        for peer in peers:
            print(f"  - {peer}")
        return False
    
    # Prepare message payload
    payload = {
        'from_node': local_node_id,
        'type': 'custom_message',
        'data': {
            'message': message,
            'target_node': target_node_id,
            'sent_via': f'{local_node_id} network'
        },
        'timestamp': time.time()
    }
    
    try:
        print(f"\nSending message to {target_node_id} at {target_address}...")
        print(f"Message: '{message}'")
        
        # Send directly to target node
        response = requests.post(
            f"{target_address}/message",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Message delivered successfully!")
            print(f"Response: {result}")
            return True
        else:
            print(f"❌ Failed to deliver message - HTTP {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error sending message: {e}")
        return False

def main():
    if len(sys.argv) < 4:
        print("Usage: python node_message.py <local_node_port> <target_node_id> <message>")
        print("Example: python node_message.py 5001 node2 'Hello, node2!'")
        print(f"Available target nodes: {list(NODE_ADDRESSES.keys())}")
        sys.exit(1)
    
    local_port = sys.argv[1]
    target_node_id = sys.argv[2]
    message = ' '.join(sys.argv[3:])
    
    print("=" * 60)
    print("NODE-TO-NODE MESSENGER")
    print("=" * 60)
    
    success = send_message_via_node(local_port, target_node_id, message)
    
    if success:
        print(f"\n✅ Message sent from your local node to {target_node_id}!")
        print(f"Check {target_node_id}'s console for the received message.")
    else:
        print(f"\n❌ Failed to send message to {target_node_id}")

if __name__ == "__main__":
    main()