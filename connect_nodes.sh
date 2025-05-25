#!/bin/bash

# Selective topology setup script
# Usage: ./connect_nodes.sh <node_id> [local_port]
# Example: ./connect_nodes.sh node1 5000

if [ $# -lt 1 ]; then
    echo "Usage: $0 <node_id> [local_port]"
    echo "Example: $0 node1 5000"
    echo "Node IDs: node1, node2, node3"
    exit 1
fi

NODE_ID=$1
LOCAL_PORT=${2:-5001}
LOCAL_URL="http://localhost:$LOCAL_PORT"

# IP addresses from your nodes.json
NODE1_IP="192.168.1.137:5001"
NODE2_IP="192.168.1.137:5002" 
NODE3_IP="192.168.1.193:5003"

echo "Setting up connections for $NODE_ID on port $LOCAL_PORT..."

# Function to add peer
add_peer() {
    local peer_address=$1
    echo "Connecting to: $peer_address"
    
    response=$(curl -s -X POST "$LOCAL_URL/add_peer" \
        -H "Content-Type: application/json" \
        -d "{\"peer_address\": \"http://$peer_address\"}")
    
    if echo "$response" | grep -q "peer_added"; then
        echo "✅ Successfully connected to $peer_address"
    else
        echo "❌ Failed to connect to $peer_address"
        echo "Response: $response"
    fi
}

# Function to check if local node is running
check_local_node() {
    if ! curl -s "$LOCAL_URL/status" > /dev/null; then
        echo "❌ Local node not running on $LOCAL_URL"
        echo "Start it with: python distributed_worker_rest.py $NODE_ID $LOCAL_PORT"
        exit 1
    fi
    echo "✅ Local node $NODE_ID is running"
}

# Wait for local node to be ready
echo "Checking if local node is running..."
check_local_node

# Sleep a bit to ensure node is fully ready
sleep 2

# Setup connections based on node ID
case $NODE_ID in
    "node1")
        echo "Node1 setup: connecting only to node2"
        add_peer "$NODE2_IP"
        ;;
    "node2") 
        echo "Node2 setup: connecting to node1 and node3"
        add_peer "$NODE1_IP"
        add_peer "$NODE3_IP"
        ;;
    "node3")
        echo "Node3 setup: connecting only to node2" 
        add_peer "$NODE2_IP"
        ;;
    *)
        echo "❌ Unknown node ID: $NODE_ID"
        echo "Valid node IDs: node1, node2, node3"
        exit 1
        ;;
esac

echo ""
echo "Connection setup complete!"
echo "Checking final status..."

# Show final status
status=$(curl -s "$LOCAL_URL/status")
peer_count=$(echo "$status" | grep -o '"peers":\[[^]]*\]' | grep -o 'http://[^"]*' | wc -l)
echo "✅ $NODE_ID now has $peer_count peer(s) connected"

# Show peer list
echo "Peers:"
echo "$status" | grep -o '"peers":\[[^]]*\]' | grep -o 'http://[^"]*' | sed 's/^/  - /'