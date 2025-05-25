#!/usr/bin/env python3
"""
Improved setup script to test the distributed worker system
This can be used to test locally or configure remote nodes
"""

import requests
import json
import time
import threading
import argparse
import sys
from datetime import datetime


class NetworkManager:
    def __init__(self, nodes=None):
        self.nodes = nodes or [
            {'id': 'node1', 'address': 'http://localhost:5000'},
            {'id': 'node2', 'address': 'http://localhost:5001'}
        ]
        self.monitoring = False

    def wait_for_nodes(self, timeout=30):
        """Wait for all nodes to become available"""
        print("Waiting for nodes to become available...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            all_ready = True
            for node in self.nodes:
                try:
                    # Try /health first (improved version), fallback to /status (original)
                    response = requests.get(
                        f"{node['address']}/health", timeout=2)
                    if response.status_code != 200:
                        # Fallback to /status for original code
                        response = requests.get(
                            f"{node['address']}/status", timeout=2)
                        if response.status_code != 200:
                            all_ready = False
                            break
                except:
                    all_ready = False
                    break

            if all_ready:
                print("✅ All nodes are ready!")
                return True

            print("⏳ Waiting for nodes...")
            time.sleep(2)

        print("❌ Timeout waiting for nodes")
        return False

    def setup_nodes(self):
        """Configure nodes to know about each other"""
        print("\n" + "="*50)
        print("SETTING UP PEER CONNECTIONS")
        print("="*50)

        success_count = 0
        total_connections = 0

        # Make each node aware of the others
        for i, node in enumerate(self.nodes):
            for j, peer in enumerate(self.nodes):
                if i != j:  # Don't add self as peer
                    total_connections += 1
                    try:
                        response = requests.post(
                            f"{node['address']}/add_peer",
                            json={'peer_address': peer['address']},
                            timeout=10
                        )
                        if response.status_code == 200:
                            result = response.json()
                            print(
                                f"✅ {node['id']} -> {peer['id']}: {result['status']}")
                            success_count += 1
                        else:
                            print(
                                f"❌ {node['id']} -> {peer['id']}: HTTP {response.status_code}")
                    except Exception as e:
                        print(
                            f"❌ Failed to connect {node['id']} to {peer['id']}: {e}")

        print(
            f"\nConnection setup: {success_count}/{total_connections} successful")
        return success_count == total_connections

    def add_test_tasks(self, task_list=None):
        """Add test tasks to the nodes"""
        if task_list is None:
            task_list = [
                {'task_id': 'simple_1', 'needs_collaboration': False,
                    'description': 'Simple computation task', 'work_time': 1},
                {'task_id': 'collab_1', 'needs_collaboration': True,
                    'description': 'Collaborative analysis task', 'work_time': 2},
                {'task_id': 'simple_2', 'needs_collaboration': False,
                    'description': 'Another simple task', 'work_time': 1},
                {'task_id': 'collab_2', 'needs_collaboration': True,
                    'description': 'Complex collaborative task', 'work_time': 3}
            ]

        print("\n" + "="*50)
        print("ADDING TEST TASKS")
        print("="*50)

        success_count = 0
        for i, task in enumerate(task_list):
            node_address = self.nodes[i % len(self.nodes)]['address']
            node_id = self.nodes[i % len(self.nodes)]['id']

            try:
                response = requests.post(
                    f"{node_address}/add_task", json=task, timeout=5)
                if response.status_code == 200:
                    result = response.json()
                    print(
                        f"✅ Added '{task['task_id']}' to {node_id}: queue size = {result['queue_size']}")
                    success_count += 1
                else:
                    print(
                        f"❌ Failed to add '{task['task_id']}' to {node_id}: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ Failed to add '{task['task_id']}' to {node_id}: {e}")

        print(f"\nTask addition: {success_count}/{len(task_list)} successful")
        return success_count

    def get_node_status(self, node_address):
        """Get status of a single node"""
        try:
            response = requests.get(f"{node_address}/status", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return {'error': f'HTTP {response.status_code}'}
        except Exception as e:
            return {'error': str(e)}

    def monitor_status(self, detailed=False):
        """Monitor the status of all nodes"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*60}")
        print(f"NODE STATUS MONITORING - {timestamp}")
        print("="*60)

        total_queue_size = 0
        active_nodes = 0

        for node in self.nodes:
            status = self.get_node_status(node['address'])

            if 'error' in status:
                print(f"\n🔴 {node['address']} ({node['id']}): OFFLINE")
                print(f"   Error: {status['error']}")
            else:
                active_nodes += 1
                queue_size = status.get('queue_size', 0)
                total_queue_size += queue_size

                print(f"\n🟢 {node['address']} ({node['id']}): ONLINE")
                print(f"   Node ID: {status.get('node_id', 'Unknown')}")
                print(f"   Queue Size: {queue_size}")
                print(
                    f"   Active Responses: {status.get('active_responses', 0)}")
                print(f"   Running: {status.get('running', 'Unknown')}")

                if detailed:
                    peers = status.get('peers', [])
                    print(f"   Peers: {len(peers)} connected")
                    for peer in peers:
                        print(f"     - {peer}")

        print(f"\n📊 SUMMARY:")
        print(f"   Active Nodes: {active_nodes}/{len(self.nodes)}")
        print(f"   Total Queue Size: {total_queue_size}")

        return active_nodes, total_queue_size

    def continuous_monitoring(self, interval=10, detailed=False):
        """Continuously monitor node status"""
        self.monitoring = True
        while self.monitoring:
            try:
                time.sleep(interval)
                if self.monitoring:  # Check again in case it changed during sleep
                    self.monitor_status(detailed)
            except KeyboardInterrupt:
                break

    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.monitoring = False

    def test_collaboration(self):
        """Test collaboration between nodes"""
        print("\n" + "="*50)
        print("TESTING COLLABORATION")
        print("="*50)

        # Add a collaborative task
        collab_task = {
            'task_id': f'collab_test_{int(time.time())}',
            'needs_collaboration': True,
            'description': 'Collaboration test task',
            'work_time': 1
        }

        if self.nodes:
            try:
                response = requests.post(
                    f"{self.nodes[0]['address']}/add_task",
                    json=collab_task,
                    timeout=5
                )
                if response.status_code == 200:
                    print(
                        f"✅ Added collaboration test task: {collab_task['task_id']}")
                    print("⏳ Waiting for task processing...")
                    time.sleep(5)
                    return True
                else:
                    print(f"❌ Failed to add collaboration test task")
                    return False
            except Exception as e:
                print(f"❌ Error testing collaboration: {e}")
                return False
        return False

    def ping_all_nodes(self):
        """Send ping to all nodes"""
        print("\n" + "="*50)
        print("PING TEST")
        print("="*50)

        for node in self.nodes:
            try:
                start_time = time.time()
                response = requests.post(
                    f"{node['address']}/message",
                    json={
                        'from_node': 'test_client',
                        'type': 'ping',
                        'data': {},
                        'timestamp': time.time()
                    },
                    timeout=5
                )
                end_time = time.time()

                if response.status_code == 200:
                    result = response.json()
                    latency = (end_time - start_time) * 1000
                    print(
                        f"✅ {node['id']}: {result['status']} (latency: {latency:.2f}ms)")
                else:
                    print(f"❌ {node['id']}: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ {node['id']}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Distributed Worker System Setup and Monitoring')
    parser.add_argument('--nodes', type=str,
                        help='JSON file with node configurations')
    parser.add_argument('--ports', type=str, default='5000,5001',
                        help='Comma-separated list of ports for localhost nodes')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Host for nodes (default: localhost)')
    parser.add_argument('--wait-timeout', type=int, default=30,
                        help='Timeout waiting for nodes to start')
    parser.add_argument('--monitor-interval', type=int,
                        default=10, help='Monitoring interval in seconds')
    parser.add_argument('--detailed', action='store_true',
                        help='Show detailed monitoring information')
    parser.add_argument('--no-tasks', action='store_true',
                        help='Skip adding test tasks')
    parser.add_argument('--ping-only', action='store_true',
                        help='Only run ping test')

    args = parser.parse_args()

    # Setup nodes configuration
    if args.nodes:
        try:
            with open(args.nodes, 'r') as f:
                nodes = json.load(f)
        except Exception as e:
            print(f"Error loading nodes file: {e}")
            sys.exit(1)
    else:
        ports = [int(p.strip()) for p in args.ports.split(',')]
        nodes = [
            {'id': f'node{i+1}', 'address': f'http://{args.host}:{port}'}
            for i, port in enumerate(ports)
        ]

    manager = NetworkManager(nodes)

    print("Distributed Worker System Setup")
    print("="*40)
    print(f"Configured nodes: {len(nodes)}")
    for node in nodes:
        print(f"  - {node['id']}: {node['address']}")

    # Ping test only
    if args.ping_only:
        manager.ping_all_nodes()
        return

    # Wait for nodes to start
    if not manager.wait_for_nodes(args.wait_timeout):
        print("❌ Not all nodes are available. Please check your node processes.")
        sys.exit(1)

    # Setup peer connections
    if not manager.setup_nodes():
        print("⚠️  Some peer connections failed, but continuing...")

    # Add test tasks (unless disabled)
    if not args.no_tasks:
        manager.add_test_tasks()

    # Test collaboration
    manager.test_collaboration()

    # Initial status check
    time.sleep(2)
    manager.monitor_status(args.detailed)

    # Ping test
    manager.ping_all_nodes()

    # Start continuous monitoring
    print(
        f"\n🔄 Starting continuous monitoring (interval: {args.monitor_interval}s)")
    print("Press Ctrl+C to stop monitoring")

    monitor_thread = threading.Thread(
        target=manager.continuous_monitoring,
        args=(args.monitor_interval, args.detailed),
        daemon=True
    )
    monitor_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping monitor...")
        manager.stop_monitoring()
        monitor_thread.join(timeout=2)


if __name__ == "__main__":
    main()
