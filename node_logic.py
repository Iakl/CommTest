import sys
import argparse
from comm_node import CommNode

NNODES = 3


def compute_average(self_consumption, average_estimations):

    self_weight = (NNODES - len(average_estimations)) / NNODES
    other_weight = 1 / NNODES

    average = self_weight*self_consumption + \
        other_weight * sum(average_estimations)

    return average


#!/usr/bin/env python3
"""
Run a communication node with specified configuration
Usage: 
  python run_node.py node1 --port 5004 --peers node2,node3
"""


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run a communication node')
    parser.add_argument(
        'node_id', help='Node ID (node1, node2, node3, or custom)')
    parser.add_argument('--port', type=int,
                        help='Port number (required for custom nodes)')
    parser.add_argument(
        '--peers', help='Comma-separated list of peer addresses (required for custom nodes)')
    parser.add_argument('--sc', type=float, default=0.0,
                        help='Self consumption value (default: 0.0)')

    return parser.parse_args()


def main():
    args = parse_arguments()
    node_id = args.node_id

    if not args.port or not args.peers or not args.sc:
        print("❌ Custom nodes require --port, --peers and --sc arguments")
        print("Example: python run_node.py custom --port 5004 --peers node2,node3 --sc 2.0")
        sys.exit(1)

    port = args.port
    peers = [peer.strip() for peer in args.peers.split(',')]
    self_consumption = args.sc

    # Create and start the node
    try:
        node = CommNode(node_id, port, peers, self_consumption)
        node.set_callback(compute_average)
        node.start()
    except KeyboardInterrupt:
        if 'node' in locals():
            node.shutdown()
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting node: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
