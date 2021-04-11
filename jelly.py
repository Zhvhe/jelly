"""
Here we construct the Jellyfish topology.
"""

import random, pdb
from collections import defaultdict

class JellyfishNet:

    """
    We use node/switch interchangeably.

    Switch IDs are counted from 0, 1, ..., num_switches-1

    We represent the network graph using a dictionary containing
    adjacency sets for each node.

    TODO:
      servers are not nodes on the graph at this time, only switches
      We should consider making servers nodes on the graph
    """
    def __init__(self, num_switches, num_switch_ports, num_servers, num_server_ports):
        self.num_switches = num_switches
        self.num_switch_ports = num_switch_ports
        self.switch_graph = defaultdict(set)
        self.all_switches = range(self.num_switches)
        self.open_switches = range(self.num_switches)
        self.construct_random_network()

        self.num_servers = num_servers
        self.all_servers = range(self.num_servers)
        self.num_server_ports = num_server_ports

    def get_graph(self):
        return self.switch_graph

    def num_open_ports(self, node):
        """
        Return number of open ports for a specified node
        """
        closed_ports = len(self.switch_graph[node]) # equivalent to number of neighbors of the node
        return self.num_switch_ports - closed_ports

    def update_open_switches(self, nodes):
        """
        Update self.open_switches to contain open switches
        """
        for switch in nodes:
            open_ports = self.num_open_ports(switch)
            if open_ports == 0 and switch in self.open_switches:
                self.open_switches.remove(switch)
            elif open_ports > 0 and switch not in self.open_switches:
                self.open_switches.append(switch)

    def connect(self, node1, node2):
        self.switch_graph[node1].add(node2)
        self.switch_graph[node2].add(node1)
        self.update_open_switches([node1, node2])

    def disconnect(self, node1, node2):
        self.switch_graph[node1].remove(node2)
        self.switch_graph[node2].remove(node1)
        self.update_open_switches([node1, node2])

    def connected(self, switch1, switch2):
        if switch1 in self.switch_graph[switch2]:
            assert switch2 in self.switch_graph[switch1],\
                "ERROR: Inconsistency between connected switches"
            return True
        return False

    def no_more_connections_possible(self):
        """
        Out of the open switches, no more connections can be made between them.
        """
        for i in xrange(len(self.open_switches)):
            for j in xrange(i+1, len(self.open_switches)):
                switch1, switch2 = self.open_switches[i], self.open_switches[j]
                if not self.connected(switch1, switch2):
                    return False
        return True

    def satisfied_connections(self):
        """
        End condition: No switches with open ports left OR
                       1 switch with a single open port (odd number of total ports)
        """
        if len(self.open_switches) == 0:
            return True

        if len(self.open_switches) == 1 and \
            self.num_open_ports(self.open_switches[0]) == 1:
            return True

        return False

    def rand_node(self, blacklist_nodes, candidates=None):
        """
        Returns a random node that doesn't belong to the blacklist.
        """
        if not candidates:
            candidates = [node for node in self.all_switches if node not in blacklist_nodes]
        else:
            candidates = list(set(candidates) - set(blacklist_nodes))
        return random.choice(candidates)

    def connect_two_nodes(self):
        """
        Randomly connect two nodes. Returns True if it's potentially possible to find
        two open nodes to connect, and False if not.
        """
        if len(self.open_switches) < 2:
            # There are less than 2 open switches
            return False

        switch1, switch2 = random.sample(self.open_switches, 2)
        if not self.connected(switch1, switch2):
            self.connect(switch1, switch2)
            self.update_open_switches([switch1, switch2])
        elif self.no_more_connections_possible():
            # The remaining open switches are already all connected
            # to each other, so we can't add any further links. This
            # means we need to break some existing links and reconfigure.
            return False

        return True

    def construct_random_network(self):
        """
        Randomly connect nodes while maximizing number of edges.
        """
        while True:
            while self.connect_two_nodes():
                # Keep connecting two nodes as long as there are two open nodes
                # available to be connected
                pass

            if self.satisfied_connections():
                # Done
                return

            """
            At this point we can't find two open switches to connect
            that aren't already connected. We need to reconfigure
            some links.

            1) Find an open switch (os)
            2) Find two closed switches that neighbor each other (s1 and s2)
               but don't neighbor os
            3) If os has only 1 free port, then we need to break one of its
               existing edges because we want to connect s1 - os - s2, so
               os needs at least 2 open ports.
            """
            node = self.open_switches[0]
            open_ports = self.num_open_ports(node)
            if (open_ports < 2):
                rand_neighbor = random.choice(list(self.switch_graph[node]))
                self.disconnect(node, rand_neighbor)

            other1 = self.rand_node(self.open_switches + [node])
            other2 = self.rand_node([node], candidates=self.switch_graph[other1])
            self.disconnect(other1, other2)
            self.connect(node, other1)
            self.connect(node, other2)

    def attach_servers(self):
        """
        Randomly assign each server to a switch. Single switch can be connected
        to at most num_server_ports.

        This should spread out roughly uniformly, but switches don't have
        unlimited capacity so we cap how many servers can be assigned
        to a single switch.
        """
        assert self.num_servers <= self.num_switches * self.num_server_ports,\
            "ERROR: Not enough switches to support server capacity"

        switches = self.all_switches * self.num_server_ports
        random.shuffle(switches)

        self.servers = {}
        self.switch_capacity = defaultdict(int)

        for server in self.all_servers:
            switch = random.choice(switches)
            switches.remove(switch)
            self.servers[server] = switch
            self.switch_capacity[switch] += 1

        assert max(self.switch_capacity.values()) <= self.num_server_ports,\
            "ERROR: switch is oversubscribed with servers"

        return self.servers, self.switch_capacity

    def generate_server_traffic(self):
        """
        We follow the paper:
          Each server randomly selects another server to send traffic to.
          Each server will receiver traffic from another server.
        """
        self.server_sender_traffic = defaultdict(list)
        candidates = self.all_servers[:]

        for sender in self.all_servers:
            receiver = self.rand_node([sender], candidates=candidates)
            self.server_sender_traffic[sender].append(receiver)
            candidates.remove(receiver)

        return self.server_sender_traffic

    def generate_nbyn_traffic(self):
        """
        We follow the paper:
          Each server selects every other server to send traffic to.
          Each server will receiver traffic from every other server.
        """
        self.server_sender_traffic = defaultdict(list)
        candidates = self.all_servers[:]

        for sender in self.all_servers:
            for receiver in self.all_servers:
                if sender == receiver:
                    continue
                self.server_sender_traffic[sender].append(receiver)

        return self.server_sender_traffic

def test():
    for i in xrange(20):
        jf = JellyfishNet(10, 4, 20, 5)
        jf.attach_servers()
        jf.generate_server_traffic()
        print jf.get_graph()

def main():
    jf = JellyfishNet(20, 4, 636, 5)
    servers, switch_cap = jf.attach_servers()
    print servers
    print switch_cap
    print jf.generate_server_traffic()
    print 'done'

if __name__ == "__main__":
    pass
