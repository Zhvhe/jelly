from jelly import JellyfishNet
from collections import defaultdict
import pdb

"""
Return a list of the shortest paths. Limit is the maximum
number of paths we want to collect.
"""
def shortest_paths(graph, src, dst, limit):
    paths = []
    paths_queue = [[src]]

    while len(paths_queue) > 0:
        path = paths_queue.pop(0)
        node = path[-1]
        if node == dst:
            paths.append(path)
            if len(paths) >= limit:
                break
        else:
            for neighbor in [n_ for n_ in graph[node] if n_ not in path]:
                paths_queue.append(path + [neighbor])

    return paths

def link_name(switch1, switch2):
    assert switch1 != switch2, "Link cannot be between the same switch"
    """
    # Uncomment below if we want to not count s1->s2 as different from s2->s1
    if switch1 > switch2:
        switch1, switch2 = switch2, switch1
    """
    return "%s-%s" % (switch1, switch2)

"""
The count is the number of distinct paths crossing over the link
in our randomly generated traffic.
"""
ecmp_8_link_count = defaultdict(int)
ecmp_64_link_count = defaultdict(int)
ksp_8_link_count = defaultdict(int)

def add_link_counts(path, counter):
    for i in xrange(1, len(path)):
        switch1, switch2 = path[i-1], path[i]
        counter[link_name(switch1, switch2)] += 1

def update_link_counts(shortest_paths):
    shortest = len(shortest_paths[0])
    processed = 0
    for path in shortest_paths:
        if len(path) == shortest:
            if processed < 8:
                add_link_counts(path, ecmp_8_link_count)
            if processed < 64:
                add_link_counts(path, ecmp_64_link_count)
        if processed < 8:
            add_link_counts(path, ksp_8_link_count)
        processed += 1

def enumerate_links(graph):
    all_links = set()
    for node, neighbors in graph.iteritems():
        for neighbor in neighbors:
            all_links.add(link_name(node, neighbor))
    return all_links

def init_counters(all_links):
    for link in all_links:
        ecmp_8_link_count[link] = 0
        ecmp_64_link_count[link] = 0
        ksp_8_link_count[link] = 0

def main():
    """
    Figure 9 shows 2750 links

    195 * 14 = 2730
    212 * 13 = 2756
    230 * 12 = 2760
    250 * 11 = 2750

    If we have 195 switches, that means 14 switch links
    we'll use 10 server ports, for a total of 24 ports which is reasonable
    :return:
    """
    switches = 212
    switch_links = 13
    servers = 686
    server_ports = 23

    print "Parameters:\n\t%d Switches\n\t%d Switch-Links\n\t%d Servers\n\t%d Server Ports" % \
          (switches, switch_links, servers, server_ports)

    jf = JellyfishNet(switches, switch_links, servers, server_ports)
    switch_graph = jf.get_graph()
    all_links = enumerate_links(switch_graph)
    init_counters(all_links)

    server_to_switch, _  = jf.attach_servers()
    sender_traffic = jf.generate_server_traffic()

    processed = 0
    for sender, receivers in sender_traffic.items():
        for receiver in receivers:
        #print "sender: %s, receiver: %s" % (sender, receiver)
            sender_switch = server_to_switch[sender]
            receiver_switch = server_to_switch[receiver]
            if sender_switch == receiver_switch:
                # Don't bother counting if they're already connected to the same switch
                continue
            ksp = shortest_paths(switch_graph, sender_switch, receiver_switch, 64)
            update_link_counts(ksp)
            processed += 1
            print "Processed send-receive pair: (%d/%d)" % (processed, len(sender_traffic))

    import matplotlib.pyplot as plt
    plt.plot(sorted(ksp_8_link_count.values()), color='blue', label="8 Shortest Paths")
    plt.plot(sorted(ecmp_64_link_count.values()), color='red', label='64-way ECMP')
    plt.plot(sorted(ecmp_8_link_count.values()), color='green', label='8-way ECMP')
    plt.title("Figure 9 Replication")
    plt.ylabel('# Distinct Paths Link is on')
    plt.xlabel('Rank of Link')
    plt.legend(loc=2)
    #plt.show()
    fig_path = 'figure9.png'
    plt.savefig(fig_path)
    print 'Saved plot to %s' % fig_path

if __name__=='__main__':
    main()
