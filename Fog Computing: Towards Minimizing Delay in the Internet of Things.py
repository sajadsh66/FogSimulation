import random
import simpy
import numpy
import networkx
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict
from tqdm import tqdm

class Results():
    def __init__(self):
        self.NFP = [[], [], [], [], [], [], [], [], [], []]
        self.NFP_L = [[], [], [], [], [], [], [], [], [], []] # no Fog processing mode for light tasks
        self.NFP_H = [[], [], [], [], [], [], [], [], [], []] # no Fog processing mode for heavy tasks
        self.AFP = [[], [], [], [], [], [], [], [], [], []]
        self.AFP_L = [[], [], [], [], [], [], [], [], [], []] # all Fog processing mode for light tasks
        self.AFP_H = [[], [], [], [], [], [], [], [], [], []] # all Fog processing mode for heavy tasks
        self.LFP = [[], [], [], [], [], [], [], [], [], []]
        self.LFP_L = [[], [], [], [], [], [], [], [], [], []] # light Fog processing mode for light tasks
        self.LFP_H = [[], [], [], [], [], [], [], [], [], []] # light Fog processing mode for heavy tasks

class Packet(object):
    def __init__(self, pkt_id, generation_time=0, size=None, type=None, src_id=None, flow_id=0, nfw=0, fstc = False, task_or_resp="task",
                 AFP=True, NFP=False, LFP=False):
        self.generation_time = generation_time
        self.size = size
        self.type = type
        self.task_or_resp = task_or_resp
        self.pkt_id = pkt_id
        self.src_id = src_id
        self.nfw = nfw
        self.flow_id = flow_id
        self.visited_fogs = []
        self.fstc = fstc # force_send_to_cloud
        self.next_hop = []
        self.handling_time = 0
        self.NFP = NFP
        self.LFP = LFP
        self.AFP = AFP
    def __repr__(self):
        return "id: {}, src: {}, time: {}, size: {}, type: {}, fstc: {}". \
            format(self.pkt_id, self.src_id, self.generation_time, self.size, self.type, self.fstc)


class PacketGenerator(object):
    """ Generates packets with given inter-arrival time distribution.
    """
    def __init__(self, env, pkt_type, pkt_gen_rate, finish=float("inf"), flow_id=0, src_id=-1, fstc = False, nfw=0, light_pkt_size=100*8,
                 heavy_pkt_size=80*8*1024, pi=0.1, pf=0.75, pc=0.15, AFP=True, NFP=False, LFP=False):
        self.src_id = src_id
        self.env = env
        self.pkt_type = pkt_type
        self.pkt_gen_rate = pkt_gen_rate # rate of a poisson process for producing light or heavy tasks
        self.light_pkt_size = light_pkt_size # bit
        self.heavy_pkt_size = heavy_pkt_size # bit
        self.finish = finish
        self.packets_sent = 0
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.flow_id = flow_id
        self.generation_time = env.now
        self.src = src_id
        self.fstc = fstc
        self.nfw = nfw
        self.pi = pi  # probability of processing itself
        self.pf = pf  # probability of sending to fog layer
        self.pc = pc  # probability of sending to cloud layer
        self.NFP = NFP
        self.LFP = LFP
        self.AFP = AFP

    def run(self):
        """The generator function used in simulations.
        """
        while self.env.now < self.finish:
            self.packets_sent += 1

            # Packet size
            if self.pkt_type=="light":
                self.packets_size = random.expovariate(1/self.light_pkt_size) # exponentially distributed with average 800 bit (100 B)
            elif self.pkt_type=="heavy":
                self.packets_size = random.expovariate(1/self.heavy_pkt_size) # exponentially distributed with average 655360 bit (80 KB)

            p = Packet(generation_time=self.env.now, size=self.packets_size, src_id=self.src_id, flow_id=self.flow_id, fstc=self.fstc,
                       nfw=self.nfw, pkt_id=self.packets_sent, type=self.pkt_type, NFP=self.NFP, AFP=self.AFP, LFP=self.LFP)
            r = random.random()

            if p.fstc == False:
                if self.LFP and p.type == 'heavy':
                    if r < (self.pc + self.pf): # send to cloud
                        p.fstc = True
                        for x in linkIF:
                            if x.src == self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    else:
                        Inode[self.src_id].process_queue.put(p) # process itself
                        if len(Inode[self.src_id].process_queue_delay) == 0:
                            Inode[self.src_id].process_queue_delay.append(0)
                        else:
                            Inode[self.src_id].process_queue_delay.append(sum(Inode[self.src_id].process_queue_pd))
                        Inode[self.src_id].process_queue_pd.append(Inode[self.src_id].process_time)
                elif self.LFP and p.type == 'light':
                    if (r <= self.pf  and self.pf != 0): # send to fog
                        for x in linkIF:
                            if x.src==self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    elif (r > self.pf and r <= (self.pc + self.pf)): # send to cloud
                        p.fstc = True
                        for x in linkIF:
                            if x.src == self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    else:
                        Inode[self.src_id].process_queue.put(p) # process itself
                        if len(Inode[self.src_id].process_queue_delay) == 0:
                            Inode[self.src_id].process_queue_delay.append(0)
                        else:
                            Inode[self.src_id].process_queue_delay.append(sum(Inode[self.src_id].process_queue_pd))
                        Inode[self.src_id].process_queue_pd.append(Inode[self.src_id].process_time)
                elif self.AFP:
                    if r <= self.pf:  # send to fog
                        for x in linkIF:
                            if x.src == self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    elif (r > self.pf and r <= (self.pc + self.pf) and self.pf != 0):  # send to cloud
                        p.fstc = True
                        for x in linkIF:
                            if x.src == self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    else:
                        Inode[self.src_id].process_queue.put(p) # process itself
                        if len(Inode[self.src_id].process_queue_delay) == 0:
                            Inode[self.src_id].process_queue_delay.append(0)
                        else:
                            Inode[self.src_id].process_queue_delay.append(sum(Inode[self.src_id].process_queue_pd))
                        Inode[self.src_id].process_queue_pd.append(Inode[self.src_id].process_time)
                elif self.NFP:
                    if r < (self.pc + self.pf): # send to cloud
                        p.fstc = True
                        for x in linkIF:
                            if x.src == self.src:
                                x.uplink_queue.put(p)
                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(p.size / x.link_rate)
                    else:
                        Inode[self.src_id].process_queue.put(p) # process itself
                        if len(Inode[self.src_id].process_queue_delay) == 0:
                            Inode[self.src_id].process_queue_delay.append(0)
                        else:
                            Inode[self.src_id].process_queue_delay.append(sum(Inode[self.src_id].process_queue_pd))
                        Inode[self.src_id].process_queue_pd.append(Inode[self.src_id].process_time)
            else: # send to cloud
                for x in linkIF:
                    if x.src == self.src:
                        x.uplink_queue.put(p)
                        if len(x.uplink_queue_trans) == 0:
                            x.uplink_queue_delay.append(0)
                        else:
                            x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                        x.uplink_queue_trans.append(p.size / x.link_rate)

            # wait for next transmission
            self.time_interval = random.expovariate(1 / self.pkt_gen_rate)  # according to a poisson process with rate Yi (for light) or Y'i (for heavy)
            yield self.env.timeout(self.time_interval)


class Link():
    def __init__(self, env, src, dest, type, propagation_delay, link_rate, id=0):
        self.env = env
        self.id = id
        self.src = src
        self.dest = dest
        self.type = type
        self.propagation_delay = propagation_delay
        self.link_rate = link_rate
        self.uplink_queue = simpy.Store(env)
        self.uplink_queue_delay = []
        self.uplink_queue_trans = []
        self.downlink_queue = simpy.Store(env)
        self.downlink_queue_delay = []
        self.downlink_queue_trans = []
        self.action1 = env.process(self.uplink())
        self.action2 = env.process(self.downlink())

    def uplink(self):
        while True:
            msg = (yield self.uplink_queue.get())
            delay = self.propagation_delay + (msg.size / self.link_rate)
            yield self.env.timeout(delay)
            msg.handling_time += (delay + self.uplink_queue_delay[0])
            del self.uplink_queue_trans[0]
            del self.uplink_queue_delay[0]
            if self.type=="IF" or self.type=="FF":
                Fnode[self.dest].dispatcher_queue.put(msg)
            elif self.type=="FC":
                Cnode[self.dest].process_queue.put(msg)
                msg.handling_time += Cnode[self.dest].estimated_waiting_time
                if msg.type=='light':
                    Cnode[self.dest].estimated_waiting_time += Cnode[self.dest].light_processing_time
                else:
                    Cnode[self.dest].estimated_waiting_time += Cnode[self.dest].heavy_processing_time

    def downlink(self):
        while True:
            msg = (yield self.downlink_queue.get())
            delay = self.propagation_delay + (msg.size / self.link_rate)
            yield self.env.timeout(delay)
            msg.handling_time += (delay + self.downlink_queue_delay[0])
            del self.downlink_queue_trans[0]
            del self.downlink_queue_delay[0]
            if self.type == "IF":
                Inode[self.src].response_queue.put(msg)
            elif self.type == "FF" or self.type == "FC":
                Fnode[self.src].dispatcher_queue.put(msg)


class Reachability_table():
    def __init__(self, n_Fog=5):
        self.propagation_delay = numpy.zeros((n_Fog, n_Fog))
        self.estimated_waiting_time = numpy.zeros((n_Fog, 1))

class IoT_Node():
    def __init__(self, env, id, connected_fog=None, pi=0.1, pf=0.75, pc=0.15, bi=0.5, light_packet_generation_rate=0.1, heavy_packet_generation_rate=0.25,
                 AFP=True, NFP=False, LFP=False, round=0):
        self.round = round
        self.env = env
        self.process_queue = simpy.Store(env)
        self.response_queue = simpy.Store(env)
        # self.response_queue = simpy.PriorityStore(env)
        self.process_queue_delay = []
        self.process_queue_pd = []
        self.packets_sent = 0
        self.total_handled_packets = 0
        self.total_handling_time = 0
        self.mean_resp_time = 0
        self.NFP = NFP
        self.LFP = LFP
        self.AFP = AFP
        self.action1 = env.process(self.packet_processor())
        self.action2 = env.process(self.response_handler())
        self.id = id
        self.connected_fog = connected_fog
        self.bi = bi # probability of node's type be light
        if random.random() <= self.bi:
            self.type = "light"
            self.process_time = 0.03 # 30 ms
            self.packet_generation_rate = light_packet_generation_rate
        else:
            self.type = "heavy"
            self.process_time = 0.4  # 400 ms
            self.packet_generation_rate = heavy_packet_generation_rate
        self.pi = pi # probability of processing itself
        self.pf = pf # probability of sending to fog layer
        self.pc = pc # probability of sending to cloud layer
        self.links = []
        self.packet_generator = PacketGenerator(env=env, pkt_type=self.type, pkt_gen_rate=self.packet_generation_rate, finish=float("inf"),
                                                src_id=self.id,light_pkt_size=100*8, heavy_pkt_size=80*8*1024, pi=self.pi, pf=self.pf, pc=self.pc,
                                                AFP=self.AFP, NFP=self.NFP, LFP=self.LFP)

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            yield self.env.timeout(self.process_time)
            msg.handling_time += (self.process_time + self.process_queue_delay[0])
            del self.process_queue_delay[0]
            del self.process_queue_pd[0]
            msg.task_or_resp = "response"
            self.response_queue.put(msg)

    def response_handler(self):
        while True:
            msg = (yield self.response_queue.get())
            self.total_handled_packets += 1
            self.total_handling_time += msg.handling_time
            self.mean_resp_time = self.total_handling_time / self.total_handled_packets
            if msg.AFP and msg.type=='light':
                results.AFP_L[self.round].append(msg.handling_time)
                results.AFP[self.round].append(msg.handling_time)
            if msg.AFP and msg.type=='heavy':
                results.AFP_H[self.round].append(msg.handling_time)
                results.AFP[self.round].append(msg.handling_time)
            elif msg.NFP and msg.type=='light':
                results.NFP_L[self.round].append(msg.handling_time)
                results.NFP[self.round].append(msg.handling_time)
            elif msg.NFP and msg.type=='heavy':
                results.NFP_H[self.round].append(msg.handling_time)
                results.NFP[self.round].append(msg.handling_time)
            elif msg.LFP and msg.type=='light':
                results.LFP_L[self.round].append(msg.handling_time)
                results.LFP[self.round].append(msg.handling_time)
            elif msg.LFP and msg.type=='heavy':
                results.LFP_H[self.round].append(msg.handling_time)
                results.LFP[self.round].append(msg.handling_time)


class Fog_Node():
    def __init__(self, env, id, n_forwarding_threshold=1, waiting_threshold_time = 0.0002):
        self.env = env
        self.process_queue = simpy.Store(env)
        self.dispatcher_queue = simpy.Store(env)
        self.action1 = env.process(self.dispatcher())
        self.action2 = env.process(self.packet_processor())
        self.id = id
        self.connected_fogs = []
        self.connected_iots= []
        self.F2I_light_ratio = random.uniform(500, 4000) # computational power in compare with light IoT node
        self.F2I_heavy_ratio = random.uniform(100, 400) # computational power in compare with heavy IoT node
        self.light_processing_time = 0.03 / self.F2I_light_ratio
        self.heavy_processing_time = 0.4 / self.F2I_heavy_ratio
        self.estimated_waiting_time = 0
        self.forwarding_threshold = n_forwarding_threshold
        self.waiting_threshold_time = waiting_threshold_time

    def dispatcher(self):
        while True:
            msg = (yield self.dispatcher_queue.get())
            if msg.task_or_resp == "task":
                if msg.fstc==True: # if force send to cloud flag is True, send it to cloud directly
                    # msg.nfw += 1
                    msg.visited_fogs.append(self.id)
                    for x in linkFC:
                        if x.src==self.id:
                            x.uplink_queue.put(msg)

                        if len(x.uplink_queue_trans) == 0:
                            x.uplink_queue_delay.append(0)
                        else:
                            x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                        x.uplink_queue_trans.append(msg.size / x.link_rate)

                else: # if force send to cloud flag is False
                    # look at it's process queue: process itself, send to neighbor fog or send to cloud
                    if msg.type=="light":
                        p_time = self.light_processing_time
                    else:
                        p_time = self.heavy_processing_time
                    if (float(self.estimated_waiting_time) + p_time) < self.waiting_threshold_time: # process itself
                        self.process_queue.put(msg)
                        msg.handling_time += self.estimated_waiting_time
                        self.estimated_waiting_time += p_time
                        reachability_table.estimated_waiting_time[self.id] = self.estimated_waiting_time
                    else:
                        if msg.nfw >= self.forwarding_threshold: # send to cloud
                            msg.fstc = True
                            msg.visited_fogs.append(self.id)
                            for x in linkFC:
                                if x.src == self.id:
                                    x.uplink_queue.put(msg)

                                if len(x.uplink_queue_trans)==0:
                                    x.uplink_queue_delay.append(0)
                                else:
                                    x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                x.uplink_queue_trans.append(msg.size / x.link_rate)
                        else: # send to a neighbor fog
                            neighbors = []
                            best_neighbor = -1
                            temp = float("inf")
                            for i in self.connected_fogs:
                                neighbors.append([i, reachability_table.estimated_waiting_time[i][0] + reachability_table.propagation_delay[self.id][i]])
                            # print("{}".format(neighbors))
                            for x in neighbors:
                                # print("{}{}".format(type(x[1]), type(temp)))
                                if x[1] < temp:
                                    best_neighbor = x[0]
                                    temp = x[1]
                            for x in linkFF:
                                if (x.src==self.id and x.dest==best_neighbor):
                                    x.uplink_queue.put(msg)
                                    if len(x.uplink_queue_trans) == 0:
                                        x.uplink_queue_delay.append(0)
                                    else:
                                        x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                    x.uplink_queue_trans.append(msg.size / x.link_rate)
                                elif (x.src==best_neighbor and x.dest==self.id):
                                    x.downlink_queue.put(msg)
                                    if len(x.downlink_queue_trans) == 0:
                                        x.downlink_queue_delay.append(0)
                                    else:
                                        x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                                    x.downlink_queue_trans.append(msg.size / x.link_rate)
            else: # send response to destination iot node or route it to preferred fog node
                if msg.src_id in self.connected_iots: # there is a direct link to the destination IoT Node
                    for x in linkIF:
                        if x.src==msg.src_id:
                            x.downlink_queue.put(msg)
                            if len(x.downlink_queue_trans) == 0:
                                x.downlink_queue_delay.append(0)
                            else:
                                x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                            x.downlink_queue_trans.append(msg.size / x.link_rate)
                else: # there is not any direct link to the destination IoT Node
                    temp = "No"
                    for i in self.connected_fogs:
                        if msg.src_id in Fnode[i].connected_iots: # the IoT Node is one hop neighbor
                            temp = "Yes"
                            for x in linkFF:
                                if (x.src==self.id and x.dest==i):
                                    x.uplink_queue.put(msg)
                                    if len(x.uplink_queue_trans) == 0:
                                        x.uplink_queue_delay.append(0)
                                    else:
                                        x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                    x.uplink_queue_trans.append(msg.size / x.link_rate)
                                elif (x.src==i and x.dest==self.id):
                                    x.downlink_queue.put(msg)
                                    if len(x.downlink_queue_trans) == 0:
                                        x.downlink_queue_delay.append(0)
                                    else:
                                        x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                                    x.downlink_queue_trans.append(msg.size / x.link_rate)

                    if temp=="No":  # the IoT Node is two hop neighbor
                        for i in self.connected_fogs:
                            for j in Fnode[i].connected_fogs:
                                if msg.src_id in Fnode[j].connected_iots:
                                    for x in linkFF:
                                        if (x.src == self.id and x.dest == i):
                                            x.uplink_queue.put(msg)
                                            if len(x.uplink_queue_trans) == 0:
                                                x.uplink_queue_delay.append(0)
                                            else:
                                                x.uplink_queue_delay.append(sum(x.uplink_queue_trans))
                                            x.uplink_queue_trans.append(msg.size / x.link_rate)
                                        elif (x.src == i and x.dest == self.id):
                                            x.downlink_queue.put(msg)
                                            if len(x.downlink_queue_trans) == 0:
                                                x.downlink_queue_delay.append(0)
                                            else:
                                                x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                                            x.downlink_queue_trans.append(msg.size / x.link_rate)

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            if msg.type=='light':
                p_time = self.light_processing_time
            else:
                p_time = self.heavy_processing_time
            msg.handling_time += p_time
            yield self.env.timeout(p_time)
            self.estimated_waiting_time -= p_time
            reachability_table.estimated_waiting_time[self.id] = self.estimated_waiting_time
            msg.task_or_resp = "response"
            self.dispatcher_queue.put(msg)


class Cloud_Server():
    def __init__(self, env, id):
        self.env = env
        self.id = id
        self.process_queue = simpy.Store(env)
        self.send_queue = simpy.Store(env)
        self.action1 = env.process(self.packet_processor())
        self.action2 = env.process(self.send())
        self.connected_fog = []
        self.links = []
        self.C2F_ratio = random.uniform(50, 200)  # computational power in compare with Fog node
        self.light_processing_time = 0.03 / (random.uniform(500, 4000) * self.C2F_ratio)
        self.heavy_processing_time = 0.4 / (random.uniform(100, 400) * self.C2F_ratio)
        self.estimated_waiting_time = 0

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            if msg.type == "light":
                process_time = self.light_processing_time
            else:
                process_time = self.heavy_processing_time
            msg.handling_time += process_time
            yield self.env.timeout(process_time)
            self.estimated_waiting_time -= process_time
            msg.task_or_resp = "response"
            self.send_queue.put(msg)

    def send(self):
        while True:
            msg = (yield self.send_queue.get())
            temp = "No"
            for i in self.connected_fog: # check connected Fog Nodes to find a link to destination IoT Node
                if msg.src_id in Fnode[i].connected_iots:  # there is a direct link to the destination IoT Node from a connected Fog Node
                    temp = "Yes"
                    for x in linkFC:
                        if (x.src == i) and (x.dest == self.id):
                            x.downlink_queue.put(msg)
                            if len(x.downlink_queue_trans) == 0:
                                x.downlink_queue_delay.append(0)
                            else:
                                x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                            x.downlink_queue_trans.append(msg.size / x.link_rate)

            if temp == "No":
                for i in self.connected_fog: # check two hop connected Fog Nodes to find a link to destination IoT Node
                    for j in Fnode[i].connected_fogs:
                        if msg.src_id in Fnode[j].connected_iots: # there is a direct link to the destination IoT Node from a two hop connected Fog Node
                            temp = "Yes"
                            for x in linkFC:
                                if (x.src == i) and (x.dest == self.id):
                                    x.downlink_queue.put(msg)
                                    if len(x.downlink_queue_trans) == 0:
                                        x.downlink_queue_delay.append(0)
                                    else:
                                        x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                                    x.downlink_queue_trans.append(msg.size / x.link_rate)

            if temp == "No":
                for i in self.connected_fog:  # check three hop connected Fog Nodes to find a link to destination IoT Node
                    for j in Fnode[i].connected_fogs:
                        for k in Fnode[j].connected_fogs:
                            if msg.src_id in Fnode[k].connected_iots:  # there is a direct link to the destination IoT Node from a three hop connected Fog Node
                                temp = "Yes"
                                for x in linkFC:
                                    if (x.src == i) and (x.dest == self.id):
                                        x.downlink_queue.put(msg)
                                        if len(x.downlink_queue_trans) == 0:
                                            x.downlink_queue_delay.append(0)
                                        else:
                                            x.downlink_queue_delay.append(sum(x.downlink_queue_trans))
                                        x.downlink_queue_trans.append(msg.size / x.link_rate)


if __name__ == '__main__':
    n_domain = 5
    n_IoT = 100 # number of IoT nodes in each domain
    n_Fog = 5 # number of fog nodes in each domain
    n_Cloud = random.randint(1, 2)  # number of cloud nodes in each domain
    pi = 0 # probability that an IoT node process the Tasks by itself
    pf = 0.85 # Probability that an IoT node send the tasks to fog layer
    pc = 0.15 # Probability that an IoT node send the tasks to cloud layer directly
    bi = 0.5  # probability of node's type be light
    waiting_threshold_time1 = [0.0002 for i in range(n_Fog)] # offloading threshold of Fog nodes (second)
    n_forwarding_threshold = 10 # maximum offload limit at the fog layer
    yi1 = 0.5 # rate of generating Light tasks by Poisson Process
    yi2 = 0.6 # rate of generating heavy tasks by Poisson Process
    F2F_link_rate = 100 * 1024 * 1024 # 100 Mbps
    F2C_link_rate = 10 * 1024 * 1024 * 1024 # 10 Gbps
    # creating connectivity tables
    I2F_connectivity_table = numpy.zeros((n_IoT, n_Fog))
    for i in range(n_IoT):
        a = random.randint(0, n_Fog - 1)
        I2F_connectivity_table[i][a] = 1
    I2F_propagation_delay = [(random.uniform(1, 2) / 1000) for i in range(n_IoT)]

    avg_expected_degree = [4 for i in range(5)]
    G = nx.expected_degree_graph(avg_expected_degree)
    F2F_connectivity_table = numpy.zeros((n_Fog, n_Fog))
    for x in G.edges:
        if x[0] != x[1]:
            F2F_connectivity_table[x[0]][x[1]] = 1
            F2F_connectivity_table[x[1]][x[0]] = 1
    F2F_propagation_delay = []
    for i in range(n_Fog):
        F2F_propagation_delay.append([(random.uniform(0.5, 1.2) / 1000) for i in range(n_Fog)])

    F2C_connectivity_table = numpy.zeros((n_Fog, n_Cloud))
    for i in range(n_Fog):
        if n_Cloud == 1:
            a = 0
        else:
            a = random.randint(0, n_Cloud - 1)
        F2C_connectivity_table[i][a] = 1
    F2C_propagation_delay = [(random.uniform(15, 35) / 1000) for i in range(n_Fog)]

    results = Results()
    ro = 10
    for z in tqdm(range(ro)):
        for r in range(3):
            env = simpy.Environment()
            q = 0.5  # fog fairness factor: it determines how the fog node selects jobs from its processing queue
            AFP = [True, False, False]  # All Fog Processing mode: IoT Nodes process their own tasks, send them to Fog or Cloud
            LFP = [False, True, False]  # Light Fog Processing mode: IoT Nodes process their own tasks, send them to Fog or Cloud (Fag Nodes just process light tasks)
            NFP = [False, False, True]  # No Fog Processing mode: IoT Nodes process their own tasks or send them to Cloud (Fag Nodes don't process tasks)
            Inode = []
            Fnode = []
            Cnode = []
            linkIF = []
            linkFF = []
            linkFC = []
            reachability_table = Reachability_table(n_Fog=n_Fog)

            # creating IoT nodes
            for i in range(n_IoT):
                Inode.append(IoT_Node(env, id=i, pi=pi, pf=pf, pc=pc, bi=bi, light_packet_generation_rate=yi1, heavy_packet_generation_rate=yi2,
                                      AFP=AFP[r], NFP=NFP[r], LFP=LFP[r], round=z))

            # creating fog nodes
            for i in range(n_Fog):
                Fnode.append(Fog_Node(env, id=i, n_forwarding_threshold=z+1, waiting_threshold_time=waiting_threshold_time1[i]))

            # creating cloud server(s)
            for i in range(n_Cloud):
                Cnode.append(Cloud_Server(env, id=i))

            # creating connectivity tables
            for i in range(n_IoT):
                for a in range(n_Fog):
                    if I2F_connectivity_table[i][a]==1:
                        if Inode[i].type=="light":
                            link_rate = 250*1024 # 250 kbps
                        else:
                            link_rate = 54*1024*1024 # 54 Mbps
                        linkIF.append(Link(env, propagation_delay=I2F_propagation_delay[i], id="{}{}".format(i,a), src=i, dest=a, type="IF",
                                           link_rate=link_rate))
                        Inode[i].connected_fog = a
                        Fnode[a].connected_iots.append(i)

            for x in G.edges:
                if x[0] != x[1]:
                    linkFF.append(Link(env, propagation_delay=F2F_propagation_delay[x[0]][x[1]], id="{}{}".format(x[0], x[1]), src=x[0], dest=x[1], type="FF", link_rate=F2F_link_rate))
                    Fnode[x[0]].connected_fogs.append(x[1])
                    Fnode[x[1]].connected_fogs.append(x[0])
                    reachability_table.propagation_delay[x[0]][x[1]] = F2F_propagation_delay[x[0]][x[1]]
                    reachability_table.propagation_delay[x[1]][x[0]] = F2F_propagation_delay[x[1]][x[0]]

            for i in range(n_Fog):
                for a in range(n_Cloud):
                    if F2C_connectivity_table[i][a]:
                        linkFC.append(Link(env, propagation_delay=F2C_propagation_delay[i], id="{}{}".format(i, a), src=i, dest=a,
                                           type="FC", link_rate=F2C_link_rate))
                        Cnode[a].connected_fog.append(i)

            env.run(until=300)

    res = [[], [], [], [], [], []]
    for z in range(ro):
        if len(results.AFP[z])==0:
            res[0].append(0)
        else:
            res[0].append(sum(results.AFP[z])/len(results.AFP[z]))

        if len(results.LFP)==0:
            res[1].append(0)
        else:
            res[1].append(sum(results.LFP[z]) / len(results.LFP[z]))

        if len(results.NFP) == 0:
            res[2].append(0)
        else:
            res[2].append(sum(results.NFP[z]) / len(results.NFP[z]))

    number_of_packets = 0
    for i in range(ro):
        number_of_packets += len(results.AFP_L[i])+len(results.AFP_H[i])+len(results.LFP_L[i])+len(results.LFP_H[i])+len(results.NFP_L[i])+len(results.NFP_H[i])
    print(number_of_packets)
    number_of_packets = 0
    for i in range(ro):
        number_of_packets += len(results.AFP[i])+len(results.LFP[i])+len(results.NFP[i])
    print(number_of_packets)

    x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    plt.plot(x, res[0], 'Dr:', x, res[1], 'ob--', x, res[2], '^k-')
    plt.xlabel('Fog Layer Offload Limit')
    plt.ylabel('Average Delay (s)')
    plt.show()
