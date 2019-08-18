"""
The basic scenario
2 IoT Nodes (Inode1 and Inode2), 2 Fog Nodes (Fnode1 and Fnode2) and one Cloud Server (Cnode):
    Inode1 is connected to Fnode1 by linkIF1 (link_id = 1)
    Inode2 is connected to Fnode2 by linkIF2 (link_id = 2)
    Fnode1 is connected to Fnode2 by linkFF  (link_id = 3)
    Fnode1 and Fnode2 are connected to Cnode by linkFC1 and linkFC2 respectivly (link_id = 4 and 5)
"""

import random
import matplotlib
import simpy
import numpy

class Packet(object): # why we should use 'object' as input for this class? what happens if we don't use it?
    pkt_id = 0
    def __init__(self, generation_time=0, size=None, type=None, src_id="a", flow_id=0, nfw=0, fstc = False, task_or_resp="task"):
        self.generation_time = generation_time
        self.size = size
        self.type = type
        self.task_or_resp = task_or_resp
        self.pkt_id += 1
        self.src_id = src_id
        self.nfw = nfw
        self.flow_id = flow_id
        self.visited_fogs = []
        self.fstc = fstc # force_send_to_cloud
        self.next_hop = []
    def __repr__(self):
        return "id: {}, src: {}, time: {}, size: {}, type: {}, fstc: {}". \
            format(self.pkt_id, self.src_id, self.generation_time, self.size, self.type, self.fstc)


class PacketGenerator(object): # why we should use 'object' as input for this class? what happens if we don't use it?
    """ Generates packets with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the packet.
    """
    def __init__(self, env, pkt_type, pkt_gen_rate, finish=float("inf"), flow_id=0, src_id="a", fstc = False, nfw=0, light_pkt_size=100*8,
                 heavy_pkt_size=80*8*1024, pi=0.1, pf=0.75, pc=0.15): # finish=float("inf")???
        self.src_id = src_id
        self.env = env
        self.pkt_type = pkt_type
        self.pkt_gen_rate = pkt_gen_rate # rate of a poisson process for producing light or heavy tasks
        self.light_pkt_size = light_pkt_size
        self.heavy_pkt_size = heavy_pkt_size
        self.finish = finish
        self.out = None
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

    def run(self):
        """The generator function used in simulations.
        """
        while self.env.now < self.finish:
            # Packet size
            if self.pkt_type=="light": # review needed
                self.packets_size = random.expovariate(1/self.light_pkt_size) # average length of 100 bytes as default (should verify)
            elif self.pkt_type=="heavy": # review needed
                self.packets_size = random.expovariate(1/self.heavy_pkt_size) # average length of 80 kbytes as default (should verify)
            self.time_interval = random.expovariate(1 / self.pkt_gen_rate)  # according to a poisson process with rate Yi (for light) or Y'i (for heavy)
            p = Packet(generation_time=self.env.now, size=self.packets_size, src_id=self.src_id, flow_id=self.flow_id, fstc=self.fstc, nfw=self.nfw)
            # wait for next transmission
            yield self.env.timeout(self.time_interval)
            r = random.random()
            if p.fstc == False:
                if r <= self.pf:
                    if self.src_id == "Inode1":
                        Inode1.send_queue.put(p)
                    elif self.src_id == "Inode2":
                        Inode2.send_queue.put(p)
                elif (r > self.pf and r <= (self.pc + self.pf)):
                    p.fstc = True
                    if self.src_id == "Inode1":
                        Inode1.send_queue.put(p)
                    elif self.src_id == "Inode2":
                        Inode2.send_queue.put(p)
                else:
                    if self.src_id == "Inode1":
                        Inode1.process_queue.put(p)
                    elif self.src_id == "Inode2":
                        Inode2.process_queue.put(p)
            else:
                if self.src_id == "Inode1":
                    Inode1.send_queue.put(p)
                elif self.src_id == "Inode2":
                    Inode2.send_queue.put(p)


class Link():
    def __init__(self, env, src, dest, propagation_delay, id=0):
        self.env = env
        self.id = id
        self.src = src
        self.dest = dest
        self.propagation_delay = propagation_delay
        self.uplink_queue = simpy.Store(env)
        self.downlink_queue = simpy.Store(env)
        self.action1 = env.process(self.uplink())
        self.action2 = env.process(self.downlink())

    def uplink(self):
        while True:
            msg = (yield self.uplink_queue.get())
            yield self.env.timeout(self.propagation_delay)
            if self.id == 1:
                Fnode1.dispatcher_queue.put(msg)
            elif self.id == 2:
                Fnode2.dispatcher_queue.put(msg)
            elif self.id == 3:
                Fnode2.dispatcher_queue.put(msg)
            elif self.id == 4 or self.id == 5:
                Cnode.process_queue.put(msg)

    def downlink(self):
        while True:
            msg = (yield self.downlink_queue.get())
            yield self.env.timeout(self.propagation_delay)
            if self.id == 1:
                Inode1.response_queue.put(msg)
            elif self.id == 2:
                Inode2.response_queue.put(msg)
            elif self.id == 3:
                Fnode1.dispatcher_queue.put(msg)
            elif self.id == 4:
                Fnode1.dispatcher_queue.put(msg)
            elif self.id == 5:
                Fnode2.dispatcher_queue.put(msg)


class IoT_Node():
    def __init__(self, env, id, connected_fog, pi=0.1, pf=0.75, pc=0.15, bi=0.5, I2F_propagation_delay = random.uniform(1,2)):
        self.env = env
        self.process_queue = simpy.Store(env)
        self.response_queue = simpy.Store(env)
        self.send_queue = simpy.Store(env)
        self.packets_sent = 0
        self.total_handled_packets = 0
        self.total_handling_time = 0
        self.mean_resp_time = 0
        self.out = None
        self.action1 = env.process(self.packet_processor())
        self.action2 = env.process(self.response_handler())
        self.action3 = env.process(self.send())
        self.id = id
        self.connected_fog = connected_fog
        self.bi = bi # probability of node's type be light
        if random.random() <= self.bi:
            self.type = "light"
            self.process_time = 0.03 # 30 ms
            self.I2F_link_rate = 250*1024 # 250 kbps
        else:
            self.type = "heavy"
            self.process_time = 0.4  # 400 ms
            self.I2F_link_rate = 54 * 1024 * 1024  # 50 Mbps
        self.I2F_propagation_delay = I2F_propagation_delay
        self.pi = pi # probability of processing itself
        self.pf = pf # probability of sending to fog layer
        self.pc = pc # probability of sending to cloud layer
        self.links = []
        self.packet_generator = PacketGenerator(env=env, pkt_type=self.type, pkt_gen_rate=0.1, finish=float("inf"), src_id=self.id,
                                                light_pkt_size=100*8, heavy_pkt_size=80*8*1024, pi=self.pi, pf=self.pf, pc=self.pc)

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            yield self.env.timeout(self.process_time)
            msg.task_or_resp = "response"
            self.response_queue.put(msg)

    def response_handler(self):
        while True:
            self.total_handled_packets += 1
            msg = (yield self.response_queue.get())
            resp_time = msg.generation_time - self.env.now # is it true?
            self.total_handling_time += resp_time
            self.mean_resp_time = self.total_handling_time / self.total_handled_packets

    def send(self):
        while True:
            msg = (yield self.send_queue.get())
            yield self.env.timeout(((msg.size / self.I2F_link_rate)) * 0.001) # ms
            if self.id == "Inode1":
                linkIF1.uplink_queue.put(msg)
            elif self.id == "Inode2":
                linkIF2.uplink_queue.put(msg)


class Fog_Node():
    def __init__(self, env, id, F2F_link_rate = 100*1024*1024, F2C_link_rate = 10*1024*1024*1024, F2I_link_rate = 250*1024,
                 n_forwarding_threshold=1, wating_threshold_time = 0.0002):
        self.env = env
        self.process_queue = simpy.Store(env)
        self.dispatcher_queue = simpy.Store(env)
        self.send2fog_queue = simpy.Store(env)
        self.send2cloud_queue = simpy.Store(env)
        self.send2iot_queue = simpy.Store(env)
        self.action1 = env.process(self.dispatcher())
        self.action2 = env.process(self.packet_processor())
        self.action4 = env.process(self.send2fog())
        self.action5 = env.process(self.send2cloud())
        self.action6 = env.process(self.send2iot())
        self.id = id
        self.reachability_table = []
        self.connected_fogs = []
        self.connected_iots= []
        self.F2F_link_rate = F2F_link_rate  # 100 Mbps
        self.F2C_link_rate = F2C_link_rate # 10 Gbps
        self.F2I_link_rate = F2I_link_rate # 250 kbps or 54 Mbps (default is 250 kbps but we change it to 54 Mbps if type of the packet be heavy)
        # F2I_link_rate and I2F_link_rate are related to type of IoT_Node (250 kbps for light and 54 Mbps for heavy)
        self.F2I_propagation_delay = random.uniform(1, 2) # ms
        self.F2F_propagation_delay = random.uniform(0.5, 1.2) # ms
        self.F2C_propagation_delay = random.uniform(15, 35) # ms
        self.F2I_light_ratio = random.uniform(500, 4000) # computational power in compare with light IoT node
        self.F2I_heavy_ratio = random.uniform(100, 400) # computational power in compare with heavy IoT node
        self.light_processing_time = 0.03 / self.F2I_light_ratio
        self.heavy_processing_time = 0.4 / self.F2I_heavy_ratio
        self.estimated_wating_time = 0
        self.forwarding_threshold = n_forwarding_threshold
        self.wating_threshold_time = wating_threshold_time
        self.links = []

    def rout(self):
        pass

    def dispatcher(self):
        while True:
            msg = (yield self.dispatcher_queue.get())
            if msg.task_or_resp == "task":
                if msg.fstc==True: # if force send to cloud flag is True
                    self.send2cloud_queue.put(msg)
                else: # if force send to cloud flag is False
                    # look at process queue: process itself, send to neighbor fog or send to cloud
                    if msg.type=="light":
                        p_time = self.light_processing_time
                    else:
                        p_time = self.heavy_processing_time

                    if self.estimated_wating_time+p_time<self.wating_threshold_time: # process itself
                        self.process_queue.put(msg)
                        self.estimated_wating_time += p_time
                    else:
                        if msg.nfw >= 1:
                            msg.fstc = True
                            self.send2cloud_queue.put(msg)
                        else:
                            self.send2fog_queue.put(msg)
            else: # send response to destination iot node or route it to preferred fog node
                if (self.id=="Fnode1" and msg.src_id=="Inode1") or (self.id=="Fnode2" and msg.src_id=="Inode2"): # doesn't work for general problem
                    self.send2iot_queue.put(msg)
                else:
                    self.send2fog_queue.put(msg)
                    

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            if msg.type=='light':
                p_time = self.light_processing_time
            else:
                p_time = self.heavy_processing_time
            yield self.env.timeout(p_time)
            self.estimated_wating_time -= p_time
            msg.task_or_resp = "response"
            self.dispatcher_queue.put(msg)

    def send2fog(self):
        while True:
            msg = (yield self.send2fog_queue.get())
            yield self.env.timeout((msg.size / self.F2F_link_rate) * 0.001) # ms
            if msg.task_or_resp == "task":
                msg.nfw += 1
                msg.visited_fogs.append(self.id)
            if self.id == "Fnode1":
                linkFF.uplink_queue.put(msg)
            else:
                linkFF.downlink_queue.put(msg)

    def send2cloud(self):
        while True:
            msg = (yield self.send2cloud_queue.get())
            yield self.env.timeout((msg.size / self.F2C_link_rate) * 0.001) # ms
            if msg.task_or_resp == "task":
                msg.nfw += 1
                msg.visited_fogs.append(self.id)
            if self.id == "Fnode1":
                linkFC1.uplink_queue.put(msg)
            else:
                linkFC2.uplink_queue.put(msg)

    def send2iot(self):
        while True:
            msg = (yield self.send2iot_queue.get())
            if msg.type == "heavy": # by default it's value  is 250 kbps
                self.F2I_link_rate = 54*1024*1024 # 54 Mbps
            yield self.env.timeout((msg.size / self.F2I_link_rate) * 0.001) # ms
            if msg.task_or_resp == "task":
                msg.nfw += 1
                msg.visited_fogs.append(self.id)
            if self.id == "Fnode1":
                linkIF1.downlink_queue.put(msg)
            else:
                linkIF2.downlink_queue.put(msg)


class Cloud_Server():
    def __init__(self, env, id, F2C_link_rate = 10*1024*1024*1024):
        self.env = env
        self.id = id
        self.process_queue = simpy.Store(env)
        self.send_queue = simpy.Store(env)
        self.action1 = env.process(self.packet_processor())
        self.action2 = env.process(self.send())
        self.connected_fog = []
        self.F2C_link_rate = F2C_link_rate # 10 Gbps
        self.links = []
        self.C2F_ratio = random.uniform(50, 200)  # computational power in compare with Fog node
        self.light_processing_time = 0.03 / (random.uniform(500, 4000) * self.C2F_ratio)
        self.heavy_processing_time = 0.4 / (random.uniform(100, 400) * self.C2F_ratio)

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            if msg.type == "light":
                process_time = self.light_processing_time
            else:
                process_time = self.heavy_processing_time
            yield self.env.timeout(process_time)
            msg.task_or_resp = "response"
            self.send_queue.put(msg)

    def send(self):
        while True:
            msg = (yield self.send_queue.get())
            yield self.env.timeout(((msg.size / self.F2C_link_rate)) * 0.001) # ms
            if msg.src_id == "Inode1":
                linkFC1.downlink_queue.put(msg)
            elif msg.src_id == "Inode2":
                linkFC2.downlink_queue.put(msg)


def Fog_Peocess_Decision(): # Decide who should process the received task
    pass

def Fog_Topology(Fnode): # Define fog nodes topology in each domain by a random graph generator with mean degree of 3
    pass

def Domain_Def(Inode, Fnode, Cnode):
    pass


if __name__ == '__main__':
    env = simpy.Environment()
    Inode1 = IoT_Node(env, id="Inode1", connected_fog=["Fnode1"], pi=0.1, pf=0.75, pc=0.15, bi=0.5)
    Inode2 = IoT_Node(env, id="Inode2", connected_fog=["Fnode2"], pi=0.1, pf=0.75, pc=0.15, bi=0.5)
    Fnode1 = Fog_Node(env, id="Fnode1", F2F_link_rate = 100*1024*1024, F2C_link_rate = 10*1024*1024*1024, F2I_link_rate = 250*1024,
                 n_forwarding_threshold=1, wating_threshold_time = 0.0002)
    Fnode2 = Fog_Node(env, id="Fnode2", F2F_link_rate = 100*1024*1024, F2C_link_rate = 10*1024*1024*1024, F2I_link_rate = 250*1024,
                 n_forwarding_threshold=1, wating_threshold_time = 0.0002)
    Cnode = Cloud_Server(env, id="Cnode", F2C_link_rate = 10*1024*1024*1024)

    linkIF1 = Link(env, propagation_delay=random.uniform(1, 2), id=1, src="Inode1", dest="Fnode1")
    linkIF2 = Link(env, propagation_delay=random.uniform(1, 2), id=2, src="Inode2", dest="Fnode2")
    linkFF = Link(env, propagation_delay=random.uniform(0.5, 1.2), id=3, src="Fnode1", dest="Fnode2")
    linkFC1 = Link(env, propagation_delay=random.uniform(15, 35), id=4, src="Fnode1", dest="Cnode")
    linkFC2 = Link(env, propagation_delay=random.uniform(15, 35), id=5, src="Fnode2", dest="Cnode")
    env.run(until=1000)
