import random
import matplotlib
import simpy
import numpy

class Packet(object):
    pkt_id = 0
    def __init__(self, generation_time=0, size=None, type=None, src="a", flow_id=0, nfw=0, fstc = False):
        self.generation_time = generation_time
        self.size = size
        self.type = type
        self.pkt_id += 1
        self.srcNode = src
        self.nfw = nfw
        self.flow_id = flow_id
        self.visitedfogs = []
        self.fstc = fstc # force_send_to_cloud
    def __repr__(self):
        return "id: {}, src: {}, time: {}, size: {}". \
            format(self.id, self.srcNode, self.generation_time, self.size)


class PacketGenerator(object):
    """ Generates packets with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the packet.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        adist : function
            a no parameter function that returns the successive inter-arrival times of the packets
        sdist : function
            a no parameter function that returns the successive sizes of the packets
        initial_delay : number
            Starts generation after an initial delay. Default = 0
        finish : number
            Stops generation at the finish time. Default is infinite


    """
    def __init__(self, env, pkt_type, pkt_gen_rate, adist='poiss', sdist='expo', finish=float("inf"), flow_id=0, src_id="a", fstc = False, nfw=0):
        self.src_id = src_id
        self.env = env
        self.pkt_type = pkt_type
        self.adist = adist
        self.pkt_gen_rate = pkt_gen_rate
        self.sdist = sdist
        self.finish = finish
        self.out = None
        self.packets_sent = 0
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.flow_id = flow_id
        self.generation_time = env.now
        self.src = src_id
        self.fstc = fstc
        self.nfw = nfw

    def run(self):
        """The generator function used in simulations.
        """
        while self.env.now < self.finish:
            # Packet size !!!
            if self.pkt_type=="light": # review needed
                self.packets_size = random.expovariate(0.01) # average lenght of 100 bytes
            elif self.pkt_type=="heavy": # review needed
                self.packets_size = random.expovariate(0.000012207) # average lenght of 80 kbytes
            p = Packet(generation_time=self.env.now, size=self.packets_size, src=self.src_id, flow_id=self.flow_id, fstc=self.fstc, nfw=self.nfw)
            # wait for next transmission
            yield self.env.timeout(self.adist())
            #p = Packet(self.env.now, self.sdist(), self.packets_sent, src=self.id, flow_id=self.flow_id)
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


class Link(): # transfer this class as a function in nodes
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
            if self.src == "Inode1":
                Fnode1.dispatcher_queue.put(msg)
            elif self.src == "Inode2":
                Fnode2.dispatcher_queue.put(msg)

    def downlink(self):
        while True:
            msg = (yield self.downlink_queue.get())
            yield self.env.timeout(self.propagation_delay)
            if self.src == "Inode1":
                Inode1.response_queue.put(msg)
            elif self.src == "Inode2":
                Inode2.response_queue.put(msg)


class IoT_Node():
    def __init__(self, env, id, connected_fog, pi=0.1, pf=0.75, pc=0.15, bi=0.5):
        self.env = env
        self.process_queue = simpy.Store(env)
        self.response_queue = simpy.Store(env)
        self.send_queue = simpy.Store(env)
        self.packets_sent = 0
        self.total_handled_packets = 0
        self.total_handling_time = 0
        self.mean_resp_time = 0
        self.out = None
        #self.action1 = env.process(self.run())
        self.action2 = env.process(self.packet_processor())
        self.action3 = env.process(self.response_handler())
        self.action4 = env.process(self.send())
        self.id = id
        self.connected_fog = connected_fog
        self.bi = bi # probability of node's type be light
        if random.random() <= self.bi:
            self.type = "light"
            self.process_time = 0.3 # 30 ms
            self.I2F_link_rate = 250*1024 # 250 kbps
        else:
            self.type = "heavy"
            self.process_time = 0.4  # 400 ms
            self.I2F_link_rate = 54 * 1024 * 1024  # 50 Mbps
        self.I2F_propagation_delay = random.uniform(1,2)
        self.pi = pi # probability of processing itself
        self.pf = pf # probability of sending to fog layer
        self.pc = pc # probability of sending to cloud layer
        PacketGenerator(env=env, pkt_type=self.type, pkt_gen_rate=0.1, finish=float("inf"), nfw=1)
        self.link = Link(env=env, src=self.id, dest=self.connected_fog, propagation_delay=self.I2F_propagation_delay)

    def packet_processor(self):
        while True:
            msg = (yield self.process_queue.get())
            yield self.env.timeout(self.process_time)
            self.response_queue.put(msg)

    def response_handler(self):
        while True:
            self.total_handled_packets += 1
            msg = (yield self.response_queue.get())
            resp_time = msg.generation_time - env.now
            self.total_handling_time += resp_time
            self.mean_resp_time = self.total_handling_time / self.total_handled_packets

    def send(self):
        while True:
            msg = (yield self.send_queue.get())
            yield self.env.timeout(msg.size / self.I2F_link_rate)
            self.link.uplink_queue.put(msg)

    """    def rout(self, r):
            if r <= self.pf:
                return "fog"
            elif (r > self.pf and r <= (self.pc + self.pf)):
                return "cloud"
            else:
                return "myself"
    """

    """    def run(self):
            # The generator function used in simulations.

            while True:
                print('running Main Process')
                #p = PacketGenerator(env=self.env, pkt_type=self.type, pkt_gen_rate=0.1, finish=float("inf"), nfw=3) # Use Packet Generator
                r = random.random()
                print('generated random number is {}'.format(r))
                dest = self.rout(r)
                if dest=="fog":
                    p.generation_time += 1
                    print('sending msg to fog node')
                    #self.fog.store1.put(p)
                    f.requests.put(p)
                elif dest=="cloud":
                    print('sending msg to server node')
                    p.generation_time += 2
                    s.store.put(p)
                else:
                    print('I will Process msg')
                    p.generation_time +=1
                    continue

                self.packets_sent += 1
                msg = (yield f.response_queue.get()) #
                print("Received message in Node class: {}".format(msg))
    """


class Fog_Node():
    def __init__(self, env, id, connected_fogs, F2F_link_rate = 100*1024*1024, F2C_link_rate = 10*1024*1024*1024, F2I_link_rate = 250*1024,
                 n_forwarding_threshold=1, wating_threshold_time = 0.0002):
        self.env = env
        self.process_queue = simpy.Store(env)
        self.dispatcher_queue = simpy.Store(env)
        self.send_queue = simpy.Store(env)
        self.action1 = env.process(self.run())
        self.action2 = env.process(self.packet_processor())
        self.action3 = env.process(self.response_handler())
        self.action4 = env.process(self.send())
        self.id = id
        self.reachability_table = []
        self.connected_fogs = connected_fogs
        self.F2F_link_rate = F2F_link_rate  # 100 Mbps
        self.F2C_link_rate = F2C_link_rate # 10 Gbps
        self.F2I_link_rate = F2I_link_rate # 250 kbps
        # F2I_link_rate and I2F_link_rate are related to type of IoT_Node (250 kbps for light and 54 Mbps for heavy)
        self.F2I_propagation_delay = random.uniform(1, 2) # ms
        self.F2F_propagation_delay = random.uniform(0.5, 1.2) # ms
        self.F2C_propagation_delay = random.uniform(15, 35) # ms
        # self.link_F2F = Link(env=env, src=self.id, dest=self.connected_fog, propagation_delay=self.I2F_propagation_delay)
        self.F2I_light_ratio = random.uniform(500, 4000) # computational power in compare with light IoT node
        self.F2I_heavy_ratio = random.uniform(100, 400) # computational power in compare with heavy IoT node
        self.light_processing_time = 0.03 / self.F2I_light_ratio
        self.heavy_processing_time = 0.4 / self.F2I_heavy_ratio
        self.estimated_wating_time = 0
        self.forwarding_threshold = n_forwarding_threshold
        self.wating_threshold_time = wating_threshold_time
        """
        self.links = []
        if :    
            p_delay = self.F2I_propagation_delay
        elif :
            p_delay = self.F2F_propagation_delay
        else:
            p_delay = self.F2C_propagation_delay 
        for l in connected_fog:
            self.links.append(Link(self.env=self.env, src=self.id, dest=l, propagation_delay=p_delay, id=0))
        """

    def rout(self):
        pass

    def run(self):
        while True:
            print('running Dispatcher')

    def packet_processor(self): # is for IoT Node. Should be modified
        while True:
            msg = (yield self.process_queue.get())
            yield self.env.timeout(self.process_time)
            self.response_queue.put(msg)

    def response_handler(self): # is for IoT Node. Should be modified
        while True:
            self.total_handled_packets += 1
            msg = (yield self.response_queue.get())
            resp_time = msg.generation_time - env.now
            self.total_handling_time += resp_time
            self.mean_resp_time = self.total_handling_time / self.total_handled_packets

    def send(self): # is for IoT Node. Should be modified
        while True:
            msg = (yield self.send_queue.get())
            yield self.env.timeout(msg.size / self.I2F_link_rate)
            self.link.uplink_queue.put(msg)



if __name__ == '__main__':
    env = simpy.Environment()
    Inode1 = IoT_Node(env)
    Inode2 = IoT_Node(env)
    Fnode1 = Fog_Node(env)
    Fnode2 = Fog_Node(env)
    Cnode = Cloud_Server(env)
    connectivity_matrix = [['Inode1','Fnode1',1], ['Inode2','Fnode2',2], ['Fnode2','Fnode2',3], ['Fnode1','Snode',4], ['Fnode2','Snode',5]]
    links = []
    i = 0
    for x in connectivity_matrix:
        i += 1
        link_id = i
        if x[0][0]==x[1][0]:
            if x[0][0]=="F":
                propagation_delay = random.uniform(0.5, 1.2)  # ms
            elif x[0][0]=="C": print("src and dest are cloud nodes")
            else: print("src and dest are fog nodes")
        else:
            if x[0][0]=="F" and x[1][0]=="C":
                propagation_delay = random.uniform(15, 35)  # ms
            elif x[0][0]=="I" and x[0][0]=="F":
                propagation_delay = random.uniform(1, 2)  # ms
            else: print("Bad links are exist")
        links.append(Link(env, propagation_delay, id=link_id, src=x[0], dest=x[1]))




