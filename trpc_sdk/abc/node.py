# -*- coding: utf-8 -*-


class Node(object):

    def __init__(self,
                 service_name='',
                 container_name='',
                 address='',
                 ip='',
                 port=0,
                 network='',
                 protocol='',
                 weight=0,
                 cost_time=0,
                 env_key='',
                 metadata=None):
        self.service_name = service_name
        self.container_name = container_name
        self.ip = ip
        port = port if port else 0
        self.port = port
        self.address = str(ip) + ":" + str(port)
        if len(address) != 0:
            self.address = address
        self.network = network
        self.protocol = protocol
        self.weight = weight
        self.cost_time = cost_time
        self.env_key = env_key
        self.metadata = metadata

    def __str__(self):
        return "service:%s,addr:%s,cost:%d" % (self.service_name, self.address, self.cost_time)
