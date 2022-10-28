from concurrent.futures import thread
import enum
import logging

from numpy import greater
import llp
import queue
import struct
import threading

class SWPType(enum.IntEnum):
    DATA = ord('D')
    ACK = ord('A')

class SWPPacket:
    _PACK_FORMAT = '!BI'
    _HEADER_SIZE = struct.calcsize(_PACK_FORMAT)
    MAX_DATA_SIZE = 1400 # Leaves plenty of space for IP + UDP + SWP header 

    def __init__(self, type, seq_num, data=b''):
        self._type = type
        self._seq_num = seq_num
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def seq_num(self):
        return self._seq_num
    
    @property
    def data(self):
        return self._data

    def to_bytes(self):
        header = struct.pack(SWPPacket._PACK_FORMAT, self._type.value, 
                self._seq_num)
        return header + self._data
       
    @classmethod
    def from_bytes(cls, raw):
        header = struct.unpack(SWPPacket._PACK_FORMAT,
                raw[:SWPPacket._HEADER_SIZE])
        type = SWPType(header[0])
        seq_num = header[1]
        data = raw[SWPPacket._HEADER_SIZE:]
        return SWPPacket(type, seq_num, data)

    def __str__(self):
        return "%s %d %s" % (self._type.name, self._seq_num, repr(self._data))

class SWPSender:
    _SEND_WINDOW_SIZE = 5
    _TIMEOUT = 1
    send_semaphore = None
    last_ack_recv = -1
    last_frame_sent = -1
    send_buffer = []

    def __init__(self, remote_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(remote_address=remote_address,
                loss_probability=loss_probability)

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()

        # TODO: Add additional state variables
        self.send_semaphore = threading.Semaphore(self._SEND_WINDOW_SIZE)

    def send(self, data):
        for i in range(0, len(data), SWPPacket.MAX_DATA_SIZE):
            self._send(data[i:i+SWPPacket.MAX_DATA_SIZE])

    def _send(self, data):
        # TODO
        self.send_semaphore.acquire()
        packet = SWPPacket(SWPType.DATA, data)
        packet.seq_num = self.last_frame_sent + 1 
        buffer_idx = packet.seq_num % self._SEND_WINDOW_SIZE  
        # self.send_buffer.append(packet)
        if len(self.send_buffer) >= 5:
            logging.error("Sending a packet when the send window is full!")
        packet_timer = threading.Timer(self._TIMEOUT, self._retransmit, [packet.seq_num])
        self.send_buffer[buffer_idx] = (packet, packet_timer)
        # self.send_buffer.insert(buffer_idx, (packet, packet_timer))

        self._llp_endpoint.send(packet.to_bytes())
        packet_timer.start()
        self.last_frame_sent = packet.seq_num
        return
        
    def _retransmit(self, seq_num):
        # TODO
        buffer_idx = seq_num % self._SEND_WINDOW_SIZE
        retransmit_pkt_info = self.send_buffer[buffer_idx]
        retransmit_pkt = retransmit_pkt_info[0]

        new_pkt_timer = threading.Timer(self._TIMEOUT, self._retransmit, [retransmit_pkt.seq_num])
        self.send_buffer[buffer_idx] = (retransmit_pkt, new_pkt_timer)
        self._llp_endpoint.send(retransmit_pkt.to_bytes())

        new_pkt_timer.start()
        return 

    def _recv(self):
        while True:
            # Receive SWP packet
            raw = self._llp_endpoint.recv()
            if raw is None:
                continue
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)

            # TODO
            if packet.type is SWPType.ACK:
                ack_seq_num = packet.seq_num
                
                if self.last_ack_recv < ack_seq_num:
                    for seq_num in range(self.last_ack_recv + 1, ack_seq_num + 1):
                        buffer_idx = seq_num % self._SEND_WINDOW_SIZE
                        sent_pkt_info = self.send_buffer.pop(buffer_idx)
                        sent_pkt_timer = sent_pkt_info[1]
                        sent_pkt_timer.cancel()

                        sent_pkt = sent_pkt_info[0]
                        self.send_semaphore.release()
                else:
                    logging.error("acknowledging a packet seq num that's already been ack")
                
                self.last_ack_recv = ack_seq_num
                
        return

class SWPReceiver:
    _RECV_WINDOW_SIZE = 5
    
    


    def __init__(self, local_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(local_address=local_address, 
                loss_probability=loss_probability)

        # Received data waiting for application to consume
        self._ready_data = queue.Queue()

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()
        
        # TODO: Add additional state variables
        self.highest_ack_seq_num = -1
        self.recv_buffer = [None for _ in range(self._RECV_WINDOW_SIZE)]


    def recv(self):
        return self._ready_data.get()

    def _recv(self):
        while True:
            # Receive data packet
            raw = self._llp_endpoint.recv()
            if raw is None:
                continue    
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            
            # TODO
            curSeqNum = packet.seq_num()
            if curSeqNum <= self.last_frame_recv or curSeqNum > self.last_frame_recv + self._RECV_WINDOW_SIZE:
                #The Packet is outside of our window range -> drop packet
                continue
            else:
                #The Packet is in range of our window -> process (ONLY DATA)
                assert packet.type() == SWP.DATA

                if packet.seq_num <= self.highest_ack_seq_num:
                    #acknowledged already and ACK can be sent back
                    ack_packet = SWPPacket(SWP.ACK, packet.data())
                    self._llp_endpoint.send(packet.to_bytes())
                    continue
                else:
                    #Received out of order -> add to buffer
                    buffer[packet.seq_num % self._RECV_WINDOW_SIZE] = packet
                    buffer_idx = 0
                    #Traverse 
                    while buffer_idx < self._RECV_WINDOW_SIZE and buffer[buffer_idx] is not None:
                        self._ready_data.put(buffer[buffer_idx])
                        buffer[buffer_idx] = None
                        self.highest_ack_seq_num = buffer_idx if self.highest_ack_seq_num == -1 else (self.highest_ack_seq_num // self._RECV_WINDOW_SIZE) * self._RECV_WINDOW_SIZE + buffer_idx
                        buffer_idx += 1
                        


                #Send an acknowledgement for the highest sequence number for which all data chunks up to and including that sequence number have been received.
                ack_packet = SWPPacket(SWP.ACK, seq_num=packet.seq_num())
                self._llp_endpoint.send(packet.to_bytes())
        return
