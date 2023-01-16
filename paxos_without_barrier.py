import os
import sys
import random
import time
from multiprocessing import Process, Barrier

import zmq

barrier = None

class Node:
    def __init__(self, id, crash_probability, num_rounds, num_nodes, barrier):
        self.id = id
        self.pid = os.getpid()
        self.crash_probability = float(crash_probability)
        self.num_rounds = num_rounds
        self.num_nodes = num_nodes

        self.value = 0 if random.random() < 0.5 else 1

        self.max_voted_round = -1
        self.max_voted_value = None
        self.propose_val = None
        self.decision = None

        self.current_round = 1

        self.timeout = 500 * self.num_nodes
        self.barrier = barrier


    def run(self):
        self.__init_sockets()

        for i in range(self.num_rounds):
            if self.__is_proposer():
                self.__propose()
            else:
                self.__accept()
            
            self.current_round += 1

    def __accept(self):   
        received = False
        self.pull_sock.RCVTIMEO = self.timeout
        
        recv_timer = time.time()

        # Join phase
        while not received:
            try:
                m: dict[str, str] = self.pull_sock.recv_json()
                msg = m["msg"]
                sender = m["sender"]

                received = True
                print(f'ACCEPTOR {self.id} RECEIVED IN JOIN PHASE: {msg}')

                if self.__is_start_message(msg):
                    self.__send_msg(sender, f"JOIN {self.max_voted_round} {self.max_voted_value}", with_fail=True)

                elif self.__is_crash_message(msg):
                    self.__send_msg(sender, msg, with_fail=False)

            except Exception as e:
                if recv_timer + self.timeout <= time.time():
                    break
            
        # Vote phase
        received = False
        while not received:
            try:
                m = self.pull_sock.recv_json()
                msg = m["msg"]
                received = True
                print(f'ACCEPTOR {self.id} RECEIVED IN VOTE PHASE: {msg}')

                if self.__is_propose_message(msg):
                    self.max_voted_round = self.current_round
                    proposed_value = msg.split(" ")[1]
                    self.max_voted_value = None if proposed_value == "None" else int(proposed_value)

                    self.__send_msg(m["sender"], f"VOTE {self.max_voted_round} {self.max_voted_value}", with_fail=True)

                elif self.__is_crash_message(msg):
                    self.__send_msg(m["sender"], msg, with_fail=False)
                    
                elif self.__is_round_change_message(msg):
                    # directly move onto the next round
                    pass
                
                self.__trigger_barrier()
                

            except Exception as e:
                if recv_timer + self.timeout <= time.time():
                    break

    def __propose(self):
        received_messages = []
        join_count = 0
        self.pull_sock.RCVTIMEO = self.timeout

        recv_max_voted_round = -1
        recv_max_voted_value = None
        
        recv_timer = time.time()

        print(f'ROUND {self.current_round} STARTED WITH INITIAL VALUE {self.value}')

        # broadcast a start message to all acceptors
        self.__broadcast(f"START", with_fail=True)
        
        while len(received_messages) < self.num_nodes:
            try:
                m = self.pull_sock.recv_json()
                msg = m["msg"]
                sender = m["sender"]
                
                print(f'LEADER OF {self.current_round} RECEIVED IN JOIN: {msg}')
                
                if self.__is_join_message(msg):
                    join_count += 1

                    decoded = self.__decode_message(msg)
                    if (decoded) and (decoded['max_voted_round'] > recv_max_voted_round):
                        recv_max_voted_round = decoded['max_voted_round']
                        recv_max_voted_value = decoded['max_voted_value']
                    
                if self.__is_start_message(msg) and sender == self.id:
                    join_count += 1
                    if self.max_voted_round > recv_max_voted_round:
                        recv_max_voted_round = self.max_voted_round
                        recv_max_voted_value = self.max_voted_value

                received_messages.append((sender, msg))
            except Exception as e:
                if recv_timer + self.timeout <= time.time():
                    break
        
        
        # check if join_count is greater than half of the nodes
        if join_count <= (self.num_nodes / 2):
            print(f'LEADER OF ROUND {self.current_round} CHANGED ROUND')
            self.__broadcast(f"ROUNDCHANGE", with_fail=False, omit_self=True)
            self.__trigger_barrier()
            return
        
        # check if any node in the quorum has voted before
        if recv_max_voted_round == -1:
            self.propose_val = self.value
        else:
            self.propose_val = recv_max_voted_value
        
        # broadcast a propose message to all acceptors
        self.__broadcast(f"PROPOSE {self.propose_val}", with_fail=True)

        received_messages = []
        vote_count = 0

        while len(received_messages) < self.num_nodes:
            try:
                m = self.pull_sock.recv_json()
                msg = m["msg"]
                sender = m["sender"]

                print(f'LEADER OF {self.current_round} RECEIVED IN VOTE PHASE: {msg}')

                if (self.__is_vote_message(msg)):
                    vote_count += 1
                    
                elif (self.__is_propose_message(msg) and sender == self.id):
                    vote_count += 1
                    decoded = self.__decode_message(msg)
                    if decoded is not None:
                        self.max_voted_round = self.current_round
                        self.max_voted_value = self.propose_val


                received_messages.append((sender, msg))
            except Exception as e:
                if recv_timer + self.timeout <= time.time():
                    break
        
        # no max value has been voted on so far in this round
        if self.max_voted_value is None:
            self.max_voted_value = self.value

        # check if vote_count is greater than half of the nodes
        if vote_count > (self.num_nodes / 2):
            print(f'LEADER OF ROUND {self.current_round} DECIDED ON VALUE {self.max_voted_value}')
            self.decision = self.max_voted_value
        self.__trigger_barrier()

        


        


    # helper methods
    def __init_sockets(self):
        ctx = zmq.Context()
        self.pull_sock = ctx.socket(zmq.PULL)
        self.pull_sock.bind(f"tcp://127.0.0.1:{5550 + self.id}")
        
        self.push_socks = []
        for i in range(self.num_nodes):
            sock = ctx.socket(zmq.PUSH)
            sock.connect(f"tcp://127.0.0.1:{5550 + i}")
            self.push_socks.append(sock)

    def __trigger_barrier(self):
        # print(f'\n----\nNODE {self.id} TRIGGERED BARRIER\n----\n')
        r = self.barrier.wait()
        if r == 0:
            self.barrier.reset()
        # print(f'\n----\nNODE {self.id} RELEASED FROM BARRIER\n----\n')

    def __broadcast(self, msg: str, with_fail=False, omit_self=False) -> None:
        for sockid in range(len(self.push_socks)):
            if omit_self and sockid == self.id:
                continue
            self.__send_msg(sockid, msg, with_fail=with_fail)

    def __send_msg(self, to: int, msg: str, with_fail=False) -> None:
        failure = False
        if with_fail:
            failure = self.__should_crash()

        if failure:
            json_msg = {"sender": self.id, "receiver": to, "msg": f"CRASH {self.id}"}
        else:
            json_msg = {"sender": self.id, "receiver": to, "msg": msg}
        
        sent = False
        while not sent:
            try:
                self.push_socks[to].send_json(json_msg)
                sent = True
            except zmq.error.Again:
                pass

    
    def __decode_message(self, msg: str) -> dict[str, int]:
        msg = msg.split(" ")
        
        max_voted_round = None
        try:
            max_voted_round = int(msg[1])
        except:
            pass


        max_voted_value = None
        try:
            max_voted_value = int(msg[2])
        except:
            pass

        return {
            "max_voted_round": max_voted_round,
            "max_voted_value": max_voted_value
        }

    def __should_crash(self) -> bool:
        return random.random() < self.crash_probability

    def __is_proposer(self) -> bool:
        return self.current_round % self.num_nodes == self.id
    
    def __is_start_message(self, msg: str) -> bool:
        return 'START' in msg
    
    def __is_crash_message(self, msg: str) -> bool:
        return 'CRASH' in msg

    def __is_propose_message(self, msg: str) -> bool:
        return 'PROPOSE' in msg
    
    def __is_round_change_message(self, msg: str) -> bool:
        return 'ROUNDCHANGE' in msg
    
    def __is_join_message(self, msg: str) -> bool:
        return 'JOIN' in msg
    
    def __is_vote_message(self, msg: str) -> bool:
        return 'VOTE' in msg

if __name__ == "__main__":
    args = sys.argv[1:]

    if '--reset' in args:
        for i in range(10):
            # kill port
            os.system(f'kill $(sudo lsof -i:{5550 + i})')
        exit(0)
        
    if len(args) != 3:
        print("Usage: python main.py <numProc> <prob> <numRounds>")
        sys.exit(1)
    
    

    numProc = int(args[0])
    prob = float(args[1])
    numRounds = int(args[2])

    barrier = Barrier(numProc)

    processes = []
    for i in range(numProc):
        n = Node(i, prob, numRounds, numProc, barrier)
        processes.append(Process(target=n.run))
    
    print(f"NUM NODES: {numProc}, CRASH PROB: {prob}, NUM ROUNDS: {numRounds}")

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    print("--------------\nDone")

