import socket, random
import threading
import pickle
import sys
import time
import hashlib
import os
from collections import OrderedDict

# This is for actual application. Not for testing
# This node_ip is for actual implementation
IPADDR = socket.gethostbyname(socket.gethostname())

INIT_SERVER_IP = "127.0.0.1"
INIT_SERVER_PORT = 8000

NEXT = 0

##
# This node_ip is for testing. Using localhost
IP = "127.0.0.1"
PORT = 3000
BUFFER = 4096

M = 10  # m-bit # 10-bit
KEY_SPACE = 2 ** M


def get_hashed_int(key):
    val = hashlib.sha1(key.encode())
    return int(val.hexdigest(), 16) % KEY_SPACE


class Node:
    def __init__(self, node_ip, node_port):
        self.node_ip = node_ip
        self.node_port = node_port
        self.node_address = (node_ip, node_port)
        self.node_id = get_hashed_int(node_ip + "," + str(node_port))
        self.node_predecessor = (node_ip, node_port)
        self.node_predecessor_id = self.node_id
        self.node_successor = (node_ip, node_port)
        self.node_successor_id = self.node_id
        # key = start_id_frm_equ and value = tuple(actual_node_id, (node_address), username)
        self.node_fingertable = OrderedDict()

        # Server Conn here
        try:
            self.node_server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Here I am binding the node's own node_ip and node_port
            self.node_server_sock.bind((IP, PORT))
            # Here it can maximum listen to as many as the entire keyspace
            self.node_server_sock.listen(KEY_SPACE)
        except socket.error as E:
            print(E)

    def node_listening_thread(self):
        while True:
            try:
                inc_conn_obj, inc_address = self.node_server_sock.accept()
                inc_conn_obj.settimeout(160)
                threading.Thread(target=self.processor_thread, args=(inc_conn_obj, inc_address)).start()
            except socket.error as E:
                print(E)

    def processor_thread(self, inc_conn_obj, inc_address):
        inc_data_form = pickle.loads(inc_conn_obj.recv(BUFFER))
        request_type = inc_data_form[0]

        # Join Request
        if request_type == 0:
            self.join_setup(inc_conn_obj, inc_data_form)
            print("Joined.")
            # Displays all client options
            self.disp_clt_opts()

        # Stabilize Request
        elif request_type == 2:
            print("Stabilizing")
            inc_conn_obj.sendall(pickle.dumps(self.node_predecessor))

        # Lookup request
        elif request_type == 3:
            self.find_successor(inc_conn_obj, inc_address, inc_data_form)

        # Update New Joined Node Successor & Predecessor Request
        elif request_type == 4:
            if inc_data_form[1] == 1:
                self.fix_successor(inc_data_form)
            else:
                self.fix_predecessor(inc_data_form)

        # Fix Fingers Request
        elif request_type == 5:
            self.update_fingertable()
            inc_conn_obj.sendall(pickle.dumps(self.node_successor))

        # Print Message Request
        elif request_type == 9:
            print("You received a message: ", inc_data_form[1])

        else:
            # Nothing to write here as the requests will be fulfilled by others accurately
            pass

    def join_setup(self, inc_conn_obj, inc_data_form):
        if inc_data_form:
            peer_ip_port = inc_data_form[1]
            peer_id = get_hashed_int(peer_ip_port[0] + "," + str(peer_ip_port[1]))
            prev_predecessor = self.node_predecessor
            self.node_predecessor = peer_ip_port
            self.node_predecessor_id = peer_id
            data_form = [prev_predecessor]
            inc_conn_obj.sendall(pickle.dumps(data_form))
            time.sleep(0.3)
            self.update_fingertable()
            self.fix_fingers()

    def find_successor(self, inc_conn_obj, inc_address, inc_data_form):
        id_to_find = inc_data_form[1]
        # Here, no need to write d_to_find <= self.node_successor_id <= because = will never work as sha1 values are unique
        # and the chances of hash collision happening is very rare.
        # Here, node_successor_id is finger table's first entry
        if self.node_id == id_to_find:
            data_form = [0, self.node_address]

        elif self.node_successor_id == self.node_id:
            data_form = [0, self.node_address]

        # If the id_to_find falls in-between the current node id and its actual successor.

        elif self.node_id > id_to_find:
            if self.node_predecessor_id < id_to_find:
                data_form = [0, self.node_address]
            elif self.node_predecessor_id > self.node_id:
                data_form = [0, self.node_address]
            else:
                data_form = [1, self.node_predecessor]

        # Remember the start id of a node id is not its actual successor in the ring
        else:
            if self.node_id > self.node_successor_id:
                data_form = [0, self.node_successor]
            else:
                for k, v in self.node_fingertable.items():
                    if k >= id_to_find:
                        break
                v = self.node_successor
                data_form = [1, v]

        inc_conn_obj.sendall(pickle.dumps(data_form))

    def fix_successor(self, inc_data_form):
        successor = inc_data_form[2]
        self.node_successor = successor
        self.node_successor_id = get_hashed_int(successor[0] + "," + str(successor[1]))

    def fix_predecessor(self, inc_data_form):
        predecessor = inc_data_form[2]
        self.node_predecessor = predecessor
        self.node_predecessor_id = get_hashed_int(predecessor[0] + "," + str(predecessor[1]))

    def start_chord(self):
        threading.Thread(target=self.node_listening_thread, args=()).start()
        threading.Thread(target=self.stabilize_chord, args=()).start()
        while True:
            # This non-thread func will carry out user/client requests.
            print("Started!")
            self.user_thread()

    def stabilize_chord(self):
        while True:
            time.sleep(1.5)
            # I dont want run stabilize if only one node
            if self.node_address == self.node_successor:
                continue
            try:
                stabilize_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                stabilize_sock.connect(self.node_successor)
                stabilize_sock.sendall(pickle.dumps([2]))
                checker_1 = pickle.loads(stabilize_sock.recv(BUFFER))
                if checker_1 == self.node_id:
                    pass
                    # No need to do anything here
            except:
                # Here, it is wise to write each type of exception for better fault tolerance
                # I found a dead node. Not responding node.
                successor_bool = False
                # Reset value everytime when func run
                v = ()
                for k, v in self.node_fingertable.items():
                    if v[0] != self.node_successor_id:
                        successor_bool = True
                        break
                if successor_bool:
                    self.node_successor = v[1]
                    self.node_successor_id = get_hashed_int(self.node_successor[0] + "," + str(self.node_successor[1]))
                    # I can also write is None but I first used same vals for successor and predecessor
                    # during init so None use-case will never occur
                    stabilize_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    stabilize_sock.connect(self.node_successor)
                    stabilize_sock.sendall(pickle.dumps([4, 0, self.node_address]))
                    stabilize_sock.close()
                else:
                    self.node_predecessor = self.node_address
                    # print("Made it here")
                    self.node_predecessor_id = self.node_id
                    self.node_successor = self.node_address
                    self.node_successor_id = self.node_id
                    print("test = ", self.node_successor, self.node_successor_id)

                self.update_fingertable()
                self.fix_fingers()
                # self.update_fing
                self.disp_clt_opts()

    def user_thread(self):
        ############### numbers
        self.disp_clt_opts()
        user_input = input("Enter corresponding integer value: \n")
        if user_input == "1":
            ip_to_connect = input("Enter node address to connect: ")
            port_to_connect = input("Enter node port to connect: ")
            self.join_node_req(ip_to_connect, int(port_to_connect))
        elif user_input == "2":
            self.node_leave()
        elif user_input == "3":
            self.message_send()
        else:
            # Everything has been handled till here
            # print("Something went wrong! Re-run the script")
            pass

    def message_send(self):
        print("Pick an node address to send message....\n")
        temp_dict = OrderedDict()
        if len(self.node_fingertable.items()) == 0:
            print("There is no successor to send message to.\n You should join the chord ring first.")
            self.user_thread()
            return

        for key, value in self.node_fingertable.items():
            if value not in temp_dict.values():
                temp_dict[key] = value
        for key, value in temp_dict.items():
            print(value[1])

        # print("Enter the node_ip and node_port of the node you want to send message to.\n")
        msg_ip = input("Enter the node receiver IP address: ")
        msg_port = input("Enter the node port: ")
        # message = input("Enter the node_ip and node_port of the node you want to send message to.\n")
        msg_address = (msg_ip, int(msg_port))
        message = input("Type your message: ")
        data_form = [9, message]
        msg_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        msg_sock.connect(msg_address)
        msg_sock.sendall(pickle.dumps(data_form))
        msg_sock.close()

    def join_node_req(self, ip_to_connect, port_to_connect):
        try:
            get_address = self.get_successor((ip_to_connect, port_to_connect), self.node_id)
            connector_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connector_sock.connect(get_address)
            data_form = [0, self.node_address]
            connector_sock.sendall(pickle.dumps(data_form))
            inc_data_form = pickle.loads(connector_sock.recv(BUFFER))
            self.node_predecessor = inc_data_form[0]
            self.node_predecessor_id = get_hashed_int(self.node_predecessor[0] + "," + str(self.node_predecessor[1]))
            self.node_successor = get_address
            self.node_successor_id = get_hashed_int(get_address[0] + "," + str(get_address[1]))
            data_form = [4, 1, self.node_address]
            sender_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sender_sock.connect(self.node_predecessor)
            sender_sock.sendall(pickle.dumps(data_form))
            sender_sock.close()
            connector_sock.close()
        except socket.error as E:
            print(E)

    def node_leave(self):
        leave_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leave_sock.connect(self.node_successor)
        leave_sock.sendall(pickle.dumps([4, 0, self.node_predecessor]))
        leave_sock.close()
        leave_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leave_sock.connect(self.node_predecessor)
        leave_sock.sendall(pickle.dumps([4, 1, self.node_successor]))
        leave_sock.close()
        print("Leaving chord ring...")

        self.fix_fingers()
        print(self.node_address, "has disconnected")
        self.node_predecessor = (self.node_ip, self.node_port)
        self.node_predecessor_id = self.node_id
        self.node_successor = (self.node_ip, self.node_port)
        self.node_successor_id = self.node_id
        self.node_fingertable.clear()

    def get_successor(self, inc_address, id_to_find):
        inc_data_form = [1, inc_address]
        get_address = inc_data_form[1]
        while inc_data_form[0] == 1:
            connector_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                connector_sock.connect(get_address)
                data_form = [3, id_to_find]
                connector_sock.sendall(pickle.dumps(data_form))
                inc_data_form = pickle.loads(connector_sock.recv(BUFFER))
                get_address = inc_data_form[1]
                connector_sock.close()
            except socket.error as E:
                print(E)
        return get_address

    def update_fingertable(self):
        for i in range(M):
            temp_id = (self.node_id + (2 ** i)) % KEY_SPACE
            # If only one node in the whole network
            if self.node_successor == self.node_address:
                # Remember here node_address is a tuple of len 2
                self.node_fingertable[temp_id] = (self.node_id, self.node_address)
                continue

            get_address = self.get_successor(self.node_successor, temp_id)
            get_id = get_hashed_int(get_address[0] + ":" + str(get_address[1]))
            self.node_fingertable[temp_id] = (get_id, get_address)

    def fix_fingers(self):
        # Initialize cursor as -1 first, -1 can never be a unique identifier id in chord ring
        cursor = -1
        cursor = self.node_successor
        while True:
            if cursor == self.node_address:
                break
            fix_fingers_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                fix_fingers_sock.connect(cursor)
                ###### numbers
                fix_fingers_sock.sendall(pickle.dumps([5]))
                cursor = pickle.loads(fix_fingers_sock.recv(BUFFER))
                fix_fingers_sock.close()
                if cursor == self.node_successor:
                    break
            except socket.error as E:
                print(E)

    def disp_clt_opts(self):
        ######
        print("\n1. Join Network\n2. Leave Network\n3. Send Message\n")


if len(sys.argv) >= 3:
    IP = sys.argv[1]
    PORT = int(sys.argv[2])
else:
    pass
    # print("No values given.")

print("The system will automatically capture your IP node_address and node_port as soon as you run the script,"
      " but for testing you need to manually enter your IP node_address and node_port.\n")

while True:
    user_username = input("Enter your username for public display: ")

    if len(user_username) < 4:
        print("The username must me at least four characters long\n")
        continue
    else:
        break

print("\nYOU MUST FIRST KNOW THE ADDRESS OF AN ARBITRARY NODE INSIDE CHORD CIRCLE TO JOIN THE NETWORK AND IT HELPS YOU FIND YOUR SUCCESSOR")
# Just running the script wont voluntarily join you to the chord network,
# It will setup your socket for potential join as a successor to the already existing nodes if any.
if __name__ == "__main__":
    node_obj = Node(IP, PORT)
    node_obj.start_chord()
    node_obj.node_server_sock.close()