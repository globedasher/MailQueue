# mail_queue.py to make a queue
#
#
###############################################################################

import datetime, signal, subprocess, sys, zmq, time, argparse
from multiprocessing import Process, Pipe
from py_core import logger


class Node():
    """
    Node Class.
    """
    next_node = None
    ident = 0
    value = 0

    def __init__(self, data, ident):
        self.value = data
        self.ident = ident


class MailQueue():
    """
    MailQueue class.
    """
    head = None

    def _pop_node(self, ident):
        #print("_pop_node")
        if not self.head:
            return None
        currentNode = self.head
        if currentNode.ident == ident:
            self.head = currentNode.next_node
            currentNode.next_node = None
            return currentNode
        while currentNode:
            if not currentNode.next_node:
                return None
            if currentNode.next_node.ident == ident:
                removal_target = currentNode.next_node
                currentNode.next_node = currentNode.next_node.next_node
                removal_target.next_node = None
                return currentNode
            currentNode = currentNode.next_node

        # If no node with that ident is found, return None
        return None

    def _insert_node(self, value, ident):
        # Create and insert a node
        new_node = Node(value, ident)
        # if no head exists on the queue
        if not self.head:
            self.head = new_node
        # if the new node value is less than the current head
        elif new_node.value < self.head.value:
            new_node.next_node = self.head
            self.head = new_node
        # else find the location to insert it
        else:
            currentNode = self.head
            while currentNode:
                # If we get to the end and haven't inserted the node, add it to
                # then end of the queue
                if not currentNode.next_node:
                    currentNode.next_node = new_node
                    break
                elif (new_node.value > currentNode.value
                    and new_node.value < currentNode.next_node.value):
                    new_node.next_node = currentNode.next_node
                    currentNode.next_node = new_node
                    break
                currentNode = currentNode.next_node

    def insert(self, value, ident):
        """
        Insert a value into the queue.  If a node with the specified ident is
        in the queue, it will be removed and a new node will be created based
        on the new value. If is not in the queue, it will be added at correct
        location.
        """
        #print("Insert", value,"Ident", ident)
        found_on_next_node = self._pop_node(ident)
        # if found_on_next_node:
        #     print("Found node:"
        #          ,found_on_next_node.value
        #          ,found_on_next_node.ident
        #          )
        self._insert_node(value, ident)


    def print_nodes(self):
        print("Print Nodes")
        if not self.head:
            print("No nodes")
        currentNode = self.head
        node_number = 0
        return_list = ""
        while currentNode:
            #print(str(currentNode.value), str(currentNode.ident), str(node_number))
            return_list += ("Date: %s, Ident: %s, Node number: %s\n" % (currentNode.value, currentNode.ident, node_number))
            #print(return_list)
            currentNode = currentNode.next_node
            node_number += 1
        return return_list

    def end_node(self):
        print("End Node")
        currentNode = self.head
        while currentNode:
            if not currentNode.next_node:
                return currentNode.value
            currentNode = currentNode.next_node

    def expire(self):
        #print("Check for expired node")
        if self.head and self.head.value <= datetime.datetime.now():
            print("Expire Date:", self.head.value
                 ,"Ident:", self.head.ident
                 )
            self.head = self.head.next_node
        else:
            #print("No nodes to expire")
            pass


class Streams():
    """
    stdin, stdout, stderr class.
    """
    out_original = ''
    err_original = ''

    def __init__(self):
        print("Initializing system.")
        #self.config_out()
        #self.config_err()

    def config_out(self):
        print("config")
        # Redirect the standard output
        self.out_original = sys.stdout
        self.osock = open("output.log", "w")
        sys.stdout = self.osock

    def config_err(self):
        print("config")
        self.err_original = sys.stderr
        self.fsock = open("error.log", "w")
        sys.stderr = self.fsock

    def close(self):
        print("close_streams")
        if self.out_original:
            sys.stdout = self.out_original
        if self.err_original:
            sys.stderr = self.err_original

    def end_runtime(self, signum, frame):
        print("\n\nUser ended runtime.")
        self.close()
        print("\n\nUser ended runtime.")
        #print(dir(frame))
        sys.exit(signum)


class ControlComms():
    def __init__(self):
        print("Creating Controls")


def test_inserts(mail_queue, tests):
    value = 0
    #print(value)
    # This is the main loop of the program. One of the functions run by it will
    # need to check for any interrupt signals.
    while value < tests:
        if value == 0:
            print(datetime.datetime.now())
        value += 1
        #print(value)
        data = datetime.datetime.now()
        offset = datetime.timedelta(seconds = value + 0)
        data = data + offset
        ident = tests - value
        mail_queue.insert(data, ident)
    return mail_queue

def comms_loop(send_pipe):
    print("comms_loop")

    listen_context = zmq.Context()
    listen_socket = listen_context.socket(zmq.REP)
    listen_socket.bind("ipc:///tmp/mail_queue_ipc")


    while True:

        #message = listen_socket.recv()
        #print("Received request: %s" % message)
        listen_socket.send(b"World")

        #print("Here")
        message = message.decode("utf-8")
        #print(message)
        if message == "insert":
            #print("Over here")
            send_pipe.send("insert")
        elif message == "list":
            send_pipe.send("list")
        #sys.exit()

def main_loop(mail_queue, streams):
    print("main_loop")

    listen_context = zmq.Context()
    listen_socket = listen_context.socket(zmq.REP)
    listen_socket.bind("ipc:///tmp/mail_queue_ipc")
    poller = zmq.Poller()
    poller.register(listen_socket)

    while True:

        ## POLL COMMS
        try:
            socks = dict(poller.poll(zmq.DONTWAIT))
        except KeyboardInterrupt:
            break
        if listen_socket in socks:
            message = listen_socket.recv()
            print("Received message: %s" % message)
            message = message.decode("utf-8")
            command, ident = message.split(":")
            #print(command, ident)
            if command == "insert":
                #print("insert")
                data = datetime.datetime.now()
                offset = datetime.timedelta(days = 5)
                data = data + offset
                mail_queue.insert(data, ident)
                listen_socket.send_string("0")
            elif command == "list":
                return_list = mail_queue.print_nodes()
                print("return_list", return_list)
                if len(return_list) == 0:
                    listen_socket.send_string("No nodes")
                else:
                    listen_socket.send_string(return_list)
            else:
                pass

        ## EXPIRE
        mail_queue.expire()
        #print("Done?")
        time.sleep(.1)
        if not mail_queue.head:
            #streams.close()
            #break
            pass

def process_init(tests):
    print("Starting as process")

    streams = Streams()

    signal.signal(signal.SIGINT, streams.end_runtime)

    mail_queue = MailQueue()
    if tests:
        mail_queue = test_inserts(mail_queue, tests)
       #mail_queue.print_nodes()

    send_pipe = Pipe()
    # To start threads, you have to use this silly syntax to pass the target
    # function to run in the thread and the arguments also need this silly
    # context where a single parameter is in a tupple with a free element
    # after.

    #comms_loop_process = Process(target=comms_loop, args=(send_pipe, ))
    #comms_loop_process.start()

    mail_queue_process = Process(target=main_loop
                       , args=(mail_queue, streams))
    mail_queue_process.start()

def insert(param=None):
    """
    Send an insert command to the process.
    """
    print("Insert id", param)

    context = zmq.Context()

    print("Transmitting commands to process.")
    socket = context.socket(zmq.REQ)
    rc = socket.connect("ipc:///tmp/mail_queue_ipc")
    #print(rc)

    print("Sending insert for ident %s" % param)
    command = "insert: %s" % (param)
    socket.send_string(command)

    message = socket.recv()
    print("Received reply: %s" % (message))

def print_list():
    """
    Get the queue
    """
    context = zmq.Context()

    print("Transmitting commands to process.")
    socket = context.socket(zmq.REQ)
    rc = socket.connect("ipc:///tmp/mail_queue_ipc")
    #print(rc)

    command = "list:"
    socket.send_string(command)

    message = socket.recv()
    message = message.decode("utf-8")
    print("Received reply: %s" % (message))

def get_args():
    """
    Get argument data passed from the command line and return a dictionary of
    the arguments.
    """
    parser = argparse.ArgumentParser()

    help_text = """Run as a process."""
    parser.add_argument(  "--runprocess"
                        , dest="process"
                        , action="store_true"
                        , help=help_text
                        )

    help_text = """Insert a value with an ident."""
    parser.add_argument("-i"
                        , "--insert"
                        , dest="insert"
                        , default=None
                        , help=help_text
                        )

    help_text = """See all nodes in queue."""
    parser.add_argument("-p"
                        , "--print"
                        , dest="print_list"
                        , action="store_true"
                        , help=help_text
                        )

    help_text = "Use -t or --tests to run test inserts on startup."
    parser.add_argument("-t"
                        , "--tests"
                        , type=int
                        , dest="tests"
                        , default=0
                        , help=help_text
                        )

    return parser.parse_args()

def main():
    args = get_args()
    print(args)

    if args.process:
        process_init(args.tests)
    elif args.insert:
        insert(args.insert)
    elif args.print_list:
        print_list()
    else:
        # This else will allow dev to run this script with no args to just run
        # test insert.
        test_queue = MailQueue()
        test_queue = test_inserts(test_queue, 10)
        test_queue = test_inserts(test_queue, 5)
        test_queue.print_nodes()

if __name__ == "__main__":
    main()
