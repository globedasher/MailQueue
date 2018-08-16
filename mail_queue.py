# mail_queue.py to make a queue
#
#
###############################################################################

import datetime, signal, subprocess, sys, zmq, time, argparse, pprint
from multiprocessing import Process, Pipe
from py_core import logger


class Node():
    """
    Node Class.
    """
    next_node = None
    ident = 0
    date = 0

    def __init__(self, data, ident):
        self.date = data
        self.ident = ident


class MailQueue():
    """
    MailQueue class.
    """
    head = None

    def _find_node(self, ident):
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
            if currentNode.ident == ident:
                return currentNode
            currentNode = currentNode.next_node

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

    def _insert_node(self, date, ident):
        # Create and insert a node
        new_node = Node(date, ident)
        # if no head exists on the queue
        if not self.head:
            self.head = new_node
        # if the new node date is less than the current head
        elif new_node.date < self.head.date:
            new_node.next_node = self.head
            self.head = new_node
        # else pop the location to insert it
        else:
            currentNode = self.head
            while currentNode:
                # If we get to the end and haven't inserted the node, add it to
                # then end of the queue
                if not currentNode.next_node:
                    currentNode.next_node = new_node
                    break
                elif (new_node.date > currentNode.date
                    and new_node.date < currentNode.next_node.date):
                    new_node.next_node = currentNode.next_node
                    currentNode.next_node = new_node
                    break
                currentNode = currentNode.next_node

    def read_nodes(self):
        """
        This method will return a dictionary with all the nodes currently in
        the queue.
        """
        #print("Read Nodes")
        return_list = []
        #return_dict = {}
        if not self.head:
            return(return_list)
            #return(return_dict)
        currentNode = self.head
        node_number = 0
        while currentNode:
            #print(str(currentNode.date), str(currentNode.ident), str(node_number))

            # Create a nested dictionary for each node in the queue
            return_node = {
                    "order": node_number,
                    "date": currentNode.date,
                    "ident": currentNode.ident,
                    "node": node_number,
                    }

            # Add the new sub dictionary to the return_dict.
            #return_dict[str(node_number)] = return_node
            return_list.append(return_node)
            # Advance to the next node
            currentNode = currentNode.next_node
            node_number += 1
            #print(node_number)
        return str(return_list)
        #return str(return_dict)

    def create(self, message):
        """
        Create a value into the queue.  If a node with the specified ident is
        in the queue, it will be removed and a new node will be created based
        on the new value. If is not in the queue, it will be added at correct
        location.
        """
        command, ident = message.split(":")
        #print(command, ident)
        date = datetime.datetime.now()
        offset = datetime.timedelta(minutes = 5)
        date = date + offset

        # Remove the node from the 
        found_on_next_node = self._pop_node(ident)
        self._insert_node(date, ident)

    def read(self, message):
        #print("Read method")
        print(message)
        command, ident = message.split(":")
        #print("read", ident)
        node = self._find_node(ident)
        print(node.date, node.ident)
        return node

    def update(self, message):
        #print("Update")
        #print(message)
        command, ident = message.split(":")
        date = datetime.datetime.now()
        offset = datetime.timedelta(days = 5)
        date = date + offset

        found_on_next_node = self._pop_node(ident)
        self._insert_node(date, ident)

    def delete(self, message):
        #print(message)
        command, ident = message.split(":")
        #print("Delete", ident)
        found_on_next_node = self._pop_node(ident)


    def end_node(self):
        """cnsert
        I don't know why I built this...
        """
        print("End Node")
        currentNode = self.head
        while currentNode:
            if not currentNode.next_node:
                return currentNode.date
            currentNode = currentNode.next_node

    def expire(self):
        """
        This method expires nodes.
        """
        #print("Check for expired node")
        if self.head and self.head.date <= datetime.datetime.now():
            print("Expire Date:", self.head.date
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


class MailClient():
    def __init__(self):
        print("Init MailClient")

    def get_ipc_socket(self):
        context = zmq.Context()
        print("Creating socket to process.")
        socket = context.socket(zmq.REQ)
        rc = socket.connect("ipc:///tmp/mail_queue_ipc")
        #print(rc)
        return socket

    def send_command(self, command, socket):
        socket.send_string(command)
        message = socket.recv()
        message = message.decode("utf-8")
        message = eval(message)
        printer = pprint.PrettyPrinter(indent=4)
        printer.pprint(message)

    def read_queue(self):
        """
        Get the queue
        """
        socket = self.get_ipc_socket()
        command = "read_queue"
        socket.send_string(command)

        message = socket.recv()
        message = message.decode("utf-8")
        message = eval(message)
        #print(type(message))
        #print("Received reply: %s" % (message))
        printer = pprint.PrettyPrinter(indent=4)
        printer.pprint(message)

    def create(self, param):
        """
        Send an create command to the process.
        """
        print("Create id", param)

        socket = self.get_ipc_socket()

        print("Sending create for ident %s" % param)
        command = "create: %s" % (param)
        socket.send_string(command)

        message = socket.recv()
        print("Received reply: %s" % (message))

    def read(self, param):
        socket = self.get_ipc_socket()
        command = "read: %s" % (param)
        self.send_command(command, socket)

    def update(self, param):
        socket = self.get_ipc_socket()
        command = "update: %s" % (param)
        self.send_command(command, socket)

    def delete(self, param):
        socket = self.get_ipc_socket()
        command = "delete: %s" % (param)
        self.send_command(command, socket)


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
        mail_queue.create(data, ident)
    return mail_queue


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
            print('whoops')
            message = listen_socket.recv()
            print("Received message: %s" % message)
            message = message.decode("utf-8")
            if "read_queue" in message:
                listen_socket.send_string(str(mail_queue.read_nodes()))
                #return_dict = mail_queue.read_nodes()
                #print("return_list", return_list)
                #if len(return_list) == 0:
                    #listen_socket.send_string("No nodes")
                #else:
                    #listen_socket.send_string(return_list)
            elif "create" in message:
                #print("create")
                mail_queue.create(message)
                listen_socket.send_string("0")
            elif "read" in message:
                response = mail_queue.read(message)
                return_dict = {
                        "date": response.date,
                        "ident": response.ident,
                        }
                print(return_dict)
                listen_socket.send_string(str(return_dict))
            elif "update" in message:
                mail_queue.update(message)
                listen_socket.send_string("0")
            elif "delete" in message:
                mail_queue.delete(message)
                listen_socket.send_string("0")
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
       #mail_queue.read_nodes()

    send_pipe = Pipe()
    # To start threads, you have to use this silly syntax to pass the target
    # function to run in the thread and the arguments also need this silly
    # context where a single parameter is in a tupple with a free element
    # after.

    mail_queue_process = Process(target=main_loop
                       , args=(mail_queue, streams))
    mail_queue_process.start()

def get_args():
    """
    Get argument data passed from the command line and return a dictionary of
    the arguments.
    """
    parser = argparse.ArgumentParser()

    help_text = """Run as a process. This will automatically accept commands from the inter-process API."""
    parser.add_argument(  "--runprocess"
                        , dest="process"
                        , action="store_true"
                        , help=help_text
                        )

    help_text = """--create <ident> to create a new node in the mail_queue."""
    parser.add_argument("-c"
                        , "--create"
                        , dest="create"
                        , default=None
                        , help=help_text
                        )

    help_text = """--read <ident> to return information from one node in the queue."""
    parser.add_argument(  "-r"
                        , "--read"
                        , dest="read_node"
                        , default=None
                        , help=help_text
                        )

    help_text = """Return data from all nodes in the queue."""
    parser.add_argument(  "--read-queue"
                        , dest="read_queue"
                        , action="store_true"
                        , help=help_text
                        )

    help_text = """--update <ident> to update the date of a specific node."""
    parser.add_argument("-u"
                        , "--update"
                        , dest="update"
                        , default=None
                        , help=help_text
                        )

    help_text = """--delete <ident> to remove a specific node from the queue."""
    parser.add_argument("-d"
                        , "--delete"
                        , dest="delete"
                        , default=None
                        , help=help_text
                        )

    help_text = "--tests <int> will run test creates on startup."
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
    elif args.create:
        client = MailClient()
        client.create(args.create)
    elif args.read_queue:
        client = MailClient()
        client.read_queue()
    elif args.read_node:
        client = MailClient()
        client.read(args.read_node)
    elif args.update:
        client = MailClient()
        client.update(args.update)
    elif args.delete:
        client = MailClient()
        client.delete(args.delete)

    else:
        # This else will allow dev to run this script with no args to just run
        # test insert.
        test_queue = MailQueue()
        test_queue = test_inserts(test_queue, 10)
        test_queue = test_inserts(test_queue, 5)
        test_queue.read_nodes()

if __name__ == "__main__":
    main()
