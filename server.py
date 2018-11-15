#!/usr/bin/python3
# Copyright 2018 EMMANUEL I. SARRIS
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import sys
if len(sys.argv) == 1:
    print("For usage info execute $./server.py --help")
    exit()

import os
import re
import time
import socket
import getpass
import datetime
import binascii
import threading
import subprocess
import numpy as np
from io import StringIO
from os import fdopen, remove
from shutil import move
from pathlib import Path
from tempfile import mkstemp
from matplotlib.font_manager import FontProperties

"""
ysls_x
The variables bellow are lists that store all the user input values, stored in file <values.exp>
"""
ysls_runner_pool_size = []
ysls_nm_memory_mb = []
ysls_nm_vcores = []
ysls_nm_heartbeat_interval_ms = []
ysls_am_heartbeat_interval_ms = []
ysls_container_memory_mb = []
ysls_container_vcores = []


"""
sys_x
The variables bellow are lists that store all the necessary variables for scripts execution, stored in file <runtime_parameters.dat>
"""
sys_attribute_values_loc = "undef"
sys_output_directory = "undef"
sys_hadoop_home = "undef"
sys_trace_location = "undef"
sys_experiment_timeout = "no_time_cap"
sys_live_track_file = "undef"
sys_recover = "no"

"""
exp_values_together
exp_values_pending
Both of the lists bellow, store the value that pending for results and for send, in the following format:
rps200,nmMem10240,nmVcores9,nmHeart1000,amHeart1000,contMem2048,contVcores1
rps200,nmMem10240,nmVcores9,nmHeart1000,amHeart1000,contMem2048,contVcores2
rps200,nmMem10240,nmVcores9,nmHeart1000,amHeart1010,contMem1024,contVcores1
etc.....
"""
exp_values_together = []
exp_values_pending = []

"""
address_port
Variable that stores IP and PORT the server will bind at, taken from sys.argv[3]
p.e
if sys.argv[3] == 192.168.1.105:11000
    address_port[0] = 192.168.1.105
    address_port[1] = 11000
"""
address_port = []

"""
heart_port
Global variable heart_port, is used for enumerating the UDP port that the server will
listen to
"""
heart_port = 30000

"""
parent_pid
parent_root_dir
parent_pid is a variavle, that is used to keep track of the directory that the server is running from.
"""
parent_pid = os.getpid()
parent_root_dir = os.getcwd()

"""
java_port_gui
Is an auto increment variable that gives out a port to a Java GUI, to listen to
java_address_list
Is a list that gives the server the ability to broadcast a message to all Java GUIs
java_address_list[x] is a tuple with [0] beeing th IP and [1] beeing the PORT of the Client
"""
java_port_gui = 40000
java_address_list = []


def main():
    global address_port
    # control flow checks
    check_argv_integrity()
    if sys.argv[1] == "--help":
        instructions()
        exit()
    elif sys.argv[1] == "--load-defaults":
        load_defaults()
        exit()
    elif sys.argv[1] == "--check-dfs":
        dfs_manipulation()
        exit()
    elif sys.argv[1] == "--check-packets":
        check_for_packets("userprompt")
        exit()
    elif sys.argv[1] == "--run-server":
        take_input_args()
        init_progress_file()
        create_working_dir()
        load_values()
        create_values_combinations()
        address_port = sys.argv[3].split(':')  # get local machine IP and PORT from user arguments
        print("INFO: IP is: " + str(address_port[0]))
        print("INFO: port is: " + str(address_port[1]))
        my_tracking_child = os.fork()
        if my_tracking_child == 0:  # child code
            keep_track_of_clients()
        run_server_loop()
    elif sys.argv[1] == "--run-standalone":
        take_input_args()
        create_working_dir()
        load_values()
        run_standalone()
        exit()


def run_server_loop():
    """
    This function keeps the server running even during an uknown exception
    Enable run_server() if you want to troubleshoot and disable while
    Enable while and disable run_server() otherwise
    """
    run_server()
    # while True:
    #     try:
    #         run_server()
    #     except IndexError as iner:
    #         print("Thank you for using YARN SLS automator")
    #         sys.exit(1)
    #     except:
    #         if os.getpid() != parent_pid:
    #             sys.exit(1)
    #         server_info()  # Wait for socket to re __init__


def server_info():
    """
    This function restart the server after 7 seconds
    """
    print("INFO: Something went wrong while running the server. Restarting in 7")
    for i in range(7, 0, -1):
        print("INFO: ..." + str(i))
        time.sleep(1)


def run_server():
    """
    This is the main runtime function of the server.
    It creates a server socket and waits for a connection.
    According to the received message, it will either send experiment values or receive experiment results.
    In the case of experiment results retrieving, this function will download the file, make a verification of the downloaded file
    by using md5sum, then will unzip the .tar.gz and then will create the coresponding graphs out the data received, by using a child process
    for that time costly grpah creation operation procedure.
    The jobs(experiment values) are in exp_values_together list and are pop-ing out to clients and to exp_values_pending.
    In case of md5sum match, the values are getting removed from exp_values_pending.
    In case of md5sum mismatch, the values are getting back in queue, with priority number one
    """
    init_progress_file()
    global exp_values_together
    global exp_values_pending
    global address_port
    ## print("DEBUG: " + str(exp_values_together))
    # for el in exp_values_together:  # debug
    # print(el)  # debug
    print("INFO: address_port<" + str(address_port) + ">")
    try:
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # create a socket object
    except socket.error as serr:
        print("ERROR: Error while trying to create socket object")
    try:
        serversocket.bind((str(address_port[0]), int(address_port[1])))  # bind to the port
    except socket.error as error:
        print("ERROR: Error while creating socket:<" + str(error) + ">")
        if re.search("Errno 98", str(error)):
            my_regex = r"([0-9]{1,3}\.?){1,4}:[0-9]{1,5}"  # Matches IPv4:PORT
            address_port = input("Please provide a diferrent <IP:PORT> pair$")
            while not re.search(my_regex, address_port):
                input("ERROR: There is something wrong with IP:PORT input")
                address_port = input("Please provide a diferrent <IP:PORT> pair$")
            address_port = address_port.split(':')
            run_server_loop()
    serversocket.listen()  # act like host
    my_inactive_dict = []
    run_java_tracking()  # runs java gui
    # while exp_values_together or exp_values_pending:
    while True:
        if not exp_values_together and not exp_values_pending:
            break
        #print("DEBUG: exp_values_together[0] is:<" + exp_values_together[0] + ">")
        with open(sys_live_track_file, 'r') as file:
            fileContent = file.readlines()
        for d in fileContent:
            if d[0] == "#":  # if its a comment line proceed.
                continue
            elif len(d) > 2:  # if its smaller than two, its a space line.
                keyvalue = d.split(":")                 # keyvalue[0] has the 1@rps100,nmMem10240,.... and keyvalue[1] has the status
                keyvalue[1] = keyvalue[1].strip()
                #print("DEBUG DEUBG: keyvalue[1] is:<" + str(keyvalue[1]) + ">")
                if keyvalue[1] == "Fail":
                    if keyvalue[0] in my_inactive_dict:  # if it is already back to queue
                        continue
                    else:
                        #print("DEBUG DEBUG: exp_values_together0 is <" + str(exp_values_together[0]) + ">")
                        my_inactive_dict.append(keyvalue[0])  # check it as added to queue
                        values = keyvalue[0].split('@')
                        values[1] = values[1].strip()
                        exp_values_together.insert(0, values[1])  # put it back at the top of the queue
                        exp_values_pending.remove(values[1])     # remove it from the pending experiments
                        #print("DEBUG DEBUG: values[1] is <" + str(values[1]) + ">")
                        #print("DEBUG DEBUG: exp_values_together0 is <" + str(exp_values_together[0]) + ">")
        print("INFO: Waiting for connection ==============>")
        clientsocket, addr = serversocket.accept()  # Establish a connection. Blocking function
        print("INFO: Got a connection from %s" % str(addr))
        msg_rcv = clientsocket.recv(16)   # Status of worker (GiveMeWork or ResultsIncoming)
        print("INFO: Message received: <" + str(msg_rcv.decode('ascii')) + ">")
        if re.search("GiveMeWork", msg_rcv.decode('ascii')) and exp_values_together:  # if true, worker is waiting for job
            exp_values_pending.append(exp_values_together[0])
            msg = "SLS_Experiment_On_Values:" + str(exp_values_together.pop(0))  # 25
            try:
                clientsocket.send(msg.encode('ascii'))
                print("INFO: Successfully send experiment values to client")
            except socket.error as serr:
                print("ERROR: Error sending Experiment Values: <" + str(serr) + ">")
        elif re.search("ResultsIncoming", msg_rcv.decode('ascii')):
            try:
                print("INFO: About to send Ack")
                clientsocket.send("Ack".encode('ascii'))
                print("INFO: Ack sent!")
            except socket.error as serr:
                print("ERROR: Error sending Ack: <" + str(serr) + ">")
            try:
                print("INFO: About to receive file name and md5sum")
                msg_rcv = clientsocket.recv(1024)
                print("INFO: Received file and md5sum:<" + str(msg_rcv.decode('ascii')) + ">")
            except socket.error as serr:
                print("ERROR: Error receiving file name and md5sum: <" + str(serr) + ">")
            msg_rcv = msg_rcv.decode('ascii')
            msg_rcv = msg_rcv.split(",")
            msg_rcv[0] = re.sub(r'^(.*:)', '', msg_rcv[0])  # Transform SendingFile: <file>, into <file>
            msg_rcv[1] = re.sub(r'^(.*:)', '', msg_rcv[1])  # Transform WithMD5Hash: <hash>, into <hash>
            print("INFO: About to receive file:<" + str(msg_rcv[0]) + ">")
            print("INFO: ...with md5sum of:<" + str(msg_rcv[1]) + ">")
            try:
                os.makedirs(sys_output_directory + msg_rcv[0] + "/")
            except OSError as oser:
                print("TERMINAL ERROR: Cannot create directory:\n" + str(oser))
            downloaded_file = open(sys_output_directory + msg_rcv[0] + "/" + msg_rcv[0] + ".tar.gz", "wb")
            try:
                print("INFO: About to send StartTransfer")
                clientsocket.send("StartTransfer".encode('ascii'))
                print("INFO: Successfully send StartTransfer")
            except socket.error as serr:
                print("ERROR: Error while sending StartTransfer acknowledgment")
            print("=========================")
            print("INFO: About to start download")
            transfered_data = clientsocket.recv(1024)
            while transfered_data:
                downloaded_file.write(transfered_data)
                transfered_data = clientsocket.recv(1024)
            downloaded_file.close()
            print("INFO: Downloaded file successfully!")
            print("=========================")
            process = subprocess.Popen("md5sum " + sys_output_directory + msg_rcv[0] + "/" + msg_rcv[0] + ".tar.gz",
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            results = str(stdout)
            print("INFO: md5sum of sent folder at localhost is:<" + results[:34] + ">")  # md5 is first 34 digits
            print("INFO: md5sum of sent folder at client is:<" + str(msg_rcv[1]) + ">")
            remove_value = msg_rcv[0].replace("_", ",")
            if results[:30] == msg_rcv[1][:30]:     # Comparing the first 30 characters of md5sums. Rest 4 digits are not taken into consideration because of possible loss of them in the network buffer
                print("INFO: md5sum match! Experiment completed successfully!")
                try:
                    exp_values_pending.remove(remove_value)
                    print("INFO: Removed values from the pending list")
                    print("INFO: Starting automatic creation of graphs")
                    unzip_process = subprocess.Popen("tar -zxvf " + sys_output_directory + msg_rcv[0] + "/" + msg_rcv[0] + ".tar.gz -C " +
                                                     sys_output_directory + msg_rcv[0],
                                                     shell=True,
                                                     stdout=subprocess.PIPE)
                    unzip_process.wait()
                    another_child = os.fork()
                    if another_child == 0:  # child code
                        os.chdir(sys_output_directory + msg_rcv[0] + "/")
                        create_graphs(sys_output_directory + msg_rcv[0] + "/")
                        sys.exit(1)
                except ValueError as verr:
                    print("ERROR: Error removing values from the pending list:" + str(verr) + "")
            else:
                print("ERROR: md5sum mismatch! Values for this experiment will be assigned to a client again")
                exp_values_together.insert(0, remove_value)
                exp_values_pending.remove(remove_value)
        clientsocket.close()


def keep_track_of_clients():
    """
    This function is beeing runned by a chil process, and listern at a UDP socket, for heartbeat messages
    from the clients that are currently running experiments.
    """
    global java_port_gui
    global java_address_list
    global address_port
    # First kill anything that runs on port 30000
    clear_port_30000 = subprocess.Popen("kill -9 $(lsof -t -i:30000)",
                                        shell=True,
                                        stdout=subprocess.PIPE)
    clear_port_30000.wait()
    """
    A thread is created to give out UDP ports to the Java GUIs that want to diplasy experiment's process
    """
    threading.Thread(target=send_port).start()
    try:
        gui_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as serr:
        print("ERROR: CHILD: GUI_TRACK: error creating the UDP socket:<" + str(serr) + ">")
        print("TERMINAL ERROR: Server will shutdown.")
        system_exit()
    """
    my_inner_child
    The process will keep active the tracking progress, unblocking it's parent from blocking recvfrom,
    in order for the parent to proceed to operation regarding the experiment's expire on clients.
    """
    ###
    my_inner_child = os.fork()
    if my_inner_child == 0:  # inner child code
        while True:
            my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server_address = (address_port[0], 30000)
            try:
                my_socket.sendto("keepGoing".encode('ascii'), server_address)
            except socket.error as serr:
                print("ERROR: INNER CHILD: Sending AliveStatus<" + str(serr) + ">")
            time.sleep(5)
    ###
    print("INFO: CHILD: About to bind at:" + str(address_port[0]) + ":" + str(heart_port))
    server_address = (address_port[0], int(heart_port))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(server_address)
    except socket.error as serr:
        print("ERROR: CHILD: error creating the UDP socket:<" + str(serr) + ">")
        print("TERMINAL ERROR: Server will shutdown.")
        system_exit()

    print("INFO: CHILD: About to start accepting heartbeats")
    my_dict = {"": ""}
    while True:
        data_to_track = []
        data, addr = sock.recvfrom(1024)
        # print("INFO: CHILD: Received heartbeat:" + data.decode('ascii')) # Enable if like. Creates lots of log...
        with open(sys_live_track_file, 'r') as file:
            fileContent = file.readlines()
        data = data.decode('ascii')
        if data != "keepGoing":  # This statement tracks the changes of the experiment's progress
            data = data.split(':')  # data[0] has experiment in rps2,nmMem1024... and data[1] has the % progress
            i, flag = 0, 0
            for d in fileContent:
                my_regex = r".*(" + str(data[0]) + ").*"
                if d[0] == "#":
                    i += 1
                    continue
                if re.search(my_regex, d):
                    keyvalue = d.split(":")
                    keyvalue[1] = keyvalue[1].strip()
                    if keyvalue[1] != "Fail":
                        flag = 1
                        break
                if flag == 0:
                    i += 1
            if flag == 1:  # experiment exists
                exp_details = fileContent[i].split(":")
                fileContent.pop(i)
                fileContent.insert(i, str(exp_details[0] + ":" + data[1]) + "\n")
                data_to_track.append(str(exp_details[0] + ":" + data[1]))
                my_dict[exp_details[0]] = (data[1], time.time())
            else:  # experiment will now be added
                line = i - 13
                key = str(str(line) + "@" + str(data[0]))
                fileContent.append(key + ":" + str(data[1]) + "\n")
                data_to_track.append(key + ":" + str(data[1]))
                my_dict[key] = (data[1], time.time())
        i, flag = 0, 0
        for d in fileContent:
            if d[0] == "#":  # if the line is a comment, proceed to the next line
                i += 1
                continue
            elif len(d) > 2:  # if the line is smaller than 2, means its a whitespace...so skip it. (error handling)
                key = d.split(":")
                key[1] = key[1].strip()
                temp = my_dict.get(key[0])  # temp0 -> value, temp1 -> time.time()
                if int(temp[0]) != 100 and key[1] != "Fail":  # if experiment hasn't finish and it's not failed, track it...!
                    elapsed_time = time.time() - temp[1]
                    if key[1] == temp[0] and elapsed_time > 40:
                        print("INFO: About to pop row<" + str(i + 1) + ">")
                        print("INFO: About to insert<" + str(key[0] + ":" + "Fail") + ">")
                        fileContent.pop(i)
                        fileContent.insert(i, str(key[0] + ":" + "Fail") + "\n")
                        data_to_track.append(str(key[0] + ":" + "Fail"))
                i += 1
        #print("DEBUG:DEBUG: FIle content to be w:<"+str(fileContent)+">")
        with open(sys_live_track_file, 'w') as file:
            file.writelines(fileContent)
        if data_to_track and java_address_list:
            for client in java_address_list:        # client is IPv4
                print("INFO: CHILD: About to send progress to client <%s>" % str(client))
                for exp in data_to_track:
                    try:
                        gui_socket.sendto(str(exp).encode('ascii'), (client, 50000))
                    except socket.error as serr:
                        print("WARN: CHILD: There was an error sending progress to %s" % str(client))
                        print("WARN: CHILD: Error:<%s>" % str(serr))


def send_port():
    """
    This function is beeing run by a thread, and stores at java_address_list all 
    tha Java GUIs Internet location, in order for the server to broadcast them
    the progress of the experiments.
    """
    global java_port_gui
    global java_address_list
    global address_port
    try:
        my_incr_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        my_incr_socket.bind((str(address_port[0]), 30001))
        my_incr_socket.listen()
    except socket.error as serr:
        print("TERMINAL: ERROR: There was an error in the Thread, while createing the socket")
        print("TERMINAL: ERROR: Error info<" + str(serr) + ">")
        system_exit()
    while True:
        flag = 0
        print("INFO: THREAD: Waiting for connection")
        try:
            clientsocket, addr = my_incr_socket.accept()
            flag = 1
            print("INFO: THREAD: Got a connection from " + str(addr))
        except socket.error as serr:
            print("WARN: THREAD: There was an error in Thread, accepting the connection")
        if flag == 1:
            msg = str(java_port_gui).encode('ascii')
            try:
                clientsocket.send(msg)
                java_address_list.append(addr[0])
                java_port_gui += 1
            except socket.error as serr:
                print("WARN: THREAD: There was an error in Thread, sending the port")
            clientsocket.close()


def system_exit():
    """
    This function terminates the process group
    """
    clear_port_30000 = subprocess.Popen("kill  -TERM -" + str(parent_pid),
                                        shell=True,
                                        stdout=subprocess.PIPE)


def run_java_tracking():
    """
    This function start the java swing gui, to track the experiment process
    """
    command = "java -jar " + parent_root_dir + "/ProcessTracking.jar " + address_port[0] + " 30001"
    print("INFO: About to execute Java GUI with command<" + str(command) + ">")
    my_java_tracking = subprocess.Popen(command,
                                        shell=True,
                                        stdout=subprocess.PIPE)


def create_values_combinations():
    """
    This function reads the user values from <values.exp> file, and creates all the possible combinations
    to feed the clients. Combinations are stored in exp_values_together list
    """
    global exp_values_together
    for rps in ysls_runner_pool_size:
        if int(rps) == 0:
            break
        for nm_mem in ysls_nm_memory_mb:
            if int(nm_mem) == 0:
                break
            for nm_vcores in ysls_nm_vcores:
                if int(nm_vcores) == 0:
                    break
                for nm_heart in ysls_nm_heartbeat_interval_ms:
                    if int(nm_heart) == 0:
                        break
                    for am_heart in ysls_am_heartbeat_interval_ms:
                        if int(am_heart) == 0:
                            break
                        for cont_mem in ysls_container_memory_mb:
                            if int(cont_mem) == 0:
                                break
                            for cont_vcores in ysls_container_vcores:
                                if int(cont_vcores) == 0:
                                    break
                                exp_values_together.append("rps" + str(rps) +
                                                           ",nmMem" + str(nm_mem) +
                                                           ",nmVcores" + str(nm_vcores) +
                                                           ",nmHeart" + str(nm_heart) +
                                                           ",amHeart" + str(am_heart) +
                                                           ",contMem" + str(cont_mem) +
                                                           ",contVcores" + str(cont_vcores))
    if sys_recover == "--recover":
        remanage_queue()


def remanage_queue():
    """
    This function runs if the server terminated unexpectedly
    This function will run through a local log file, and reange the queue to the experiments that
    no need to run, if they are already completed or assigned.
    """
    global exp_values_together
    global exp_values_pending
    with open(sys_live_track_file, 'r') as f:
        alive_experiments = f.readlines()
    for line in alive_experiments:
        if line[0] == "#":
            continue
        else:
            line.strip()
            keyvalue = line.split(":")  # keyvalue[0] is experiment in 1@rps2,nmMem1024... and keyvalue[1] is the % progress
            key = keyvalue.split("@")
            key[1].strip()  # key[1] is experiment in 1@rps2,nmMem1024...
            if key[1] in exp_values_together:
                if keyvalue[1] == "Fail":   # If experiment is failed it needs to be restarted
                    continue
                elif keyvalue[1] != "100":  # if experiment is not completed it needs to go to penging list
                    exp_values_pending.append(key[1])
                    exp_values_together.remove(key[1])
                elif keyvalue[1] == "100":  # if experiment is completed it needs to be removed from the queue
                    exp_values_together.remove(key[1])


def run_standalone():
    """
    This function iterates through all the values assigned at attributes in values.exp
    It creates corresponding directories, runs the experiment, and stores the result
    in the correct directory.
    Directory name example:
    rps2_nmMem1024_nmVcores2_nmHeart50_amHeart50_contMem256_contVcores1
    Which translates to:
    rps2       -> yarn.sls.runner.pool.size=2
    nmMem1024  -> yarn.sls.nm.memory.mb=1024
    nmVcores2  -> yarn.sls.nm.vcores=2
    nmHeart50  -> yarn.sls.nm.heartbeat.interval.ms=50
    amHeart50  -> yarn.sls.am.heartbeat.interval.ms=50
    contMem256 -> yarn.sls.container.memory.mb=256
    contVcores1-> yarn.sls.container.vcores=1
    """
    global ysls_runner_pool_size
    global ysls_nm_memory_mb
    global ysls_nm_vcores
    global ysls_nm_heartbeat_interval_ms
    global ysls_am_heartbeat_interval_ms
    global ysls_container_memory_mb
    global ysls_container_vcores

    outputdir = os.getcwd()
    for rps in ysls_runner_pool_size:
        if int(rps) == 0:
            break
        for nm_mem in ysls_nm_memory_mb:
            if int(nm_mem) == 0:
                break
            for nm_vcores in ysls_nm_vcores:
                if int(nm_vcores) == 0:
                    break
                for nm_heart in ysls_nm_heartbeat_interval_ms:
                    if int(nm_heart) == 0:
                        break
                    for am_heart in ysls_am_heartbeat_interval_ms:
                        if int(am_heart) == 0:
                            break
                        for cont_mem in ysls_container_memory_mb:
                            if int(cont_mem) == 0:
                                break
                            for cont_vcores in ysls_container_vcores:
                                if int(cont_vcores) == 0:
                                    break
                                try:
                                    final_directory = outputdir + "/rps" + str(rps) + \
                                        "_nmMem" + str(nm_mem) + \
                                        "_nmVcores" + str(nm_vcores) + \
                                        "_nmHeart" + str(nm_heart) + \
                                        "_amHeart" + str(am_heart) + \
                                        "_contMem" + str(cont_mem) + \
                                        "_contVcores" + str(cont_vcores)
                                    print("INFO: I will create " + final_directory)
                                    os.makedirs(final_directory)
                                    os.chdir(final_directory)
                                    print("INFO: Directory created successfully at, " + final_directory)
                                except OSError:
                                    print("ERROR: You are currently at: " + os.getcwd())
                                    print("ERROR: You are trying to create the dir: " + final_directory)
                                    print("ERROR: There was an error creating that directory.")
                                    exit()
                                run_experiment(rps, nm_mem, nm_vcores, nm_heart, am_heart, cont_mem, cont_vcores, final_directory)
    print("INFO: Finished Execution!")


def run_experiment(rps, nm_mem, nm_vcores, nm_heart, am_heart, cont_mem, cont_vcores, outputdir):
    """
    This function displays information about the experiment that is about to run, and calls the specific function to execute it.
    """
    command = sys_hadoop_home + "share/hadoop/tools/sls/bin/slsrun.sh " + \
        "--tracetype=RUMEN --tracelocation=" + sys_trace_location + " --output-dir=" + str(outputdir)
    print("-------------------------------------------------------")
    print("INFO: Experiment will start based on the following attributes")
    print("INFO: yarn.sls.runner.pool.size: " + str(rps))
    print("INFO: yarn.sls.nm.memory.mb: " + str(nm_mem))
    print("INFO: yarn.sls.nm.vcores: " + str(nm_vcores))
    print("INFO: yarn.sls.nm.heartbeat.interval.ms: " + str(nm_heart))
    print("INFO: yarn.sls.am.heartbeat.interval.ms: " + str(am_heart))
    print("INFO: yarn.sls.container.memory.mb: " + str(cont_mem))
    print("INFO: yarn.sls.container.vcores: " + str(cont_vcores))
    print("INFO: Output directory of above experiment: " + str(outputdir))
    print("INFO: The command to run the experiment is: " + str(command))
    print("INFO: Experiment starts at " + str(datetime.datetime.now().time()) + " o'clock:")
    change_attributes_at_sls_runner(rps, nm_mem, nm_vcores, nm_heart, am_heart, cont_mem, cont_vcores)
    kill_running_sls()
    run_sls(command)
    create_graphs(outputdir)
    print("INFO: Experiment ends at " + str(datetime.datetime.now().time()) + " o'clock:")
    print("-------------------------------------------------------")


def run_sls(command):
    """
    This function runs the Yarn Scheduler Load Simulator
    """
    print("Command to run: " + str(command))
    exp_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    if sys_experiment_timeout == "no_time_cap":
        exp_process.wait()
    else:
        try:
            wait_timeout(exp_process)
        except RuntimeError:
            print("Experiment terminated due to user input arguments")


def wait_timeout(proc):
    """
    This function waits for a process to finish, else raises exception after timeout
    """
    start = time.time()
    end = start + float(sys_experiment_timeout)
    interval = min(float(sys_experiment_timeout) / 1000.0, .25)
    while True:
        result = proc.poll()
        if result is not None:
            return result
        if time.time() >= end:
            raise RuntimeError("Process timed out due to user input arguments.")
        time.sleep(interval)


def kill_running_sls():
    """
    This function terminates, just in case, any up and running SLSRunners
    """
    process = subprocess.Popen("kill `jps | grep \"SLSRunner\" | cut -d \" \" -f 1`", shell=True, stdout=subprocess.PIPE)
    process.wait()


def change_attributes_at_sls_runner(rps, nm_mem, nm_vcores, nm_heart, am_heart, cont_mem, cont_vcores):
    """
    This function changes the parameters of SLS. Specifically it parses the sls-runner.xml and changes the values of
    the attributes, regarding the ones the user issued on load_exp_values()
    """
    sls_runner_path = sys_hadoop_home + "etc/hadoop/sls-runner.xml"
    print("Sls_Runner to open: " + sls_runner_path)
    if not os.path.isfile(sls_runner_path):
        print("ERROR: sls_runner.xml could not be located in $(HADOOP_HOME)/etc/hadoop/. Script will now exit.")
        exit()
    file_desc, abs_path = mkstemp()
    print("INFO: The abs_path of temp folder is: " + abs_path)
    line_above = "empty"
    with fdopen(file_desc, 'w') as new_file:
        with open(sls_runner_path) as old_file:
            for line in old_file:
                if re.search("<name>yarn.sls.runner.pool.size</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(rps) + "</value>\n"))
                elif re.search("<name>yarn.sls.nm.memory.mb</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(nm_mem) + "</value>\n"))
                elif re.search("<name>yarn.sls.nm.vcores</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(nm_vcores) + "</value>\n"))
                elif re.search("<name>yarn.sls.nm.heartbeat.interval.ms</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(nm_heart) + "</value>\n"))
                elif re.search("<name>yarn.sls.am.heartbeat.interval.ms</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(am_heart) + "</value>\n"))
                elif re.search("<name>yarn.sls.container.memory.mb</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(cont_mem) + "</value>\n"))
                elif re.search("<name>yarn.sls.container.vcores</name>", line_above):
                    new_file.write(line.replace(line, "    <value>" + str(cont_vcores) + "</value>\n"))
                else:
                    new_file.write(line)
                line_above = line
    # Remove original file
    remove(sls_runner_path)
    # Move and rename new file
    move(abs_path, sls_runner_path)


def load_values():
    """
    Loads the attribute's values from the given [attribute_values_location]
    """
    global ysls_runner_pool_size
    global ysls_nm_memory_mb
    global ysls_nm_vcores
    global ysls_nm_heartbeat_interval_ms
    global ysls_am_heartbeat_interval_ms
    global ysls_container_memory_mb
    global ysls_container_vcores
    print("INFO: File to load values from is <" + str(sys_attribute_values_loc) + ">")
    my_values_file = Path(sys_attribute_values_loc)
    if my_values_file.is_file():
        with open(my_values_file) as fl:
            for line in fl:
                if re.search("ysls_runner_pool_size", line):
                    temp_ysls_runner_pool_size = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_nm_memory_mb", line):
                    temp_ysls_nm_memory_mb = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_nm_vcores", line):
                    temp_ysls_nm_vcores = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_nm_heartbeat_interval_ms", line):
                    temp_ysls_nm_heartbeat_interval_ms = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_am_heartbeat_interval_ms", line):
                    temp_ysls_am_heartbeat_interval_ms = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_container_memory_mb", line):
                    temp_ysls_container_memory_mb = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
                if re.search("ysls_container_vcores", line):
                    temp_ysls_container_vcores = (re.sub("[^0-9,.]", "", (re.sub(r'.*=', '', line))))
        ysls_runner_pool_size = temp_ysls_runner_pool_size.split(',')
        ysls_nm_memory_mb = temp_ysls_nm_memory_mb.split(',')
        ysls_nm_vcores = temp_ysls_nm_vcores.split(',')
        ysls_nm_heartbeat_interval_ms = temp_ysls_nm_heartbeat_interval_ms.split(',')
        ysls_am_heartbeat_interval_ms = temp_ysls_am_heartbeat_interval_ms.split(',')
        ysls_container_memory_mb = temp_ysls_container_memory_mb.split(',')
        ysls_container_vcores = temp_ysls_container_vcores.split(',')
    else:
        print("ERROR: There was a problem with the given values file")
        exit()
    print("INFO: Experiments will run for all the following values:")
    print("INFO: ysls_runner_pool_size:" + str(ysls_runner_pool_size))
    print("INFO: ysls_nm_memory_mb:" + str(ysls_nm_memory_mb))
    print("INFO: ysls_nm_vcores:" + str(ysls_nm_vcores))
    print("INFO: ysls_nm_heartbeat_interval_ms:" + str(ysls_nm_heartbeat_interval_ms))
    print("INFO: ysls_am_heartbeat_interval_ms:" + str(ysls_am_heartbeat_interval_ms))
    print("INFO: ysls_container_memory_mb:" + str(ysls_container_memory_mb))
    print("INFO: ysls_container_vcores:" + str(ysls_container_vcores))


def create_working_dir():
    """
    Checks if direcotry exists. If not it creates it.
    """
    if not os.path.exists(sys_output_directory):
        print("INFO: Directory does not exist. Do you want to create it? (Y/n)")
        dircondition = input("Enter your choise$ ")
        if dircondition == "Y" or dircondition == "y":
            try:
                os.makedirs(sys_output_directory)
                os.chdir(sys_output_directory)
                print("INFO: Directory created and changed working directory to " + os.getcwd())
            except OSError:
                print("INFO: ERROR: Directory could not be created. Exiting.")
                exit()
    else:
        if not os.listdir(sys_output_directory):
            try:
                os.chdir(sys_output_directory)
                print("INFO: Changed directory to " + sys_output_directory)
            except OSError:
                print("ERROR: Something went wrong changing the directory.")
        else:
            if sys_recover != "--recover":
                print("INFO: Given directory is not empty. Select an empty directory and try again.")
                exit()


def load_defaults():
    """
    This function copies the sample-configuration files at $(HADOOP_HOME)/etc/hadoop/ taken from $(HADOOP_HOME)/share/hadoop/tools/sls/sample-conf
    """
    cp_command = "yes | cp -rf " + sys.argv[3] + "share/hadoop/tools/sls/sample-conf/* " + sys.argv[3] + "etc/hadoop/"
    exp_process = subprocess.Popen(cp_command, shell=True, stdout=subprocess.PIPE)
    exp_process.wait()
    print("INFO: Experiment process return code is: " + str(exp_process.returncode))
    if exp_process.returncode == 0:
        print("INFO: Default configuration files loaded successfully")
    else:
        print("ERROR: There was a problem loading the configuration files. Make sure they exist at $(HADOOP_HOME)/share/hadoop/tools/sls/sample-conf/")


def dfs_manipulation():
    """
    This function checks what user want to do on DFS
    """
    if sys.argv[2] != "--hadoop-home":
        instructions()
    if sys.argv[4] == "--just-check":
        check_dfs()
    elif sys.argv[4] == "--restart":
        restart_dfs()
        check_dfs()
    else:
        instructions()


def check_dfs():
    """
    This function checks if the dfs is running
    """
    process = subprocess.Popen("jps", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    results = str(stdout)
    flag = 0
    if results.find("NameNode") > -1:
        flag += 1
    if results.find("SecondaryNameNode") > -1:
        flag += 1
    if results.find("DataNode") > -1:
        flag += 1
    if flag == 3:
        print("INFO: dfs is up and running!")
    else:
        print("WARN: dfs is not running correctly or not running at all. Run <jps> for more information")


def restart_dfs():
    """
    This function restarts or starts the dfs
    """
    command = "sudo -u " + getpass.getuser() + " " + sys.argv[3] + "sbin/stop-dfs.sh"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    command = "yes | hdfs namenode -format"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    command = sys.argv[3] + "sbin/start-dfs.sh"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    print("INFO: dfs has been restarted!")


def check_for_packets(invoke):
    """
    This function checks if the necessary packets are available in the system. If not it prompts for installations, otherwise it exits the script.
    """
    process = subprocess.Popen("apt-cache show python3-matplotlib", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    results = str(stdout).split("\\n")
    if len(results) < 4:   # if the length of the results list is less than four, it means the there is no info for the packet, so the packet is not installed.
        print("INFO: The necessary packets are not installed.")
        if invoke == "justcheck":
            exit()
        user_choise = input("Do you want to install them now? (Y | N)")
        if user_choise == 'y' or user_choise == 'Y':
            process = subprocess.Popen("apt-get install python3-pip python3-matplotlib", shell=True, stdout=subprocess.PIPE)
            process.wait()
            process = subprocess.Popen("python3 -m pip install metplotlib", shell=True, stdout=subprocess.PIPE)
            process.wait()
        if user_choise == 'N' or user_choise == 'n':
            print("INFO: This python3 script cannot run without the necessary packets. Please install python3-matplotlib and continue.")
    else:
        print("INFO: All the necessary packets are installed!")


def create_graphs(results_dir):
    """
    This function creates plots out of Yarn Scheduler Load Simulator output file <realtimetrack.json>
    Parameters of the $(HADOOP_HOME)/share/hadoop/tools/sls/bin/slsrun.sh output file, are the following:
    0 time
    1 jvm_free_memory
    2 jvm_max_memory
    3 jvm_total_memory
    4 running_applications
    5 running_containers
    6 cluster_allocated_memory
    7 cluster_allocated_vcores
    8 cluster_available_memory
    9 cluster_available_vcores
    10 queue_root_sls_queue_1_allocated_memory
    11 queue_root_sls_queue_1_allocated_vcores
    12 scheduler_allocate_timecost
    13 scheduler_commit_success_timecost
    14 scheduler_commit_failure_timecost
    15 scheduler_commit_success_throughput
    16 scheduler_commit_failure_throughput
    17 scheduler_handle_timecost
    18 scheduler_handle_NODE_ADDED_timecost
    19 scheduler_handle_NODE_REMOVED_timecost
    20 scheduler_handle_NODE_UPDATE_timecost
    21 scheduler_handle_NODE_RESOURCE_UPDATE_timecost
    22 scheduler_handle_NODE_LABELS_UPDATE_timecost
    23 scheduler_handle_APP_ADDED_timecost
    24 scheduler_handle_APP_REMOVED_timecost
    25 scheduler_handle_APP_ATTEMPT_ADDED_timecost
    26 scheduler_handle_APP_ATTEMPT_REMOVED_timecost
    27 scheduler_handle_CONTAINER_EXPIRED_timecost
    28 scheduler_handle_RELEASE_CONTAINER_timecost
    29 scheduler_handle_KILL_RESERVED_CONTAINER_timecost
    30 scheduler_handle_MARK_CONTAINER_FOR_PREEMPTION_timecost
    31 scheduler_handle_MARK_CONTAINER_FOR_KILLABLE_timecost
    32 scheduler_handle_MARK_CONTAINER_FOR_NONKILLABLE_timecost
    33 scheduler_handle_MANAGE_QUEUE_timecost
    The above metrics that are SLS's output to create plots for data analysis purposes, are stored in <realtimetrack_valeus> as follows:
    realtimetrack_values[:,0] = time
    realtimetrack_values[:,1] = jvm_free_memory
    realtimetrack_values[:,2] = jvm_max_memory
    etc....
    """
    check_for_packets("justcheck")
    import matplotlib.pyplot as plt
    realtimetrack_values = []   # initialization of list
    with open(results_dir + "realtimetrack.json") as fl:
        i = 0
        for line in fl:
            # Info: temp_values = a list with all the values and their names, of experiment's result line X. We are getting rid of first character, because in many lines it is a comma, and messes with splt() function that is based on <,>
            temp_values = line[1:].split(',')
            realtimetrack_values.append([])  # initialization of a list as the first element of the original list (2d array a.k.a. Matrix)
            for my_string in temp_values:
                # Info: (re.sub("[^0-9.]", "", my_string)) This keeps only <.> and <0-9> in the string my_string
                # Info: (re.sub(r'.*:', '', my_string))    This removes everything up to the <:> in my_string
                realtimetrack_values[i].append(re.sub("[^0-9.]", "", (re.sub(r'.*:', '', my_string))))
            i += 1
        temp_apps = []
        temp_cont = []
        jvm_free_memory = []
        jvm_max_memory = []
        jvm_total_memory = []
        cluster_allocated_memory = []
        cluster_available_memory = []
        cluster_allocated_vcores = []
        cluster_available_vcores = []
        scheduler_allocate_timecost = []
        scheduler_handle_NODE_ADDED_timecost = []
        scheduler_handle_NODE_REMOVED_timecost = []
        scheduler_handle_NODE_UPDATE_timecost = []
        scheduler_handle_APP_ADDED_timecost = []
        scheduler_handle_CONTAINER_EXPIRED_timecost = []
        for i in range(0, len(realtimetrack_values)):
            # Info: [i][4] and [i][5] are for the first graph
            temp_apps.append(float(realtimetrack_values[i][4]))  # running applications
            temp_cont.append(float(realtimetrack_values[i][5]))  # running containers
            # Info: the same for second graph
            jvm_free_memory.append(float(realtimetrack_values[i][1]))  # jvm_free_memory
            jvm_max_memory.append(float(realtimetrack_values[i][2]))  # jvm_max_memory
            jvm_total_memory.append(float(realtimetrack_values[i][3]))  # jvm_total_memory
            # Info: the same for third graph
            cluster_allocated_memory.append(float(realtimetrack_values[i][6]))  # cluster_allocated_memory
            cluster_available_memory.append(float(realtimetrack_values[i][8]))  # cluster_available_memory
            # Info: the same for fourth graph
            cluster_allocated_vcores.append(float(realtimetrack_values[i][7]))  # cluster_allocated_vcores
            cluster_available_vcores.append(float(realtimetrack_values[i][9]))  # cluster_available_vcores
            # Info: the same for fifth graph
            scheduler_allocate_timecost.append(float(realtimetrack_values[i][12]))  # scheduler_allocate_timecost
            scheduler_handle_NODE_ADDED_timecost.append(float(realtimetrack_values[i][18]))  # scheduler_handle_NODE_ADDED_timecost
            scheduler_handle_NODE_REMOVED_timecost.append(float(realtimetrack_values[i][19]))  # scheduler_handle_NODE_REMOVED_timecost
            scheduler_handle_NODE_UPDATE_timecost.append(float(realtimetrack_values[i][20]))  # scheduler_handle_NODE_UPDATE_timecost
            scheduler_handle_APP_ADDED_timecost.append(float(realtimetrack_values[i][23]))  # scheduler_handle_APP_ADDED_timecost
            scheduler_handle_CONTAINER_EXPIRED_timecost.append(float(realtimetrack_values[i][27]))  # scheduler_handle_CONTAINER_EXPIRED_timecost

        # Creation and save, of graph Cluster running applications & containers
        plt.figure()
        plt.title('Cluster running applications & containers')
        plt.xlabel('Time (s)')
        plt.ylabel('Number')
        plt.plot(temp_apps, color='blue', label='temp_apps')
        plt.plot(temp_cont, color='red', label='temp_cont')
        fontP = FontProperties()
        fontP.set_size('xx-small')
        legend = plt.legend(loc='upper center', shadow=True, prop=fontP)
        legend.get_frame().set_facecolor('C0')
        plt.savefig('ClusterRunningApplicationsAndContainers.png')
        # Creation and save, of graph JVM memory
        plt.figure()
        plt.title('JVM memory')
        plt.xlabel('Time (s)')
        plt.ylabel('Memory (GB)')
        plt.plot(jvm_free_memory, color='blue', label='jvm_free_memory')
        plt.plot(jvm_max_memory, color='red', label='jvm_max_memory')
        plt.plot(jvm_total_memory, color='cyan', label='jvm_total_memory')
        fontP = FontProperties()
        fontP.set_size('xx-small')
        legend = plt.legend(loc='upper center', shadow=True, prop=fontP)
        legend.get_frame().set_facecolor('C0')
        plt.savefig("JVMmemory.png")
        # Creation and save, of graph Cluster allocated & available memory
        plt.figure()
        plt.title('Cluster allocated & available memory')
        plt.xlabel('Time (s)')
        plt.ylabel('Memory (GB)')
        plt.plot(cluster_allocated_memory, color='blue', label='cluster_allocated_memory')
        plt.plot(cluster_available_memory, color='red', label='cluster_available_memory')
        fontP = FontProperties()
        fontP.set_size('xx-small')
        legend = plt.legend(loc='upper center', shadow=True, prop=fontP)
        legend.get_frame().set_facecolor('C0')
        plt.savefig("ClusterAllocatedAndAvailableMemory.png")
        # Creation and save, of graph Cluster allocated & available vcores
        plt.figure()
        plt.title('Cluster allocated & available vcores')
        plt.xlabel('Time (s)')
        plt.ylabel('Number')
        plt.plot(cluster_allocated_vcores, color='blue', label='cluster_allocated_vcores')
        plt.plot(cluster_available_vcores, color='red', label='cluster_available_vcores')
        fontP = FontProperties()
        fontP.set_size('xx-small')
        legend = plt.legend(loc='upper center', shadow=True, prop=fontP)
        legend.get_frame().set_facecolor('C0')
        plt.savefig("ClusterAllocatedAndAvailableVcores.png")
        # Creation and save, of graph Scheduler allocate & handle operations timecost
        plt.figure()
        plt.title('Scheduler allocate & handle operations timecost')
        plt.xlabel('Time (s)')
        plt.ylabel('Timecost (ms)')
        plt.plot(scheduler_allocate_timecost, color='blue', label='scheduler_allocate_timecost')
        plt.plot(scheduler_handle_NODE_ADDED_timecost, color='red', label='scheduler_handle_NODE_ADDED_timecost')
        plt.plot(scheduler_handle_NODE_REMOVED_timecost, color='cyan', label='scheduler_handle_NODE_REMOVED_timecost')
        plt.plot(scheduler_handle_NODE_UPDATE_timecost, color='magenta', label='scheduler_handle_NODE_UPDATE_timecost')
        plt.plot(scheduler_handle_APP_ADDED_timecost, color='yellow', label='scheduler_handle_APP_ADDED_timecost')
        plt.plot(scheduler_handle_CONTAINER_EXPIRED_timecost, color='green', label='scheduler_handle_CONTAINER_EXPIRED_timecost')
        fontP = FontProperties()
        fontP.set_size('xx-small')
        legend = plt.legend(loc='upper center', shadow=True, prop=fontP)
        legend.get_frame().set_facecolor('C0')
        plt.savefig("SchedulerAllocateAndHandleOperationsTimecost.png")

        print("INFO: All the graphs created successfully!")


def take_input_args():
    """
    This function stores sys.argv arguments into global variables
    """
    global sys_attribute_values_loc
    global sys_output_directory
    global sys_hadoop_home
    global sys_trace_location
    global sys_experiment_timeout
    global sys_live_track_file
    if not os.path.isfile(os.getcwd() + "/runtime_parameters.dat"):
        print("ERROR: Can't find file runtime_parameters.dat")
        exit()
    with open(os.getcwd() + "/runtime_parameters.dat") as fl:
        for line in fl:
            if re.search("sys_attribute_values_loc", line):
                sys_attribute_values_loc = (re.sub(r'.*=', '', line)).rstrip()
            elif re.search("sys_output_directory", line):
                sys_output_directory = (re.sub(r'.*=', '', line)).rstrip()
            elif re.search("sys_hadoop_home", line):
                sys_hadoop_home = (re.sub(r'.*=', '', line)).rstrip()
            elif re.search("sys_trace_location", line):
                sys_trace_location = (re.sub(r'.*=', '', line)).rstrip()
            elif re.search("sys_experiment_timeout", line):
                sys_experiment_timeout = (re.sub(r'.*=', '', line)).rstrip()
            elif re.search("sys_live_track_file", line):
                sys_live_track_file = (re.sub(r'.*=', '', line)).rstrip()
    print("INFO: Parameters loaded successfully from ./runtime_parameters.dat")
    print("INFO: sys_attribute_values_loc<" + sys_attribute_values_loc + ">")
    print("INFO: sys_output_directory<" + sys_output_directory + ">")
    print("INFO: sys_hadoop_home<" + sys_hadoop_home + ">")
    print("INFO: sys_trace_location<" + sys_trace_location + ">")
    print("INFO: sys_experiment_timeout<" + sys_experiment_timeout + ">")


def check_argv_integrity():
    """
    Check if the user arguments are correct
    """
    global sys_recover
    flag = 0
    if sys.argv[1] == "--run-server":
        if len(sys.argv) in [4, 5] and sys.argv[2] == "--bind-at":
            flag = 1
            if len(sys.argv) == 5:
                print("HI %s" % str(len(sys.argv)))
                if sys.argv[4] == "--recover":
                    sys_recover = "--recover"
                else:
                    instructions()
                    exit()
    elif sys.argv[1] == "--run-standalone":
        flag = 1
    elif sys.argv[1] == "--help":
        flag = 1
    elif sys.argv[1] == "--load-defaults":
        if len(sys.argv) == 4 and sys.argv[2] == "--hadoop-home":
            flag = 1
    elif sys.argv[1] == "--check-dfs":
        if len(sys.argv) == 5 and sys.argv[2] == "--hadoop-home":
            flag = 1
    elif sys.argv[1] == "--check-packets":
        flag = 1
    if flag == 0:
        instructions()
        exit()


def init_progress_file():
    try:
        os.remove(sys_live_track_file)
    except OSError as oser:
        print("ERROR: There was an error deleting the old track file:<" + str(oser) + ">")
    my_file = open(sys_live_track_file, "w+")
    my_file.write("#Lines are like csv with values (withoutnames)\n")
    my_file.write("#format\n")
    my_file.write("#Before @ is the number of the experiment\n")
    my_file.write("#After @ till the : are the experiment parameters\n")
    my_file.write("#After : is the progress of the experiment\n")
    my_file.write("#rps200,nmMem10240,nmVcores9,nmHeart1000,amHeart1000,contMem2048,contVcores2\n")
    my_file.write("#    Which translates to:\n")
    my_file.write("#    rps2       -> yarn.sls.runner.pool.size=2\n")
    my_file.write("#    nmMem1024  -> yarn.sls.nm.memory.mb=1024\n")
    my_file.write("#    nmVcores2  -> yarn.sls.nm.vcores=2\n")
    my_file.write("#    nmHeart50  -> yarn.sls.nm.heartbeat.interval.ms=50\n")
    my_file.write("#    amHeart50  -> yarn.sls.am.heartbeat.interval.ms=50\n")
    my_file.write("#    contMem256 -> yarn.sls.container.memory.mb=256\n")
    my_file.write("#    contVcores1-> yarn.sls.container.vcores=1\n")


def instructions():
    """
    This function provides instructions for the usage of the script
    """
    print(" server.py comes under the MIT License.")
    print(" Created by Emmanuel Sarris for University of Patras.")
    print(" Please populate <runtime_parameters.dat> and <values.exp> files, accordingly")
    print(" Usage:  server.py --help                             // displays help")          #
    print("         server.py [run_type] \\                       // command to run excperiment")         # 0     1
    print("             {--bind-at [IPv4:port]}                             // Specify port for run-server option")                                # 8     9
    print("             {--recover}                             // This option recovers the execution of the server, after unexcpected shutdown")                                # 8     9
    print("         server.py --load-defaults \\                  // loads the default configuration files in $(HADOOP_HOME)/etc/hadoop/")
    print("             --hadoop-home [hadoop_home]                         // that exist in $(HADOOP_HOME/share/hadoop/tools/sls/sample-conf/")
    print("         server.py --check-dfs \\                      // checks if dfs(Namenode, SecondaryNamenode, Datanode) is started")
    print("             --hadoop-home [hadoop_home] \\")
    print("             [check_type]")
    print("         server.py --check-packets                    // checks if the necessary python3-packets exist and prompts for installation if not.")
    print("------")
    print(" [run_type] can be")
    print("     --run-server       the original values will be loaded in sls-runner.xml")
    print("     --run-standalone       the original values will be loaded in sls-runner.xml")
    print(" [IPv4:port]              specifies which Internet location the server will run at")
    print("------")
    print(" [check_type] can be")
    print("     --just-check            simply checks if dfs JPS are running")
    print("     --restart               restart all dfs JPS processes")
    print(" [hadoop-home]               $HADOOP_HOME")
    print("------")


main()
