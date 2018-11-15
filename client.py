#!/usr/bin/python3
# Copyright 2018 EMMANUEL I. SARRIS
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import sys
if len(sys.argv) == 1:
    print("For usage info execute $./client.py --help")
    exit()

import os
import re
import time
import socket
import getpass
import datetime
import subprocess
import numpy as np
from io import StringIO
from os import fdopen, remove
from shutil import move
from pathlib import Path
from tempfile import mkstemp


"""
sys_x
The variables bellow are lists that store all the necessary variables for scripts execution, stored in file <runtime_parameters.dat>
"""
sys_attribute_values_loc = "undef:"
sys_output_directory = "undef:"
sys_hadoop_home = "undef:"
sys_trace_location = "undef:"
sys_experiment_timeout = "no_time_cap"

"""
Global variables needed for error checking and simplicity
"""
global values
curwordir = "undef:"
experiment_folder = "undef:"  # absolute path of directory to compress
experiment_folder_name = "undef:"  # name of directory to compress
my_socket = "undef:"
interval = 7
my_child = "undef:"


def main():
    check_argv_integrity()
    address_port = sys.argv[2].split(":")
    host = address_port[0]
    port = int(address_port[1])
    print("INFO: IP is: " + str(host))
    print("INFO: port is: " + str(port))
    # connection to hostname on the port.
    take_input_args()
    interval_connect(host, port)


def interval_connect(host, port):
    """
    This function tries to connect repeatedly to the server
    """
    global my_socket
    global my_child
    global interval  # Determines the amount of time in seconds, to sleep. Time is needed for socket to get re__init__
    while True:
        establish_connection(host, port)
        try:
            msg = "GiveMeWork"
            my_socket.send(msg.encode('ascii'))
        except socket.error as serr:
            print("ERROR: Error sending GiveMeWork to server: <" + str(serr) + ">")
            time.sleep(interval)
            interval_connect(host, port)
        try:
            print("INFO: About to receive job from server")
            msg = my_socket.recv(4096)
        except socket.error as serr:
            print("ERROR: Error receiving work from server %s" % str(serr))
            time.sleep(interval)
            interval_connect(host, port)
        msg_rcv = (msg.decode('ascii'))
        print("INFO: Message received is: <" + str(msg_rcv) + ">")
        if str(msg_rcv[:24]) == "SLS_Experiment_On_Values":
            close_socket()
            dfs_manipulation()
            my_child = os.fork()
            if my_child == 0:  # child code
                send_progress(str(msg_rcv), host)
            start_experiment(str(msg_rcv))
            os.kill(my_child, 9)
            establish_connection(host, port)
            send_results()
            close_socket()
            send_progress(str(msg_rcv), host, 1)
        else:
            print("ERROR: Server didn't give me to do job!!")
        time.sleep(interval)


def send_progress(exp_values, host, final=0):
    """
    This function sends heartbeats to the server. If the server does not receive a
    heartbeat, assumes that the client stoped working.
    """
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = (host, 30000)
    if final == 0:
        start = time.time()
        while True:
            time_so_far = ((time.time() - start) * 100) // int(sys_experiment_timeout)  # percentage of completion
            message = exp_values[25:] + ":" + str(int(time_so_far))
            if time_so_far == 100:
                continue
            print("INFO: CHILD: About to send heartbeat<" + str(message[25:]) + ">")
            try:
                my_socket.sendto(message.encode('ascii'), server_address)
            except socket.error as serr:
                print("ERROR: CHILD: Sending heartbeat<" + str(serr) + ">")
            time.sleep(3)
    else:
        print("INFO: About to send completion status")
        message = exp_values[25:] + ":" + "100"
        try:
            my_socket.sendto(message.encode('ascii'), server_address)
        except socket.error as serr:
            print("ERROR: Sending completion status<" + str(serr) + ">")


def establish_connection(host, port):
    """
    This function establishes connection to a server
    """
    global my_socket
    global interval
    try:
        # create a socket object
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("INFO: Socket created successfully!")
    except socker.error as serr:
        print("ERROR: Cannot create the socket object:<" + str(serr) + ">")
        time.sleep(interval)
        interval_connect(host, port)
    try:
        # connect to server
        my_socket.connect((host, port))
        print("INFO: Connected to server " + str(host) + ":" + str(port) + " successfully!")
    except socket.error as serr:
        print("ERROR: Cannot connect to server " + str(host) + ":" + str(port))
        print("ERROR: Cannot connect to server socket error:<" + str(serr) + ">")
        time.sleep(interval)
        interval_connect(host, port)


def close_socket():
    """
    This function closes the socket
    """
    try:
        my_socket.close()
    except socket.error as serr:
        print("ERROR: While closing the socket:<" + str(serr) + ">")


def send_results():
    """
    This function upload to the server the experiment results, in .tar.gz format.
    It also calculates the md5sum of the .tar.gz and sends it to the server, in order
    for the server to validate the integrity of file  uploaded form here.
    """
    global my_socket
    global experiment_folder  # absolute path folder to compress
    global experiment_folder_name  # name of folder to compress
    global interval
    msg = "ResultsIncoming:"
    try:
        print("INFO: About to send ResultsIncoming msg")
        my_socket.send(msg.encode('ascii'))
        print("INFO: Sent ResultsIncoming msg")
    except socket.error as serr:
        print("ERROR: Error sending ResulsIncoming msg:<" + str(serr) + ">")
    try:
        print("INFO: About to receive Ack for transfer")
        msg = my_socket.recv(4)
        msg = msg.decode('ascii')
        print("INFO: Message received as Ack is:<" + str(msg) + ">")
        if re.search(r"\w*{Ack}\w*", msg):
            print("ERROR: Error while communicating with server")
            time.interval()
    except socket.error as serr:
        print("ERROR: Error receiving Ack from server:<" + str(serr) + ">")
    print("INFO: Current working directory is: <" + os.getcwd() + ">")
    my_cwd = str(os.getcwd())
    os.chdir(experiment_folder)
    cmd = "tar -czvf " + experiment_folder + ".tar.gz *"
    exp_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    exp_process.wait()
    process = subprocess.Popen("md5sum " + experiment_folder + ".tar.gz",
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    os.chdir(my_cwd)
    results = str(stdout)
    print("INFO: md5sum of sent folder is:<" + results[:33] + ">")  # md5 is first 34 digits
    print("INFO: About to Send filename for transfer <" + "SendingFile:" + str(experiment_folder_name) + ">")
    try:
        send_msg = "SendingFile:" + str(experiment_folder_name) + ",WithMD5Hash:" + str(results[:33])
        my_socket.send(send_msg.encode('ascii'))
    except socket.error as serr:
        print("ERROR: Error sending <SendingFile:<file>> to server")
        print("ERROR: Error is <" + str(serr) + ">")
    try:
        print("INFO: About to receive StartTransfer for transfer")
        msg = my_socket.recv(14)
        msg = msg.decode('ascii')
        print("INFO: Message received for StartTransfer auth is:<" + str(msg) + ">")
        if re.search(r"\w*{StartTransfer}\w*", msg):
            print("ERROR: Error while StartTransfer-ing with server")
            time.interval()
    except socket.error as serr:
        print("ERROR: Error receiving StartTransfer from server:<" + str(serr) + ">")
    print("=========================")
    print("INFO: Starting file upload")
    print("INFO: Experiment folder absolute path:<" + str(experiment_folder) + ">")
    print("INFO: Experiment folder name:<" + str(experiment_folder_name) + ">")
    print("INFO: About to start file transfer")
    with open(experiment_folder + ".tar.gz", 'rb') as f:
        line_to_send = f.read(2048)
        while line_to_send:
            my_socket.send(line_to_send)
            line_to_send = f.read(2048)
    print("INFO: File transfer completed")
    print("=========================")


def start_experiment(msg):
    """
    This functino starts the SLS experiment
    """
    global values
    global curwordir
    global my_child
    values = re.sub("[^0-9.,]", "", (re.sub(r'.*:', '', msg)))
    values = values.split(',')
    print("INFO: Values object looks like:<" + str(values) + ">")
    create_working_dir()
    #print("DEBUG: curwordir at start_experiment is:<" + str(curwordir) + ">")
    command = sys_hadoop_home + "share/hadoop/tools/sls/bin/slsrun.sh " + \
        "--tracetype=RUMEN --tracelocation=" + sys_trace_location + " --output-dir=" + str(curwordir)
    #print("DEBUG: command:<" + str(command) + ">")
    change_attributes_at_sls_runner(values[0],
                                    values[1],
                                    values[2],
                                    values[3],
                                    values[4],
                                    values[5],
                                    values[6])
    kill_running_sls()
    run_sls(command)
    print("INFO: Experiment completed. Will proceed to file transfer")


def take_input_args():
    """
    This function stores sys.argv arguments into global variables
    """
    global sys_attribute_values_loc
    global sys_output_directory
    global sys_hadoop_home
    global sys_trace_location
    global sys_experiment_timeout
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
    print("INFO: Parameters loaded successfully from ./runtime_parameters.dat")
    print("INFO: sys_attribute_values_loc<" + sys_attribute_values_loc + ">")
    print("INFO: sys_output_directory<" + sys_output_directory + ">")
    print("INFO: sys_hadoop_home<" + sys_hadoop_home + ">")
    print("INFO: sys_trace_location<" + sys_trace_location + ">")
    print("INFO: sys_experiment_timeout<" + sys_experiment_timeout + ">")


def create_working_dir():
    """
    Checks if direcotry exists. If not it creates it.
    """
    global curwordir
    if not os.path.exists(sys_output_directory):
        print("INFO: Directory does not exist. Do you want to create it? (Y/n)")
        dircondition = input("Enter your choise$")
        if dircondition == "Y" or dircondition == "y":
            try:
                os.makedirs(sys_output_directory)
                os.chdir(sys_output_directory)
                print("INFO: Directory created and changed working directory to " + os.getcwd())
                curwordir = str(os.getcwd())
            except OSError:
                print("INFO: ERROR: Directory could not be created. Exiting.")
                exit()
    else:
        # if not os.listdir(sys_output_directory):
        try:
            os.chdir(sys_output_directory)
            curwordir = str(os.getcwd())
            print("INFO: Changed directory to " + sys_output_directory)
        except OSError:
            print("ERROR: Something went wrong changing the directory.")
        # else:
        #     print("ERROR: Given directory is not empty. Select an empty directory and try again.")
        #     exit()
    #print("DEBUG: create_working_dir curwordir is:<" + curwordir + ">")


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


def kill_running_sls():
    """
    This method terminates, just in case, any up and running SLSRunners
    """
    process = subprocess.Popen("kill `jps | grep \"SLSRunner\" | cut -d \" \" -f 1`", shell=True, stdout=subprocess.PIPE)
    process.wait()


def run_sls(command):
    """
    This function runs the Yarn Scheduler Load Simulator
    """
    global experiment_folder
    global experiment_folder_name
    folder_at_final_dir = "rps" + str(values[0]) + \
        "_nmMem" + str(values[1]) + \
        "_nmVcores" + str(values[2]) + \
        "_nmHeart" + str(values[3]) + \
        "_amHeart" + str(values[4]) + \
        "_contMem" + str(values[5]) + \
        "_contVcores" + str(values[6])
    final_directory = sys_output_directory + folder_at_final_dir
    experiment_folder = final_directory
    experiment_folder_name = folder_at_final_dir
    try:
        print("I will create " + final_directory)
        os.makedirs(final_directory)
        os.chdir(final_directory)
        print("Directory created successfully at, " + final_directory)
    except OSError:
        print("ERROR: You are currently at: " + os.getcwd())
        print("ERROR: You are trying to create the dir: " + final_directory)
        print("ERROR: There was an error creating that directory.")
        exp_process = subprocess.Popen("rm -rf " + str(final_directory), shell=True, stdout=subprocess.PIPE)
    command = command + "/" + folder_at_final_dir
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


def dfs_manipulation():
    """
    This function checks what user want to do on DFS
    """
    if check_dfs() == "error":
        print("INFO: Will attemp to restart dfs")
        restart_dfs()


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
        return "ok"
    else:
        print("WARN: dfs is not running correctly or not running at all. Run <jps> for more information")
        return "error"


def restart_dfs():
    """
    This function restart or starts the dfs
    """
    command = "sudo -u " + getpass.getuser() + " " + sys_hadoop_home + "sbin/stop-dfs.sh"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    command = "yes | hdfs namenode -format"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    command = sys_hadoop_home + "sbin/start-dfs.sh"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    print("dfs has been restarted!")


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


def check_argv_integrity():
    """
    Check if the user arguments are correct
    """
    global sys_hadoop_home
    flag = 0
    if sys.argv[1] == "--connect-at":
        if len(sys.argv) == 3:
            flag = 1
    elif sys.argv[1] == "--help":
        instructions()
        exit()
    elif sys.argv[1] == "--load-defaults":
        if len(sys.argv) == 4 and sys.argv[2] == "--hadoop-home":
            load_defaults()
            exit()
    elif sys.argv[1] == "--check-dfs":
        if len(sys.argv) == 5 and sys.argv[2] == "--hadoop-home":
            sys_hadoop_home = sys.argv[3]
            dfs_manipulation()
            exit()
    if flag == 0:
        instructions()
        exit()


def instructions():
    """
    This function provides instructions for the usage of the script
    """
    print(" client.py comes under the MIT License.")
    print(" Created by Emmanuel Sarris for University of Patras.")
    print(" Please populate <runtime_parameters.dat> and <values.exp> files, accordingly")
    print(" Usage:  client.py --help                             // displays help")
    print("         client.py --connect-at [IPv4:Port]           // connect at server")
    print("         client.py --load-defaults \\                  // loads the default configuration files in $(HADOOP_HOME)/etc/hadoop/")
    print("             --hadoop-home [hadoop_home]              // that exist in $(HADOOP_HOME/share/hadoop/tools/sls/sample-conf/")
    print("         client.py --check-dfs \\                      // checks if dfs(Namenode, SecondaryNamenode, Datanode) is started")
    print("             --hadoop-home [hadoop_home] \\")
    print("             [check_type]")
    print("------")
    print(" [IPv4:port]                 specifies which Internet location the client will connect to")
    print("------")
    print(" [check_type] can be")
    print("     --just-check            simply checks if dfs JPS are running")
    print("     --restart               restart all dfs JPS processes")
    print(" [hadoop-home]               $HADOOP_HOME")
    print("------")


main()
