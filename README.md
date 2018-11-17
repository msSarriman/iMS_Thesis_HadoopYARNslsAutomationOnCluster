# Apache Hadoop YARN SLS, Server-Client model

The purposes of this Thesis project are the following:

  - Distributed cluster exectuion of YARN SLS
  - Centralized processing of the simulation results
  - Decentralized inspection of experiment's progress.

#### Architecture Features

  - Server assigns experiment using a builtin Queue, created form `values.exp` file values.
  - Echo heartbeat communication through UDP, for client inspection from Server
  -- Server reissues an experiment if a client is not echo-ing for a period of time.
  - Server keeps a file log for experiment queue recovery, in case of fatal shutdown.
  - Server creates plots, for decision making on the experiment results.
  - TCP communications for message and file transaction between server-client.


You can also:
  - Use built-in command line arguments on `./server.py` to troubleshoot Hadoop installation and execution. See `./server.py --help` for a detailed list.

### Tech

Frameworks and tools:

*  [Apache Hadoop 3.1.1]
*  [Scheduler Load Simulator]
*  [Netbeans 8.2]
*  [Sublime Text 3]
*  [Oracle VM VirtualBox 5.2.18]

Programming:

* [Python 3.6.6]
* [Java 1.8.0_181-b13]
* [Java Swing]

OS and Sandboxes:
* Host: MS Windows 10 Pro
* Guest: Ubuntu xfce 18.04.1 LTS, Bionic Beaver (One Server/Three Slaves)


### Installation

Refer to [Apache's Installation Guide](https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-common/SingleCluster.html) to install and setup Apache Hadoop.
Refer to [matplotlib.org](https://matplotlib.org/faq/installing_faq.html) to install `matplotlib` for Python3.

Start up the HDFS:

```sh
$ cd $(HADOOP_HOME)
$ ./sbin/start-dfs.sh
$ jps
```

Populate the neccessary files (see `Extra Files` section), and run the Server with `./server.py` python file (run `./server.py --help` for execution details).
On Client side execute the `./client.py` (run `./client.py --help` for execution details).

If the JavaGUI (experiments process overview) needs to open standalone, run it with the command: `prompt# java -jar ProcessTracking.jar [ServerIPv4] [port]` (default port is 30001).

For production environments (Apache Hadoop compiled from source), find the `sbin` folder under `hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/sbin`

### UMLs

See UMLs for more information on the scripts.

### Extra Files

* `real_time_tracking.dat`
Log file for server recovery.
* `runtime_parameters.dat`
Necessary parameters for server and client execution. 
* `values.exp`
The values for which the SLS will run.

License
----

MIT



   [Apache Hadoop 3.1.1]: <https://hadoop.apache.org/docs/r3.1.1/>
   [Scheduler Load Simulator]: <https://hadoop.apache.org/docs/r3.1.1/hadoop-sls/SchedulerLoadSimulator.html>
   [Python3]: <https://docs.python.org/3/>
   [Netbeans 8.2]: <https://netbeans.org/>
   [Oracle VM VirtualBox 5.2.18]: <https://www.virtualbox.org/>
   [Sublime Text 3]: <https://www.sublimetext.com/3>
   [Java 1.8.0_181-b13]: <https://www.java.com/en/download/>
   [Java Swing]: <https://en.wikipedia.org/wiki/Swing_(Java)>
