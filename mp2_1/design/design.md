# System Design Document

## Build

    docker build --platform linux/arm64 -t csce438_env .

    docker run -it --name csce438_mp2_1_container -v $(pwd)/mp2_1:/home/csce438/mp2_1 liuyidockers/csce438_env:latest

    ./setup-438-env.sh

    cd mp2_1

## Run

Compile the code using the provided makefile:

    make -j4

To clear the directory (and remove .txt files):

    make clean

To run the coordinator without glog messages:

    ./coordinator -p <portNum>

To run the coordinator with glog messages:

    GLOG_logtostderr=1 ./coordinator -p 9090

To run the server without glog messages (port number is optional):

    ./tsd -c <clusterId> -s <serverId> -h <coordinatorIP> -k <coordinatorPort> -p <portNum>

To run the server with glog messages:

    GLOG_logtostderr=1 ./tsd -c <clusterId> -s <serverId> -h <coordinatorIP> -k <coordinatorPort> -p <portNum>

To run the client, you need to open another terminal window, and enter into the launched docker container:

    docker exec -it csce438_container bash
    cd mp2_1
    ./tsc -h <coordinatorIP> -k <coordinatorPort> -u <userId>

To run the server with glog messages:

    GLOG_logtostderr=1 ./tsc -h <coordinatorIP> -k <coordinatorPort> -u <userId>

## Components

### 1. Coordinator

The system uses heartbeat messages to ensure that the servers are alive and functioning correctly. The `GetServer` function is used by the client to retrieve the server information before establishing a connection.

#### Heartbeat Implementation

`Heartbeat`: Receives heartbeat messages from servers. It updates the last heartbeat time and missed heartbeat status for the server. If the server is new, it adds the server to the appropriate cluster based on the metadata received.

On the server side, a new thread is created to send heartbeat messages periodically. The first heartbeat message sent by the server includes the necessary context to register the server with the coordinator.

#### GetServer Implementation

`GetServer`: Retrieves the correct server information based on a formula: ((clientId - 1) % 3) + 1
Once the correctId is generated, we send the ServerInfo to the client from our array of zNodes

On the client side the `GetServer` function is crucial to establish a connection with the correct server. Before connecting to the server, the client calls the `GetServer` function to obtain the server information, ensuring a successful connection to the appropriate server in the cluster.

## Test Cases

### 1.

![UI Step 1](images/t1u1.png)

### 2.

![UI Step 1](images/t2u1.png)

### 3.

![UI Step 1](images/t3u1.png)
![UI Step 2](images/t3u2.png)
