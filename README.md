# ZKTools

**ZKTools** is a collection of utility code written around **Apache ZooKeeper**.

Currently it includes the following three components:

- ZooKeeperClient
- ClusterManager
- TaskManager

## ZooKeeperClient
**ZooKeeperClient** is a wrapper around ZooKeeper. 
The purpose of **ZooKeeperClient** is to abstract out common tasks in using ZooKeeper, namely, session management and watcher management.

- Session Management

    **ZooKeeperClient** automatically reconnects to ZooKeeper servers when a session is dead.
    So an application does not need to look after the session life-cycle.
    **ZooKeeperClient** is a proxy for basic ZooKeeper commands.
    A certain a sequence of operations may need to be executed in the same session.
    ZooKeeperClient provides **ZooKeeperSession**, a wrapper of the active session, for that purpose.

- Watcher Management

    **ZooKeeperClient** allows an application to set/unset watchers very easy.
    ZooKeeper's watcher is a trigger mechanism for various events happening in ZooKeeper.
    An event may be generated when the state of session changes, or data in ZooKeeper changes.
    It is a one-time trigger. This means that the watcher is gone once a trigger is fired. 
    In order to keep watching the same node, an application must set a watcher again. 
    ZooKeeperClient does that automatically. 
    An application can specify a watcher as a simple lambda expression which is invoked whenever the watched znode is changed. 

### Usage
First, you need to create an instance of `ZooKeeperClient`.
```
ZooKeeperClient zkClient = ZooKeeperClient.create(connectString, sessionTimeoust);
```

Using `ZooKeeperClient` you can manipulate znodes, such as creating, deleting, setting data, getting data, and watching.

There are a variety of *create* methods, creating a node with data or with `CreateMode`, etc. 
We are showing the simplest method here.

```
ZNode node = new ZNode("/test")
zkClient.create(node);
```

You can write data to the node and read it back. You must specify a serializer for your data. 
Data is opaque to ZooKeeperClient. 

```
Serializer<String> stringSerializer = new StringSerializer();

// Write data to the node
zkClient.setData(node, "hello", stringSerializer);
 
// Read data from the node. getData() returns a NodeData object which holds deserialized data and Stat structure of znode.
NodeData<String> nodeData = zkClient.getData(node, stringSerializer)
System.out.println(nodeData.value);
```

You can watch a node. In the following example, the data change handler prints the node data. 
It is invoked whenever the node is changed.

```
WatchHandle handle = zkClient.watch(
    node,
    nodeData -> {
        // Print node data whenever the node is updated.
        System.out.println("node data: " + nodeData.value)
    },
    stringSerializer
);
```

You can watch a node with its children. In the following example, the data change handler prints the node data and all child data.
It is invoked on node change, child creation/deletion and child data change, etc. 

```
WatchHandle handle = zkClient.watch(
    node,
    (nodeData, childDataMap) -> {
        // Print node data and child data whenever the node or its child is updated.
        System.out.println("node data: " + nodeData.value)
        for (Map.Entry<ZNode, NodeData> entry : childDataMap.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue().value)
        }
    },
    stringSerializer, // for parent node
    stringSerializer  // for child nodes
);
```

When a data change handler is invoked, ZooKeeperClient automatically sets a new watcher to the znode for next data change event. 
So, once the data change handler is set it is invoked automatically whenever the node is changes.
To stop this, you must close the `WatchHandle` returned from `watch()` method.

```
handle.close();
```

There are more features not described here, such as node manipulation methods, session event handlers, ACL, 
and explicit use of ZooKeeper session. 

## ClusterManager
**ClusterManager** keeps track of servers participating in a cluster and assigns partitions to them. 
Also, it notifies clients of the server locations and the current partition assignment. It does not include a RPC layer.

`ManagedServer` interface is an abstraction of a server node through which `ClusterManager` interacts with your server code. 
A server node joins the cluster by registering the instance of `ManagedServer` to `ClusterManager`.
Similary, `ManagedClient` interface is an abstract of a client through which `ClusterManager` interacts with you client code.

### Usage
First, you need to create a znode that represents a cluster using the `CreateCluster` tool.
This is a one time set up per the cluster. 

```
java com.wepay.zktools.clustermgr.tools.CreateCluster -z <zookeeperConnectString> -p <numberOfPartitions> -r <clusterRootPath>"
```

`<clusterRootPath>` is a path to the znode. This also sets up cluster metadata in ZooKeeper.

Your server code and client code must create an instance of `ClusterManager`.

```
ZNode clusterRoot = new ZNode("<clusterRootPath>");
PartitionAssignmentPolicy partitionAssignmentPolicy = new DynamicPartitionAssignmentPolicy();
ClusterManager clusterManager = ClusterManager.create(zooKeeperClient, clusterRoot, partitionAssignmentPolicy);
```

A server joins the cluster by registering its implementation of `ManagedServer`.

```
ManagedServer managedServer = new MyManagedServer(...);
clusterManager.manage(managedServer);
```

Once a server joins the cluster, the partition assignment is recomputed using the partition assignment policy,
and all servers in the cluster will be notified of the new assignment.

Similarly, a client joins the cluster by registering its implementation of `ManagedClient`.

```
ManagedClient managedClient = new MyManagedClient(...);
clusterManager.manage(managedClient);
```

The client is notified of the locations (hosts and ports) of all servers and the current partition assignment.

## TaskManager
**TaskManager** is a simple task assignment system that distributes tasks to participating nodes. 
A task here is a repeating work that we don't want to run on more than one node at the same time. 
TaskManager guarantees a task is assigned to only one node. 
If the node owning the task went down, the task manager automatically moves the ownership of the task to one of the surviving nodes.

Task must implement two methods, `execute(TaskContext taskContext)` and `nexStartTime()`.
`execute(TaskContext taskContext)` is the body of task logic. 
`nextStartTime()` must return the time for next execution.

### Usage
The usage of TaskManager is simple. For example,

Create an instance of `ZooKeeperClient`.

```
ZooKeeperClient zkClient = ZooKeeperClient.create(connectString, sessionTimeout);
```

Create an instance of `TaskManager`. This wraps an instance of `ScheduledExecutorService`.

```
ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
ZNode root = new ZNode("/my/tasks"); // This znode represents a cluster
TaskManager taskManager = TaskManager.create(zkClient, root, scheduledExecutorService);
```

Add your tasks to the task manger. A task is identified by the name given when adding it to the task manager

```
Task task1 = new MyTask(...);
Task task2 = new MyAnotherTask(...);
 
taskManager.add("myTask", task1, 1000);        // Register MyTask with the name "myTask"
taskManager.add("myAnotherTask", task2, 1000); // Register MyAnotherTask with the name "myAnotherTask"
```

Basically, that's all. Above three steps should be done in all nodes that participate in task execution. 
A long running task should check if the task is still assigned to the node every so often by calling `isEnabled()` method
if the `TaskContext` passed to the execute method. `isEnabled()` returns true if the task is still assigned 
to the executing node, otherwise it returns false.

```
class MyTask {
    ...
    public execute(TaskContext taskContext) {
        ...
        while (taskContext.isEnabled()) {
            // Do the work
            ...
        }
        ...
    }
    ...
}
```
