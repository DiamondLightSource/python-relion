.. role:: python(code)
  :language: python
  :class: highlight

================
Nodes and graphs
================

``relion.protonode`` provides some basic functionality for setting up a data collection workflow. The base object 
is ``relion.protonode.protonode.ProtoNode`` which has an associated environment, along with various hidden attributes. The 
latter include lists that hold any incoming and outgoing nodes and a record of which incoming nodes have been called etc. 

Nodes can be linked together with a specification of data to be transferred between the nodes in a few different ways. These 
data are transferred between the node environments. The data may be specified in one of the following ways:

* a dictionary (or list of dictionaries) with fixed values: :python:`node01.link_to(node_02, traffic={"data01": 1, "data02": 2})`
* a tuple of keys ``(key01, key02)`` that specifies the transfer ``node02.environment[key01] = node01.environment[key02]``: :python:`node01.link_to(node_02, share=(key01, key02))`
* the result of the node call ``node()`` will be passed into the environment of the node being linked to: :python:`node01.link_to(node_02, result_as_traffic=True)`
* with the same format as the ``share`` option but propagating to all connected nodes and to all nodes they are connected to and so on: ``node01.propagate((key01, key02))``

The ``environment`` behaves similarly to a dictionary and is intended to store any data needed by the node, either for 
processing or to pass onwards to other nodes. 

-----------
Environment
-----------

The environment itself contains a series of dictionaries which are searched in a specific order for the key provided. 
If a key is not found ``None`` is returned, rather than raising a ``KeyError``. The dictionary hierarchy in 
``environment`` is:

* ``base``: searched first, updated with :python:`environment[key] = value`
* ``propagate.store``: accessed directly through :python:`environment.propagate[key]`, only accessed by :python:`environment[key]` if not empty
* ``escalate.store``: similar to ``propagate`` but rather than a dictionary is another node's ``environment`` allowing for a recursive search

------
Graphs
------

Nodes may be collected into a graph for better organisation and pipelining. The ``relion.protonode.protograph.ProtoGraph`` 
class provides this basic functionality. It inherits from ``ProtoNode`` and as such may be connected to other nodes, or 
other graphs, allowing similar structures to any level of depth. A list of nodes must be provided on initialisation of the 
graph. Nodes may then be added subsequently with the ``add_node`` method.

When a graph is called all nodes within the graph will be called in order with any specified traffic being passed between them. By 
in order it is here meant that a node will only be called once all its predecessors have been called.