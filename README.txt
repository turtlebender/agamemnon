==============
Agamemnon
==============

Agamemnon is a thin library built on top of pycassa.  It allows you to use the Cassandra
database (<http://cassandra.apache.org>) as a graph database.  Much of the api was inspired
by the excellent neo4j.py project (<http://components.neo4j.org/neo4j.py/snapshot/>)

Documentation
==============
Soon, I promise.  For now, take a look at agamemnon.functional_tests   This is a fairly complete
example of basic behaviors.  The unit tests are very "unit test-y" and might not be a great way
to understand how to work with the library.  They mostly focus on making sure that the correct
information is being sent to Cassandra.  But the functional tests are designed to actually interact
with cassandra.  Note, the functional tests require a instance of Cassandra running at localhost:9160

Thanks To
=============
This project is an extension of the globusonline.org project and is being used to power the upcoming 
version of globusonline.org.  I'd like to thank Ian Foster and Steve Tuecke for leading that project,
and all of the members of the cloud services team for participating in this effort, especially:
 Vijay Anand, Kyle Chard, Martin Feller and Mike Russell for helping with design and testing.  I'd
also like to thank Bryce Allen for his help with some of the python learning curve.
