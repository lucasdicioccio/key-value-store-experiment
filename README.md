
This project is an experiment to build a proof-of-concept distributed key-value
store using distributed-Haskell.

My goal here is to learn about distributed-haskell. 

This project is not an attempt to built a key-value store that you can use in
your product. In particular, here I care little about performance and
not at all about consistency.
Feel free to take some ideas but I wouldn't dare using it myself at this stage.


There are two files: 
	- app.hs is  the Haskell code
	- load.rb is a fake load generator

To compile use GHC (tested with 7.4.2):

	ghc --make ./app.hs
	
To run use:
	./app <port> <first-key> <last-key>
	<port>: a port number
	[<first-key>,<last-key>): the keyspace that this node will claim (keys are Integer values).

Then you can write commands to it:
	- cache will dump the current state
	- keys  will dump the list of keys
	- get <key> will get a key (Int) and print Just its value of Nothing
	- set <key> <value> will set a key (Int) to a value (String) will print the pid of the process holding the key/value pair (may be local if no-one is interested in that key)

To run with synthetic load, use:

	#in a terminal
	ruby load.rb 1000 500000 | ./app 9990 0 500
	#in another terminal
	ruby load.rb 1000 500000 | ./app 9991 500 1000

Each of this two commands will start a local cache on port 9990 (resp. 9991).
The cache will be responsible for keys from 0 to 499 (resp. 500 to 999)
A Ruby process will generate a synthetic load of 50000 events (50% get/set) 
for keys between 0 and 999.
