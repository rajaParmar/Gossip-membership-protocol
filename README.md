Gossip-membership-protocol

This project was about implementing a membership protocol using Gossip protocol under a distributed network setting.

This project was done towards completion of the MOOC cloud computing concepts 1 offered by UIUC on coursera.

It involved writing a Gossip membership protocol that satisfied

i) Completeness all the time: 
	a) Every non-faulty process must detect every node join, failure, and leave

ii) Accuracy of failure detection
	a) When there are no message losses and message delays are small. 
	b) When there are message losses, completeness must be satisfied and accuracy must be high. 
	c) It must achieve all of these even under simultaneous multiple failures.

The distributed network setting was already provided as part of the starter code.

The protocol was tested with various instances of test cases provided by the instructor and it succesfully passed all the test cases complying with the properties mentioned above.

The protocol was written entirely using C++.

The protocol implementation resides in the files MP1Node.cpp and respective signatures in MP1Node.h, feel free to read and run the protocol on the testcases provided.