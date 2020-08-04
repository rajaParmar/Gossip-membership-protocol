/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <string>
#include <random>
#include <set>
#include <ctime>
#include <cstdlib>
#include <unordered_map>


int m[4];
// int address_size = 1;
string addresses = "";

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
    this->heartbeat_count = 0;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
    //convert_list_to_string();
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    m[0] = 128*128*128;
    m[1] = 128*128;
    m[2] = 128;
    m[3] = 1;
    Address joinaddr;
    joinaddr = getJoinAddress();
    convert_list_to_string();
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        //address_size = 1;
        Address first = getJoinAddress();
        int i=0;
        while(i<=5){
            addresses+=first.addr[i++];
        }
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
int to_id(const char *string){
    int val = 0;
    for(int i=0;i<4;i++){
        val += (m[i] * ((int)string[i]));
    }
    return val;
}
int to_port(const char *str){
    return (int)str[4];
}

string from_id(int id){
    char temp_addr[4];
    int i=3;
    while(id > 0){
        temp_addr[i--] = (char)(id%128);
        id/=128;
    }
    string int_id(temp_addr,4);
    return int_id;
}

string from_port(int port){
    char temp_port = (char)(port);
    string temp(temp_port,1);
    return temp;
}

char * create_message(int type,const char *data,int data_size){
    size_t message_size = 0;
    queue<string> temp;
    message_size+=data_size;
    message_size+=sizeof(MessageHdr);
    MessageHdr *message = (MessageHdr *)malloc(sizeof(char)*message_size);
    if(type == 1){
        message->msgType = JOINREP;
        memcpy((char *)(message+1),addresses.c_str(),addresses.size());
    }
    if(type == 2){
        message->msgType = ACCEPTKAR;
        memcpy((char *)(message + 1),data,data_size);
    }
    if(type == 3){
        message->msgType = HB;
        memcpy((char *)(message + 1),data,data_size);
    }
		if(type == 4){
			message->msgType = GOSSIP;
			memcpy((char *)(message + 1),data,data_size);
		}
    return (char *)(message);
}

Address* get_address_from_string(const char *address){
    Address *to_send_address = (Address *)malloc(sizeof(Address));
    memset(to_send_address,0,sizeof(Address));
    int i=0;
    while(i<=4){
        to_send_address->addr[i] = address[i];
        i++;
    }
    return to_send_address;

}

string get_string_from_Address(Address *add){
    char *add_string = (char *)(malloc(sizeof(char) * 6));
    //string add_string;
    for(int i=0;i<=5;i++){
        add_string[i] = add->addr[i];
    }
    string ret(add_string,6);
    free(add_string);
    return ret;
}

void print_message(string s){
    for(auto i : s){
        cout <<(int)(i);
    }
}

void MP1Node::print_membership_list(){
    printAddress(&memberNode->addr);
    cout << endl;
    for(auto i : memberNode->memberList){
        string id = from_id(i.getid());
        string port = from_port(i.getport());
        string temp = (id) + (port);
        Address *node_addr = get_address_from_string(temp.c_str());
        printAddress(node_addr);
        cout <<" heartbeat:";
        cout <<  i.getheartbeat() <<" TS:" <<i.gettimestamp()<<endl;
    }
}

void MP1Node::gossip(char *message,int size){
	int random_gen = ceil((memberNode->memberList.size()) * 0.5);
	set<int> indices;
	for(int i=0;i<random_gen;i++){
			int temp = (rand() % (memberNode->memberList.size()));
			if(indices.find(temp)!=indices.end())
					i--;
			else indices.insert(temp);
	}
	string gossip_message(message,size);
	char *final_gossip_message = create_message(GOSSIP,gossip_message.c_str(),size);
	for(auto i : indices){
			int id = this->memberNode->memberList[i].getid();
			int port = this->memberNode->memberList[i].getport();
			string temp = (from_id(id)) + (from_port(port));
			Address *send_addr = get_address_from_string(temp.c_str());
			emulNet->ENsend(&this->memberNode->addr, send_addr, (char *)final_gossip_message, size + sizeof(MessageHdr));
	}
}

bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    //cout << "time :" << par->getcurrtime() << endl;
    MessageHdr *message = (MessageHdr *)(data);
    if(message->msgType == JOINREQ){
        char *sender;
        sender =  (char *)(message + 1);
        Address *to_send_address = get_address_from_string(sender);
        char *send_message = create_message(JOINREP,addresses.c_str(),addresses.size());
        //send message
        emulNet->ENsend(&this->memberNode->addr, to_send_address, (char *)send_message, addresses.size() + sizeof(MessageHdr));
        string new_node(sender,6);
        addresses += new_node;
        free(to_send_address);
				free(message);
    }
    else if(message->msgType == JOINREP){
        //cout << "Reply message at ";
        //printAddress(&memberNode->addr);
        string system_nodes_addresses;
        size -= sizeof(MessageHdr);
        system_nodes_addresses = string((char *)(message+1),size);

        vector<string> nodes_i_can_connect;
        while( system_nodes_addresses.size() > 0 ){
            nodes_i_can_connect.push_back(system_nodes_addresses.substr(0,6));
            system_nodes_addresses = system_nodes_addresses.substr(6);
        }
        for(int i=0;i<nodes_i_can_connect.size();i++){
            const char* temp = nodes_i_can_connect[i].c_str();
            int id = to_id(temp);
            int port = to_port(temp);
            MemberListEntry temp_entry(id,port,0,par->getcurrtime());
            memberNode->memberList.push_back(temp_entry);
            Address *temp_addr = get_address_from_string(temp);
            log->logNodeAdd(&memberNode->addr,temp_addr);
            char *send_message = create_message(ACCEPTKAR,get_string_from_Address(&memberNode->addr).c_str(),6);
            emulNet->ENsend(&this->memberNode->addr, temp_addr, (char *)send_message, 6 + sizeof(MessageHdr));
            free(temp_addr);
        }
        memberNode->inGroup = true;
				free(message);
    }
    else if(message->msgType == ACCEPTKAR){
        //cout << "ACCEPTKAR request aa gayi! at";
        string sender_id((char *)(message + 1),6);
        const char * const_sender_id = sender_id.c_str();
        Address *sender_addr = get_address_from_string(const_sender_id);
        int id = to_id(const_sender_id);
        int port = to_port(const_sender_id);
        MemberListEntry temp_entry(id,port,0,par->getcurrtime());
        memberNode->memberList.push_back(temp_entry);
        log->logNodeAdd(&memberNode->addr,sender_addr);
        free(sender_addr);
				free(message);
    }
    else if(message->msgType == HB){
        //HB message
        // printAddress(&memberNode->addr);
        size -= sizeof(MessageHdr);
        string cpp_string_hb((char *)(message + 1),size);
        vector<pair<int,int>> mem_list = parse_list(cpp_string_hb);
				bool flag = false;
				for(int j = 0;j<memberNode->memberList.size();j++){
					if(memberNode->memberList[j].getid() == mem_list[0].first){
						if(memberNode->memberList[j].getheartbeat() <= mem_list[0].second){
							flag = true;
						}
						break;
					}
				}
				//flag = true;
				if(flag == false)
					return false;
				else{
					for(auto i : mem_list ){
						for(int j = 0;j<memberNode->memberList.size();j++){
							if(memberNode->memberList[j].id == i.first){
								if(i.second > memberNode->memberList[j].getheartbeat()){
									memberNode->memberList[j].setheartbeat(i.second);
									memberNode->memberList[j].settimestamp(par->getcurrtime());
								}
							}
						}
					}
					gossip((char *)(message + 1),cpp_string_hb.size());
				}
    }
		else{
			size -= sizeof(MessageHdr);
			string cpp_string_hb((char *)(message + 1),size);
			vector<pair<int,int>> mem_list = parse_list(cpp_string_hb);
			bool flag = false;
			for(int j = 0;j<memberNode->memberList.size();j++){
				if(memberNode->memberList[j].getid() == mem_list[0].first){
					if(memberNode->memberList[j].getheartbeat() <= mem_list[0].second){
						flag = true;
					}
					break;
				}
			}
			//flag = true;
			if(flag == false)
				return false;
			else{
				for(auto i : mem_list ){
					for(int j = 0;j<memberNode->memberList.size();j++){
						if(memberNode->memberList[j].id == i.first){
							if(i.second > memberNode->memberList[j].getheartbeat()){
								memberNode->memberList[j].setheartbeat(i.second);
								memberNode->memberList[j].settimestamp(par->getcurrtime());
							}
						}
					}
				}
				gossip((char *)(message + 1),cpp_string_hb.size());
			}
		}
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */

int convert_to_ascii(string a){
    int num = 0;
    while(a.size()!=0){
        num = num * 10 + (a[0] - '0');
        a = a.substr(1);
    }
    return num;
}
vector<pair<int,int>> MP1Node::parse_list(string mem_list){
    vector<pair<int,int>> parsed_list;
    int i=0;
    while(i<mem_list.size()){
        string id = mem_list.substr(i,4);
        i+=4;
        string hb_count="";
        while(mem_list[i]!=','){
            hb_count+=mem_list[i++];
        }
        i++;
        parsed_list.push_back({to_id(id.c_str()),convert_to_ascii(hb_count)});
    }
    return parsed_list;
}
string MP1Node::convert_list_to_string(){
    //string data = "";
    char c_str_id[5];
    c_str_id[4]='\0';
    memcpy(c_str_id,&memberNode->addr.addr,4);
    string heartbeat_message(c_str_id,4);
    //this->heartbeat_count++;
    heartbeat_message+=std::to_string(this->heartbeat_count) + ",";
    for(auto i : memberNode->memberList){
        string temp_id = from_id(i.getid());
        heartbeat_message+=temp_id + std::to_string(i.getheartbeat()) +",";
    }
    return heartbeat_message;
}
void MP1Node::send_heartbeat(){
    int random_gen = ceil((memberNode->memberList.size()) *0.5);
    set<int> indices;
    for(int i=0;i<random_gen;i++){
        int temp = (rand() % (memberNode->memberList.size()));
        if(indices.find(temp)!=indices.end())
            i--;
        else indices.insert(temp);
    }
    this->heartbeat_count++;
    string heartbeat_msg = convert_list_to_string();
    char *final_heartbeat_msg = create_message(HB,heartbeat_msg.c_str(),heartbeat_msg.size());
    for(auto i : indices){
        int id = this->memberNode->memberList[i].getid();
        int port = this->memberNode->memberList[i].getport();
        string temp = (from_id(id)) + (from_port(port));
        Address *send_addr = get_address_from_string(temp.c_str());
        emulNet->ENsend(&this->memberNode->addr, send_addr, (char *)final_heartbeat_msg, heartbeat_msg.size() + sizeof(MessageHdr));
    }
}
void MP1Node::nodeLoopOps() {
    //1. send heartbeats!!!!
    printAddress(&memberNode->addr);
		Address ja = getJoinAddress();
		if(0 == memcmp((char *)&(memberNode->addr.addr), (char *)(&ja), sizeof(memberNode->addr.addr))){
			print_membership_list();
			cout << endl;
		}
    send_heartbeat();
		//vector<int> remove_pos;
		vector<MemberListEntry>::iterator it;
		it = memberNode->memberList.begin();
		int pos = 0;
		for(int i=0;i<memberNode->memberList.size();i++){
			if(((par->getcurrtime()) - memberNode->memberList[i].gettimestamp()) >= TFAIL){
				int id,port;
				id = memberNode->memberList[i].getid();
				port = memberNode->memberList[i].getport();
				Address *addr = get_address_from_string((from_id(id) + from_port(port)).c_str());
				log->logNodeRemove(&memberNode->addr,addr);
				memberNode->memberList.erase(it + i);
			}
			pos++;
		}
		// for(auto i : remove_pos){
		//
		// }
    return;
}
// print_membership_list();
				// cout << endl;
/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d ",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
