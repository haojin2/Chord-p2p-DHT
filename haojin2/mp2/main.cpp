#include <vector>
#include <queue>
#include <unordered_map>
#include <map>
#include <iostream>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <string>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "message.cpp"
#include "node.cpp"

#define FIND_SUCCESSOR 1
#define JOIN 2
#define LEAVE 3
#define SHOW 4
#define GET_SUCCESSOR 5
#define GET_PREDECESSOR 6
#define SET_PREDECESSOR 7
#define CLOSEST_PRECEDING_FINGER 8
#define ACK 9
#define RETURN_SUCCESSOR 10
#define RETURN_PREDECESSOR 11
#define RETURN_CPF 12
#define UPDATE_FINGER_TABLE 13

using namespace std;

const int M = 8;
const int max_id = 255;
int msg_count = 0;
int type_count[14] = {0};

mutex count_mtx;
FILE * output = stdout;
std::vector<thread*> all_threads;

void node_thread(int node_id);

// get_port(int id)
// get the port number for a node according to the node id
// input: id -- id of the node
// return: port of the node
string get_port(int id) {
    return to_string(id+10000);
}

// increment_count(int type)
// increment the message count
// input: int type -- type of the message sent
// return: none
void increment_count(int type) {
    count_mtx.lock();
    msg_count++;
    type_count[type]++;
    count_mtx.unlock();
}

// reply_message(int fd,message & m)
// send a reply for a previous request
// input: fd -- file descriptor of the socket
//        m -- the messages made for reply
// return: none
void reply_message(int fd, message & m)
{
    write(fd, &m, sizeof(message));
    increment_count(m.command);
}

// send_message(message & m)
// send message to the designated destination
// input: m -- message to be sent
// return: sock_fd -- the socket's file descriptor
int send_message(message & m) {

    int s;
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET; /* IPv4 only */
    hints.ai_socktype = SOCK_STREAM; /* TCP */

    s = getaddrinfo(NULL, get_port(m.dest).c_str(), &hints, &result);

    if (s != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        exit(1);
    }

    connect(sock_fd, result->ai_addr, result->ai_addrlen);

    write(sock_fd, &m, sizeof(message));
    increment_count(m.command);

    freeaddrinfo(result);
    return sock_fd;
}

// send_join(int node_id, int arb_node_id)
// tell node with node_id to join the system with the help of node arb_node_id
// and wait till the reply comes back
// input: node_id -- node that will be joined
//        arb_node_id -- node that will help node node_id to join the system
// return: none
void send_join(int node_id, int arb_node_id) {
    thread* t = new thread(node_thread, node_id);
    all_threads.push_back(t);

    ::sleep(1);

    message m(JOIN, arb_node_id, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// send_leave(int node_id)
// tell node node_id to leave the system and wait till the reply comes back
// input: node_id -- node that will leave the system
// return: none
void send_leave(int node_id) {
    message m(LEAVE, -1, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// send_show(int node_id)
// tell a node to show all its keys and wait till the operation is over
// input: node_id -- node that will display all the keys it holds
// return: none
void send_show(int node_id) {
    message m(SHOW, -1, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// send_find_successor(int node_id, int id)
// tell node node_id to find the successor of id, 
// wait till the reply comes back
// input: node_id -- node to perform the find operation
//        id -- the id to be searched
// return: successor of id
int send_find_successor(int node_id, int id) {
    message m(FIND_SUCCESSOR, id, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != RETURN_SUCCESSOR) cout << "error" <<endl;
    close(out_fd);
    return m.id;
}

// send_closest_preceding_finger(int node_id, int id)
// tell node node_id to find the closest preceding finger of id, 
// wait till the reply comes back
// input: node_id -- node to perform the operation
//        id -- the id to be searched
// return: the node with the closest preceding finger of id
int send_closest_preceding_finger(int node_id, int id) {
    message m(CLOSEST_PRECEDING_FINGER, id, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != RETURN_CPF) cout << "error" <<endl;
    close(out_fd);
    return m.id;
}

// send_get_successor(int node_id)
// tell node node_id to return its successor, 
// wait till the reply comes back
// input: node_id -- node to return its successor
// return: successor of node_id
int send_get_successor(int node_id) {
    message m(GET_SUCCESSOR, -1, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != RETURN_SUCCESSOR) cout << "error" <<endl;
    close(out_fd);
    return m.id;
}

// send_get_predecessor(int node_id)
// tell node node_id to return its predecessor, 
// wait till the reply comes back
// input: node_id -- node to return its predecessor
// return: predecessor of node_id
int send_get_predecessor(int node_id) {
    message m(GET_PREDECESSOR, -1, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != RETURN_PREDECESSOR) cout << "error" <<endl;
    close(out_fd);
    return m.id;
}

// send_set_predecessor(int node_id, int id)
// tell node node_id to set its predecessor to be id, 
// wait till the reply comes back
// input: node_id -- node to set its predecessor
//        id -- the id of the new predecessor
// return: none
void send_set_predecessor(int node_id, int id) {
    message m(SET_PREDECESSOR, id, -1, node_id, -1);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// send_update_finger_table(int node_id, int id,int idx)
// tell node node_id to set its idxth finger to be id  
// wait till the reply comes back
// input: node_id -- node to update its own finger table
//        id -- the id of the new finger
//        idx -- index of the finger table entry to be set
// return: none
void send_update_finger_table(int node_id, int id, int idx)
{
    message m(UPDATE_FINGER_TABLE, id, -1, node_id, idx);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// send_update_successor(int node_id, node & data, int idx)
// tell node node_id to set its idxth finger to be data.nodes[0]  
// wait till the reply comes back
// input: node_id -- node to update its own finger table
//        data -- a structure that contains the required information
//        idx -- index of the finger table entry to be set
// return: none
void send_update_finger_table(int node_id, node & data, int idx)
{
    message m(UPDATE_FINGER_TABLE, data.nodes[0], data.id, node_id, idx);
    int out_fd = send_message(m);
    read(out_fd, &m, sizeof(message));
    if (m.command != ACK) cout << "error" <<endl;
    close(out_fd);
    return;
}

// range(int low, int lowoffset, int high, int highoffset, int arg)
// check if arg is with in the range specified by low and high
// input: low -- the lower limit of the range
//        lowoffset -- 1 indicates that low is included in the range,
//                     0 indicates that low is not included in the range
//        high -- the high limit of the range
//        highoffset -- 1 indicates that high is included in the range,
//                      0 indicates that high is not included in the range
//        arg -- the number to be checked
// return: if arg is in the range specified by low and high
bool range(int low, int lowoffset, int high, int highoffset, int arg)
{
    if (arg > max_id || arg < 0)
    {
        return false;
    }
    if (low == high)
    {
        if (lowoffset == highoffset && lowoffset == 0)
        {
            return (arg != low);
        }
        else{
            return true;
        }
    }
    else
    {
        if (low<high)
        {
            return ( ( arg > (low-lowoffset) ) && ( arg < ( high + highoffset ) ) );
        }
        else
        {
            return (((low-lowoffset)<arg) || (arg<(high+highoffset)));
        }
    }
}

// closest_preceding_finger(int id, node & data)
// looks up for the closest preceding finger for id
// input: id -- id to be checked
//        data -- a structure that contains the required information
// return: the closest preceding finger for id
int closest_preceding_finger(int id, node & data) {
    for (int i = M-1; i > -1; --i)
    {
        if (range(data.id,0,id,0,data.nodes[i]))
        {
            return data.nodes[i];
        }
    }
    return data.id;
}

// find_predecessor(int id, node & data)
// looks up for the predecessor for id
// input: id -- id to be checked
//        data -- a structure that contains the required information
// return: the predecessor for id
int find_predecessor(int id, node & data) {
    int n = data.id;
    int suc = data.nodes[0];
    while(!range(n,0,suc,1,id))
    {
        if (n==data.id) {
            n = closest_preceding_finger(id, data);
        }
        else {
            n = send_closest_preceding_finger(n, id);
        }
        if (n==data.id) {
            suc = data.nodes[0];
        }
        else {
            suc = send_get_successor(n);
        }
    }
    return n;
}

// init_finger_table(int arb_node_id, node & data)
// let arb_node_id help to initialize finger table
// input: arb_node_id -- id for the helper node
//        data -- a structure that contains the required information
// return: none
void init_finger_table(int arb_node_id, node & data) {
    data.nodes[0] = send_find_successor(arb_node_id, data.start[0]);
    data.predecessor = send_get_predecessor(data.nodes[0]);
    send_set_predecessor(data.nodes[0], data.id);

    for (int i = 0; i < M-1; ++i)
    {
        if (range(data.id, 1, data.nodes[i], 0, data.start[i+1])) {
            data.nodes[i+1] = data.nodes[i];
        }
        else {
            data.nodes[i+1] = send_find_successor(arb_node_id, data.start[i+1]);
        }
    }
}

// update_others_join(node & data)
// the update_others() function for join function
// input: data -- a structure that contains the required information
// return: none
void update_others_join(node & data) {
    for (int i = 0; i < M; ++i)
    {
        int tmp = data.id - pow(2,i) + 1;
        int arg = (tmp>=0)? tmp:(tmp+max_id+1)%255;
        int p = find_predecessor(arg, data);
        if (p != data.id)
        {
            send_update_finger_table(p, data.id, i);
        }
    }
}

// update_others_join(node & data)
// the update_others() function for leave function
// input: data -- a structure that contains the required information
// return: none
void update_others_leave(node & data) {
    for (int i = 0; i < M; ++i)
    {
        int tmp = data.id - pow(2,i) + 1;
        int arg = (tmp>=0)? tmp:(tmp+max_id+1)%255;
        int p = find_predecessor(arg, data);
        if (p != data.id)
        {
            send_update_finger_table(p, data, i);
        }
    }
}

// message_handler(int fd, node & data)
// handle message sent from file descriptor fd
// input: data -- a structure that contains the required information
// return: if node should terminate itself
bool message_handler(int fd, node & data)
{
    message m;
    message reply;
    read(fd, &m, sizeof(message));

    switch (m.command) {
        case FIND_SUCCESSOR:
        {
            int pre = find_predecessor(m.id, data);
            int suc;
            if (pre == data.id)
            {
                suc = data.nodes[0];
            }
            else {
                suc = send_get_successor(pre);
            }
            reply.setContent(RETURN_SUCCESSOR, suc, data.id, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case JOIN:
        {
            if (m.id==-1)
            {
                data.predecessor=data.id;
                for (int i = 0; i < M; ++i)
                {
                    data.nodes[i]=data.id;
                }
                for (int i = 0; i < max_id+1; ++i)
                {
                    data.keys[i]=1;
                }
            }
            else
            {
                init_finger_table(m.id, data);
                update_others_join(data);
                for (int i = data.predecessor+1; range(data.predecessor,0,data.id,1,i); i=(i+1)%(max_id+1))
                {
                    data.keys[i]=1;
                }
            }
            reply.setContent(ACK, data.id, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case LEAVE:
        {
            if (data.predecessor != data.id)
            {
                update_others_leave(data);
                send_set_predecessor(data.nodes[0], data.predecessor);
            }
            reply.setContent(ACK, data.id, -1, m.src, -1);
            reply_message(fd, reply);
            close(fd);
            return true;
        }
        case SHOW:
        {
            string out = to_string(data.id);
            for (auto it = data.keys.begin(); it != data.keys.end(); ++it) {
                out = out + " " + to_string(it->first);
            }
            fprintf(output, "%s\n", out.c_str());
            reply.setContent(ACK, -1, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case GET_SUCCESSOR:
        {
            reply.setContent(RETURN_SUCCESSOR, data.nodes[0], -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case GET_PREDECESSOR:
        {
            reply.setContent(RETURN_PREDECESSOR, data.predecessor, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case SET_PREDECESSOR:
        {
            if (range(data.predecessor,0,data.id,0,m.id)) {
                for (int i = (data.predecessor+1); range(data.predecessor,0,m.id,1,i);i=(i+1)%(max_id+1))
                {   
                    data.keys.erase(i);        
                }
            }
            else {
                for (int i = m.id+1; range(m.id,0,data.predecessor,1,i);i=(i+1)%(max_id+1))
                {   
                    data.keys[i] = 1;        
                }
            }
            data.predecessor=m.id;
            reply.setContent(ACK, m.id, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case CLOSEST_PRECEDING_FINGER:
        {
            int n = closest_preceding_finger(m.id, data);
            reply.setContent(RETURN_CPF, n, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
        case UPDATE_FINGER_TABLE:
        {
            int i =  m.finger_idx;
            if (m.src == -1) {
                if (range(data.id,1,data.nodes[i],0,m.id))
                {
                    data.nodes[i] = m.id;
                    if (data.predecessor != m.id) {
                        send_update_finger_table(data.predecessor,m.id,i);
                    }
                }
            }
            else {
                if (data.nodes[i] == m.src)
                {
                    data.nodes[i] = m.id;
                    if (data.predecessor != m.src){// && data.predecessor != data.id) {
                        node tmp=node(m.src,m.id);
                        send_update_finger_table(data.predecessor,tmp,i);
                    }
                }
            }
            reply.setContent(ACK, -1, -1, m.src, -1);
            reply_message(fd, reply);
            break;
        }
    }

    close(fd);
    return false;
}

// node_thread(int node_id)
// main execution for each node
// input: node_id -- identifier for the node
// return: none
void node_thread(int node_id)
{
    int s;
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    s = getaddrinfo(NULL, get_port(node_id).c_str(), &hints, &result);
    if (s != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        exit(1);
    }

    int optval = 1;
    setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

    if ( ::bind(sock_fd, result->ai_addr, result->ai_addrlen) != 0 ) {
        perror("bind()");
        exit(1);
    }

    if ( listen(sock_fd, 10) != 0 ) {
        perror("listen()");
        exit(1);
    }

    freeaddrinfo(result);

    node data(node_id);
    bool leave = false;

    while (!leave) {
        int fd = accept(sock_fd, NULL, NULL);
        leave = message_handler(fd, data);
    }

    close(sock_fd);
    return;
}

// main(int argc, char ** argv)
// coordinator thread
// input: argc -- count of bash argument passed in
//        argv -- array of bash arguments
// return: 0
int main(int argc, char ** argv)
{
    if (argc == 3 && strcmp(argv[1], "-g") == 0) {
        output = fopen(argv[2], "w+");
        printf("Writing to file %s\n", argv[2]);
    } else if (argc != 1) {
        cout << "Usage: ./chord [ -g output_filename ]" << endl;
        return 1;
    }

    map<int, int> nodes;
    nodes[0] = 1;
    send_join(0, -1);

    while (1) {
        cout << "Your command:" << endl;
        char input[256]={'\0'};
        vector<char*> v1;
        vector<string> v2;
        cin.getline(input,256);

        char * pch;
        pch = strtok (input," ");
        while (pch != NULL) {
            v1.push_back(pch);
            pch = strtok (NULL," ");
        }
        for(size_t i=0;i<v1.size();i++)
        {
            v2.push_back(string(v1[i]));
        }

        if( v2.size() == 0 ) { }
        else if (v2.size() == 1 && v2[0]=="count") {
            cout << "Message count: " << msg_count << endl;
        }
        else if (v2.size() == 1 && v2[0]=="quit") {
            while(nodes.size()!=0)
            {
                send_leave(nodes.begin()->first);
                nodes.erase(nodes.begin()->first);
            }
            for (size_t i = 0; i < all_threads.size(); ++i)
            {
                all_threads[i]->join();
                delete all_threads[i];
            }
            fclose(output);
            return 0;
        }
        else if((v2[0]=="Join" || v2[0]=="join") && v2.size()==2)
        {
            int node=stoi(v2[1]);
            if (node > max_id || nodes.find(node) != nodes.end()) {
                cout << "Invalid node id " << node << endl;
            } else {
                nodes[node] = 1;
                send_join(node, nodes.begin()->first);
            }
        }
        else if ((v2[0]=="Find" || v2[0]=="find") && v2.size()==3)
        {
            int node = stoi(v2[1]);
            int key = stoi(v2[2]);
            if (key > max_id || nodes.find(node) == nodes.end()) {
                cout << "Invalid input " << endl;
            } else {
                int location = send_find_successor(node, key);
                cout << "Found key " << key << " at node " << location << endl;
            }
        }
        else if ((v2[0]=="Leave" || v2[0]=="leave") && v2.size()==2)
        {
            int node=stoi(v2[1]);
            if (nodes.find(node) == nodes.end()) {
                cout << "Invalid node id " << node << endl;
            } else if (nodes.size() == 1) {
                cout << "Only one node in the network" << endl;
            } else {
                nodes.erase(node);
                send_leave(node);
            }
        }
        else if (v2[0]=="show" && v2.size()==2)
        {
            if (v2[1] == "all") {
                for (auto it = nodes.begin(); it != nodes.end(); ++it)
                {
                    if (it->second == 1) send_show(it->first);
                }
            }
            else {
                int node=stoi(v2[1]);
                if (nodes.find(node) == nodes.end()) {
                    cout << "Invalid node id " << node << endl;
                } else {
                    send_show(node);
                }
            }
        }
    }
    
    return 0;
}
