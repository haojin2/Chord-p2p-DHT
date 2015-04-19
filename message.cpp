#include <cstring>
#include <chrono>

using namespace std;

class message
{
    public:
        int command;
        int id;
        int src;
        int dest;
        int finger_idx;

        message() {};

        message(int command, int id, int src, int dest, int finger_idx)
        {
            this->command = command;
            this->id = id;
            this->src = src;
            this->dest = dest;
            this->finger_idx = finger_idx;
        };

        void setContent(int command, int id, int src, int dest, int finger_idx)
        {
            this->command = command;
            this->id = id;
            this->src = src;
            this->dest = dest;
            this->finger_idx = finger_idx;
        };
};
