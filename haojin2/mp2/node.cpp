#include <map>
#include <cmath>
using namespace std;

class node
{
public:
	node(int id) {
		this->id = id;
		for (int i = 0; i < 8; ++i)
		{
			start[i]=(id+power(2,i))%256;
		}
	};
	node(int id, int suc)
	{
		this->id = id;
		nodes[0]=suc;
	}
	int power(int base,int index)
	{
		if (index==0)
		{
			return 1;
		}
		else return power(base,index-1)*base;
	}

	int id;
	int predecessor;
	int nodes[8];
	int start[8];
	map<int,int> keys;

};