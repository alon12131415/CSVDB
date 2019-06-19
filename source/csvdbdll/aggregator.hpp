#ifndef CSVDB_AGGREGATOR_H
#define CSVDB_AGGREGATOR_H
#include "tablevalue.hpp"
#include <iostream>
#include <fstream>
namespace csvdb
{
	class Aggregator
	{
	public:
		virtual void update(TableValue*) = 0;
		Aggregator() {reset();};
		virtual void reset()
		{
			delete currVal;
			currVal = nullptr;
		};
		virtual TableValue* getTableValue() {return currVal;};
		~Aggregator(){delete currVal;};
		TableValue* currVal = nullptr;
	protected:
		virtual std::ofstream& writeToStream(std::ofstream& os)
		{
			os << currVal;
			return os;
		};
		friend std::ofstream& operator<<(std::ofstream& os, Aggregator& agg){
			return agg.writeToStream(os);
		};
	};
}


#endif
