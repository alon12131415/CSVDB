#include "aggs.hpp"
#include "tableValues\tableint.hpp"
#include "tableValues\tablevalue.hpp"
#include <iostream>
#include <fstream>


namespace csvdb
{


	void minAgg::update(TableValue* val)
	{
		if (val == nullptr)
			return;
		if (currVal == nullptr || *val < *currVal)
		{
			delete currVal;
			currVal = val->clone();
		}
	}
	void maxAgg::update(TableValue* val)
	{
		if (val == nullptr)
			return;
		if (currVal == nullptr || *val > *currVal)
		{
			delete currVal;
			currVal = val->clone();
		}
	}
	void sumAgg::update(TableValue* val)
	{
		if (val == nullptr)
			return;
		if (currVal == nullptr)
		{
			currVal = val->clone();
			return;
		}
		*currVal += *val;
	}
	void countAgg::update(TableValue* val)
	{
		count += val->isNull() ? 0 : 1;
	}
	std::ofstream& countAgg::writeToStream(std::ofstream& os){
		os << count;
		return os;
	}
	void avgAgg::reset()
	{
		count.reset();
		sum.reset();
	}
	void avgAgg::update(TableValue* val)
	{
		count.update(val);
		sum.update(val);
	}

}
