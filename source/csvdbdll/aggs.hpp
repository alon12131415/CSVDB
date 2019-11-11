#ifndef CSVDB_AGGS_H
#define CSVDB_AGGS_H
#include "aggregator.hpp"
#include "tableValues/tablevalue.hpp"
#include "tableValues/tableint.hpp"
#include "tableValues/tablefloat.hpp"
namespace csvdb
{
	class minAgg : public Aggregator
	{
	public:
		void update(TableValue*);
	};

	class maxAgg : public Aggregator
	{
	public:
		void update(TableValue*);
	};

	class sumAgg : public Aggregator
	{
	public:
		void update(TableValue*);
	};

	class countAgg : public Aggregator
	{
	public:
		void update(TableValue*);
		void reset() {count = 0;};
		TableValue* getTableValue() {
			TableInt* c = new TableInt(count);
			c->amInull = false;
			return c;
		};
		uint64_t count;
	protected:
		std::ofstream& writeToStream(std::ofstream& os);
	};

	class avgAgg : public Aggregator
	{
	public:
		void update(TableValue*);
		void reset();
		TableValue* getTableValue() {return new TableFloat(sum.currVal->getFloatVal() / count.count);}
	private:
		countAgg count;
		sumAgg sum;
	};

}



#endif
