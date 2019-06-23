#ifndef CSVDB_TABLEINT_H
#define CSVDB_TABLEINT_H

#include "tablevalue.hpp"
#include <string>

namespace csvdb
{
	class TableInt : public TableValue
	{
	public:
		TableInt() : val(0) {};
		TableInt(int i) : val(i) {};
		TableInt(std::string);
		TableValue* clone() {TableInt* c = new TableInt(val); c->amInull = amInull; return c;};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		TableInt& operator+=(TableValue&);
		bool isNull() const;
		std::string getValue() const;
		double getFloatVal() {return val;};
		int64_t getVal() const;
		bool amInull;
	private:
		int64_t val;
		std::ifstream& readFromStream(std::ifstream&);
		std::ofstream& writeToCSV(std::ofstream&);
	};
}
#endif
