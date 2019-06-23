#ifndef CSVDB_TABLEFLOAT_H
#define CSVDB_TABLEFLOAT_H

#include "tablevalue.hpp"
#include <string>

namespace csvdb
{
	class TableFloat : public TableValue
	{
	public:
		TableFloat() : val(0) {};
		TableFloat(double i) : val(i) {};
		TableFloat(std::string);
		TableValue* clone() {TableFloat* c = new TableFloat(val); c->amInull = amInull; return c;};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		TableFloat& operator+=(TableValue&);
		bool isNull() const;
		double getFloatVal() {return val;};
		std::string getValue() const;
		double getVal() const;
	private:
		double val;
		bool amInull;
		std::ifstream& readFromStream(std::ifstream&);
		std::ofstream& writeToCSV(std::ofstream&);
	};
}
#endif
