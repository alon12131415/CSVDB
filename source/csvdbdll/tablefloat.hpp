#ifndef CSVDB_TABLEFLOAT_H
#define CSVDB_TABLEFLOAT_H

#include "tablevalue.hpp"

namespace csvdb
{
	class TableFloat : public TableValue
	{
	public:
		TableFloat() : val(0) {};
		TableFloat(double i) : val(i) {};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		bool isNull() const;
		double getVal() const;
	private:
		double val;
		bool amInull;
		std::ifstream& readFromStream(std::ifstream&);
	};
}
#endif