#ifndef CSVDB_TABLEINT_H
#define CSVDB_TABLEINT_H

#include "tablevalue.hpp"

namespace csvdb
{
	class TableInt : public TableValue
	{
	public:
		TableInt() : val(0) {};
		TableInt(int i) : val(i) {};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		bool isNull() const;
		int64_t getVal() const;
	private:
		int64_t val;
		bool amInull;
		std::ifstream& readFromStream(std::ifstream&);
	};
}
#endif