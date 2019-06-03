#ifndef CSVDB_TABLETIMESTAMP_H
#define CSVDB_TABLETIMESTAMP_H

#include "tablevalue.hpp"
#include <string>
#include <iostream>

namespace csvdb
{
	class TableTimestamp : public TableValue
	{
	public:
		TableTimestamp() : val(0) {};
		TableTimestamp(int i) : val(i) {};
		TableTimestamp(std::string);
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		bool isNull() const;
		std::string getValue() const;
		uint64_t getVal() const;
	private:
		uint64_t val;
		bool amInull;
		std::ifstream& readFromStream(std::ifstream&);
		std::ofstream& writeToCSV(std::ofstream&);
	};
}
#endif
