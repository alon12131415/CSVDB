#ifndef CSVDB_TABLEVARCHAR_H
#define CSVDB_TABLEVARCHAR_H

#include "tablevalue.hpp"
#include <string>

namespace csvdb
{
	class TableVarchar : public TableValue
	{
	public:
		TableVarchar() : val() {};
		TableVarchar(char* i) : val(i) {};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		bool isNull() const;
		std::string getVal() const;
	private:
		std::string val;
		std::ifstream& readFromStream(std::ifstream&);
		std::ofstream& writeToCSV(std::ofstream&);
	};
}
#endif