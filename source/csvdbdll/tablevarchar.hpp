#ifndef CSVDB_TABLEVARCHAR_H
#define CSVDB_TABLEVARCHAR_H

#include "tablevalue.hpp"
#include <string>

namespace csvdb
{
	class TableVarchar : public TableValue
	{
	public:
		TableVarchar() : val("") {};
		TableVarchar(char* i) : val(i) {};
		TableVarchar(std::string i) : val(i) {};
		bool operator<(const TableValue&) const;
		bool operator<=(const TableValue&) const;
		bool operator>(const TableValue&) const;
		bool operator>=(const TableValue&) const;
		bool operator==(const TableValue&) const;
		bool operator!=(const TableValue&) const;
		bool isNull() const;
		void replaceAll(std::string& str, const std::string& from, const std::string& to);
		std::string getValue() const;
		std::string getVal() const;
		std::string val = nullptr;
	private:
		std::ifstream& readFromStream(std::ifstream&);
		std::ofstream& writeToCSV(std::ofstream&);
	};
}
#endif
