#include "tablevarchar.hpp"
#include <fstream>
#include <iostream>
// #include <regex>
#include <sstream>

namespace csvdb
{
	std::string TableVarchar::getValue() const
	{
		if (isNull())
			return "[varchar] ";
		return "[varchar] " + val;
	}
	std::ifstream& TableVarchar::readFromStream(std::ifstream& is)
	{
		std::getline(is, val, '\0');
		return is;
	}
	void TableVarchar::replaceAll(std::string& str, const std::string& from, const std::string& to) {
	    if(from.empty())
	        return;
	    size_t start_pos = 0;
	    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
	        str.replace(start_pos, from.length(), to);
	        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
	    }
	}
	std::ofstream& TableVarchar::writeToCSV(std::ofstream& os)
	{
		size_t start_pos = 0;
		size_t prev_pos = 0;
		os << "\"";
		while((start_pos = val.find("\"", start_pos)) != std::string::npos)
		{
			os << val.substr(prev_pos, start_pos);
			os << "\"";
			prev_pos = ++start_pos;
		}
		os << val.substr(prev_pos, val.length());
		os << "\"";
		return os;
	}

	std::string TableVarchar::getVal() const
	{
		return val;
	}
	bool TableVarchar::operator<(const TableValue& other) const
	{
		return val < dynamic_cast<const TableVarchar&>(other).val;
	}
	bool TableVarchar::operator<=(const TableValue& other) const
	{
		return val <= dynamic_cast<const TableVarchar&>(other).val;
	}
	bool TableVarchar::operator>(const TableValue& other) const
	{
		return val > dynamic_cast<const TableVarchar&>(other).val;
	}
	bool TableVarchar::operator>=(const TableValue& other) const
	{
		return val >= dynamic_cast<const TableVarchar&>(other).val;
	}
	bool TableVarchar::operator==(const TableValue& other) const
	{
		return val == dynamic_cast<const TableVarchar&>(other).val;
	}
	bool TableVarchar::operator!=(const TableValue& other) const
	{
		return val != dynamic_cast<const TableVarchar&>(other).val;
	}
	TableVarchar& TableVarchar::operator+=(TableValue& other)
	{
		val += dynamic_cast<TableVarchar&>(other).val;
		return *this;
	}
	bool TableVarchar::isNull() const
	{
		return false;
	}
}
