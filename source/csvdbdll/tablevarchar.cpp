#include "tablevarchar.hpp"
#include <fstream>
#include <iostream>

namespace csvdb
{
	std::ifstream& TableVarchar::readFromStream(std::ifstream& is)
	{
		std::getline(is, val, '\0');
		return is;
	}
	std::ofstream& TableVarchar::writeToCSV(std::ofstream& os)
	{
		os << val;
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
	bool TableVarchar::isNull() const
	{
		return false;
	}
}
