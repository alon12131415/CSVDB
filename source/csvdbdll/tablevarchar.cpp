#include "tablevarchar.hpp"
#include <fstream>

namespace csvdb
{
	std::ifstream& TableVarchar::readFromStream(std::ifstream& is)
	{
		std::getline(is, val);
		return is;
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