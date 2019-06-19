#include "tabletimestamp.hpp"
#include <fstream>

namespace csvdb
{
	TableTimestamp::TableTimestamp(std::string str)
	{
		if (str == "")
		{
			amInull = true;
			return;
		}
		amInull = false;
		val = std::stoull(str);
	}
	std::string TableTimestamp::getValue() const
	{
		return "[timestamp] " + std::to_string(val);
	}
	std::ifstream& TableTimestamp::readFromStream(std::ifstream& is)
	{
		unsigned char buffer[9];
		is.read(reinterpret_cast<char*>(buffer), 9);
		if (buffer[0] == 0)
		{
			amInull = true;
			return is;
		}
		amInull = false;
		val = 0;
		for (int i = 1; i < 9; i++)
		{
			val <<= 8;
			val |= buffer[i];
		}
		return is;
	}
	std::ofstream& TableTimestamp::writeToCSV(std::ofstream& os)
	{
		if(amInull)	return os;
		os << val;
		return os;
	}
	uint64_t TableTimestamp::getVal() const
	{
		return val;
	}
	bool TableTimestamp::operator<(const TableValue& other) const
	{
		if (amInull){
			return true;
		}
		if (other.isNull()){
			return false;
		}
		return val < dynamic_cast<const TableTimestamp&>(other).val;
	}
	bool TableTimestamp::operator<=(const TableValue& other) const
	{
		return val <= dynamic_cast<const TableTimestamp&>(other).val;
	}
	bool TableTimestamp::operator>(const TableValue& other) const
	{
		return val > dynamic_cast<const TableTimestamp&>(other).val;
	}
	bool TableTimestamp::operator>=(const TableValue& other) const
	{
		return val >= dynamic_cast<const TableTimestamp&>(other).val;
	}
	bool TableTimestamp::operator==(const TableValue& other) const
	{
		return val == dynamic_cast<const TableTimestamp&>(other).val;
	}
	bool TableTimestamp::operator!=(const TableValue& other) const
	{
		return val != dynamic_cast<const TableTimestamp&>(other).val;
	}
	TableTimestamp& TableTimestamp::operator+=(TableValue& other)
	{
		val += dynamic_cast<TableTimestamp&>(other).val;
		return *this;
	}
	bool TableTimestamp::isNull() const
	{
		return amInull;
	}
}
