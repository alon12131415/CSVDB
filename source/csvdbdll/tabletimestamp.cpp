#include "tabletimestamp.hpp"
#include <fstream>

namespace csvdb
{
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
	uint64_t TableTimestamp::getVal() const
	{
		return val;
	}
	bool TableTimestamp::operator<(const TableValue& other) const
	{
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
	bool TableTimestamp::isNull() const
	{
		return amInull;
	}
}