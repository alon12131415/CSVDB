#include "stdafx.h"
#include "tableint.hpp"
#include <fstream>

namespace csvdb
{
	std::ifstream& TableInt::readFromStream(std::ifstream& is)
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
	int64_t TableInt::getVal() const
	{
		return val;
	}
	bool TableInt::operator<(const TableValue& other) const
	{
		return val < dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::operator<=(const TableValue& other) const
	{
		return val <= dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::operator>(const TableValue& other) const
	{
		return val > dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::operator>=(const TableValue& other) const
	{
		return val >= dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::operator==(const TableValue& other) const
	{
		return val == dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::operator!=(const TableValue& other) const
	{
		return val != dynamic_cast<const TableInt&>(other).val;
	}
	bool TableInt::isNull() const
	{
		return amInull;
	}
}