#include "tableint.hpp"
#include <fstream>
#include <iostream>
#include <string>
namespace csvdb
{
	TableInt::TableInt(std::string str)
	{
		if (str == "")
		{
			amInull = true;
			return;
		}
		amInull = false;
		val = std::stoll(str);
	}

	std::string TableInt::getValue() const
	{
		return "[int] " + std::to_string(val);
	}
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
	std::ofstream& TableInt::writeToCSV(std::ofstream& os)
	{

		if(amInull)
		{
			return os;
		}
		os << val;
		return os;
	}
	int64_t TableInt::getVal() const
	{
		return val;
	}
	bool TableInt::operator<(const TableValue& other) const
	{
		if (amInull){
			return true;
		}
		if (other.isNull()){
			return false;
		}
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
	TableInt& TableInt::operator+=(TableValue& other)
	{
		val += dynamic_cast<TableInt&>(other).val;
		return *this;
	}
	bool TableInt::isNull() const
	{
		return amInull;
	}
}
