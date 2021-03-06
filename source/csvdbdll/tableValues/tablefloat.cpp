#include "tablefloat.hpp"
#include <fstream>
#include <iostream>

namespace csvdb
{
	TableFloat::TableFloat(std::string str)
	{
		if (str == "")
		{
			amInull = true;
			return;
		}
		amInull = false;
		val = std::atof(str.c_str());
	}
	std::string TableFloat::getValue() const
	{
		return "[float] " + std::to_string(val);
	}
	std::ifstream& TableFloat::readFromStream(std::ifstream& is)
	{
		unsigned char buffer[8];
		is.read(reinterpret_cast<char*>(buffer), 8);
		uint64_t intVal = 0;
		for (int i = 0; i < 8; i++)
		{
			intVal <<= 8;
			intVal |= buffer[i];
		}
		val = *reinterpret_cast<double*>(&intVal);
		amInull = intVal == 0x8000000000000000;//null is represented as a negative zero double, -0.0
		return is;
	}
	std::ofstream& TableFloat::writeToCSV(std::ofstream& os)
	{
		if(amInull)	return os;
		os << val;
		return os;
	}
	double TableFloat::getVal() const
	{
		return val;
	}
	bool TableFloat::operator<(const TableValue& other) const
	{
		if (amInull){
			return true;
		}
		if (other.isNull()){
			return false;
		}
		return val < dynamic_cast<const TableFloat&>(other).val;
	}
	bool TableFloat::operator<=(const TableValue& other) const
	{
		return val <= dynamic_cast<const TableFloat&>(other).val;
	}
	bool TableFloat::operator>(const TableValue& other) const
	{
		return val > dynamic_cast<const TableFloat&>(other).val;
	}
	bool TableFloat::operator>=(const TableValue& other) const
	{
		return val >= dynamic_cast<const TableFloat&>(other).val;
	}
	bool TableFloat::operator==(const TableValue& other) const
	{
		return val == dynamic_cast<const TableFloat&>(other).val;
	}
	bool TableFloat::operator!=(const TableValue& other) const
	{
		return val != dynamic_cast<const TableFloat&>(other).val;
	}
	TableFloat& TableFloat::operator+=(TableValue& other)
	{
		val += dynamic_cast<TableFloat&>(other).val;
		return *this;
	}
	bool TableFloat::isNull() const
	{
		return amInull;
	}
}
