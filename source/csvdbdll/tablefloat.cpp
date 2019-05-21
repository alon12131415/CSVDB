#include "tablefloat.hpp"
#include <fstream>
#include <iostream>

namespace csvdb
{
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
	double TableFloat::getVal() const
	{
		return val;
	}
	bool TableFloat::operator<(const TableValue& other) const
	{
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
	bool TableFloat::isNull() const
	{
		return amInull;
	}
}