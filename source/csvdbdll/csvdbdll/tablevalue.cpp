#include "stdafx.h"
#include "tablevalue.hpp"
#include <iostream>

namespace csvdb
{
	std::ifstream& operator>>(std::ifstream& is, TableValue& val)
	{
		return val.readFromStream(is);
	}

	bool TableValue::satisfiesWhere(WhereOperand op, TableValue const * val) const
	{
		if (op == WhereOperand::none)	return true;
		if (isNull())	return op == WhereOperand::isNull;
		switch (op)
		{
		case(none):	return true;//no where
		case(less):			return *this < *val;
		case(lessEqual):	return *this <= *val;
		case(greater):		return *this > *val;
		case(greaterEqual):	return *this >= *val;
		case(equal):		return *this == *val;
		case(notEqual):		return *this != *val;
		case(WhereOperand::isNull):		return false;
		case(isNotNull):	return true;
		}
		return true;//never reached
	}
}