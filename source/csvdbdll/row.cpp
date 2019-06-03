#include "row.hpp"
#include <fstream>
#include <ostream>
#include <iostream>

namespace csvdb
{
	std::ofstream& operator<<(std::ofstream& os, const Row& row)
	{
		if (row.mode == writeMode::NSV)
		{
			for (int i = 0; i < row.len; i++)
			{
				os << *row.vals[i];
				os << '\0';
			}
		}
		else
		{
			for (int i = 0; i < row.len; i++)
			{
				os << *row.vals[i];
				if (i != row.len - 1)
					os << ',';
			}
			os << std::endl;
		}
		return os;
	}

	bool Row::operator<(const Row& other) const
	{
		// for (int i = 0; i < len; i++){
		// 	std::cout << vals[i]->getValue() << " - " << other.vals[i]->getValue() << " | ";
		// }
		// std::cout << std::endl;
		for (int i = 0; i < Row::orderNum; i++)
		{

			int ind = Row::orderIndeces[i];
			if (*vals[ind] < *other.vals[ind])
			{
				return 1 - Row::orderDirections[i];
			}
			if (*other.vals[ind] < *vals[ind])
			{
				return Row::orderDirections[i];
			}
		}
		return false;
	}

}
