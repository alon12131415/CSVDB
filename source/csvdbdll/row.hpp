#ifndef CSVDB_ROW_H
#define CSVDB_ROW_H
#include "tablevalue.hpp"
#include <algorithm>

namespace csvdb
{
	enum writeMode {CSV, NSV};
	class Row
	{

	public:
		Row(TableValue** _vals, int _len) : vals(_vals), len(_len) {};
		writeMode mode = CSV;
		TableValue** vals;
		static int orderNum;
		static int* orderIndeces;
		static int* orderDirections;
		bool operator<(const Row&) const;
	private:
		int len;

	friend std::ofstream& operator<<(std::ofstream&, const Row&);
	};
}

#endif
