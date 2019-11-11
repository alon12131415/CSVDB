#ifndef CSVDB_ROW_H
#define CSVDB_ROW_H
#include "tableValues/tablevalue.hpp"
#include <algorithm>

namespace csvdb
{
	enum writeMode {CSV, NSV};
	class Row
	{

	public:
		Row(TableValue** _vals, int _len) : vals(_vals), len(_len) {};
		~Row();
		writeMode mode = CSV;
		TableValue** vals = nullptr;
		static int orderNum;
		static int* orderIndeces;
		static int* orderDirections;
		bool operator<(const Row&) const;
		int len = 0;

	friend std::ofstream& operator<<(std::ofstream&, const Row&);
	};
}

#endif
