#ifndef CSVDB_COLUMN_H
#define CSVDB_COLUMN_H

#include <fstream>
#include <string>
#include "tablevalue.hpp"

namespace csvdb
{
	class TableValue;

	enum TableValueType {csvdbInt = 0, csvdbVarchar = 1, csvdbFloat = 2, csvdbTimestamp = 3};
	
	class Column
	{
	public:
		Column(char*, TableValueType);
		~Column();
		void setOp(WhereOperand);
		void setWhereConst(TableValue*);
		void setFP(char*);
		void getRow();
		bool finished = false;
		bool passedTheWhere;
		int fileMode;
		WhereOperand op = WhereOperand::none;
		TableValue* whereConst;
		TableValue* lastVal;
		std::ifstream currFile;
	};
}

#endif