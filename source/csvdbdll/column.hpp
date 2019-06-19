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
		Column(char*, TableValueType, int, int);
		~Column();
		void setOp(WhereOperand);
		void setWhereConst(TableValue*);
		void setFP(int);
		void getRow();
		void refreshLastVal();
		bool finished = false;
		TableValue* lastVal = nullptr;
		bool passedTheWhere;
	private:
		TableValueType fieldType;
		char* myBuff;
		int current_batch = 0;
		int current_fp_index = 0;
		int file_sizes;
		int file_num;
		std::string base_file_path;
		std::ios_base::openmode fileMode;
		WhereOperand op = WhereOperand::none;
		TableValue* whereConst = nullptr;
		std::ifstream currFile;
	};
}

#endif
