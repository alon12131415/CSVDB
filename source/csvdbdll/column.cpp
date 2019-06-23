#include "column.hpp"
#include <string>
#include "tableValues\tableint.hpp"
#include "tableValues\tabletimestamp.hpp"
#include "tableValues\tablevarchar.hpp"
#include "tableValues\tablefloat.hpp"
#include "tableValues\tablevalue.hpp"
#include <iostream>

const std::string kPathSeparator =
#ifdef _WIN32
    "\\";
#else
    "/";
#endif


namespace csvdb
{
	Column::Column(char* path, TableValueType fieldType, int _file_sizes, int _file_num)
	{
		file_sizes = _file_sizes;
		file_num = _file_num;
		base_file_path = std::string(path) + kPathSeparator;
		fileMode = std::ios::in | std::ios::binary;
		int bufSize = 1048576;
		myBuff = new char[bufSize];
		currFile.rdbuf()->pubsetbuf(myBuff, bufSize);
		currFile.open(base_file_path + "0.ga", fileMode);
		this->fieldType = fieldType;
		refreshLastVal();
	}
	void Column::refreshLastVal()
	{
		switch (fieldType)
		{
		case(csvdbInt):
			lastVal = new TableInt();
			break;
		case(csvdbVarchar):
			lastVal = new TableVarchar();
			break;
		case(csvdbFloat):
			lastVal = new TableFloat();
			break;
		case(csvdbTimestamp):
			lastVal = new TableTimestamp();
			break;
		}
	}
	Column::~Column()
	{
		currFile.close();
		delete lastVal;
		delete whereConst;
		delete[] myBuff;
	}
	void Column::setOp(WhereOperand op_)
	{
		op = op_;
	}
	void Column::setWhereConst(TableValue* whereConst_)
	{
		whereConst = whereConst_;
	}
	void Column::setFP(int index)
	{
		currFile.close();
		currFile.open(base_file_path + kPathSeparator + std::to_string(index) + ".ga", fileMode);
	}
	void Column::getRow()
	{
		if (current_batch > file_sizes - 1){
			current_batch = 0;
			current_fp_index++;
			if (current_fp_index >= file_num){
				finished = true;
				passedTheWhere = true;
				return;
			}
			setFP(current_fp_index);
		}
		currFile >> *lastVal;
		passedTheWhere = lastVal->satisfiesWhere(op, whereConst);
		finished = currFile.eof();
		current_batch++;
	}
}
