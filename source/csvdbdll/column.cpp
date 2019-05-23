#include "column.hpp"
#include <string>
#include "tableint.hpp"
#include "tabletimestamp.hpp"
#include "tablevarchar.hpp"
#include "tablefloat.hpp"
#include "tablevalue.hpp"
#include <iostream>

namespace csvdb
{
	Column::Column(char* path, TableValueType fieldType)
	{
		fileMode = std::ios::in | std::ios::binary;
		currFile.open(path, fileMode);
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
	}
	void Column::setOp(WhereOperand op_)
	{
		op = op_;
	}
	void Column::setWhereConst(TableValue* whereConst_)
	{
		whereConst = whereConst_;
	}
	void Column::setFP(char* newPath)
	{
		currFile.close();
		currFile.open(newPath, fileMode);
	}
	void Column::getRow()
	{
		currFile >> *lastVal;
		passedTheWhere = lastVal->satisfiesWhere(op, whereConst);
		finished = currFile.eof();
	}
}