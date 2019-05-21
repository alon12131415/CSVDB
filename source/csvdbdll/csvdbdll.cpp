// csvdbdll.cpp : Defines the exported functions for the DLL application.
//

#include "column.hpp"
#include "tablevalue.hpp"
#include "tableint.hpp"
#include "tablevarchar.hpp"
#include "tablefloat.hpp"
#include <iostream>

namespace csvdb
{
	class TableValue;
}

extern "C"
{
	__declspec(dllexport) csvdb::Column* Column_new(char* path, int tableValueType)
	{
		return new csvdb::Column(path, (csvdb::TableValueType)tableValueType);
	}
	__declspec(dllexport) void Column_setFP(csvdb::Column* column, char* path)
	{
		column->setFP(path);
	}
	__declspec(dllexport) void Column_setOp(csvdb::Column* column, int whereOperand)
	{
		column->setOp((csvdb::WhereOperand)whereOperand);
	}
	__declspec(dllexport) void Column_setWhereConst(csvdb::Column* column, csvdb::TableValue* whereConst)
	{
		column->setWhereConst(whereConst);
	}
	__declspec(dllexport) void Column_getRow(csvdb::Column* column)
	{
		column->getRow();
	}

	__declspec(dllexport) int64_t Column_getIntVal(csvdb::Column* column)
	{
		return dynamic_cast<csvdb::TableInt*>(column->lastVal)->getVal();
	}
	__declspec(dllexport) char* Column_getVarcharVal(csvdb::Column* column)
	{
		std::string str = dynamic_cast<csvdb::TableVarchar*>(column->lastVal)->getVal();
		char* ca = new char[str.size() + 1];
		std::copy(str.begin(), str.end(), ca);
		ca[str.size()] = '\0';
		return ca;
	}
	__declspec(dllexport) void DeleteVarchar(char* c)
	{
		delete[] c;
	}
	__declspec(dllexport) double Column_getFloatVal(csvdb::Column* column)
	{
		return dynamic_cast<csvdb::TableFloat*>(column->lastVal)->getVal();
	}

	__declspec(dllexport) bool Column_getFinished(csvdb::Column* column)
	{
		return column->finished;
	}
	__declspec(dllexport) bool Column_getPassedTheWhere(csvdb::Column* column)
	{
		return column->passedTheWhere;
	}
	__declspec(dllexport) bool Column_getIsNull(csvdb::Column* column)
	{
		return column->lastVal->isNull();
	}

	__declspec(dllexport) void Column_delete(csvdb::Column* column)
	{
		delete column;
	}

	__declspec(dllexport) csvdb::TableValue* CreateIntWhereConst(int val)
	{
		return new csvdb::TableInt(val);
	}

	__declspec(dllexport) csvdb::TableValue* CreateVarcharWhereConst(char* val)
	{
		return new csvdb::TableVarchar(val);
	}
	
	__declspec(dllexport) csvdb::TableValue* CreateFloatWhereConst(double val)
	{
		return new csvdb::TableFloat(val);
	}
}