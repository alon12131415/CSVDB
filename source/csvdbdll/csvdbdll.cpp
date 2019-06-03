// csvdbdll.cpp : Defines the exported functions for the DLL application.
//

#include "table.hpp"
#include "column.hpp"
#include "tablevalue.hpp"
#include "tableint.hpp"
#include "tabletimestamp.hpp"
#include "tablevarchar.hpp"
#include "tablefloat.hpp"
#include <iostream>

namespace csvdb
{
	class TableValue;
}

extern "C"
{
	csvdb::Column* Column_new(char* path, int tableValueType, int file_sizes, int file_num)
	{
		return new csvdb::Column(path, (csvdb::TableValueType)tableValueType, file_sizes, file_num);
	}
	void Column_setFP(csvdb::Column* column, int index)
	{
		column->setFP(index);
	}
	void Column_setOp(csvdb::Column* column, int whereOperand)
	{
		column->setOp((csvdb::WhereOperand)whereOperand);
	}
	void Column_setWhereConst(csvdb::Column* column, csvdb::TableValue* whereConst)
	{
		column->setWhereConst(whereConst);
	}
	void Column_getRow(csvdb::Column* column)
	{
		column->getRow();
	}

	int64_t Column_getIntVal(csvdb::Column* column)
	{
		return dynamic_cast<csvdb::TableInt*>(column->lastVal)->getVal();
	}

	uint64_t Column_getTimestampVal(csvdb::Column* column)
	{
		return dynamic_cast<csvdb::TableTimestamp*>(column->lastVal)->getVal();
	}
	char* Column_getVarcharVal(csvdb::Column* column)
	{
		std::string str = dynamic_cast<csvdb::TableVarchar*>(column->lastVal)->getVal();
		char* ca = new char[str.size() + 1];
		std::copy(str.begin(), str.end(), ca);
		ca[str.size()] = '\0';
		return ca;
	}
	void DeleteVarchar(char* c)
	{
		delete[] c;
	}
	double Column_getFloatVal(csvdb::Column* column)
	{
		return dynamic_cast<csvdb::TableFloat*>(column->lastVal)->getVal();
	}

	bool Column_getFinished(csvdb::Column* column)
	{
		return column->finished;
	}
	bool Column_getPassedTheWhere(csvdb::Column* column)
	{
		return column->passedTheWhere;
	}
	bool Column_getIsNull(csvdb::Column* column)
	{
		return column->lastVal->isNull();
	}

	void Column_delete(csvdb::Column* column)
	{
		delete column;
	}

	csvdb::TableValue* CreateIntWhereConst(int val)
	{
		return new csvdb::TableInt(val);
	}

	csvdb::TableValue* CreateTimestampWhereConst(int val)
	{
		return new csvdb::TableTimestamp(val);
	}

	csvdb::TableValue* CreateVarcharWhereConst(char* val)
	{
		return new csvdb::TableVarchar(val);
	}

	csvdb::TableValue* CreateFloatWhereConst(double val)
	{
		return new csvdb::TableFloat(val);
	}

	csvdb::Table* Table_Create(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldsCount, int* fields,
		int whereField, int whereOp, csvdb::TableValue* whereConst,
		int fileSize, int fileNum,
		int orderNum, int* orderIndeces,
		int* orderDirections, char* basePath)
	{
		return new csvdb::Table(neededFieldsCount, neededFieldsBasePath, neededFieldsType,
		fieldsCount, fields,
		whereField, whereOp, whereConst,
		fileSize, fileNum,
		orderNum, orderIndeces,
		orderDirections, basePath);
	}

	void Table_select(csvdb::Table* table, char* outPath)
	{
		table->select(outPath);
	}

	void Table_delete(csvdb::Table* table)
	{
		delete table;
	}
}
