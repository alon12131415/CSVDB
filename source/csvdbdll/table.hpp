#ifndef CSVDB_TABLE_H
#define CSVDB_TABLE_H

#include "column.hpp"

namespace csvdb
{
	class Table
	{
	public:
		Table(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldsCount, int* fields, 
		int whereField, int whereOp, TableValue* whereConst,
		int fileSize, int fileNum);
		~Table();
		void select(char* outPath);
	private:
		Column** columns = nullptr;
		int neededFieldsCount;
		int whereField = -1;
		int fieldCount;
		int* fields;
	};
}

#endif