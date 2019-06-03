#ifndef CSVDB_TABLE_H
#define CSVDB_TABLE_H

#include "column.hpp"
#include "row.hpp"
#include <vector>

namespace csvdb
{


	class Table

	{
	public:
		Table(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldsCount, int* fields,
		int whereField, int whereOp, TableValue* whereConst,
		int fileSize, int fileNum,
		int orderNum, int* orderIndeces,
		int* orderDirections, char* basePath);
		~Table();
		void select(char* outPath);
	private:
		void selectOrder(char* outPath);
		bool operator() (const Row&, const Row&);
		std::string sortFile(int);
		std::string mergeFiles(const std::vector<std::string>&, int, int, int);
		std::string merge(std::string, std::string, int);
		Row getRowFromNSV(std::ifstream&);
		Column** columns = nullptr;
		int* neededFieldsType;
		int neededFieldsCount;
		int whereField = -1;
		int fieldCount;
		int* fields;
		int fileSize;
		int fileNum;
		int orderNum;
		int* orderIndeces;
		int* orderDirections;
		std::string basePath;
	};
}

#endif
