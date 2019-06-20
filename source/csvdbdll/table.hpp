#ifndef CSVDB_TABLE_H
#define CSVDB_TABLE_H

#include "column.hpp"
#include "row.hpp"
#include "aggregator.hpp"
#include <vector>

namespace csvdb
{


	enum aggTypes {min = 0, max = 1, sum = 2, count = 3, avg = 4};

	class Table

	{
	public:
		Table(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldsCount, int* fields,
		int whereField, int whereOp, TableValue* whereConst,
		int fileSize, int fileNum,
		int orderNum, int* orderIndeces,
		int* orderDirections, char* basePath,
		int groupNum, int* groupIndeces,
		int aggsNum, int* aggsTypes, int* aggsFields,
		int havingField, int havingOp, csvdb::TableValue* havingConst);
		~Table();
		void select(const char*);
	private:
		void selectOrder(const char*);
		void selectGroup(const char*);
		bool operator() (const Row&, const Row&);
		std::vector<Row*> requestOrder();
		void writeGroup(TableValue**, Aggregator**, std::ofstream&);
		void updateAggs(Aggregator** ,Row*);
		void resetAggs(Aggregator**);
		void writeRow(Row*, std::ofstream&);
		void flushRows();
		std::string sortFile(int);
		std::string mergeFiles(const std::vector<std::string>&, int, int, int);
		std::string merge(std::string, std::string, int);
		std::string getValue(std::ifstream&);
		void getRowFromCSV(Row*, std::ifstream&);
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
		int groupNum;
		int* groupIndeces;
		int aggsNum;
		int* aggsTypes;
		int* aggsFields;
		int havingOp;
		TableValue* havingConst;
		int havingField;
		std::string fileNameToWrite = "ordertmp";
		int filesWrittenNum = 0;
		std::vector<std::string> filesWritten;
		std::vector<Row*> rowsToWrite;
		std::string basePath;
	};
}

#endif
