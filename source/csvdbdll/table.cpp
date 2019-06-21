#include "table.hpp"
#include "row.hpp"
#include "tableint.hpp"
#include "tablevarchar.hpp"
#include "tabletimestamp.hpp"
#include "tablefloat.hpp"
#include "tablevalue.hpp"
#include "aggregator.hpp"
#include "aggs.hpp"
#include <fstream>
#include <iostream>
#include <parallel/algorithm>
#include <vector>
#include <sstream>
#include <chrono>

namespace csvdb
{
	int Row::orderNum = 0;
	int* Row::orderIndeces = nullptr;
	int* Row::orderDirections = nullptr;
	Table::Table(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldCount, int* fields,
		int whereField, int whereOp, TableValue* whereConst,
		int fileSize, int fileNum,
		int orderNum, int* orderIndeces,
		int* orderDirections, char* basePath,
		int groupNum, int* groupIndeces,
		int aggsNum, int* aggsTypes, int* aggsFields,
		int havingField, int havingOp, csvdb::TableValue* havingConst)
	{
		columns = new Column*[neededFieldsCount];
		for(int i = 0; i < neededFieldsCount; ++i)
		{
			columns[i] = new Column(neededFieldsBasePath[i], (TableValueType)neededFieldsType[i], fileSize, fileNum);
		}
		if((WhereOperand)whereOp != WhereOperand::none)
		{
			columns[whereField]->setOp((WhereOperand)whereOp);
			columns[whereField]->setWhereConst(whereConst);
		}
		this->havingField = havingField;
		this->havingOp = havingOp;
		this->havingConst = havingConst;
		this->neededFieldsCount = neededFieldsCount;
		this->whereField = whereField;
		this->neededFieldsType = neededFieldsType;
		this->fileSize = fileSize;
		this->fileNum = fileNum;
		this->fieldCount = fieldCount;
		this->fields = fields;
		this->orderNum = orderNum;
		this->orderIndeces = orderIndeces;
		this->orderDirections = orderDirections;
		this->basePath = std::string(basePath);
		this->groupNum = groupNum;
		this->groupIndeces = groupIndeces;
		this->aggsNum = aggsNum;
		this->aggsTypes = aggsTypes;
		this->aggsFields = aggsFields;
		Row::orderNum = orderNum;
		Row::orderIndeces = orderIndeces;
		Row::orderDirections = orderDirections;
	}
	Table::~Table()
	{
		for(int i = 0; i < neededFieldsCount; ++i)
		{
			delete columns[i];
		}
		delete[] columns;
	}
	void Table::select(const char* outPath)
	{
		if (aggsNum > 0)
		{
			selectGroup(outPath);
			return;
		}
		if (orderNum > 0)
		{
			selectOrder(outPath);
			return;
		}
		std::ofstream outFile;
		outFile.open(outPath, std::ios::out);
		outFile.precision(9);

		while(true)
		{
			TableValue** vals = new TableValue*[fieldCount];
			for(int i = 0; i < neededFieldsCount; ++i)
			{
				columns[i]->getRow();
			}
			if(columns[0]->finished)	break;
			if(whereField != -1 && !columns[whereField]->passedTheWhere)	continue;

			for (int i = 0; i < fieldCount; i++)
			{
				vals[i] = columns[fields[i]]->lastVal->clone();
			}
			Row row = Row(vals, fieldCount);
			outFile << row;
		}
		outFile.close();
	}
	void Table::selectGroup(const char* outPath)
	{

		std::vector<Row*> rows = requestOrder();
		std::ifstream currFile;
		std::ofstream outFile;
		outFile.open(outPath, std::ios::out | std::ios::binary);
		Row* row;
		if (rows.size() == 0)
		{
			currFile.open(basePath + "atmp.csv", std::ios::in | std::ios::binary);
			int bufSize = 1048576;
			char* myBuff = new char[bufSize];
			currFile.rdbuf()->pubsetbuf(myBuff, bufSize);
			row = new Row(new TableValue*[fieldCount] {nullptr}, fieldCount);
		}
		else
		{
			row = nullptr;
		}
		TableValue** prevGroup = groupNum == 0 ? nullptr : new TableValue*[groupNum] {nullptr};

		Aggregator** aggregators = new Aggregator*[aggsNum];
		for (int i = 0; i < aggsNum; i++)
		{
			aggTypes type = (aggTypes) aggsTypes[i];
			switch (type)
			{
				case (min):
				aggregators[i] = new minAgg();
				break;
				case (max):
				aggregators[i] = new maxAgg();
				break;
				case (sum):
				aggregators[i] = new sumAgg();
				break;
				case (count):
				aggregators[i] = new countAgg();
				break;
				case (avg):
				aggregators[i] = new avgAgg();
				break;
			}
		}
		if (rows.size() == 0)
		{
			getRowFromCSV(row, currFile);
		}
		else
		{
			row = rows[0];
		}
		if (currFile.eof())
		{
			outFile.close();
			currFile.close();
			delete row;
			return;
		}
		for (int i = 0; i < groupNum; i++)
		{
			prevGroup[i] = row->vals[groupIndeces[i]]->clone();
		}
		int i = 0;
		while ((rows.size() == 0 && !currFile.eof()) || i++ < rows.size())
		{
			bool changedGroup = false;
			for (int i = 0; i < groupNum; i++)
			{
				if(*row->vals[groupIndeces[i]] != *prevGroup[i])
				{
					changedGroup = groupNum != 0;
					break;
				}
			}
			if (changedGroup)
			{
				writeGroup(prevGroup, aggregators, outFile);
				for (int i = 0; i < groupNum; i++)
				{
					delete prevGroup[i];
					prevGroup[i] = row->vals[groupIndeces[i]]->clone();
				}
				resetAggs(aggregators);
			}
			updateAggs(aggregators, row);
			if (rows.size() == 0)
				getRowFromCSV(row, currFile);
			else
			{
				row = rows[i];
			}
		}
		writeGroup(prevGroup, aggregators, outFile);
		outFile.close();
		currFile.close();
		flushRows();
		if(orderNum > 0)
		{
			std::string out = mergeFiles(filesWritten, 0, filesWritten.size(), 0);
			std::remove(outPath);
			std::rename(out.c_str(), outPath);
		}
		std::remove((basePath + "atmp.csv").c_str());
		if (rows.size() == 0) delete row;
	}

	void Table::writeGroup(TableValue** group, Aggregator** aggs, std::ofstream& os)
	{
		TableValue** vals = new TableValue*[fieldCount] {nullptr};
		for (int i = 0; i < aggsNum; i++)
		{
			vals[aggsFields[i]] = aggs[i]->getTableValue()->clone();
		}
		for (int i = 0; i < groupNum; i++)
		{
			vals[groupIndeces[i]] = group[i]->clone();
		}
		Row* row = new Row(vals, fieldCount);
		if(row->vals[havingField]->satisfiesWhere((WhereOperand)havingOp, havingConst))
		{
			writeRow(row, os);
		}
	}

	void Table::writeRow(Row* row, std::ofstream& os)
	{
		if(orderNum == 0)
		{
			os << *row;
			delete row;
			return;
		}
		rowsToWrite.push_back(row);
		if (rowsToWrite.size() > fileSize)
		{
			flushRows();
		}
	}

	void Table::flushRows()
	{
		if(rowsToWrite.size() == 0)	return;
		if (orderNum > 0) __gnu_parallel::sort(rowsToWrite.begin(), rowsToWrite.end(), [](const Row* row1, const Row* row2) {return *row1 < *row2;});
		std::string fileName = basePath + fileNameToWrite + std::to_string(filesWrittenNum);
		std::ofstream outFile;
		int bufSize = 1048576;
		char* myBuff = new char[bufSize];
		outFile.rdbuf()->pubsetbuf(myBuff, bufSize);
		outFile.open(fileName, std::ios::binary | std::ios::out);
		outFile.precision(9);
		for (const Row* row : rowsToWrite){
			outFile << *row;
			delete row;
		}
		outFile.close();
		delete[] myBuff;
		rowsToWrite.clear();
		filesWrittenNum++;
		filesWritten.push_back(fileName);
	}

	void Table::updateAggs(Aggregator** aggs, Row* row)
	{
		for (int i = 0; i < aggsNum; i++)
		{
			aggs[i]->update(row->vals[aggsFields[i]]);
		}
	}
	void Table::resetAggs(Aggregator** aggs)
	{
		for (int i = 0; i < aggsNum; i++)
		{
			aggs[i]->reset();
		}
	}

	std::vector<Row*> Table::requestOrder()
	{
		std::vector<Row*> rows;
		int orderNum = this->orderNum;
		int* orderIndeces = this->orderIndeces;
		int* orderDirections = this->orderDirections;
		int aggsNum = this->aggsNum;
		this->orderNum = groupNum;
		this->orderIndeces = groupIndeces;
		this->orderDirections = groupNum == 0 ? nullptr : new int[groupNum] {0};
		this->aggsNum = 0;
		Row::orderNum = this->orderNum;
		Row::orderIndeces = this->orderIndeces;
		Row::orderDirections = this->orderDirections;
		if (fileNum > 1)
		{
			select((basePath + "atmp.csv").c_str());
		}
		else
		{
			int fileIndex = 0;
			std::string fileName = basePath + "tmp" + std::to_string(fileIndex);
			// rows.reserve(fileSize);
			for(int i = 0; i < neededFieldsCount; ++i)
			{
				columns[i]->setFP(fileIndex);
			}
			for (int i = 0; i < fileSize; i++)
			{
				TableValue** vals = new TableValue*[fieldCount];
				for(int i = 0; i < neededFieldsCount; ++i)
				{
					columns[i]->getRow();
				}
				if(columns[0]->finished)	break;
				if(whereField != -1 && !columns[whereField]->passedTheWhere)	continue;
				for (int i = 0; i < fieldCount; i++)
				{
					vals[i] = columns[fields[i]]->lastVal->clone();
				}
				Row* row = new Row(vals, fieldCount);
				rows.push_back(row);
			}
			__gnu_parallel::sort(rows.begin(), rows.end(), [](const Row* row1, const Row* row2) {return *row1 < *row2;});
		}
		delete this->orderDirections;
		Row::orderNum = orderNum;
		Row::orderIndeces = orderIndeces;
		Row::orderDirections = orderDirections;
		this->aggsNum = aggsNum;
		this->groupNum = this->orderNum;
		this->orderNum = orderNum;
		this->orderIndeces =  orderIndeces;
		this->orderDirections =  orderDirections;
		return rows;
	}

	void Table::selectOrder(const char* outPath)
	{
		std::vector<std::string> fpList;
		for (int i = 0; i < fileNum; i++)
		{
			fpList.push_back(sortFile(i));
		}
		std::string out = mergeFiles(fpList, 0, fpList.size(), 0);
		std::remove(outPath);
		std::rename(out.c_str(), outPath);
	}

	std::string Table::sortFile(int fileIndex)
	{
		std::string fileName = basePath + "tmp" + std::to_string(fileIndex);
		std::vector<Row*> rows;
		// rows.reserve(fileSize);
		for(int i = 0; i < neededFieldsCount; ++i)
		{
			columns[i]->setFP(fileIndex);
		}
		for (int i = 0; i < fileSize; i++)
		{
			TableValue** vals = new TableValue*[fieldCount];
			for(int i = 0; i < neededFieldsCount; ++i)
			{
				columns[i]->getRow();
			}
			if(columns[0]->finished)	break;
			if(whereField != -1 && !columns[whereField]->passedTheWhere)	continue;
			for (int i = 0; i < fieldCount; i++)
			{
				vals[i] = columns[fields[i]]->lastVal->clone();
			}
			Row* row = new Row(vals, fieldCount);
			rows.push_back(row);
		}
		__gnu_parallel::sort(rows.begin(), rows.end(), [](const Row* row1, const Row* row2) {return *row1 < *row2;});
		std::ofstream outFile;
		int bufSize = 1048576;
		char* myBuff = new char[bufSize];
		outFile.rdbuf()->pubsetbuf(myBuff, bufSize);
		outFile.open(fileName, std::ios::binary | std::ios::out);
		outFile.precision(9);
		for (const Row* row : rows){
			outFile << *row;
			delete row;
		}
		outFile.close();
		delete[] myBuff;
		rows.clear();
		return fileName;
	}

	std::string Table::mergeFiles(const std::vector<std::string>& fpList, int begin, int end, int it)
	{
		if (end - begin == 1){
			return fpList.at(begin);
		}
		if (end - begin == 2){
			return merge(fpList.at(begin),fpList.at(begin + 1),it + 1);
		}

		std::string file1 = mergeFiles(fpList, begin + (end - begin)/2, end, it);
		std::string file2 = mergeFiles(fpList, begin , begin + (end - begin)/2, it);
		return merge(file1, file2,it + 1);

	}

	std::string Table::merge(std::string file1, std::string file2, int it){
		std::string outFilePath = basePath + "tmp_" + std::to_string(it) + "_" + file1.substr(basePath.length() + 3);
		std::ofstream out;
		out.open(outFilePath, std::ios::out | std::ios::binary);
		std::ifstream reader1, reader2;
		reader1.open(file1, std::ios::in | std::ios::binary);
		reader2.open(file2, std::ios::in | std::ios::binary);
		Row* row1 = new Row(new TableValue*[fieldCount] {nullptr}, fieldCount);
		getRowFromCSV(row1, reader1);
		Row* row2 = new Row(new TableValue*[fieldCount] {nullptr}, fieldCount);
		getRowFromCSV(row2, reader2);
		while (true)
		{
			if (*row1 < *row2)
			{
				out << *row1;
				getRowFromCSV(row1, reader1);
				if (reader1.eof())
				{
					while (!reader2.eof())
					{
						out << *row2;
						getRowFromCSV(row2, reader2);
					}
					break;
				}
			}
			else
			{
				out << *row2;
				getRowFromCSV(row2, reader2);
				if (reader2.eof())
				{
					while (!reader1.eof())
					{
						out << *row1;
						getRowFromCSV(row1, reader1);
					}
					break;
				}
			}
		}
		delete row1;
		delete row2;
		out.close();
		reader1.close();
		reader2.close();
		return outFilePath;
	}

	void Table::getRowFromCSV(Row* row, std::ifstream& file)
	{
		for (int i = 0; i < fieldCount; i++)
		{
			std::string valueString = getValue(file);
			if (file.eof())
			{
				return;
			}
			TableValueType type = (TableValueType)neededFieldsType[fields[i]];
			TableValue* val;
			switch (type)
			{
			case(TableValueType::csvdbInt):
				val = new TableInt(valueString);
				break;
			case(TableValueType::csvdbVarchar):
				val = new TableVarchar(valueString);
				break;
			case(TableValueType::csvdbFloat):
				val = new TableFloat(valueString);
				break;
			case(TableValueType::csvdbTimestamp):
				val = new TableTimestamp(valueString);
				break;
			}
			delete row->vals[i];
			row->vals[i] = val;
		}
	}


	std::string Table::getValue(std::ifstream& file)
	{
		std::stringstream ss("");
		char curr;
		file.get(curr);
		if (curr != '"')
		{
			if (curr == '\n') return "";
			ss << curr;
			while(file.get(curr) && curr != ',' && curr != '\n')
			{
				ss << curr;
			}
			return ss.str();
		}
		bool prevBrackets = false;
		while(file.get(curr))
		{
			if (curr == '"')
			{
				if (prevBrackets) ss << curr;
				prevBrackets = !prevBrackets;
				continue;
			}
			if (prevBrackets)
			{
				break;
			}
			ss << curr;
		}
		return ss.str();
	}


}
