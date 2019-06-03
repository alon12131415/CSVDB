#include "table.hpp"
#include "row.hpp"
#include "tableint.hpp"
#include "tabletimestamp.hpp"
#include "tablevarchar.hpp"
#include "tablefloat.hpp"
#include "tablevalue.hpp"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include <sstream>

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
		int* orderDirections, char* basePath)
	{
		columns = new Column*[neededFieldsCount];
		for(int i = 0; i < neededFieldsCount; ++i)
		{
			columns[i] = new Column(neededFieldsBasePath[i], (TableValueType)neededFieldsType[i], fileSize, fileNum);
		}
		this->neededFieldsCount = neededFieldsCount;
		if((WhereOperand)whereOp != WhereOperand::none)
		{
			columns[whereField]->setOp((WhereOperand)whereOp);
			columns[whereField]->setWhereConst(whereConst);
			this->whereField = whereField;
		}
		this->neededFieldsType = neededFieldsType;
		this->fileSize = fileSize;
		this->fileNum = fileNum;
		this->fieldCount = fieldCount;
		this->fields = fields;
		this->orderNum = orderNum;
		this->orderIndeces = orderIndeces;
		this->orderDirections = orderDirections;
		this->basePath = std::string(basePath);
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
	void Table::select(char* outPath)
	{
		if (orderNum > 0)
		{
			selectOrder(outPath);
			return;
		}
		std::ofstream outFile;
		outFile.open(outPath, std::ios::out);
		outFile.precision(9);

		TableValue** vals = new TableValue*[fieldCount];
		while(true)
		{
			for(int i = 0; i < neededFieldsCount; ++i)
			{
				columns[i]->getRow();
			}
			if(columns[0]->finished)	break;
			if(whereField != -1 && !columns[whereField]->passedTheWhere)	continue;

			for (int i = 0; i < fieldCount; i++)
			{
				vals[i] = columns[fields[i]]->lastVal;
			}
			Row row = Row(vals, fieldCount);
			outFile << row;
		}
		delete[] vals;
		outFile.close();
	}
	void Table::selectOrder(char* outPath)
	{
		std::vector<std::string> fpList;
		for (int i = 0; i < fileNum; i++)
		{
			std::cout << "sorting file: " << i << std::endl;
			fpList.push_back(sortFile(i));
		}
		std::string out = mergeFiles(fpList, 0, fpList.size(), 0);
		std::rename(out.c_str(), outPath);
	}

	std::string Table::sortFile(int fileIndex)
	{
		std::string fileName = basePath + "tmp" + std::to_string(fileIndex);
		std::vector<Row> rows;
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
				vals[i] = columns[fields[i]]->lastVal;
				columns[fields[i]]->refreshLastVal(); //else val is overwritten on next row
			}
			Row* row = new Row(vals, fieldCount);
			// row->mode = writeMode::NSV;
			rows.push_back(*row);
		}
		std::cout << "starting to sort rows" << std::endl;
		std::sort(rows.begin(), rows.end());
		std::cout << "starting to sort the rows" << std::endl;
		std::ofstream outFile;
		outFile.open(fileName);
		outFile.precision(9);
		for (const Row& row : rows){
			outFile << row;
		}
		outFile.close();
		for (const Row& row : rows){
			for (int i = 0; i < fieldCount; i++)
			{
				delete row.vals[i];
			}
			delete[] row.vals;
			// delete row;
		}
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
		out.open(outFilePath);
		std::ifstream reader1, reader2;
		reader1.open(file1, std::ios::in | std::ios::binary);
		reader2.open(file2, std::ios::in | std::ios::binary);
		Row row1 = getRowFromNSV(reader1);
		Row row2 = getRowFromNSV(reader2);
		while (true){
			if (row1 < row2){
				out << row1;
				row1 = getRowFromNSV(reader1);
				if (reader1.eof()){
					while (!reader2.eof()){
						out << row2;
						row2 = getRowFromNSV(reader2);
					}
					break;
				}
			} else {
				out << row2;
				row2 = getRowFromNSV(reader2);
				if (reader2.eof()){
					while (!reader1.eof()){
						out << row1;
						row1 = getRowFromNSV(reader1);
					}
					break;
				}
			}
		}
		out.close();
		reader1.close();
		reader2.close();
		return outFilePath;
	}

	Row Table::getRowFromNSV(std::ifstream& file){
		std::string helper;
		TableValue** vals = new TableValue*[fieldCount];
		std::getline(file, helper);
		std::stringstream ss(helper);
		for (int i = 0; i < fieldCount; i++)
		{
			std::getline(ss, helper, ',');
			if (file.eof())
				return Row({}, 0);
			TableValueType type = (TableValueType)neededFieldsType[fields[i]];
			TableValue* val;
			switch (type)
			{
			case(TableValueType::csvdbInt):
				val = new TableInt(helper);
				break;
			case(TableValueType::csvdbVarchar):
				val = new TableVarchar(helper);
				break;
			case(TableValueType::csvdbFloat):
				val = new TableFloat(helper);
				break;
			case(TableValueType::csvdbTimestamp):
				val = new TableTimestamp(helper);
				break;
			}
			// file.ignore(1);
			vals[i] = val;
		}
		return Row(vals, fieldCount);
	}

}
