#include "table.hpp"
#include <fstream>

namespace csvdb
{
	Table::Table(int neededFieldsCount, char** neededFieldsBasePath, int* neededFieldsType,
		int fieldCount, int* fields, 
		int whereField, int whereOp, TableValue* whereConst,
		int fileSize, int fileNum)
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
		this->fieldCount = fieldCount;
		this->fields = fields;
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
		std::ofstream outFile;
		outFile.open(outPath, std::ios::out);
		while(true)
		{
			for(int i = 0; i < neededFieldsCount; ++i)
			{
				columns[i]->getRow();
			}
			if(columns[0]->finished)	break;
			if(whereField != -1 && !columns[whereField]->passedTheWhere)	continue;
			for(int i = 0; i < fieldCount; ++i)
			{
				outFile << *columns[fields[i]]->lastVal;
				if(i == fieldCount - 1)
					outFile << std::endl;
				else
					outFile << ',';
			}
		}
		outFile.close();
	}
}