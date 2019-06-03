#ifndef CSVDB_TABLEVALUE_H
#define CSVDB_TABLEVALUE_H
#include <string>

namespace csvdb
{
	enum WhereOperand {less = 0, lessEqual = 1, greater = 2, greaterEqual = 3, equal = 4,
						notEqual = 5, isNull = 6, isNotNull = 7, none = -1};

	class TableValue
	{
	public:
		virtual bool operator<(const TableValue&) const = 0;
		virtual bool operator<=(const TableValue&) const = 0;
		virtual bool operator>(const TableValue&) const = 0;
		virtual bool operator>=(const TableValue&) const = 0;
		virtual bool operator==(const TableValue&) const = 0;
		virtual bool operator!=(const TableValue&) const = 0;
		virtual bool isNull() const = 0;
		virtual std::string getValue() const = 0;
		bool satisfiesWhere(WhereOperand, TableValue const *) const;
	private:
		virtual std::ifstream& readFromStream(std::ifstream&) = 0;
		virtual std::ofstream& writeToCSV(std::ofstream&) = 0;
		friend std::ifstream& operator>>(std::ifstream&, TableValue&);
		friend std::ofstream& operator<<(std::ofstream&, TableValue&);
	};

	std::ifstream& operator>>(std::ifstream&, TableValue&);
	std::ofstream& operator<<(std::ofstream&, TableValue&);
}

#endif
