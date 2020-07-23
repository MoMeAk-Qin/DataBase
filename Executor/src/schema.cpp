/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"

#include <string>
#include <iostream>
#include <string.h>

using namespace std;

namespace badgerdb {

TableSchema TableSchema::fromSQLStatement(const string& sql) {
  string tableName;
  vector<Attribute> attrs;
  bool isTemp = false;
  vector<string> words;
  /*spilt the string by " (),;"*/
  const char* sep = " (),;";
  char* c_sql = (char*)sql.c_str();
  char* word = strtok(c_sql, sep);
  while (word)
  {
    words.push_back(word);
    word = strtok(NULL, sep);
  }
  std::size_t index = 0;
  while (index < words.size())
  {
    /*create table save table name*/
    if (words[index] == "CREATE")
    {
      index++;
    }
    else if (words[index] == "TABLE")
    {
      index++;
      tableName = words[index];
      index++;
    }
    else /*add attribute*/
    {
      string attrName = words[index];
      DataType attrType;
      int maxSize;
      bool isUnique = false;
      bool isNotNull = false;
      index++;
      if (words[index] == "INT")
      {
        attrType = INT;
        index++;
        maxSize = 4;
      }
      else
      {
        if (words[index] == "CHAR")
        {
          attrType = CHAR;
        }
        else
        {
          attrType = VARCHAR;
        }
        index++;
        maxSize = atoi(words[index].c_str());
        index++;
      }
      if (words[index] == "UNIQUE")
      {
        isUnique = true;
        index++;
      }
      if (words[index] == "NOT")
      {
        isNotNull = true;
        index += 2;
      }
      Attribute temp(attrName, attrType, maxSize, isUnique, isNotNull);
      attrs.push_back(temp);
    }
  }
  return TableSchema(tableName, attrs, isTemp);
}

void TableSchema::print() const {
  std::cout << "Table Name: " << tableName << std::endl;
  for (int i = 0; i < attrs.size(); i++)
  {
    Attribute temp = attrs[i];
    int num = 0;
    while (num < temp.attrName.size())
    {
      cout << temp.attrName[i];
      num++;
    }
    while (num < 10)
    {
      cout << ' ';
      num++;
    }
    cout << '|';
    if (temp.attrType == INT)
    {
      cout << "INT" << "       |";
    }
    else if (temp.attrType == CHAR)
    {
      cout << "CHAR" << "(" << temp.maxSize << ")" << "   |";
    }
    else
    {
      cout << "VARCHAR" << "(" << temp.maxSize << ")" << "|";
    }
    if (temp.isNotNull == true)
    {
      cout << "Not Null" << "  |";
    }
    else
    {
      cout << "Null" << "      |";
    }
    if (temp.isUnique == true)
    {
      cout << "Unique" << "    |";
    }
    else
    {
      cout << "Not Unique" << "|";
    }
  }
}

}  // namespace badgerdb