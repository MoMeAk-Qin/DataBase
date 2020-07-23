/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"
#include <string>
#include <sstream>
#include <iostream>
#include <string.h>
#include <file_iterator.h>
#include <page_iterator.h>
#include "exceptions/invalid_page_exception.h"
#include "exceptions/invalid_record_exception.h"

using namespace std;

namespace badgerdb {

static string long2bytes(long a){
 string res = "";
 for(int i = 0; i < 8; ++i) res += (unsigned char)a >> ((7 - i) * 8);
 return res;
}

static string int2bytes(int a){
  unsigned char b1 = (unsigned char)a >> 24;
  unsigned char b2 = (unsigned char)a >> 16;
  unsigned char b3 = (unsigned char)a >> 8;
  unsigned char b4 = (unsigned char)a;
  string res(4, 'a');
  res[0] = b1;
  res[1] = b2;
  res[2] = b3;
  res[3] = b4;
  return res;
}

static vector<string> split(string s, char c){
    vector<string> res;
    int i = 0, j, k;
    string cur = "";
    while(s[i]){
        if(s[i] == c){
            /*remove strings with length 0 and strings which begin with space*/
            if(cur != "") {
                j = 0, k = cur.size()-1;
                while(cur[j] == ' ') j++;
                while(cur[k] == ' ') k--;
                res.push_back(cur.substr(j, k-j+1));
            }
            cur = "";
            i++;
            continue;
        }
        cur += s[i];
        i++;
    }
    if(cur != "") {
                j = 0, k = cur.size()-1;
                while(cur[j] == ' ') j++;
                while(cur[k] == ' ') k--;
                res.push_back(cur.substr(j, k-j+1));
            }


    return res;
  }

RecordId HeapFileManager::insertTuple(const string& tuple,
                                      File& file,
                                      BufMgr* bufMgr) {
  RecordId recordId;
  bool foundSpace;
  Page *pagePtr;
  PageId  pageId;
  Page currentPage;
  for(FileIterator iter = file.begin(); iter!=file.end(); iter++){
      currentPage = *iter;
      pageId = currentPage.page_number();
      // 查询当前page是否有spaceforRecord
      bufMgr->readPage(&file, pageId, pagePtr);
      if(pagePtr->hasSpaceForRecord(tuple)){
          recordId = pagePtr->insertRecord(tuple);
          bufMgr->unPinPage(&file, pageId, true);
          foundSpace = true; // 找到空间,插入记录成功
          break;
      }
      bufMgr->unPinPage(&file, pageId, false); // 当前读入页空间不足, 同样需要unpin
  }
  if(!foundSpace){
      // 之前的页都没用空间, bufMgr 需要为file 分配新页
      bufMgr->allocPage(&file, pageId, pagePtr);
      recordId = pagePtr->insertRecord(tuple);
      bufMgr->unPinPage(&file, pageId, true);
  }
    return recordId;
}

void HeapFileManager::deleteTuple(const RecordId& rid,
                                  File& file,
                                  BufMgr* bugMgr) {
  Page currentPage;
  PageId pageId;
  Page* pagePtr;
  bool isDeleted = false;
  for(FileIterator iter = file.begin(); iter!=file.end();iter++){
      currentPage = *iter;
      pageId = currentPage.page_number();
      // 如果找到和rid.pageid相同的,从bufMgr读该page
      if(rid.page_number==pageId){
          bugMgr->readPage(&file, pageId, pagePtr);
          pagePtr->deleteRecord(rid);
          bugMgr->unPinPage(&file, pageId, true);
          isDeleted = true;
      }
  }

  if(!isDeleted){
        cout<<"Delete Record failed!"<<endl;
  }
}

string HeapFileManager::createTupleFromSQLStatement(const string& sql,
                                                    const Catalog* catalog) {
  string tableName = split(sql, ' ')[2];
  TableId tid = catalog->getTableId(tableName);
  TableSchema tableSchema = catalog->getTableSchema(tid);
  string content = split(split(sql, '(')[1], ')')[0];
  vector<string> attrs = split(content, ',');
  string head;
  head += long2bytes((long)&tableSchema);
  int len = 9;
  string data = "";
  int maxSize;
  for(int i = 0; i < attrs.size(); ++i){
    maxSize = tableSchema.getAttrMaxSize(i);
    len += maxSize;
    switch(tableSchema.getAttrType(i)){
        case(INT):
            data += int2bytes(atoi(attrs[i].c_str()));
            break;
        case(CHAR):
            for(int j = maxSize; j > attrs[i].size() - 2; --j)
            {
                data += '\0';
            }
            data += attrs[i].substr(1, attrs[i].size()-2);
            break;
        case(VARCHAR):
            for(int j = maxSize; j > attrs[i].size() - 2; --j)
            {
                data += '\0';
            }
            data += attrs[i].substr(1, attrs[i].size()-2);
            break;
    }
  }
  head += (unsigned char)(len);
  return head + data;
}
}  // namespace badgerdb