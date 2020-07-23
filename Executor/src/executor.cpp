/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>
#include <exception>
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_record_exception.h"
#include "storage.h"

using namespace std;

namespace badgerdb
{
static string int2str(int a){
    string temp = "";
    if(!a) return "0";
    int n = 0;
    while(a){
        temp += '0' + a % 10;
        a /= 10;
        n++;
    }
    string res = temp;
    for(int i = 0; i < n; ++i){
        res[i] = temp[n - i - 1];
    }
    return res;
}

static int unsigned bytes2int(string a){
    return (a[0] << 24) + (a[1] << 16) + (a[2] << 8) + a[3];
}

static string translate(string a, TableSchema _tableSchema){
    int i = 9;
    int maxSize;
    string ret = "";
    for(int j = 0; j < _tableSchema.getAttrCount(); ++j){
        maxSize = _tableSchema.getAttrMaxSize(j);
        switch(_tableSchema.getAttrType(j)){
            case(INT):
                ret += int2str(bytes2int(a.substr(i, maxSize)));
                break;
            case(CHAR):
            case(VARCHAR):
                while(!a[i]) i++, maxSize--;
                ret += a.substr(i, maxSize);

        }
        i += maxSize;
        ret += '\t';
    }
    return ret.substr(0, ret.size() - 1);
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

void TableScanner::print()
{
    int page_number = tableFile.readHeader().num_pages;
    Page* page;
    string cur;
    int j;
    cout << tableFile.filename() << endl;
    for(int i = 1; i < page_number; ++i){
        try{
            bufMgr->readPage(&tableFile, i, page);
            for(j = 1;; ++j){
                cur = translate(page->getRecord({i, j}), tableSchema);
                cout << cur << endl;
            }
            bufMgr->unPinPage(&tableFile, i, false);
        }
        catch(InvalidRecordException e){
            bufMgr->unPinPage(&tableFile, i, false);
        }
    }
    bufMgr->flushFile(&tableFile);
}

JoinOperator::JoinOperator(const File& leftTableFile,
                           const File& rightTableFile,
                           const TableSchema& leftTableSchema,
                           const TableSchema& rightTableSchema,
                           const Catalog* catalog,
                           BufMgr* bufMgr)
    : leftTableFile(leftTableFile),
      rightTableFile(rightTableFile),
      leftTableSchema(leftTableSchema),
      rightTableSchema(rightTableSchema),
      resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)),
      catalog(catalog),
      bufMgr(bufMgr),
      isComplete(false)
{
    // nothing
}

TableSchema JoinOperator::createResultTableSchema(
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema)
{
    vector<Attribute> attrs;
    string attrname;
    int left_count = leftTableSchema.getAttrCount();
    int right_count = rightTableSchema.getAttrCount();
    for(int i = 0; i < left_count; ++i) 
    {
        attrs.push_back(Attribute(leftTableSchema.getAttrName(i),
                                    leftTableSchema.getAttrType(i),
                                    leftTableSchema.getAttrMaxSize(i),
                                    leftTableSchema.isAttrNotNull(i),
                                    leftTableSchema.isAttrUnique(i)));
    }
    for(int i = 0; i < right_count; ++i)
    {
        attrname = rightTableSchema.getAttrName(i);
        if(!leftTableSchema.hasAttr(rightTableSchema.getAttrName(i)))
        {
            attrs.push_back(Attribute(attrname, 
                                        rightTableSchema.getAttrType(i),
                                        rightTableSchema.getAttrMaxSize(i),
                                        rightTableSchema.isAttrNotNull(i),
                                        rightTableSchema.isAttrUnique(i)));
        }
    }
    return TableSchema("TEMP_TABLE", attrs, true);
}

void JoinOperator::printRunningStats() const
{
    cout << "# Result Tuples: " << numResultTuples << endl;
    cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
    cout << "# I/Os: " << numIOs << endl;
}

bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile)
{   
    vector<int> lefattrindex;
    vector<int> rigattrindex;
    int left_count = leftTableSchema.getAttrCount();
    for(int i = 0; i < left_count; ++i){
        if(rightTableSchema.hasAttr(leftTableSchema.getAttrName(i))){
            rigattrindex.push_back(rightTableSchema.getAttrNum(leftTableSchema.getAttrName(i)));
            lefattrindex.push_back(i);
        }
    }
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    int left_page_number = leftTableFile.readHeader().num_pages;
    int right_page_number = rightTableFile.readHeader().num_pages;
    int i, j, k, l;
    Page* pageID[left_page_number+1];
    File* left_file = const_cast<File*>(&leftTableFile);
    File* right_file = const_cast<File*>(&rightTableFile);
    for(i = 1; i < left_page_number; ++i)
    {
        bufMgr->readPage(left_file, i, pageID[i]);
        numUsedBufPages++;
        numIOs++;
    }
    string left_res, right_res;
    string res_str;
    for(i = 1; i < right_page_number; ++i)
    {
        
        bufMgr->readPage(right_file, i, pageID[left_page_number]);
        numUsedBufPages++;
        numIOs++;

        for(j = 1; j < left_page_number; ++j)
        {
            for(k = 1;; ++k)
            {
                try{
                    left_res = translate(pageID[j]->getRecord({j, k}), leftTableSchema);
                }
                catch(InvalidRecordException e){
                    break;
                }
                for(l = 1;; ++l)
                {
                    try{
                        right_res = translate(pageID[left_page_number]->getRecord({i,l}), rightTableSchema);
                    }
                    catch(InvalidRecordException e)
                    {
                        break;
                    }
                    vector<string> lef = split(left_res, '\t');
                    vector<string> rig = split(right_res, '\t');
                    int n = lefattrindex.size();
                    int w = 0;
                    for(int q = 0; q < n; ++q){
                        if (lef[lefattrindex[q]] != rig[rigattrindex[q]])
                        {
                            res_str = "";
                            break;
                        }
                        else
                        {
                            string res = "";
                            for(int e = 0; e < lef.size(); ++e){
                                if(leftTableSchema.getAttrType(e) == INT) res += lef[e] + ',';
                                else res += "'" + lef[e] + "',";
                            }
                            for(int e = 0; e < rig.size(); ++e){
                                if(w >= rigattrindex.size() || e != rigattrindex[w]) {
                                    if(rightTableSchema.getAttrType(e) == INT) res += rig[e] + ',';
                                    else res += "'" + rig[e] + "',";
                                }
                                else w++;
                            }
                            res_str = res.substr(0, res.size()-1);
                        }
                    }
                    if(res_str != "")
                    {
                        res_str = HeapFileManager::createTupleFromSQLStatement("insert into TEMP_TABLE (" + res_str + ");", catalog);
                        RecordId rid = HeapFileManager::insertTuple(res_str, resultFile, bufMgr);
                        numResultTuples++;
                    }
                }
            }
        }
        bufMgr->unPinPage(right_file, i, false);
    }
    for(i = 1; i < left_page_number; ++i)
    {
        bufMgr->unPinPage(left_file, i, false);
    }

    bufMgr->flushFile(&resultFile);

    isComplete = true;
    return true;
}

bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File& resultFile)
{
    vector<int> lefattrindex;
    vector<int> rigattrindex;
    int left_count = leftTableSchema.getAttrCount();
    for(int i = 0; i < left_count; ++i){
        if(rightTableSchema.hasAttr(leftTableSchema.getAttrName(i))){
            rigattrindex.push_back(rightTableSchema.getAttrNum(leftTableSchema.getAttrName(i)));
            lefattrindex.push_back(i);
        }
    }
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;

    int left_page_number = leftTableFile.readHeader().num_pages;
    int right_page_number = rightTableFile.readHeader().num_pages;
    int block_number = bufMgr->numofBlocks();
    int i, j, k, l, h, m;
    Page* pageID[block_number + 1];
    string left_res, right_res;
    string res_str;
    File* left_file = const_cast<File*>(&leftTableFile);
    File* right_file = const_cast<File*>(&rightTableFile);
    for(h = 1; h < left_page_number; h += block_number - 1)
    {
        for(m = h; m < h + block_number - 1 && m < left_page_number; ++m)
        {
            bufMgr->readPage(left_file, m, pageID[m - h]);
            numUsedBufPages++;
            numIOs++;
        }
        for(i = 1; i < right_page_number; ++i)
        {
            bufMgr->readPage(right_file, i, pageID[block_number]);
            numUsedBufPages++;
            numIOs++;
            for(j = 0; j < m -h; ++j)
            {
                for(k = 1;; ++k)
                {
                    try
                    {
                        left_res = translate(pageID[j]->getRecord({j + h, k}), leftTableSchema);
                    }
                    catch(InvalidRecordException e)
                    {
                        break;
                    }
                    for(l = 1;; ++l)
                    {
                        try
                        {
                            right_res = translate(pageID[block_number]->getRecord({i, l}), rightTableSchema);
                        }
                        catch(InvalidRecordException e)
                        {
                            break;
                        }
                        vector<string> lef = split(left_res, '\t');
                        vector<string> rig = split(right_res, '\t');
                        int n = lefattrindex.size();
                        int w = 0;
                        for(int q = 0; q < n; ++q){
                            if (lef[lefattrindex[q]] != rig[rigattrindex[q]])
                            {
                                res_str = "";
                                break;
                            }
                            else
                            {
                                string res = "";
                                for(int e = 0; e < lef.size(); ++e){
                                    if(leftTableSchema.getAttrType(e) == INT) res += lef[e] + ',';
                                    else res += "'" + lef[e] + "',";
                                }
                                for(int e = 0; e < rig.size(); ++e){
                                    if(w >= rigattrindex.size() || e != rigattrindex[w]) {
                                        if(rightTableSchema.getAttrType(e) == INT) res += rig[e] + ',';
                                        else res += "'" + rig[e] + "',";
                                    }
                                    else w++;
                                }
                                res_str = res.substr(0, res.size()-1);
                            }
                        }
                        if(res_str != "")
                        {
                            res_str = HeapFileManager::createTupleFromSQLStatement("insert into TEMP_TABLE (" + res_str + ");", catalog);
                            HeapFileManager::insertTuple(res_str, resultFile, bufMgr);
                            numResultTuples++;
                        }
                    }
                }
            }
            bufMgr->unPinPage(right_file, i, false);
        }
        for(i = h; i < h + block_number - 1 && i < left_page_number; ++i)
        {
            bufMgr->unPinPage(left_file, i, false);
        }
    }
    bufMgr->flushFile(&resultFile);
    isComplete = true;
    return true;
}

BucketId GraceHashJoinOperator::hash(const string& key) const
{
    std::hash<string> strHash;
    return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator ::execute(int numAvailableBufPages, File& resultFile)
{
    if (isComplete)
        return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;

    // TODO: Execute the join algorithm

    isComplete = true;
    return true;
}

}  // namespace badgerdb
