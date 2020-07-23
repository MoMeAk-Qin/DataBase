/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	for(FrameId i = 0; i < numBufs; i++){
		if (bufDescTable[i].dirty == true)
		{	
			/*write page back*/
			bufDescTable[i].file->writePage(bufPool[i]);
			bufDescTable[i].dirty = false;
		}
	}
	/*deallocate all*/
	delete[] bufPool;
	delete[] bufDescTable;
	delete hashTable;
}

void BufMgr::advanceClock()
{	
	/**
	 * clockhand to next frame
	 */
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{	
	/**
	 * implement the clock replacement  algorithm 
	 */
	std::uint32_t pin_count = 0;
	while (true)
	{
		/*advance clock pointer*/
		advanceClock();
		/*valid set -> no -> call Set() in the frame -> use frame*/
		if (bufDescTable[clockHand].valid == false)
		{
			frame = clockHand;
			return;
		}
		/*refbit -> yes -> clear refbir -> advance clock pointer*/
		if (bufDescTable[clockHand].refbit == true)
		{
			bufDescTable[clockHand].refbit = false;
			continue;
		}
		/*pinned -> advance clock pointer*/
		if (bufDescTable[clockHand].pinCnt > 0)
		{
			pin_count++;
			if (pin_count == numBufs)
			{	
				/*exception*/
				throw BufferExceededException();
			}
			else
			{
				/*advance clock pointer*/
				continue;
			}
		}
		/*flush page to disk*/
		if (bufDescTable[clockHand].dirty == true)
		{
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
		}
		/*use frame*/
		frame = clockHand;
		if (bufDescTable[clockHand].valid == true)
		{
			try
			{
				/*the frame is just cleared, remove it from hash table*/
				hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			}
			catch(HashNotFoundException e)
			{
			}
		}
		break;
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameid;
	try
	{
		/*page in the buffer pool whether or not*/
		hashTable->lookup(file, pageNo, frameid);
		bufDescTable[frameid].refbit = true;
		bufDescTable[frameid].pinCnt++;
		page = &bufPool[frameid];
	}
	catch(HashNotFoundException e)
	{
		/*allocate a buffer pool*/
		FrameId new_frameid;
		allocBuf(new_frameid);
		/*read the page from disk into the buffer pool frame*/
		bufPool[new_frameid] = file->readPage(pageNo);
		/*insert the page into the hashtable*/
		hashTable->insert(file, pageNo, new_frameid);
		/*invoke Set() on the frame to see it up properly*/
		bufDescTable[new_frameid].Set(file, pageNo);
		page = &bufPool[new_frameid];
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameid;
	try
	{
		hashTable->lookup(file, pageNo, frameid);
		if (bufDescTable[frameid].pinCnt > 0)
		{
			bufDescTable[frameid].pinCnt--;
			if (dirty == true)
			{	
				/*set the dirty bit*/
				bufDescTable[frameid].dirty = true;
			}
			return;
		}
		else if (bufDescTable[frameid].pinCnt == 0)
		{	
			/*if pin count is already 0 throw error*/
			throw PageNotPinnedException(file->filename(), bufDescTable[frameid].pageNo, frameid);
		}
	}
	catch(HashNotFoundException e)
	{
		
	}
}

void BufMgr::flushFile(const File* file) 
{
	for (unsigned int i = 0; i < numBufs; i++)
	{
		/*search file in buffer pool*/
		if (bufDescTable[i].file == file)
		{
			if (bufDescTable[i].pinCnt != 0)
			{
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if (bufDescTable[i].valid == false)
			{
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			else if (bufDescTable[i].valid == true)
			{	
				/*case (a)*/
				if (bufDescTable[i].dirty == true)
				{
					bufDescTable[i].file->writePage(bufPool[i]);
					bufDescTable[i].dirty = false;
				}
				/*case (b) (c)*/
				hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
				bufDescTable[i].Clear();
			}
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameid;
	/**
	 * allocate an empty page
	 * to obtain a buffer pool frame
	 */
	allocBuf(frameid);
	bufPool[frameid] = file->allocatePage();
	page = &bufPool[frameid];
	pageNo = page->page_number();
	hashTable->insert(file, pageNo, frameid);
	bufDescTable[frameid].Set(file ,pageNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameid;
	try
	{	
		/*that frame is freed and correspondingly entry from hash table is also removed*/
		hashTable->lookup(file, PageNo, frameid);
		bufDescTable[frameid].Clear();
		hashTable->remove(file, PageNo);
		file->deletePage(PageNo);
	}
	catch(HashNotFoundException e)
	{
		file->deletePage(PageNo);
	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
