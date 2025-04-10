#include "heapfile.h"
#include "error.h"

// TODO
// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // Create the file if it doesn't exist.
        status = db.createFile(fileName);
        if (status != OK) return status;

        // Open the newly created file.
        status = db.openFile(fileName, file);
        if (status != OK) return status;

        // Allocate the header page.
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) return status;
        hdrPage = reinterpret_cast<FileHdrPage*>(newPage);

        // Initialize header page fields.
        strncpy(hdrPage->fileName, fileName.c_str(), sizeof(hdrPage->fileName));
        hdrPage->fileName[sizeof(hdrPage->fileName) - 1] = '\0';
        hdrPage->firstPage = -1;
        hdrPage->lastPage = -1;
        hdrPage->recCnt = 0; 

        // Allocate the first data page.
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            // Cleanup header page if data page allocation fails.
            bufMgr->unPinPage(file, hdrPageNo, true);
            return status;
        }

        // Initialize the first data page.
        newPage->init(newPageNo);
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;

        // Unpin pages and ensure they're marked as dirty.
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);

        // Close the file to ensure changes are flushed.
        status = db.closeFile(file);
        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// Constructor for HeapFile: opens the specified file, and pins both
// the header page and the first data page into the buffer pool.
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status status;
    Page* pagePtr;

    cout << "opening file " << fileName << endl;

    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // Retrieve the page number of the file's header page.
        status = filePtr->getFirstPage(headerPageNo);

        // Read the header page into the buffer pool and get a pointer to it.
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);

        // Cast the raw page pointer to our file header structure.
        headerPage = reinterpret_cast<FileHdrPage*>(pagePtr);
        hdrDirtyFlag = false;

        // Set curPageNo to the first actual data page (after the header).
        curPageNo = headerPage->firstPage;

        // Read the first data page into the buffer pool.
        status = bufMgr->readPage(filePtr, curPageNo, pagePtr);

        // Save a pointer to the current data page.
        curPage = pagePtr;

        // Mark the current data page as clean (no modifications yet).
        curDirtyFlag = false;

        curRec = NULLRID;
        returnStatus = OK;
    }
    else
    {
        // Opening the file failed — set the error status and return.
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}


// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// TODO
// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;
    // If the required record is not on the current page, switch pages.
    if (curPage == NULL || curPageNo != rid.pageNo) {
        // If there is a page currently pinned, unpin it.
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // Read in the page that contains the desired record.
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) return status;
        curPageNo = rid.pageNo;
        curDirtyFlag = false; // newly read page is clean.
    }
    // Get the record from the currently pinned page.
    return curPage->getRecord(rid, rec);
}

// TODO

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

// TODO
const Status HeapFileScan::scanNext(RID & outRid)
{
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    int nextPageNo;
    Record rec;

    // If curPage is NULL, we need to start the scan from the first page
    if (curPage == NULL) {
        // Read the first page
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK) return status;
        
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        
        // Get first record on page
        status = curPage->firstRecord(curRec);
        if (status != OK && status != NORECORDS) return status;
        
        // If page has no records, we'll move to the next page
        if (status == NORECORDS) {
            curRec = NULLRID;
        }
    }
    
    while (true) {
        //if curRec is NULLRID get the first record on this page
        if (curRec.pageNo == NULLRID.pageNo && curRec.slotNo == NULLRID.slotNo) {
            status = curPage->firstRecord(curRec);
            
            //if there are no records on this page
            if (status != OK) {
                status = curPage->getNextPage(nextPageNo);
                
                //no next page
                if (nextPageNo == -1) {
                    return FILEEOF;
                }
                
                //unpin the current page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK) return status;
                
                //read the next page
                status = bufMgr->readPage(filePtr, nextPageNo, curPage);
                if (status != OK) return status;
                
                //update current page information
                curPageNo = nextPageNo;
                curDirtyFlag = false;
                curRec = NULLRID;
                
                //continue to the next iteration to get the first record from this page
                continue;
            }
        } else {
            //next record on curent page
            status = curPage->nextRecord(curRec, nextRid);
            
            //end of page
            if (status != OK) {
                //try next page
                status = curPage->getNextPage(nextPageNo);
                
                //no next page, end of file
                if (nextPageNo == -1) {
                    return FILEEOF;
                }
                
                //unpin
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK) return status;
                
                //read next page
                status = bufMgr->readPage(filePtr, nextPageNo, curPage);
                if (status != OK) return status;
                
                //update current page
                curPageNo = nextPageNo;
                curDirtyFlag = false;
                curRec = NULLRID;
                
                //continue to the next iteration to get the first record from this page
                continue;
            } else {
                // Update curRec to the next record
                curRec = nextRid;
            }
        }
        
        if (curPage == nullptr) return FILEEOF;
        status = curPage->getRecord(curRec, rec);
        if (status != OK) return status;
        
        //check if the record matches the filter condition
        if (matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
        
        //record doesn't match, continue to the next record
    }
    
    // Should never reach here
    return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// TODO
// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
	//My Implementation DOWNWARD

    // Inserts the record described by rec into the file returning the RID of the inserted record in outRid.
    
     // check if curPage is NULL. If so, make the last page the current page and read it into the buffer.
    if (curPage == nullptr) {
        //headerPage->lastPage gives page number of last page
        //readPage Status
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK) {
            return status;
        }
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
        //page was just read in and hasn't been modified yet so curDirtyFlag = F.
    }
 
    // Call curPage->insertRecord to insert the record. If successful, remember to DO THE BOOKKEEPING. 
    // That is, you have to update data fields such as recCnt, hdrDirtyFlag, curDirtyFlag, etc.
    //insertion Status
    status = curPage->insertRecord(rec, outRid);


    //If can't insert into the current page, then create a new page, initialize it properly, 
    //modify the header page content properly, link up the new page appropriately, make the 
    //current page to be the newly allocated page, then try to insert the record. Don't forget 
    //bookkeeping that must be done after successfully inserting the record.

    //if no space to insert then you gotta make a new page
    if (status == NOSPACE) {
        //Allocates new page from the buffer pool.
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        newPage->init(newPageNo);
        newPage->setNextPage(-1); //pointer to -1 = last page

        Page* oldLastPage = curPage; 
        //Sets the nextPage pointer of the prev last page to the page number of the new page
        oldLastPage->setNextPage(newPageNo);
        bufMgr->unPinPage(filePtr, curPageNo, true);
        //This unpins the previous last page marking it to dirty.

        //Bookkeeping
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = false;

        //insert the rec into the new page.
        status = curPage->insertRecord(rec, outRid);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, curPageNo, true);
            return status;
        }

        //header page -> the new last page and increases page count.
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
    }

    //Successful insertion check
    if (status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
    }
    return status;

  
  
  
  
  
  
  
  
  
  
  
  
}
