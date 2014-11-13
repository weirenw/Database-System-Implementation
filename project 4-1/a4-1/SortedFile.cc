#include <fstream>
#include <stdio.h>    // rename

#include "Errors.h"
#include "SortedFile.h"
#include "HeapFile.h"

#define safeDelete(p) {  delete p; p = NULL; }

using std::ifstream;
using std::ofstream;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir; 
extern char* tpch_dir;

int SortedFile::Create (char* fpath, void* startup) 
{
  	table = getTableName((tpath=fpath).c_str());
 	typedef struct { OrderMaker* o; int l; } *pOrder;
  	pOrder po = (pOrder)startup;
  	sortOrder = po -> o;
  	runLength = po -> l;
  	tool.Open(0, fpath);
	buffer = new Page();
  	return 1;
}

int SortedFile::Open (char* fpath) 
{
	allocMem();
	table = getTableName((tpath=fpath).c_str());
	int ftype;
	ifstream ifs(metafName());
	FATALIF(!ifs, "Meta file missing.");
	ifs >> ftype >> *sortOrder>> runLength;
	ifs.close();
	tool.Open(1, fpath);
	buffer = new Page();
	return 1;
}

int SortedFile::Close() 
{
	ofstream ofs(metafName());  // write meta data
	ofs << "1\n" << *sortOrder << '\n' << runLength << std::endl;
	ofs.close();
	if(mode==WRITE) 
	{
		merge();  // write actual data, merge the data in differential file with disk file
	}
	freeMem();
	int returncode = tool.Close();
	if(buffer != NULL)
		delete buffer;
	buffer = NULL;
	return returncode;
}

void SortedFile::Add (Record& addme) 
{
	startWrite();
	inPipe->Insert(&addme);
}

void SortedFile::Load (Schema& myschema, char* loadpath) 
{
	startWrite();
	FILE* ifp = fopen(loadpath, "r");
	FATALIF(ifp==NULL, loadpath);

	Record next;
	buffer->EmptyItOut();  // creates the first page
	while (next.SuckNextRecord(&myschema, ifp)) 
		Add(next);
	// theFile.addPage(&curPage);  // writes the last page  -- added in close()
}

void SortedFile::MoveFirst () 
{
	startRead();
	tool.GetPage(buffer, curPageIdx=0);
}

int SortedFile::GetNext (Record& fetchme) 
{
	/* TODO: We don't need to switch mode here, do we?? */
	startRead();
	while (!buffer->GetFirst(&fetchme)) 
	{
		if(++curPageIdx > tool.lastIndex()) 
			return 0;  // no more records
		tool.GetPage(buffer, curPageIdx);
	}
	return 1;
}

int SortedFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) 
{
	startRead();
	OrderMaker queryorder, cnforder;
	OrderMaker::queryOrderMaker(*sortOrder, cnf, queryorder, cnforder);
	ComparisonEngine cmp;
	if (!binarySearch(fetchme, queryorder, literal, cnforder, cmp)) 
		return 0; // query part should equal
	do{
		if (cmp.Compare(&fetchme, &queryorder, &literal, &cnforder)) 		
			return 0; // query part should equal
		if (cmp.Compare(&fetchme, &literal, &cnf)) 
			return 1;   // matched
	}while(GetNext(fetchme));
	return 0;  // no matching records
}

void SortedFile::merge() 
{
	inPipe->ShutDown();
	Record fromFile, fromPipe;
	bool fileNotEmpty = !tool.empty(), pipeNotEmpty = outPipe->Remove(&fromPipe);

	HeapFile tmp;
	tmp.Create(const_cast<char*>(tmpfName()), NULL);  // temporary file for the merge result; will be renamed in the end
	//I need to open the tmp again since my implementation for HeapFile::Create(...) is create the file
	//then close that file.
	tmp.Open(const_cast<char*>(tmpfName()));
	ComparisonEngine ce;

	// initialize
	if (fileNotEmpty) 
	{
		tool.GetPage(buffer, curPageIdx=0);           // move first
		fileNotEmpty = GetNext(fromFile);
	}

  	// two-way merge
	while(fileNotEmpty && pipeNotEmpty)
	{
		if(ce.Compare(&fromFile, &fromPipe, sortOrder) > 0)
		{
			tmp.Add(fromPipe);
			pipeNotEmpty = outPipe->Remove(&fromPipe);
		}
		else
		{
			tmp.Add(fromFile);
			fileNotEmpty = GetNext(fromFile);
		}
	}
	//now, we should check whether there are left in File or Pipe
	if(fileNotEmpty)
	{
		while(fileNotEmpty)
		{
			tmp.Add(fromFile);
			fileNotEmpty = GetNext(fromFile);
		}
	}
	if(pipeNotEmpty)
	{
		while(pipeNotEmpty)
		{
			tmp.Add(fromPipe);
			pipeNotEmpty = outPipe->Remove(&fromPipe);
		}
	}
  	tmp.Close();
//	const char *heapUseMetaFile = (GenericDBFile::getTableName(tmpfName())+".meta").c_str();
//	remove(heapUseMetaFile);
	FATALIF(rename(tmpfName(), tpath.c_str()), "Merge write failed.");  // rename returns 0 on success
	deleteQ();
}

int SortedFile::binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp) 
{
	// the initialization part deals with successive calls:
	// after a binary search, the first record matches the literal and no furthur binary search is necessary
	if (!GetNext(fetchme)) 
		return 0;
	int result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
	if (result > 0) return 0;
	else if (result == 0) return 1;

	// binary search -- this finds the page (not record) that *might* contain the record we want
	off_t low=curPageIdx, high=tool.lastIndex(), mid=(low+high)/2;
	for (; low<mid; mid=(low+high)/2) 
	{
		tool.GetPage(buffer, mid);
		FATALIF(!GetNext(fetchme), "empty page found");
		result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
		if (result<0) low = mid;
		else if (result>0) high = mid-1;
		else high = mid;  // even if they're equal, we need to find the *first* such record
	}

	tool.GetPage(buffer, low);
	do{   // scan the located page for the record matching record literal
		if (!GetNext(fetchme)) return 0;
		result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
	}while (result<0);
	return result==0;
}

const char* SortedFile::metafName() const 
{
	std::string p(dbfile_dir);
	return (p+table+".meta").c_str();
}

void SortedFile::allocMem() 
{
	FATALIF (sortOrder != NULL, "File already open.");
	sortOrder = new OrderMaker();
	useMem = true;
}

void SortedFile::freeMem() 
{
	if (useMem) 
	safeDelete(sortOrder);
	useMem = false;
}
