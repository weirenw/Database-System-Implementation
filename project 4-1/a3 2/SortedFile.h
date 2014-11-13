#ifndef SORTED_FILE_H_
#define SORTED_FILE_H_

#include <string>
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"


class SortedFile: public GenericDBFile{
	static const size_t PIPE_BUFFER_SIZE = PIPE_SIZE;
public:
	enum Mode{READ, WRITE, UNKNOWN} mode;
	SortedFile(): sortOrder(NULL), runLength(0), inPipe(NULL), outPipe(NULL), biq(NULL), useMem(false), mode(UNKNOWN) {}
	~SortedFile() {}

	int Create (char* fpath, void* startup);
	int Open (char* fpath);
	int Close ();

	void Add (Record& addme);
	void Load (Schema& myschema, char* loadpath);

	void MoveFirst();
	int GetNext (Record& fetchme);
	int GetNext (Record& fetchme, CNF& cnf, Record& literal);

protected:
	void startWrite();
	void startRead();

private:
	OrderMaker* sortOrder;    // may come from startup or meta file; need to differentiate
	int runLength;

	std::string tpath;
	std::string table;
  
	Pipe *inPipe, *outPipe;
	BigQ *biq;
	int curPageIdx;

	const char* metafName() const; // meta file name
	// temp file name used in the merge phase
	inline const char* tmpfName() const;  

	void merge();    // merge BigQ and File
	int binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp);

	bool useMem;     // this is used to indicate whether SortInfo is passed or created
                   // it is default to false, and set in allocMem()
	void allocMem();
	void freeMem();

	void createQ() 
	{
		inPipe = new Pipe(PIPE_BUFFER_SIZE);
		outPipe = new Pipe(PIPE_BUFFER_SIZE);
		biq = new BigQ(*inPipe, *outPipe, *sortOrder, runLength);
	} 

	void deleteQ() 
	{
		delete biq; delete inPipe; delete outPipe;
		biq = NULL; inPipe = outPipe = NULL;
	}
};



const char* SortedFile::tmpfName() const 
{
	return (tpath+".tmp").c_str();
}

inline void SortedFile::startRead() 
{
	if(mode == WRITE)
	{
		merge();
		mode = READ;
		return;
	}
	else if(mode == READ)
		return;
	else
		mode = READ;
	return;
}

inline void SortedFile::startWrite() 
{
	if (mode==WRITE) return;
	mode = WRITE;
	createQ();
}

#endif
