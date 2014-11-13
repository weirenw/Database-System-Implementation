#ifndef _HEAP_FILE_H_
#define _HEAP_FILE_H_
#include "GenericDBFile.h"
#include <iostream>

class HeapFile: public GenericDBFile{
public:
	HeapFile():GenericDBFile(){
	}
	~HeapFile()
	{}
	int Create (char *fpath, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif
