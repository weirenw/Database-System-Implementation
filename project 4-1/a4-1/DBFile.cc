#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Errors.h"
#include <iostream>
#include <fstream>

//constructor, need to set myInternalVar to NULL
DBFile::DBFile(): myInternalVar(NULL) {}

//destructor, be careful with myInterval, when myInterval != NULL,
//we need to delete it and then set it to NULL
DBFile::~DBFile() 
{ 
	if(myInternalVar != NULL)
		delete myInternalVar;
	myInternalVar = NULL;
}

//now in this second assignment, we need to consider the file type
//we need to create a meta file with the binary file which store file type
//information about that file
int DBFile::Create (char* fpath, fType ftype, void* startup) 
{
	FATALIF(myInternalVar != NULL, "File already opened.");
		createFile(ftype);
	return myInternalVar->Create(fpath, startup);
}

//in this assignment, we need to consider the file type when we try to open it. 
int DBFile::Open (char* fpath) 
{
	FATALIF(myInternalVar != NULL, "File already opened.");
	int ftype = heap;  // use heap file by default
	ifstream ifs((GenericDBFile::getTableName(fpath)+".meta").c_str());
	//if can not open corresponding file, then we know there is something wrong
	if(!ifs)
	{
		std::cout << "In DBFile::Open(...): can not open the file \"" << GenericDBFile::getTableName(fpath)+".meta" << "\"" << endl;
		exit(1);
	}
	if (ifs) 
	{
		ifs >> ftype;  // the first line contains file type
		ifs.close();
	}
	createFile(static_cast<fType>(ftype));
	return myInternalVar->Open(fpath);
}

//this is a private function that really create the corresponding myInternalVar
void DBFile::createFile(fType ftype) 
{
	switch (ftype) 
	{
		case heap: myInternalVar = new HeapFile(); break;
		case sorted: myInternalVar = new SortedFile(); break;
		case tree: break;
		default: myInternalVar = NULL;
	}
	FATALIF(myInternalVar == NULL, "Invalid file type.");
}

int DBFile::Close() 
{
	return myInternalVar->Close();
}

void DBFile::Add (Record& addme) 
{
	return myInternalVar->Add(addme);
}

void DBFile::Load (Schema& myschema, char* loadpath) 
{
	return myInternalVar->Load(myschema, loadpath);
}

void DBFile::MoveFirst() 
{
	return myInternalVar->MoveFirst();
}

int DBFile::GetNext (Record& fetchme) 
{
	return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) 
{ 
	return myInternalVar->GetNext(fetchme, cnf, literal);
}
