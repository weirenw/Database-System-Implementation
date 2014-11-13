#include "GenericDBFile.h"

int GenericDBFile::Create(char *fPath, void *startup)
{return 0;}
int GenericDBFile::Open(char *fpath)
{return 0;}
int GenericDBFile::Close()
{return 0;}
void GenericDBFile::Load(Schema &myschema, char *loadpath)
{}
void GenericDBFile::MoveFirst()
{}
void GenericDBFile::Add(Record &addme)
{}
int GenericDBFile::GetNext(Record &fetchme)
{return 0;}
int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{return 0;}
