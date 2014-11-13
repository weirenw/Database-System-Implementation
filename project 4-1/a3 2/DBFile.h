#ifndef DBFILE_H
#define DBFILE_H

#include <string>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"

typedef enum {heap, sorted, tree} fType;


/* This is the public (handle) interface
 * should not be inherited.
 */
class DBFile {
public:
  DBFile();
  ~DBFile();
  int Create (char* fpath, fType ftype, void* startup);
  int Open (char* fpath);
  int Close ();

  void Add (Record& addme);
  void Load (Schema& myschema, char* loadpath);

  void MoveFirst();
  int GetNext (Record& fetchme);
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);
private:
  GenericDBFile* myInternalVar;
  
  void createFile(fType ftype);
};

#endif
