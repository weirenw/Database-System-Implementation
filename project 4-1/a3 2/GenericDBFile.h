#ifndef _GENERIC_DBFILE_H_
#define _GENERIC_DBFILE_H_

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string>

//this is a class used by DBFile, later this class will be inherited by
//some classes which concerned with file type
//by set some functions to pure virtual function, we are not allowed to create a
//instance for "GenericDBFile"
class GenericDBFile {
public:
	//we set default mode is read
  GenericDBFile(){
		buffer = NULL;	
		currentWritingPage = -1;
		currentReadingPage = 0;
		currentReadingRecord = 0;
		fileChanged = false;
		curPageIdx = 0;
  }
  virtual ~GenericDBFile() {}

  virtual int Create (char* fpath, void* startup);
  virtual int Open (char* fpath);
  virtual int Close() = 0;

  virtual void Add (Record& addme) = 0;
  virtual void Load (Schema& myschema, char* loadpath);

  // this function does not deal with spanned records
  virtual void MoveFirst () = 0;
  virtual int GetNext (Record& fetchme);
  virtual int GetNext (Record& fetchme, CNF& cnf, Record& literal) = 0;

  // Extracts the table name from the file path, the string between the last '/' and '.' 
  //i.e. no prefix and postfix. This function should be used by all class variable and 
  //this class's subclasses
  static std::string getTableName(const char* fpath) {
    std::string path(fpath);
    size_t start = path.find_last_of('/'),
           end   = path.find_last_of('.');
    return path.substr(start+1, end-start-1);
  }

 protected:
	//I only define one buffer for both both reading and writing as the assignment request
	Page *buffer;
	//tool is for actually read page and write page
	//in fact, tool will work on buffer
	File tool;
	//I need two variables to keep record of where the user has his reading pointer on
	//in fact, these two variables act as pointer to the disk file.
	//currentReadingPage begins from 0, currentReadingRecord begins from 0, too.
	int currentReadingPage;
	//currentReadingPage is a little weird, this indicate the index of a record that hasn't
	//accessed, it is a record that after the "factual current working record"
	int currentReadingRecord;
	//writing does not need a record pointer since we always append record to the file
	int currentWritingPage;
	//this variable tell us whether user has add record to the disk file or not.
	bool fileChanged;
	//this variable is used for indicate which page we are operating on
	int curPageIdx;
};

#endif
