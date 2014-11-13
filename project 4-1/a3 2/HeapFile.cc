#include "HeapFile.h"
#include <string.h>

//if we use this function, then we will create a new bin disk file
//no matter there is already a same-name file existed or not
//and also in the second assignment, we need to create a meta disk file
//with the corresponding bin disk file
int HeapFile::Create (char *f_path, void *startup) {
	//there are something that bewilder me, why the f_path would changed after call getTableName
	char *fpath = strdup(f_path);
	const char *metafile = (GenericDBFile::getTableName(f_path)+".meta").c_str();
	FILE *fp = fopen(metafile, "w");
	fprintf(fp, "0\n");
	fclose(fp);

	tool.Open(0, fpath);
	tool.Close();
	return 1;	
}

//this function should only be called only once, and also should be closed later 
void HeapFile::Load (Schema &f_schema, char *loadpath) {
	FILE* fp = fopen(loadpath, "r");
	Record next;
	while(next.SuckNextRecord(&f_schema, fp))
		Add(next);
}


//we do not care whether this file is shared by other program or not
int HeapFile::Open (char *f_path) {
	tool.Open(1, f_path);
	return 1;
}

//implicitly we suppose the "File tool" has attached to one real disk file,
//and we open that disk file in a file mode for both reading and writing.
//we should consider three cases:
//(I) before this function call, we are performing writing
//(II) before this function call, we are performing reading
//(III) This function call is the first call we call after execute the program
//but no matter what the case is, our purpose is after this function call, any 
//continuous function should act as switch from the starting of the program
void HeapFile::MoveFirst () {
	//first case, before this function call, we are perform writing
	if(currentWritingPage >= 0)
	{
		if(fileChanged == true)
			tool.AddPage(buffer, currentWritingPage);
		//remember to set "currentWritingPage = -1";
		currentWritingPage = -1;
		fileChanged = false;
	}
	if(buffer != NULL)
		buffer->EmptyItOut();
	buffer = NULL;
	currentReadingPage = 0;
	currentReadingRecord = 0;
}
	

int HeapFile::Close () {
	if(fileChanged == true)
	{
		tool.AddPage(buffer, currentWritingPage);
	}
	//do not forget change the values below
	currentWritingPage = -1;
	currentReadingPage = 0;
	currentReadingRecord = 0;
	if(buffer != NULL)
	{
		buffer->EmptyItOut();
		delete buffer;
		buffer = NULL;
	}
	tool.Close();
	buffer = NULL;
	
	return 1;
}


//we use "currentWritingPage" to tell whether before this adding we are writing or reading.
//if "currentWritingPage > =0 ", then we know previously we are perform writing.
//otherwise, if "buffer == NULL", then we know, this adding is our first action after
//this programm execute,
//otherwise we know before this adding we are performing reading.
//user can perform Add anytime he like, so there are three cases we need to handle
//(I)before this add(..) call, the user is perform Add(..)
//(II)before this add(..) call, the user is perform reading
//(III)this add call is the first call after user executing the program
void HeapFile::Add (Record &rec) {
	//if currentWritingPage >= 0, then we know before this call we performed write
	if(currentWritingPage >= 0)
	{
		//if buffer has enough space to add another record, add it
		//and set fileChanged to true, later we can write it into disk file
		if(buffer->Append(&rec) == 1)
		{
			fileChanged = true;	
		}
		else
		{
			//if buffer has no space to stuff a new record,
			//first check whether this buffer is changed, if so
			// write the buffer back to the correct page of the disk file
			if(fileChanged == true)
			{
				tool.AddPage(buffer, currentWritingPage);
				buffer->EmptyItOut();
				//change currentPage and fileChanged 
				//now our currentPage will 1 bigger than file.GetLength()
				buffer->Append(&rec);
				fileChanged = true;
				currentWritingPage++;	
			}
			else
			{
				//if buffer has never been changed, no need to write it back
				buffer->EmptyItOut();
				buffer->Append(&rec);
				currentWritingPage++; 
				fileChanged = true;
			}
		}
	}
	else
	{
		//our first action, and the first action is this writing(adding)
		if(buffer == NULL)
		{
			//now two cases, one is the disk file have no data page
			//other case is the disk file has at least one data page
			//we will use the last data page to add the new record to if there is a
			//last data page
			currentWritingPage = tool.GetLength() - 2;
			if(currentWritingPage < 0)
			{
				buffer = new Page();
				currentWritingPage = 0;
				buffer->Append(&rec);
				fileChanged = true;	
			}
			else
			{
				buffer = new Page();
				tool.GetPage(buffer, currentWritingPage);
				if(buffer -> Append(&rec) == 1)
					fileChanged = true;
				else
				{
					buffer->EmptyItOut();
					buffer->Append(&rec);
					currentWritingPage ++;
					fileChanged = true;
				}		
			}	
		}
		else //before this writing(adding), we are performing reading
		{
			buffer->EmptyItOut();
			//we are sure there is at least one data page in disk file	
			currentWritingPage = tool.GetLength() - 2;
			tool.GetPage(buffer, currentWritingPage);
			if(buffer->Append(&rec))
				fileChanged = true;
			else
			{
				buffer->EmptyItOut();
				buffer->Append(&rec);
				currentWritingPage++;
				fileChanged = true;
			}
		}
	}
}

//since user can switch from reading to writing, or from writing to reading
//or perform this function as the first function since the programm execute.
//(1)before this action we are performing writing, then write the buffer back
//   to disk file if "fileChanged == true", otherwise, delete buffer, read page
//   back to buffer indicated by "currentReadingPage" and "currentReadingRecord"
//   traverse the buffer in order to reach to the right record.
//(2)before this action we are performing reading, easy.
//(3)first action since starting the program.
int HeapFile::GetNext (Record &fetchme) {
	if(currentWritingPage >= 0)
	{
		if(fileChanged == true)
			tool.AddPage(buffer, currentWritingPage);
		//remember to set "currentWritingPage = -1";
		currentWritingPage = -1;
		fileChanged = false;
		buffer->EmptyItOut();
		//we are sure there is at least one data page in the disk file
		tool.GetPage(buffer, currentReadingPage);
		//now we should get rid of the records that we have read in before
		for(int i = 0; i < currentReadingRecord; ++i)
			buffer->GetFirst(&fetchme);
		while(1)
		{
			//we need to test result
			int result = buffer->GetFirst(&fetchme);
			//do not forget to increment currentReadingRecord
			currentReadingRecord++;
			//if result=0, then we know current reading buffer have no record left
			//we need to read in the next record from the next page, but the next page
			//may be empty of records or there is no next data page left
			if(result == 0)
			{
				//anyway, we first try to read in the next data page
				//if there is no next page, we cout some info and return 0
				if(currentReadingPage == tool.GetLength() - 2)
				{
					std::cout << "These is no record left in the disk binary file\n";
					//before we return, we can do some thing that make the program
					//more friendly
				//	currentReadingPage = 0;
				//	currentReadingRecord = 0;
					return 0;
				}
				else//read in the next page no matter whether it is a null page or not
				{
					currentReadingPage++;
					currentReadingRecord = 0;
					tool.GetPage(buffer, currentReadingPage);	
				}
			}
			else//we really get the next record, so we are done, just return 1.
				return 1;
		}
	}
	else
	{
		//if this function is the first function we called since the program
		//begin executing, then we return the first record in the first data page
		if(buffer == NULL)//buffer == NULL means we call this function the first time
		{
			//but we may encounter there is no data page in the disk file or 
			//all data pages contain no data
			if(tool.GetLength() == 1)
			{
				std::cout << "These is no data page in the disk file\n";
				return 0;
			}
			else
			{
				//if there are some data page(s), we read in the first data page
				//in fact we are sure currentReadingPage =0, but for safety, we 
				//set it again in this block
				currentReadingPage = 0;
				currentReadingRecord = 0;
				//do not new Page() since buffer == NULL in this ramification
				buffer = new Page();
				tool.GetPage(buffer, currentReadingPage);
				while(1)
				{
					int result = buffer->GetFirst(&fetchme);
					currentReadingRecord++;
					if(result == 0)//this means the first data page is null
					{
						//if these is the only page left in disk binary file, then we know
						//we are done
						if(currentReadingPage == tool.GetLength() - 2)	
						{
							std::cout << "There is no record left in the binary disk file!\n";
							//before return 0, we do more things to make the program more friendly
				//			currentReadingPage = 0;
				//			currentReadingRecord = 0;
							return 0;		
						}
						else // read in the next page
						{
							currentReadingPage ++;
							currentReadingRecord = 0;
							tool.GetPage(buffer, currentReadingPage);
						}
					}
					else //we get the valid record, just return 1
						return 1;
				}
			}	
		}
		else // if before this function call, we performed reading
		{
			//we use the same test method as the above two cases
			while(1)
			{
				int result = buffer->GetFirst(&fetchme);
				if(result == 0)
				{
					//we will try to read in the next data page
					if(currentReadingPage == tool.GetLength() - 2)
					{
						std::cout << "There is no record left in the binary disk file!\n";
					//	currentReadingPage = 0;
				//		currentReadingRecord = 0;
						return 0;
					}
					else //there is still some data page left
					{
						currentReadingPage ++;
						currentReadingRecord = 0;
						tool.GetPage(buffer, currentReadingPage);	
					}
				}
				else
				{
					//do not forget increment currentReadingRecord when we acutally read the next record
					currentReadingRecord ++;
					return 1;
				}
			}
		}
	}
}

//thie version is much like another overloaded version, the only difference
//is that we test the record we get, if it satify the cnf requirement, then we
//return it, otherwise, we get next record.
//in total, there are only three cases, and each case has two subcases:
//(I)we switch from writing to this reading
//	(1) we can get a valid record, return 1;
//	(2) we can not get a valid record, return 0;
//(II)before this call, we perform reading
//	(1) we can get a valid record, return 1;
//	(2) we can not get a valid record, return 0;
//(III)this GetNext call is the first action we do after the program begin executing
//	(1) we can get a valid record, return 1;
//	(2) we can not get a valid record, return 0; 
int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//first define a ComparisonEngine, use to test whether the reading record is valid or not
	ComparisonEngine comp; 
	//currentWritingPage indicate whether we are performing writing or not
	if(currentWritingPage >= 0)
	{
		if(fileChanged == true)
			tool.AddPage(buffer, currentWritingPage);
		//remember to set "currentWritingPage = -1";
		currentWritingPage = -1;
		fileChanged = false;
		buffer->EmptyItOut();
		//we are sure there is at least one data page in the disk file
		tool.GetPage(buffer, currentReadingPage);
		//now we should get rid of the records that we have read in before
		for(int i = 0; i < currentReadingRecord; ++i)
			buffer->GetFirst(&fetchme);
		while(1)
		{
			//we need to test result
			int result = buffer->GetFirst(&fetchme);
			//do not forget to increment currentReadingRecord
			currentReadingRecord++;
			//if result=0, then we know current reading buffer have no record left
			//we need to read in the next record from the next page, but the next page
			//may be empty of records or there is no next data page left
			if(result == 0)
			{
				//anyway, we first try to read in the next data page
				//if there is no next page, we cout some info and return 0
				if(currentReadingPage == tool.GetLength() - 2)
				{
					std::cout << "These is no record left in the disk binary file\n";
					//before we return, we can do some thing that make the program
					//more friendly
				//	currentReadingPage = 0;
				//	currentReadingRecord = 0;
					return 0;
				}
				else//read in the next page no matter whether it is a null page or not
				{
					currentReadingPage++;
					currentReadingRecord = 0;
					tool.GetPage(buffer, currentReadingPage);	
				}
			}
			//now we need to test the record, whether it satisfy the cnf or not
			else if(comp.Compare(&fetchme, &literal, &cnf)) 
				return 1;
		}
	}
	else
	{
		//if this function is the first function we called since the program
		//begin executing, then we return the first record in the first data page
		if(buffer == NULL)//buffer == NULL means we call this function the first time
		{
			//but we may encounter there is no data page in the disk file or 
			//all data pages contain no data
			if(tool.GetLength() == 1)
			{
				std::cout << "These is no data page in the disk file\n";
				return 0;
			}
			else
			{
				//if there are some data page(s), we read in the first data page
				//in fact we are sure currentReadingPage =0, but for safety, we 
				//set it again in this block
				currentReadingPage = 0;
				currentReadingRecord = 0;
				buffer = new Page();
				tool.GetPage(buffer, currentReadingPage);
				while(1)
				{
					int result = buffer->GetFirst(&fetchme);
					currentReadingRecord++;
					if(result == 0)//this means the first data page is null
					{
						//if these is the only page left in disk binary file, then we know
						//we are done
						if(currentReadingPage == tool.GetLength() - 2)	
						{
							std::cout << "There is no record left in the binary disk file!\n";
							//before return 0, we do more things to make the program more friendly
				//			currentReadingPage = 0;
				//			currentReadingRecord = 0;
							return 0;		
						}
						else // read in the next page
						{
							currentReadingPage ++;
							currentReadingRecord = 0;
							tool.GetPage(buffer, currentReadingPage);
						}
					}
					//now we should test the record, whether it satisfy the cnf or not
					else if(comp.Compare(&fetchme, &literal, &cnf))
						return 1;
				}
			}	
		}
		else // if before this function call, we performed reading
		{
			//we use the same test method as the above two cases
			while(1)
			{
				int result = buffer->GetFirst(&fetchme);
				if(result == 0)
				{
					//we will try to read in the next data page
					if(currentReadingPage == tool.GetLength() - 2)
					{
						std::cout << "There is no record left in the binary disk file!\n";
				//		currentReadingPage = 0;
				//		currentReadingRecord = 0;
						return 0;
					}
					else //there is still some data page left
					{
						currentReadingPage ++;
						currentReadingRecord = 0;
						tool.GetPage(buffer, currentReadingPage);	
					}
				}
				//now we should test wether the record satisfy the cnf or not
				else
				{
					//anyway we should increment currentReadingRecord if we actually read into the next record
					currentReadingRecord ++;
					if(comp.Compare(&fetchme, &literal, &cnf))
						return 1;
				}
			}
		}
	}
}
