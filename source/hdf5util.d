module dataframe.hdf5util;
import hdf5.hdf5;
import std.conv;
import std.csv;
import std.datetime;
import std.exception;
import std.range:array, stride,only;
import std.stdio;
import std.variant;
import std.string:isNumeric;
alias KalVariant=Algebraic!(string,int,long, DateTime, float,double);
import std.typecons:tuple,Tuple;

extern(C) herr_t H5_init_library();

enum DumpMode
{
	unlink,
	truncate,
	append,
}

hsize_t getUserBlockSize(string filename)
{
	hsize_t usize;
	auto testval = H5F.is_hdf5(filename);
	enforce(testval>0, new Exception("Input HDF5 file is not HDF: "~filename));
	auto ifile = H5F.open (filename, H5F_ACC_RDONLY, H5P_DEFAULT);
	enforce(ifile>=0, new Exception("Cannot open input HDF5 file: "~filename));
	auto  plist = H5F.get_create_plist (ifile);
  	enforce(plist>=0, new Exception("Cannot get file creation plist for file "~filename));
	H5P.get_userblock (plist, &usize);
	H5P.close (plist);
  	H5F.close (ifile);
  	return usize;
  }

void setUserBlock(string filename, ubyte[] buf)
{
	auto usize=getUserBlockSize(filename);
	if (usize<buf.length)
		throw new Exception("Attempted to set user block for file: "~ filename~ " but user block is only "~
			to!string(usize) ~ " bytes long and buffer is "~to!string(buf.length)~" bytes long");
	auto f=File(filename,"wb+");
	f.rewind();
	f.rawWrite(buf);
	f.flush();
	f.close();
}

ubyte[] getUserBlock(string filename)
{
	ubyte[] buf;
	auto usize=getUserBlockSize(filename);
	buf.length=cast(size_t) usize;
	auto f=File(filename,"rb+");
	f.rewind();
	auto numbytes=f.rawRead(buf);
	buf.length=numbytes.length;
	f.close();
	return buf;
}

hid_t friendlyH5Create(string filename, hsize_t userBlockSize, bool truncateNotThrow )
{
	import std.file:exists;
	H5open();
	H5_init_library();
	if ((!truncateNotThrow)&&exists(filename))
		throw new Exception("friendlyH5Create: attempt to create file that already exists: "~filename);
	userBlockSize=computeUserBlockSize(userBlockSize);
	writefln("%s userblock size",userBlockSize);
	auto plist = H5Pcreate(H5P_FILE_CREATE); //H5P_FILE_CREATE);
	//writefln("%s",H5P.get_class_name(plist));
	//stdout.flush;
	//auto plist=H5P_DEFAULT;
	H5P.set_userblock(plist, userBlockSize) ;
	auto file_id = H5F.create(filename, H5F_ACC_TRUNC, plist, H5P_DEFAULT);
	//auto file_id = H5F.create(filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	H5P.close(plist);
	return file_id;
}

hsize_t computeUserBlockSize (hsize_t ublock_size)
{
	hsize_t where = 512;
	if (ublock_size == 0)
		return 0;
	while (where < ublock_size)
		where *= 2;
	return (where);
}



string[] hdf5Contents(string filename)
{
	import std.exception:enforce;
	import std.file:exists;
	enforce(filename.exists,new Exception("hdf5Contents - file: "~filename~" does not exist!"));
	auto file=H5F.open(filename,H5F_ACC_RDWR, H5P_DEFAULT);
	auto files=cast(string[])objectList(file);
	H5F.close(file);
	return files;
}

/+
bool isEquityMain(hid_t group, string ticker, string filename)
{
	import hdf5.wrap;
	import hdf5.bindings.enums;
	import hdf5.bindings.api;
	if (!filename.toLower.canFind("equitymain"))
		return false;
	auto dataset = H5D.open2(group, ticker, H5P_DEFAULT);
	auto dataspace = H5D.get_space(dataset);    /* dataspace handle */
	auto rank      = H5S.get_simple_extent_ndims(dataspace);
	H5D.close(dataset);
	if (rank>=3)
		throw new Exception("isEquityMain: unknown data format for "~filename~"; rank="~rank.to!string);
	return (rank==2);	
}
+/
