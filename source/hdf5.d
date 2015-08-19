module dataframe.hdf5;
import dataframe.common;
import dataframe.typed;
import hdf5.hdf5;
import dataframe.hdf5util;
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
static import std.traits;

enum CHUNKSIZE=260;
alias DataTypes=Tuple!(string[],"columnTitles", ColumnType[],"columnTypes",int[],"offsets",int[],"sizes",int,"totalSize");
// ColumnType[]


hid_t createDataType(DataFrameTyped frame, string name="")
{
	auto tid=H5T.create(H5TClass.Compound,frame.columnSizeOf);
	long offset=0L;
	foreach(colTitle;frame.columnTitles)
	{
		//writefln("%s,%s,%s",colTitle,offset,frame.columnTypes[colTitle].columnSizeOf);
		H5T.insert(tid,colTitle,offset,frame.columnTypes[colTitle].toH5Type);
		offset+=frame.columnTypes[colTitle].columnSizeOf;
	}
	return tid;
}

size_t columnSizeOf(ColumnType[] types)
{
	size_t ret;
	foreach(type;types)
		ret+=type.columnSizeOf;
	return ret;
}

size_t columnSizeOf(DataFrameTyped frame)
{
	size_t ret;
	foreach(title;frame.columnTitles)
		ret+=frame.columnTypes[title].columnSizeOf;
	return ret;
}

size_t columnSizeOf(ColumnType type)
{
	switch(type) with(ColumnType)
	{
		case Int:
			return int.sizeof;
		case Long:
			return long.sizeof;
		case Double:
			return double.sizeof;
		case Date:
			return std.datetime.Date.sizeof;
		case DateTime:
			return std.datetime.DateTime.sizeof;
		default:
			throw new Exception("unknown type: "~type.to!string);
	}
}


ubyte[] toBytes(DataFrameTyped frame)
{
	auto colBytes=frame.columnSizeOf;
	ubyte[] ret = new ubyte[colBytes*frame.numRows];
	foreach(row;0..frame.numRows)
	{
		auto rowOffset=row*colBytes;
		auto cellOffset=rowOffset;
		foreach(colTitle;frame.columnTitles)
		{
			switch(frame.columnTypes[colTitle]) with(ColumnType)
			{
				case Int:
					*(cast(int*)&ret[cellOffset])=frame.values.ints[colTitle][row];
					cellOffset+=int.sizeof;
					break;
				case Long:
					*(cast(long*)&ret[cellOffset])=frame.values.longs[colTitle][row];
					cellOffset+=long.sizeof;
					break;
				case Double:
					*(cast(double*)&ret[cellOffset])=frame.values.doubles[colTitle][row];
					cellOffset+=double.sizeof;
					break;
				case Date:
					cellOffset+=std.datetime.Date.sizeof;
					break;
				case DateTime:
					cellOffset+=std.datetime.DateTime.sizeof;
					break;
				default:
					break;
			}
		}
	}
	return ret;
}

auto dataFrameTypedFromFloats(float[] data,string[] columnTitles)
{
	DataFrameTyped ret;
	ColumnType[] columnTypes;
	auto numCols=columnTitles.length;
	auto numRows=data.length/numCols;
	ret.setColumnTitles(columnTitles);
	columnTypes.length=numCols;
	foreach(ref type;columnTypes)
		type=ColumnType.Double;
	ret.setColumnTypes(columnTypes);
	ret.setRows(numRows);
	foreach(row;0..data.length/numCols)
	{
		foreach(col;0..numCols)
		{
			ret[row,columnTitles[col]]=data[row*numCols+col].to!double;
		}
	}
	return ret;
}

DataTypes dataTypesForHDF5(string filename, string datasetName)
{
	string[] names;
	ColumnType[] types;
	int[] offsets,sizes;
	auto file = H5F.open(filename, H5F_ACC_RDONLY, H5P_DEFAULT);
	auto dataset = H5D.open2(file, datasetName, H5P_DEFAULT);
	auto s1_tid = H5D.get_type(dataset);
	ColumnType type;
	switch(H5T.get_class(s1_tid)) with (H5TClass)
	{
		case Integer:
			type=ColumnType.Int;
			auto ord = H5Tget_order(type);
			auto sgn = H5Tget_sign(type);
			auto sz = H5Tget_size(type).to!int;
			writefln("Integer byte order = %s",ord); // H5TOrderLE or BE
			writefln("Integer sign = %s",sgn); // H5T SGN None or 2
  			writefln("Integer size = %s",sz);
  			return DataTypes([],[ColumnType.Int],[],[sz],sz);
  		case Float:
  			return DataTypes([],[ColumnType.Double],[],[],0);
	 	case Compound:
		    auto sz = H5Tget_size(s1_tid).to!int;
		    auto nmemb = H5Tget_nmembers(s1_tid);
			writefln("  %s bytes",sz);
			writefln("  %s members",nmemb);
			foreach(i;0..nmemb)
			{
		        auto s2_tid = H5T.get_member_type(s1_tid, i);
		        enforce(H5Tget_class(s2_tid) != H5TClass.Compound);
				enforce(H5T.get_class(s2_tid) != H5TClass.Array);
		        writefln("    %s: type code %s offset %s size %s",
		                      H5T.get_member_name(s1_tid, i),
		                      H5T.get_class(s2_tid),
		                      H5T.get_member_offset(s1_tid, i),
		                      H5T.get_size(s2_tid));
		        names~=H5T.get_member_name(s1_tid, i);
		        types~=H5T.get_class(s2_tid).h5ClassToColumnType(H5T.get_size(s2_tid).to!int);
		        offsets~=H5T.get_member_offset(s1_tid, i).to!int,
		        sizes~=H5T.get_size(s2_tid).to!int;
		    }
		    writefln("returning: %s,%s,%s,%s,%s",names,types,offsets,sizes,sz);
		    stdout.flush;
		    return DataTypes(names,types,offsets,sizes,sz);
		default:
  			return DataTypes([],[],[],[],0);
	}
}

ColumnType h5ClassToColumnType(H5TClass classType, int len)
{
	switch(classType) with(H5TClass)
	{
		case Integer:
			switch(len)
			{
				case 1,2,4:
					return ColumnType.Int;
				case 8:
					return ColumnType.Long;
				default:
					throw new Exception("weird length: "~len.to!string);
			}
		case Float:
			return ColumnType.Double;
		default:
			throw new Exception("unknown HDF5 class: "~classType.to!string);
	}
	assert(0);
}
hid_t toH5Type(ColumnType type)
{
	switch(type) with(ColumnType)
	{
		case Int:
			return H5T_NATIVE_INT;
		case Long:
			return H5T_NATIVE_LLONG;
		case Double:
			return H5T_NATIVE_DOUBLE;
		default:
			throw new Exception("unknown type: "~ type.to!string);
	}
}

DataFrameTyped  dataFrameTypedFromHDF5DataSet(string filename,string datasetName)
{
	auto file = H5F.open(filename, H5F_ACC_RDONLY, H5P_DEFAULT);
	auto dataset = H5D.open2(file, datasetName, H5P_DEFAULT);

	auto dataType  = H5D.get_type(dataset);     /* datatype handle */
	auto t_class     = H5T.get_class(dataType);
	auto order     = H5T.get_order(dataType);
	auto size  = H5T.get_size(dataType);
	auto dataspace = H5D.get_space(dataset);    /* dataspace handle */
	auto rank      = H5S.get_simple_extent_ndims(dataspace);
	hsize_t[2]     dims_out;
	auto status_n  = H5S.get_simple_extent_dims(dataspace, dims_out);
	enforce(rank==1,
		new Exception("only handle vector ie rank 1 tables currently and rank="~to!string(rank)));
	writefln("dims=%s",dims_out);
	writefln("size=%s",size);
	writefln("total=%s",size*dims_out[0]);
	stdout.flush;
	auto data = new ubyte[dims_out[0]*size];
	H5D.read(dataset, dataType, H5S_ALL, H5S_ALL, H5P_DEFAULT, data.ptr);
	//debug writefln("%s", "read passed");
	H5T.close(dataType);
	H5S.close(dataspace);
	H5D.close(dataset);
	DataFrameTyped ret;
	auto meta=dataTypesForHDF5(filename,datasetName);
	ret.setColumnTitles(meta.columnTitles);
	ret.setColumnTypes(meta.columnTypes);
	foreach(row;0..dims_out[0])
	{
		auto rowOffset=meta.totalSize*row;
		int j=0;
		auto cellOffset=rowOffset;
		foreach(colTitle;ret.columnTitles)
		{
			cellOffset=rowOffset+meta.offsets[j];
			//writefln("%s,%s,%s,%s,%s,%s",row,j,cellOffset,colTitle,ret.columnTypes[colTitle],meta.sizes[j]);
			stdout.flush;
			switch(ret.columnTypes[colTitle])
			{
				case ColumnType.Int,ColumnType.Long:				
					switch(meta.sizes[j])
					{
						case 1:
							ret.values.ints[colTitle]~=(*(cast(char*)(&data[cellOffset]))).to!int;
							break;
						case 2:
							ret.values.ints[colTitle]~=(*(cast(ushort*)(&data[cellOffset]))).to!int;
							break;
						case 4:
							ret.values.ints[colTitle]~=(*(cast(int*)(&data[cellOffset])));
							break;
						case 8:
							ret.values.longs[colTitle]~=*cast(long*)(&data[cellOffset]);
							break;
						default:
							writefln("skipping unknown field len: %s",colTitle);
							break;
					}
					break;
				case ColumnType.Double:
					switch(meta.sizes[j])
					{
						case 4:
							ret.values.doubles[colTitle]~=(*cast(float*)&data[cellOffset]).to!double;
							break;
						case 8:
							ret.values.doubles[colTitle]~=*cast(double*)&data[cellOffset];
							break;
						default:
							writefln("skipping unknown field len: %s",meta.sizes[j]);
							break;
					}
					break;
				default:
					writefln("skipping %s",ret.columnTypes[colTitle]);
					break;
			}
			++j;
		}
		++ret.numRows;
	}
	return ret;
}




DataFrameTyped toHDF5(DataFrameTyped frame, string filename, string datasetName, DumpMode mode=DumpMode.append,
	bool extensible=true)
{
	import std.file:exists;
	hid_t file;
	bool fileExists=filename.exists;
	if (fileExists)
		file=H5F.open(filename,H5F_ACC_RDWR, H5P_DEFAULT);
	else
		file = friendlyH5Create(filename,100*1024*1024,true);
	//H5F.create(filename, H5F_ACC_TRUNC , H5P_DEFAULT, H5P_DEFAULT);

	hsize_t[1] chunk_dims =[CHUNKSIZE];
    auto dataType = frame.createDataType;
    ubyte[] junk;
    junk.length=H5T.get_size(dataType);
    writefln("%s data set length", junk.length);
	auto data=frame.toBytes;
    writefln("%s data set bytes", data.length);
	hsize_t[]  dim = [frame.numRows];
	//auto space = H5S.create_simple(dim);
    if ((H5L.exists(file,datasetName,H5P_DEFAULT))) // does file contain our dataset
	{
		auto dataset = H5D.open2(file, datasetName, H5P_DEFAULT);
		if ((mode==DumpMode.append) || (mode==DumpMode.truncate))
		{
			// we should check here that it is an extensible dataset
			auto dataTypeData  = H5D.get_type(dataset);     /* datatype handle */
			auto t_class     = H5T.get_class(dataTypeData);
			auto order     = H5T.get_order(dataTypeData);
			auto size  = H5T.get_size(dataTypeData);
			auto dataspace = H5D.get_space(dataset);    /* dataspace handle */
			auto rank      = H5S.get_simple_extent_ndims(dataspace);
			hsize_t[1]     dims_out,   offset;
			auto status_n  = H5S.get_simple_extent_dims(dataspace, dims_out);
			switch(mode)
			{
				case DumpMode.append:	dim=[dims_out[0]+frame.numRows];
								offset[0] = dims_out[0];
	    							break;
				case DumpMode.truncate:	dim=[frame.numRows];
								offset[0]=0;
								break;
				default:				assert(0);
			}
			H5D.set_extent(dataset, dim);
			auto filespace = H5D.get_space(dataset); 
	    	auto dim2=[frame.numRows];
			H5S.select_hyperslab(filespace, H5SSeloper.Set, offset, dim2);
			auto dataspace2 = H5S.create_simple(dim2);
			H5D.write(dataset, dataType, dataspace2, filespace, H5P_DEFAULT, cast(ubyte*)data.ptr);
			H5T.close(dataType);
		    H5S.close(dataspace2);
			H5D.close(dataset);
			return frame;
		}
		else // need to destroy dataset but keep others in this file
		{
			enforce(mode==DumpMode.unlink);
			H5L.h5delete(file,datasetName,H5P_DEFAULT);
		}  			
	}
			
	hsize_t[1] maxdims = extensible?[H5S_UNLIMITED]:[frame.numRows];
	
	auto cparms = H5P.create(H5P_DATASET_CREATE); // Modify dataset creation properties, i.e. enable chunking.
	//debug writefln("* h5p simple created"); stdout.flush;
	H5P.set_chunk( cparms, chunk_dims);
    //debug writefln("* h5p set chunk"); stdout.flush;
    auto dataspace = H5S.create_simple(dim, maxdims);
	debug writefln("* h5s simple created"); stdout.flush;
	//auto cparms = H5P.create(H5P_DATASET_CREATE); // Modify dataset creation properties, i.e. enable chunking.
    H5P.set_fill_value (cparms, dataType, cast(void*)&junk);
    //auto cparms=H5P_DEFAULT;
    debug writefln("* creating dataset");
    auto dataset = H5D.create2(file, datasetName, dataType, dataspace, H5P_DEFAULT, cparms, H5P_DEFAULT);
    // tried to disable the above - what follows on this line is wrong auto dataset = H5D.create2(file, datasetName, dataType, dataspace, H5P_DEFAULT,H5P_DEFAULT, H5P_DEFAULT);
    debug writefln("* dataset created");
	auto filespace = H5D.get_space(dataset); 
	debug writefln("* writing data");
    H5D.write(dataset, dataType, dataspace,filespace, H5P_DEFAULT, cast(ubyte*)data.ptr);
    //H5D.write(dataset,dataType,H5S_ALL,H5S_ALL,H5P_DEFAULT,cast(ubyte*)data.ptr);
    debug writefln("* finished writing data");
	H5T.close(dataType);
    H5S.close(dataspace);
	H5D.close(dataset);
	//H5D.close(filespace);
	debug writefln("* finished closing objects");
	return frame;
}




auto dataFrameTypedFromSimpleHDF5Array(string filename, string groupName, string ticker, string[] columnTitles)
{
	import std.stdio:writef,writefln;
	import std.file:exists;
	hsize_t[2]     dims;
	float[] data;
	H5open();
	H5_init_library();
	enforce(exists(filename),new Exception(filename~" does not exist!"));
	auto file=H5F.open(filename,H5F_ACC_RDWR, H5P_DEFAULT);
	//auto groupID = (groupName !is null)?H5G.open2(file, groupName, H5P_DEFAULT):file;
	auto groupID = H5G.open2(file, groupName, H5P_DEFAULT);
	//enforce(dataSetExists(groupID,ticker), new Exception(filename~" does not contain "~ticker~"!"));
	writefln("GT=%s/%s",groupName,ticker);
	stdout.flush;
	auto dataset = H5D.open2(groupID, ticker, H5P_DEFAULT);
	auto dataspace = H5D.get_space(dataset);    /* dataspace handle */
	auto rank      = H5S.get_simple_extent_ndims(dataspace);
	auto status  = H5S.get_simple_extent_dims(dataspace, dims);
 	enforce(dims[1]==columnTitles.length);
 	writefln("dims = %s,rows= %s, columnTitles = %s",dims[1],dims[0],columnTitles.length);
  	data.length=dims[0]*dims[1];
    H5D.read(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, cast(ubyte*)data.ptr);
    H5G.close(groupID);
    H5F.close(file);
	return data.dataFrameTypedFromFloats(columnTitles);
}
