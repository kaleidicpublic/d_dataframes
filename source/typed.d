module dataframe.typed;
import dataframe.dataframe;
import std.conv;
import std.csv;
import std.datetime;
import std.exception;
import std.range:array, stride,only;
import std.stdio;
import std.variant;
import std.string:isNumeric;
import std.typecons:tuple,Tuple;


DataFrameTyped typedFrameFromCSV(string filename, string[] titles)
{
	DataFrame frame;
	frame=frame.loadCSVFile(filename,(titles.length>0));
	DataFrameTyped typedFrame;
	auto newTitles=(titles.length==0)?
		frame.indexTitle~frame.columnTitles:
		titles;
	typedFrame=typedFrame.setColumnTitles(newTitles)
				.setColumnTypes(frame.findColumnTypes)
				.loadCSVFile(filename,newTitles,(titles.length==0));
	return typedFrame;
}

struct DataFrameTypedRow
{
	DataFrameTyped *frame;
	size_t rowNumber;

/*	auto opIndexAssign(T)(T value, string colName)
	{
		import std.algorithm:canFind;
		enforce(frame.columnTitles.canFind(colName));
		(*frame)[rowNumber,colName]=value;
		return this;
	}
*/
	auto opDispatch(string colName)()
	{
		import std.algorithm:canFind;
		enforce(frame.columnTitles.canFind(colName));
		return (*frame).loadCell!KalVariant(rowNumber,colName);	
	}
	void opDispatch(string colName,T)(T value)
	{
		import std.algorithm:canFind;
		enforce(frame.columnTitles.canFind(colName));
		(*frame)[rowNumber,colName]=value;
	}
/*	T loadCell(T)(string series)
	{
		return (*frame).loadCell!T(rowNumber,series);
	}*/
}


struct DataFrameTyped
{
	string title;
	string[] columnTitles;
	ColumnType[string] columnTypes;

	char separator=',';
	char quote='\"';
	size_t numRows;
	struct Values
	{
		double[][string] doubles;
		int[][string] ints;
		long[][string] longs;
		std.datetime.Date[][string] dates;
		std.datetime.DateTime[][string] dateTimes;
		string[][string] strings;
	}
	Values values;
	size_t[string] stringSizes;

	
	auto setRows(size_t rows)
	{
		this.length=rows;
		return this;
	}

	auto insertColumn(T)(string title,ColumnType type,T[] vals)
	{
		this.columnTitles~=title;
		this.columnTypes[title]=type;
		final switch(type) with(ColumnType)
		{
			case Double:
				values.doubles[title]=vals;
				break;
			case Int:
				values.ints[title]=vals;
				break;
			case Long:
				values.longs[title]=vals;
				break;
			case Date:
				values.dates[title]=vals;
				break;
			case DateTime:
				values.dateTimes[title]=vals;
				break;
		}
		return this;
	}

	auto deleteColumn(string title)
	{
		import std.algorithm:countUntil;
		auto i=columnTitles.countUntil(title);
		enforce(i>=0);
		final switch(columnTypes[title]) with(ColumnType)
		{
			case Double:
				values.doubles.remove(title);
				break;
			case Int:
				values.ints.remove(title);
				break;
			case Long:
				values.longs.remove(title);
				break;
			case Date:
				values.dates.remove(title);
				break;
			case DateTime:
				values.dateTimes.remove(title);
				break;
			case String:
				values.strings.remove(title);
				break;
		}
		columnTypes.remove(title);
		if (i==0)
			columnTitles=columnTitles[1..$];
		else if (i==columnTitles.length)
			columnTitles=columnTitles[0..$-1];
		else
			columnTitles=columnTitles[0..i]~columnTitles[i+1..$];
		return this;
	}
	void mergeCell(DataFrameTyped frame, string series, size_t rowNum)
	{
		final switch(frame.columnTypes[series]) with(ColumnType)
		{
			case Double:
				values.doubles[series]~=frame.values.doubles[series][rowNum];
				return;
			case Int:
				values.ints[series]~=frame.values.ints[series][rowNum];
				return;
			case Long:
				values.longs[series]~=frame.values.longs[series][rowNum];
				return;
			case Date:
				values.dates[series]~=frame.values.dates[series][rowNum];
				return;
			case DateTime:
				values.dateTimes[series]~=frame.values.dateTimes[series][rowNum];
				return;
			case String:
				values.strings[series]~=frame.values.strings[series][rowNum];
				return;
		}

	}
	void appendCell(T)(string series, T value)
	{
		final switch(columnTypes[series]) with(ColumnType)
		{
			case Double:
				values.doubles[series]~=value.to!double;
			case Int:
				values.ints[series]~=value.to!int;
			case Long:
				values.longs[series]~=value.to!long;
			case Date:
				values.dates[series]~=value.to!Date;
			case DateTime:
				values.dateTimes[series]~=value.to!DateTime;
			case String:
				values.strings[series]~=value.to!string;
		}
	}

	T loadCell(T)(size_t row, string series)
	if(std.traits.isNumeric!T)
	{
		final switch(columnTypes[series]) with(ColumnType)
		{
			case Double:
				return values.doubles[series][row].to!T;
			case Int:
				return values.ints[series][row].to!T;
			case Long:
				return values.longs[series][row].to!T;
			case Date,DateTime:
				throw new Exception("cannot convert date/datetime to numeric type "~T.stringof);
			case String:
				throw new Exception("cannot convert string to numeric type "~T.stringof);
		}
		assert(0);
		//return (loadCell!(T[])(series,row,row+1))[0];
	}
	T loadCell(T)(size_t row, string series)
	if(is(T==DateTime) || is(T==Date))
	{
		final switch(columnTypes[series]) with(ColumnType)
		{
			case Double,Int,Long:
				throw new Exception("cannot convert number to date type");
			case Date:
				return values.dates[series][row].to!T;
			case DateTime:
				return values.dateTimes[series][row].to!T;
			case String:
				return cast(std.datetime.DateTime)SysTime.fromSimpleString(values.strings[series][row]);
		}
		assert(0);
		//return (loadCell!(T[])(series,row,row+1))[0];
	}
	T loadCell(T)(size_t row, string series)
	if(is(T==string))
	{
		final switch(columnTypes[series]) with(ColumnType)
		{
			case Double:
				return values.doubles[series][row].to!T;
			case Int:
				return values.ints[series][row].to!T;
			case Long:
				return values.longs[series][row].to!T;
			case Date:
				return values.dates[series][row].to!T;
			case DateTime:
				return values.dateTimes[series][row].to!T;
			case String:
				return values.strings[series][row];
		}
		assert(0);
		//return (loadCell!(T[])(series,row,row+1))[0];
	}
/*
	T loadCell(T)(string series, size_t start, size_t end)
	{
		final switch(columnTypes[series]) with(ColumnType)
		{
			case Double:
				return values.doubles[series][start..end].to!T;
			case Int:
				return values.ints[series][start..end].to!T;
			case Long:
				return values.longs[series][start..end].to!T;
			case Date:
				return values.dates[series][start..end].to!T;
			case DateTime:
				return values.dateTimes[series][start..end].to!T;
			case String:
				return values.strings[series][start..end].to!T;
		}
	}
*/
	DataFrameTyped setTitle(string title)
	{
		this.title=title;
		return this;
	}
	DataFrameTyped setColumnTitles(string[] titles)
	{
		this.columnTitles=titles;
		return this;
	}
	DataFrameTyped setColumnTypes(ColumnType[] columnTypes)
	{
		foreach(i,title;columnTitles)
			this.columnTypes[title]=columnTypes[i];
		enforce(this.columnTitles.length==this.columnTypes.keys.length);
		return this;
	}

	size_t length() @property
	{
		return this.numRows;
	}

	void length(size_t rows) @property
	{
		if (rows==this.numRows)
			return;
		foreach(col;columnTitles)
		{
			final switch(columnTypes[col]) with(ColumnType)
			{
				case Double:
					this.values.doubles[col].length=rows;
					break;
				case Int:
					this.values.ints[col].length=rows;
					break;
				case Long:
					this.values.longs[col].length=rows;
					break;
				case Date:
					this.values.dates[col].length=rows;
					break;
				case DateTime:
					this.values.dateTimes[col].length=rows;
					break;
				case String:
					this.values.strings[col].length=rows;
					break;
			}
		}
		this.numRows=rows;
	}

	DataFrameTyped setIndexValues(T)(T[] indexValues)
	{
		this.length=indexValues.length;
		final switch(this.indexType) with(ColumnType)
		{
			case Double:
				foreach(i,value;indexValues)
					this.values.doubles[i*numDoubleCols]=value;
				break;
			case Int:
				foreach(i,value;indexValues)
					this.values.ints[i*numIntCols]=value;
				break;
			case Long:
				foreach(i,value;indexValues)
					this.values.longs[i*numLongCols]=value;
				break;
			case String:
				foreach(i,value;indexValues)
					this.values.strings[i*numStringCols]=value;
				break;
			case Date:
				foreach(i,value;indexValues)
					this.values.dates[i*numDateCols]=value;
				break;
			case DateTime:
				foreach(i,value;indexValues)
					this.values.datetimes[i*numDateTimeCols]=value;
				break;
		}
		foreach(i,value;indexValues)
			this.indexValues[i]=indexValues;
		return this;
	}

/*	DataFrameTyped setCellValues(KalVariant[][] cellValues)
	{
		foreach(i,row;cellValues)
		{
			foreach(j,cell;row)
			{
				this[i,j+1]=cell;
			}
		}
		return this;
	}
	DataFrameTyped setAllValues(KalVariant[][] values)
	{
		foreach(i,row;values)
		{
			this.indexValues[i]=values[i][0];
			foreach(j,cell;row[1..$])
			{
				this[i,j+1]=cell;
			}
		}
		return this;
	}
*/
	DataFrameTyped loadCSVFile(string csv, string[] columnTitles=[],bool skipFirst=false)
	{
		auto file=std.file.read(csv);
		return loadCSV(cast(string) file,columnTitles,skipFirst);
	}

	DataFrameTyped setSeparator(char separator)
	{
		this.separator=separator;
		return this;
	}
	DataFrameTyped setQuote(char separator)
	{
		this.quote=separator;
		return this;
	}

	DataFrameTyped mergeFrames(DataFrameTyped frame)
	{
		enforce((frame.title=="") || (frame.title==this.title) || (this.title==""));
		enforce(frame.columnTitles.length==0 || frame.columnTitles==this.columnTitles || this.columnTitles.length==0);
		enforce(frame.columnTypes==this.columnTypes);
		foreach(colTitle;frame.columnTitles)
		{
			foreach(rowNum;0..frame.numRows)
				mergeCell(frame,colTitle,rowNum);
		}
		this.numRows+=frame.numRows;
		// should do sort and uniq
		return this;
	}

	size_t numCols()
	{
		return columnTypes.length;
	}

	auto opIndex(size_t row)
	{
		return DataFrameTypedRow(&this,row);
	}
	/*
	auto opIndexAssign(DataFrameTypedRow rowData,size_t rowNumber)
	{
		foreach(j,col;cols)
		{
			this[rowNumber,columnTitles[j]]=rowData.frame[]
	}
	*/
	T opIndex(T)(size_t row, size_t col)
	{
		//enforce((row>=0) && (col>=0) && (col <=numCols) &&(row<=indexValues.length));
		return loadCell!T(columnTitles[col],row,row+1);
	}

	T opIndex(T)(size_t row, string col)
	{
		return loadCell!T(col,row,row+1);
	}

	T opIndex(T)(size_t[] rows, size_t[] cols)
	{
		T[][] ret;
		ret.length=rows.length;
		foreach(ref line;ret)
			line.length=cols.length;
		foreach(i,row;rows)
		{
			foreach(j,col;cols)
			{
				ret[i][j]=loadCell!T(columnTitles[col],row,row+1);
			}
		}
		return ret;
	}

	T opIndexAssign(T)(T value, size_t row, size_t col)
	{
		return opIndexAssign!T(value,row,this.columnTitles[col]);
	}

	T opIndexAssign(T)(T value, size_t row, string col)
	{
		//stdout.writefln("opIndexAssign %s,%s,%s",value,row,col);
		//stdout.writefln("this.values.strings.keys=%s",this.values.strings.keys);
		//stdout.writefln("T=%s",typeid(T));
		//stdout.flush;
		// enforce type safety for columns
		//enforce((row>=0) && (col>=0) && (col <=numCols) &&(row<=indexValues.length));
		final switch(columnTypes[col]) with(ColumnType)
		{
			case Double:
				this.values.doubles[col][row]=value.to!double;
				return value;
			case Int:
				this.values.ints[col][row]=value.to!int;
				return value;
			case Long:
				this.values.longs[col][row]=value.to!long;
				return value;
			case String:
				this.values.strings[col][row]=value.to!string;
				return value;
			case ColumnType.Date:
				static if(is(T==std.datetime.Date))
					this.values.dates[col][row]=value;
				else static if(is(T==std.datetime.DateTime))
					this.values.dates[col][row]=value.dateTimeToDate;
				else static if(is(T==std.datetime.string))
					this.values.dates[col][row]=value.stringToDate;
				return value;
			case ColumnType.DateTime:
				static if(is(T==std.datetime.DateTime))
					this.values.dateTimes[col][row]=value;
				else static if(is(T==std.datetime.Date))
					this.values.dateTimes[col][row]=value.dateToDateTime;
				else static if(is(T==std.datetime.string))
					this.values.dateTimes[col][row]=value.stringToDateTime;
				return value;
		}
	}

	T[] columnValues(T)(string col)
	{
		final switch(columnTypes[col]) with(ColumnType)
		{
			case Double:
				return this.values.doubles[col];
			case Int:
				return this.values.ints[col];
			case Long:
				return this.values.longs[col];
			case String:
				return this.values.strings[col];
			case Date:
				return this.values.dates[col];
			case DateTime:
				return this.values.dateTimes[col];
		}
	}
	T[] columnValues(T)(size_t col)
	{
		return this.columnValues(this.columnTitles[col]);
	}

	ColumnType[] findColumnTypes()
	{
		ColumnType[] ret;
		foreach(title;this.columnTitles)
			ret~=this.columnTypes[title];
		return ret;
	}
	size_t[] opSlice(size_t i)(size_t start, size_t end)
	if ((i==0)||(i==1))
	{
		return iota(start,end);
	}

	size_t opDollar(size_t i)()
	{
		static if (i==0)
			return numRows;
		else static if(i==1)
			return numCols;
		else static assert(0);
	}

	string toString()
	{
		string ret="Kaleidic Typed Dataframe: "~this.title~"\n\n";

		foreach(j;0..numCols)
			ret~="\t"~this.columnTitles[j];
		ret~="\n";
		//log("numRows="~numRows.to!string);
		//log("numCols="~numCols.to!string);

		foreach(i;0..numRows)
		{
			//log("row: "~i.to!string~": "~this.indexValues[i].to!string);
			foreach(j;columnTitles)
				ret~=loadCell!string(i,j)~"\t";
			ret~="\n";
		}
		return ret;
	}

}

