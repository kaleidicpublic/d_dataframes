module dataframe.variant;
import std.file;
import dataframe.common;
import dataframe.csv;
import std.conv;
import std.csv;
import std.datetime;
import std.exception;
import std.range:array, stride,only;
import std.stdio;
import std.variant;
import std.string:isNumeric;
import std.typecons:tuple,Tuple;

struct DataFrame
{
	string title;
	string indexTitle;
	KalType indexType;
	string[] columnTitles;
	KalType[] columnTypes;
	KalVariant[] indexValues;
	KalVariant[] cellValues;
	bool setSkipHeader=false;
	char separator=',';
	char quote='\"';

/*	auto asPriceBars(PriceBarType type, int dp)(KalDate date)
	{
		PriceBar!(type,dp)[string] ret;
		ret.length=numRows;
		foreach(i;0..numRows)
		{
			auto bar = new PriceBar!(type,dp);
			bar.date=date;
			bar.open=this[i,1].to!double;
			bar.high=this[i,2].to!double;
			bar.low=this[i,3].to!double;
			bar.close=this[i,4].to!double;
			bar.volume=this[i,5].to!double;
			bar.openInterest=this[i,6].to!double;
			ret[this[i,0].to!string]=bar;
		}
		return ret;
	}
*/	DataFrame setCellDimensions(size_t rows, size_t cols)
	{
		this.columnTitles.length=cols;
		this.columnTypes.length=cols;
		this.indexValues.length=rows;
		this.cellValues.length=rows*cols;
		return this;
	}
	DataFrame setCellColumns(size_t cols)
	{
		this.columnTitles.length=cols;
		this.columnTypes.length=cols;
		return this;		
	}
	DataFrame setTitle(string title)
	{
		this.title=title;
		return this;
	}
	DataFrame setIndexTitle(string indexTitle)
	{
		this.indexTitle=indexTitle;
		return this;
	}
	DataFrame setIndexType(KalType type)
	{
		this.indexType=type;
		return this;
	}
	DataFrame setColumnTitles(string[] titles)
	{
		this.columnTitles=titles;
		return this;
	}
	DataFrame setColumnTypes(KalType[] columnTypes)
	{
		this.columnTypes=columnTypes;
		this.columnTitles.length=columnTypes.length;
		return this;
	}
	DataFrame setIndexValues(T)(T[] indexValues)
	{
		foreach(i,value;indexValues)
			this.indexValues[i]=indexValues;
		return this;
	}
	DataFrame setCellValues(KalVariant[][] cellValues)
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
	DataFrame setAllValues(KalVariant[][] values)
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

	DataFrame loadCSVFile(string csv, bool hasHeader=false)
	{
		auto file=std.file.read(csv);
		return loadCSV(cast(string) file,hasHeader);
	}

	DataFrame setSkipFirstRow()
	{
		this.setSkipHeader=true;
		return this;
	}
	DataFrame setNoSkipFirstRow()
	{
		this.setSkipHeader=false;
		return this;
	}
	DataFrame setSeparator(char separator)
	{
		this.separator=separator;
		return this;
	}
	DataFrame setQuote(char separator)
	{
		this.quote=separator;
		return this;
	}

	size_t numCols()
	{
		return columnTypes.length+1;
	}

	size_t length()
	{
		return indexValues.length;
	}
	alias numRows=length;

	KalVariant opIndex(size_t row, size_t col)
	{
		enforce((row>=0) && (col>=0) && (col <=numCols) &&(row<=indexValues.length));
		if(col==0)
			return indexValues[row];
		else
			return cellValues[row*numCols+col-1];
	}

	auto opIndex(size_t[] rows, size_t[] cols)
	{
		KalVariant[][] ret;
		ret.length=rows.length;
		foreach(ref line;ret)
			line.length=cols.length;
		foreach(i,row;rows)
		{
			foreach(j,col;cols)
			{
				ret[i][j]=(col==0)?indexValues[row]:cellValues[row*numCols+col-1];
			}
		}
		return ret;
	}

	KalVariant opIndexAssign(T)(T value, size_t row, size_t col)
	{
		// enforce type safety for columns
		enforce((row>=0) && (col>=0) && (col <=numCols) &&(row<=indexValues.length));
		auto val=value.to!KalVariant;
		if (col==0)
			indexValues[row]=val;
		else
			cellValues[row*numCols+col-1]=val;
		return val;
	}

	auto columnValues(size_t col)
	{
		KalVariant[] ret;
		foreach(i;0..numRows)
			ret~=this[i,col];
		return ret;
	}
	ColumnType columnType(size_t col)
	{
		auto data=columnValues(col);
		if (data.isDate!DateTime)
		{
			if (data.isDate!Date)
				return ColumnType.Date;
			return ColumnType.DateTime;
		}
		else if (data.isDouble)
		{
			if (data.isInteger!int)
				return ColumnType.Int;
			if (data.isInteger!long)
				return ColumnType.Long;
			return ColumnType.Double;
		}
		return ColumnType.String;
	}
	ColumnType[] findColumnTypes()
	{
		ColumnType[] ret;
		foreach(i;0..numCols)
			ret~=columnType(i);
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
		string ret="Kaleidic Dataframe: "~this.title~"\n\n";

		ret~=this.indexTitle;
		foreach(j;1..numCols)
			ret~="\t"~this.columnTitles[j-1];
		ret~="\n";
		//log("numRows="~numRows.to!string);
		//log("numCols="~numCols.to!string);

		foreach(i;0..numRows)
		{
			//log("row: "~i.to!string~": "~this.indexValues[i].to!string);
			foreach(j;0..numCols)
				ret~=this[i,j].to!string~"\t";
			ret~="\n";
		}
		return ret;
	}
}

