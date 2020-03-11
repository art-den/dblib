# dblib
Simple C++ library to get access to different DBMSs. SQLite, Firebird and PostgreSQL are supported. 
Author compiles library by Visual Studio and tests it only usder Microsoft Widnows.

## How to use
Installation is not required. Add sources of library to you project. Support of C++17 for compiler is required

## Examples

### Simple example
```cpp
#include <stdio.h>
#include "dblib/dblib_sqlite.hpp"

using namespace dblib;

int main()
{
	// load sqlite.dll

	auto lib = create_sqlite_lib();
	lib->load();

	// connect to inmemory databsase

	auto conn = lib->create_connection(":memory:", {});
	conn->connect();

	// create transaction and statement

	auto st = conn->create_transaction()->create_statement();

	// create simple table

	st->execute("create table simple_table (fld integer)");

	// fill the table

	st->execute("insert into simple_table(fld) values (1)");

	// fill table via prepared statement with indexed parameter

	st->prepare("insert into simple_table(fld) values (?1)");
	st->set_int32(1, 42);
	st->execute();

	// fill table via prepared statement with named parameter

	st->prepare("insert into simple_table(fld) values (:val)");
	st->set_int32(":val", 4242);
	st->execute();

	// select from table

	st->execute("select * from simple_table");
	while (st->fetch())
	{
		printf("fld by index = %d\n", st->get_int32(1));
		printf("fld by name = %d\n", st->get_int32("fld"));
	}

	// result columns

	st->execute("select * from simple_table");

	printf("Columns count is = %d\n", st->get_columns_count());
	printf("1st column name is = %s\n", st->get_column_name(1).c_str());
	printf("1st column type is = %s\n", field_type_to_string(st->get_column_type(1)).c_str());
}
```
