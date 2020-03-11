# dblib
Simple C++ library to get access to different DBMSs. SQLite, Firebird and PostgreSQL are supported. 
Author compiles library by Visual Studio and tests it only under Microsoft Widnows.

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
	st->set_int32(1, 42); // index of 1st parameter is 1
	st->execute();

	// fill table via prepared statement with named parameter
	// here is exapmple with :val, but @val and $val placeholders are 
	// also supported

	st->prepare("insert into simple_table(fld) values (:val)");
	st->set_int32(":val", 4242);
	st->execute();

	// select from table

	st->execute("select * from simple_table");
	while (st->fetch())
	{
		printf("fld by index = %d\n", st->get_int32(1)); // index of 1st field is 1
		printf("fld by name = %d\n", st->get_int32("fld"));
	}

	// result columns

	st->execute("select * from simple_table");

	printf("Columns count is = %d\n", st->get_columns_count());
	printf("1st column name is = %s\n", st->get_column_name(1).c_str());
	printf("1st column type is = %s\n", field_type_to_string(st->get_column_type(1)).c_str());
}
```

### Null in query result
```cpp
	st->execute("insert into simple_table(fld) values (null)");

	st->execute("select * from simple_table");
	while (st->fetch())
	{
		// 1st way. Use is_null()

		if (st->is_null("fld"))
			printf("fld is null\n");
		else
			printf("fld is %d\n", st->get_int32("fld"));

		// 2nd way. Use std::optional return result of get_*_opt funtions

		auto fld_res = st->get_int32_opt("fld");

		if (!fld_res)
			printf("fld is null\n");
		else
			printf("fld is %d\n", *fld_res);

		// 3rd way. Use default result values (get_*_or functions)

		auto fld_value = st->get_int32_or("fld", -1);
		if (fld_value == -1)
			printf("fld is null or -1\n");
		else
			printf("fld is %d\n", fld_value);
	}
```

### Null as parameter in prepared statement
```cpp
	st->prepare("delete from simple_table where fld=:value_to_delete");

	// 1st way. Use set_null

	st->set_null(":value_to_delete");
	st->execute();

	// 2nd way. Use std::optional for parameter value

	std::optional<int> value = {}; // value is null

	st->set_int32_opt(":value_to_delete", value);
	st->execute();

	value = 42;
	st->set_int32_opt(":value_to_delete", value);
	st->execute();
```

### Simple transaction example
```cpp
	// 1st way. Creating and use dblib::Transaction

	auto transaction1 = conn->create_transaction(); // transactions have autostart flag on by default
	auto st1 = transaction1->create_statement();
	st1->execute("create table transact_table1 (val  integer)");
	transaction1->commit();

	// 2nd way. Use dblib::statement function for transactions

	auto st2 = conn->create_transaction()->create_statement();
	st2->execute("create table transact_table2 (val integer)");

	st2->commit_and_start_transaction(); // commit and then begin transaction

	st2->execute("insert into transact_table2(val) values (555)");
	st2->rollback_transaction();
```
### Little more extended transaction example
```cpp
	// Transaction parameters (ignored for SQLite):

	// read only transaction

	auto transaction3 = conn->create_transaction(TransactionAccess::Read);
	auto st3 = transaction3->create_statement();
	st3->execute("delete from simple_table"); // Ups... Not allowed

	// More transaction parameters

	TransactionParams tp;
	tp.access = TransactionAccess::Read;
	tp.level = TransactionLevel::RepeatableRead;
	tp.lock_resolution = LockResolution::Wait;
	tp.autostart = false;
	tp.auto_commit_on_destroy = true;
	tp.lock_time_out = 333; // value in seconds

	auto transaction4 = conn->create_transaction(tp);

	transaction4->start();
```

### Type conversion
Library automatically converts type of parameters and result fields
```cpp
	// type conversion for parameter values

	st->prepare("insert into simple_table (fld) values(:value)");

	st->set_int32(":value", 111); // no type conversion. param is integer
	st->execute();

	st->set_u8str(":value", "777"); // string -> integer
	st->execute();

	st->set_double(":value", 123.0); // double -> integer
	st->execute();

	// type conversion for query result

	st->execute("select fld from simple_table");

	if (st->fetch())
	{
		int int_value = st->get_int32("fld"); // no conversion
		std::string str_value = st->get_str_utf8("fld"); // integer -> string
		double double_value = st->get_double("fld"); // integer -> double
		float float_value = st->get_float("fld"); // integer -> float
	}
```

### Misc
```cpp
	// How much changed were done during last statemment execution?

	st->execute("delete from simple_table");

	printf(
		"%d tuples deleted\n",
		st->get_changes_count()
	);

	// ID of last inserted row (SQLite only)

	st->execute("create table table_pk(pk integer primary key, int_value integer)");

	st->execute("insert into table_pk(int_value) values(333)");

	printf(
		"Last inserted PK is %d\n",
		(int32_t)st->get_last_row_id()
	);
```
