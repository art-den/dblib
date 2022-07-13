/*

Copyright (c) 2015-2022 Artyomov Denis (denis.artyomov@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

#pragma once

#include "dblib_conf.hpp"
#include "dblib.hpp"
#include "sqlite_c_api/sqlite3.h"

namespace dblib {


// fwd.
class SqliteConnection; typedef std::shared_ptr<SqliteConnection> SqliteConnectionPtr;
class SQLiteTransaction; typedef std::shared_ptr<SQLiteTransaction> SQLiteTransactionPtr;
class SQLiteStatement; typedef std::shared_ptr<SQLiteStatement> SQLiteStatementPtr;

struct DBLIB_API SqliteApi
{
	decltype(sqlite3_close)                *f_sqlite3_close = nullptr;
	decltype(sqlite3_exec)                 *f_sqlite3_exec = nullptr;
	decltype(sqlite3_open_v2)              *f_sqlite3_open_v2 = nullptr;
	decltype(sqlite3_errmsg)               *f_sqlite3_errmsg = nullptr;
	decltype(sqlite3_errstr)               *f_sqlite3_errstr = nullptr;
	decltype(sqlite3_prepare_v2)           *f_sqlite3_prepare_v2 = nullptr;
	decltype(sqlite3_prepare16_v2)         *f_sqlite3_prepare16_v2 = nullptr;
	decltype(sqlite3_bind_parameter_count) *f_sqlite3_bind_parameter_count = nullptr;
	decltype(sqlite3_bind_blob)            *f_sqlite3_bind_blob = nullptr;
	decltype(sqlite3_bind_double)          *f_sqlite3_bind_double = nullptr;
	decltype(sqlite3_bind_int)             *f_sqlite3_bind_int = nullptr;
	decltype(sqlite3_bind_int64)           *f_sqlite3_bind_int64 = nullptr;
	decltype(sqlite3_bind_null)            *f_sqlite3_bind_null = nullptr;
	decltype(sqlite3_bind_text)            *f_sqlite3_bind_text = nullptr;
	decltype(sqlite3_column_count)         *f_sqlite3_column_count = nullptr;
	decltype(sqlite3_column_name)          *f_sqlite3_column_name = nullptr;
	decltype(sqlite3_step)                 *f_sqlite3_step = nullptr;
	decltype(sqlite3_column_blob)          *f_sqlite3_column_blob = nullptr;
	decltype(sqlite3_column_bytes)         *f_sqlite3_column_bytes = nullptr;
	decltype(sqlite3_column_double)        *f_sqlite3_column_double = nullptr;
	decltype(sqlite3_column_int)           *f_sqlite3_column_int = nullptr;
	decltype(sqlite3_column_int64)         *f_sqlite3_column_int64 = nullptr;
	decltype(sqlite3_column_text)          *f_sqlite3_column_text = nullptr;
	decltype(sqlite3_column_text16)        *f_sqlite3_column_text16 = nullptr;
	decltype(sqlite3_column_type)          *f_sqlite3_column_type = nullptr;
	decltype(sqlite3_finalize)             *f_sqlite3_finalize = nullptr;
	decltype(sqlite3_reset)                *f_sqlite3_reset = nullptr;
	decltype(sqlite3_bind_parameter_index) *f_sqlite3_bind_parameter_index = nullptr;
	decltype(sqlite3_changes)              *f_sqlite3_changes = nullptr;
	decltype(sqlite3_last_insert_rowid)    *f_sqlite3_last_insert_rowid = nullptr;
	decltype(sqlite3_create_function_v2)   *f_sqlite3_create_function_v2 = nullptr;
	decltype(sqlite3_value_type)           *f_sqlite3_value_type = nullptr;
	decltype(sqlite3_value_text16)         *f_sqlite3_value_text16 = nullptr;
	decltype(sqlite3_value_text)           *f_sqlite3_value_text = nullptr;
	decltype(sqlite3_user_data)            *f_sqlite3_user_data = nullptr;
	decltype(sqlite3_result_text16)        *f_sqlite3_result_text16 = nullptr;
	decltype(sqlite3_result_value)         *f_sqlite3_result_value = nullptr;
	decltype(sqlite3_result_text)          *f_sqlite3_result_text = nullptr;
	decltype(sqlite3_busy_timeout)         *f_sqlite3_busy_timeout = nullptr;
	decltype(sqlite3_extended_errcode)     *f_sqlite3_extended_errcode = nullptr;
};

enum class SqliteMultiThreadMode
{
	Default,
	FullMutex,
	NoMutex
};

enum class SqliteAutoVacuum
{
	Default = -1,
	None = 0,
	Full = 1,
	Incremental = 2,
};

enum class SqliteJournalMode
{
	Default,
	Delete,
	Truncate,
	Persist,
	Memory,
	WAL,
	Off,
};

enum class SqliteForignKey
{
	Default,
	True,
	False
};

enum class SqliteCILike
{
	Default,
	True,
	False
};

struct DBLIB_API SqliteConfig
{
	SqliteForignKey foreign_keys = SqliteForignKey::Default;

	SqliteAutoVacuum auto_vacuum = SqliteAutoVacuum::Default;

	SqliteCILike case_sensitive_like = SqliteCILike::Default;

	int cache_size = 0; // default

	SqliteJournalMode journal_mode = SqliteJournalMode::Default;

	size_t page_size = 0; // default

	SqliteMultiThreadMode multi_thread_mode = SqliteMultiThreadMode::Default;
};


/* class SqliteLib */

class DBLIB_API SqliteLib
{
public:
	virtual ~SqliteLib();

	virtual void load(const FileName &dyn_lib_file_name = {}) = 0;
	virtual bool is_loaded() const = 0;

	virtual const SqliteApi& get_api() = 0;

	virtual SqliteConnectionPtr create_connection(const std::wstring &file_name, const SqliteConfig &config) = 0;
	virtual SqliteConnectionPtr create_connection(const std::string &file_name_utf8, const SqliteConfig &config) = 0;
};

typedef std::shared_ptr<SqliteLib> SqliteLibPtr;


/* class SqliteConnection */

class DBLIB_API SqliteConnection : public Connection
{
public:
	virtual sqlite3* get_instance() = 0;
	virtual SQLiteTransactionPtr create_sqlite_transaction(const TransactionParams &transaction_params) = 0;
};


/* SQLiteTransaction */

class DBLIB_API SQLiteTransaction : public Transaction
{
public:
	virtual SQLiteStatementPtr create_sqlite_statement() = 0;
};


/* class SQLiteStatement */

class DBLIB_API SQLiteStatement : public Statement
{
public:
	virtual sqlite3_stmt* get_stmt() = 0;
};


DBLIB_API SqliteLibPtr create_sqlite_lib();

} // namespace dblib
