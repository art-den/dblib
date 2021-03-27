/*

Copyright (c) 2015-2020 Artyomov Denis (denis.artyomov@gmail.com)

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

#include <map>
#include <string>

#include "dblib/dblib_conf.hpp"
#include "dblib/dblib.hpp"
#include "dblib/postgresql_c_api/libpq-fe.h"

namespace dblib {

class PgConnection; typedef std::shared_ptr<PgConnection> PgConnectionPtr;
class PgTransaction; typedef std::shared_ptr<PgTransaction> PgTransactionPtr;
class PgStatement; typedef std::shared_ptr<PgStatement> PgStatementPtr;

struct DBLIB_API PgApi
{
	decltype(PQconnectdbParams)           *PQconnectdbParams = nullptr;
	decltype(PQfinish)                    *PQfinish = nullptr;
	decltype(PQstatus)                    *PQstatus = nullptr;
	decltype(PQerrorMessage)              *PQerrorMessage = nullptr;
	decltype(PQexec)                      *PQexec = nullptr;
	decltype(PQprepare)                   *PQprepare = nullptr;
	decltype(PQsendQueryParams)           *PQsendQueryParams = nullptr;
	decltype(PQsendQueryPrepared)         *PQsendQueryPrepared = nullptr;
	decltype(PQsetSingleRowMode)          *PQsetSingleRowMode = nullptr;
	decltype(PQgetResult)                 *PQgetResult = nullptr;
	decltype(PQresultStatus)              *PQresultStatus = nullptr;
	decltype(PQresStatus)                 *PQresStatus = nullptr;
	decltype(PQresultErrorMessage)        *PQresultErrorMessage = nullptr;
	decltype(PQresultVerboseErrorMessage) *PQresultVerboseErrorMessage = nullptr;
	decltype(PQresultErrorField)          *PQresultErrorField = nullptr;
	decltype(PQntuples)                   *PQntuples = nullptr;
	decltype(PQnfields)                   *PQnfields = nullptr;
	decltype(PQbinaryTuples)              *PQbinaryTuples = nullptr;
	decltype(PQfname)                     *PQfname = nullptr;
	decltype(PQftype)                     *PQftype = nullptr;
	decltype(PQfsize)                     *PQfsize = nullptr;
	decltype(PQcmdTuples)                 *PQcmdTuples = nullptr;
	decltype(PQgetvalue)                  *PQgetvalue = nullptr;
	decltype(PQgetlength)                 *PQgetlength = nullptr;
	decltype(PQgetisnull)                 *PQgetisnull = nullptr;
	decltype(PQnparams)                   *PQnparams = nullptr;
	decltype(PQparamtype)                 *PQparamtype = nullptr;
	decltype(PQdescribePrepared)          *PQdescribePrepared = nullptr;
	decltype(PQclear)                     *PQclear = nullptr;
	decltype(PQputCopyData)               *PQputCopyData = nullptr;
	decltype(PQputCopyEnd)                *PQputCopyEnd = nullptr;
	
};

struct DBLIB_API PgConnectParams
{
	using Items = std::map<std::string, std::string>;

	std::string host;
	int port = -1;
	std::string db_name;
	std::string user;
	std::string password;
	int connect_timeout = -1;
	std::string encoding = "UTF8";

	Items other_items;
};

class DBLIB_API PgLib
{
public:
	virtual ~PgLib();

	virtual void load(const std::wstring_view dyn_lib_file_name = {}) = 0;
	virtual bool is_loaded() const = 0;
	virtual const PgApi& get_api() = 0;
	virtual PgConnectionPtr create_connection(const PgConnectParams& connect_params) = 0;
};

using PgLibPtr = std::shared_ptr<PgLib>;

class DBLIB_API PgBuffer
{
public:
	void clear();

	void begin_tuple();

	void write_int32_opt(Int32Opt value);
	void write_int64_opt(Int64Opt value);
	void write_float_opt(FloatOpt value);
	void write_double_opt(DoubleOpt value);
	void write_u8str_opt(const StringOpt& text);
	void write_wstr_opt(const WStringOpt& text);
	void write_date_opt(const DateOpt& date);
	void write_time_opt(const TimeOpt& time);
	void write_timestamp_opt(const TimeStampOpt& ts);

	void end_tuple();

	const char* get_data() const;
	uint32_t get_size() const;

private:
	mutable std::vector<char> data_;
	bool header_added_ = false;
	mutable bool footer_added_ = false;
	size_t start_tuple_pos_ = 0;
	static constexpr unsigned BeBuffSize = 8;
	char be_buffer_[BeBuffSize] = {};
	uint16_t col_count_ = 0;
	std::string utf8_buffer_;

	void add_header();
	void add_footer() const;

	template <typename T>
	void write_opt(const std::optional<T> &value);

	template <typename T>
	void write_value(T value);

	void write_null();
	void write_len(uint32_t len);
};

class DBLIB_API PgConnection : public Connection
{
public:
	virtual PGconn* get_connection() = 0;

	virtual PgTransactionPtr create_pg_transaction(const TransactionParams& transaction_params) = 0;
};

class DBLIB_API PgTransaction : public Transaction
{
public:
	virtual PgStatementPtr create_pg_statement() = 0;
};

class DBLIB_API PgStatement : public Statement
{
public:
	virtual void put_copy_data(const char *data, int data_len) = 0;
	virtual void put_buffer(const PgBuffer &buffer) = 0;
};

// Date, time and timestamp conversions in or from internal PG format

DBLIB_API int32_t dblib_date_to_pg_date(const Date& date);

DBLIB_API int64_t dblib_time_to_pg_time(const Time& time);

DBLIB_API int64_t dblib_timestamp_to_pg_timestamp(const TimeStamp& ts);

DBLIB_API Date pg_date_to_dblib_date(int32_t pg_date);

DBLIB_API Time pg_time_to_dblib_time(int64_t pg_time, int32_t *date_rest = nullptr);

DBLIB_API TimeStamp pg_ts_to_dblib_ts(int64_t pg_ts);


DBLIB_API PgLibPtr create_pg_lib();

} // namespace dblib
