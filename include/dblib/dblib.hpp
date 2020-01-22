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

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <optional>

#include "dblib_conf.hpp"
#include "dblib_consts.hpp"

namespace dblib {

// fwd.
class Exception;
class Connection;
class Transaction; typedef std::shared_ptr<Transaction> TransactionPtr;
class Statement; typedef std::shared_ptr<Statement> StatementPtr;

constexpr TransactionLevel DefaultTransactionLevel = TransactionLevel::Default;

struct DBLIB_API TransactionParams
{
public:
	TransactionParams() = default;
	TransactionParams(TransactionAccess access, bool autostart = true, bool auto_commit_on_destroy = true);
	TransactionParams(TransactionLevel level, bool autostart = true, bool auto_commit_on_destroy = true);
	TransactionParams(LockResolution lock_resolution, bool autostart = true, bool auto_commit_on_destroy = true);

	TransactionAccess access = TransactionAccess::ReadAndWrite;
	TransactionLevel level = TransactionLevel::RepeatableRead;
	LockResolution lock_resolution = LockResolution::Wait;
	bool autostart = true;
	bool auto_commit_on_destroy = true;
	uint32_t lock_time_out = 1; // seconds
};


class DBLIB_API Connection
{
public:
	virtual ~Connection();

	virtual void connect() = 0;
	virtual void disconnect() = 0;
	virtual bool is_connected() const = 0;

	virtual bool supports_sequences() const = 0;

	virtual TransactionPtr create_transaction(const TransactionParams& transaction_params = {}) = 0;

	virtual void set_default_transaction_level(TransactionLevel level) = 0;
	virtual TransactionLevel get_default_transaction_level() const = 0;

	virtual void direct_execute(std::string_view sql) = 0;
};

typedef std::shared_ptr<Connection> ConnectionPtr;

class DBLIB_API Transaction
{
public:
	virtual ~Transaction();

	virtual ConnectionPtr get_connection() = 0;
	virtual StatementPtr create_statement() = 0;

	virtual void start();
	virtual void commit();
	virtual void commit_and_start();
	virtual void rollback();
	virtual void rollback_and_start();

	TransactionState get_state() const;

protected:
	virtual void internal_start() = 0;
	virtual void internal_commit() = 0;
	virtual void internal_rollback() = 0;

	void set_state(TransactionState new_state);

	void check_not_started();
	void check_started();

private:
	TransactionState state_ = TransactionState::Undefined;
};

struct DBLIB_API Date
{
	int year = 0;
	int month = 0;
	int day = 0;
};

DBLIB_API bool operator == (const Date &date1, const Date &date2);
DBLIB_API bool operator != (const Date &date1, const Date &date2);


struct DBLIB_API Time
{
	int hour = 0;
	int min = 0;
	int sec = 0;
	int msec = 0;
};

DBLIB_API bool operator == (const Time &time1, const Time &time2);
DBLIB_API bool operator != (const Time &time1, const Time &time2);


struct DBLIB_API TimeStamp
{
	Date date;
	Time time;
};

DBLIB_API bool operator == (const TimeStamp &ts1, const TimeStamp &ts2);
DBLIB_API bool operator != (const TimeStamp &ts1, const TimeStamp &ts2);


enum class IndexOrNameType
{
	Index,
	Name
};

class DBLIB_API IndexOrName
{
public:
	IndexOrName(size_t index);
	IndexOrName(std::string_view name);
	IndexOrName(const char *name);
	IndexOrName(const std::string &name);

	IndexOrNameType get_type() const;

	size_t get_index() const;
	std::string_view get_name() const;

private:
	std::variant<size_t, std::string_view> value_;
};

using Int32Opt       = std::optional<int32_t>;
using Int64Opt       = std::optional<int64_t>;
using FloatOpt       = std::optional<float>;
using DoubleOpt      = std::optional<double>;
using StringOpt      = std::optional<std::string>;
using StringViewOpt  = std::optional<std::string_view>;
using WStringOpt     = std::optional<std::wstring>;
using WStringViewOpt = std::optional<std::wstring_view>;
using DateOpt        = std::optional<Date>;
using TimeOpt        = std::optional<Time>;
using TimeStampOpt   = std::optional<TimeStamp>;

class DBLIB_API Statement
{
public:
	virtual ~Statement();

	// prepare statements

	virtual void prepare(std::string_view sql, bool use_native_parameters_syntax = false) = 0;
	virtual void prepare(std::wstring_view sql, bool use_native_parameters_syntax = false) = 0;

	virtual StatementType get_type() = 0;

	// execute statements

	virtual void execute() = 0;
	virtual void execute(std::string_view sql) = 0;
	virtual void execute(std::wstring_view sql) = 0;

	virtual size_t get_changes_count() = 0;

	virtual int64_t get_last_row_id() = 0;

	virtual std::string get_last_sql() const = 0;

	virtual bool fetch() = 0;

	// parameters

	virtual size_t get_params_count() const = 0;
	virtual ValueType get_param_type(const IndexOrName& param) = 0;

	virtual void set_null(const IndexOrName& param) = 0;

	virtual void set_int32_opt(const IndexOrName& param, Int32Opt value) = 0;
	void set_int32(const IndexOrName& param, int32_t value);
	void set(const IndexOrName& param, Int32Opt value);
	void set(const IndexOrName& param, int32_t value);

	virtual void set_int64_opt(const IndexOrName& param, Int64Opt value) = 0;
	void set_int64(const IndexOrName& param, int64_t value);
	void set(const IndexOrName& param, Int64Opt value);
	void set(const IndexOrName& param, int64_t value);

	virtual void set_float_opt(const IndexOrName& param, FloatOpt value) = 0;
	void set_float(const IndexOrName& param, float value);
	void set(const IndexOrName& param, FloatOpt value);
	void set(const IndexOrName& param, float value);

	virtual void set_double_opt(const IndexOrName& param, DoubleOpt value) = 0;
	void set_double(const IndexOrName& param, double value);
	void set(const IndexOrName& param, DoubleOpt value);
	void set(const IndexOrName& param, double value);

	virtual void set_u8str_opt(const IndexOrName& param, const StringOpt&text) = 0;
	void set_u8str(const IndexOrName& param, const std::string& text);
	void set(const IndexOrName& param, const StringOpt& text);
	void set(const IndexOrName& param, const std::string& text);

	virtual void set_wstr_opt(const IndexOrName& param, const WStringOpt&text) = 0;
	void set_wstr(const IndexOrName& param, const std::wstring& text);
	void set(const IndexOrName& param, const WStringOpt& text);
	void set(const IndexOrName& param, const std::wstring& text);

	virtual void set_date_opt(const IndexOrName& param, const DateOpt &date) = 0;
	void set_date(const IndexOrName& param, const Date& date);
	void set(const IndexOrName& param, const DateOpt& date);
	void set(const IndexOrName& param, const Date& date);

	virtual void set_time_opt(const IndexOrName& param, const TimeOpt &time) = 0;
	void set_time(const IndexOrName& param, const Time& time);
	void set(const IndexOrName& param, const TimeOpt& time);
	void set(const IndexOrName& param, const Time& time);

	virtual void set_timestamp_opt(const IndexOrName& param, const TimeStampOpt &ts) = 0;
	void set_timestamp(const IndexOrName& param, const TimeStamp& ts);
	void set(const IndexOrName& param, const TimeStampOpt& ts);
	void set(const IndexOrName& param, const TimeStamp& ts);

	virtual void set_blob(const IndexOrName& param, const char *blob_data, size_t blob_size) = 0;

	// results

	virtual size_t get_columns_count() = 0;
	virtual ValueType get_column_type(const IndexOrName& index) = 0;
	virtual std::string get_column_name(size_t index) = 0;

	virtual bool is_null(const IndexOrName &column) const = 0;

	virtual Int32Opt get_int32_opt(const IndexOrName& column) = 0;
	int32_t get_int32(const IndexOrName& column);
	int32_t get_int32_or(const IndexOrName& column, int32_t value_if_null);
	
	virtual Int64Opt get_int64_opt(const IndexOrName& column) = 0;
	int64_t get_int64(const IndexOrName& column);
	int64_t get_int64_or(const IndexOrName& column, int64_t value_if_null);
	
	virtual FloatOpt get_float_opt(const IndexOrName& column) = 0;
	float get_float(const IndexOrName& column);
	float get_float_or(const IndexOrName& column, float value_if_null);
	
	virtual DoubleOpt get_double_opt(const IndexOrName& column) = 0;
	double get_double(const IndexOrName& column);
	double get_double_or(const IndexOrName& column, double value_if_null);

	virtual StringOpt get_str_utf8_opt(const IndexOrName& column) = 0;
	std::string get_str_utf8(const IndexOrName& column);
	std::string get_str_utf8_or(const IndexOrName& column, std::string_view value_if_null);
	
	virtual WStringOpt get_wstr_opt(const IndexOrName& column) = 0;
	std::wstring get_wstr(const IndexOrName& column);
	std::wstring get_wstr_or(const IndexOrName& column, std::wstring_view value_if_null);
	
	virtual DateOpt get_date_opt(const IndexOrName& column) = 0;
	Date get_date(const IndexOrName& column);
	Date get_date_or(const IndexOrName& column, const Date& value_if_null);
	
	virtual TimeOpt get_time_opt(const IndexOrName& column) = 0;
	Time get_time(const IndexOrName& column);
	Time get_time_or(const IndexOrName& column, const Time& value_if_null);
	
	virtual TimeStampOpt get_timestamp_opt(const IndexOrName& column) = 0;
	TimeStamp get_timestamp(const IndexOrName& column);
	TimeStamp get_timestamp_or(const IndexOrName& column, const TimeStamp& value_if_null);
	
	virtual void get_blob_data(const IndexOrName& column, char* dst, size_t size) = 0;
	virtual size_t get_blob_size(const IndexOrName& column) = 0;

	// transactions
	
	virtual TransactionPtr get_transaction() = 0;

	void start_transaction();
	void commit_transaction();
	void commit_and_start_transaction();
	void rollback_transaction();
	void rollback_and_start_transaction();
};


} //namespace dblib
