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

#include <assert.h>

#include "dblib/dblib.hpp"
#include "dblib_type_cvt.hpp"

namespace dblib {

/* struct TransactionParams */

TransactionParams::TransactionParams(TransactionAccess access, bool autostart, bool auto_commit_on_destroy) :
	access(access),
	autostart(autostart),
	auto_commit_on_destroy(auto_commit_on_destroy)
{}

TransactionParams::TransactionParams(TransactionLevel level, bool autostart, bool auto_commit_on_destroy) :
	level(level),
	autostart(autostart),
	auto_commit_on_destroy(auto_commit_on_destroy)
{}

TransactionParams::TransactionParams(LockResolution lock_resolution, bool autostart, bool auto_commit_on_destroy) :
	lock_resolution(lock_resolution),
	autostart(autostart),
	auto_commit_on_destroy(auto_commit_on_destroy)
{}


/* class Connection */

Connection::~Connection()
{}


/* class Transaction */

Transaction::~Transaction()
{}

void Transaction::start()
{
	check_not_started();
	internal_start();
}

void Transaction::commit()
{
	check_started();
	internal_commit();
}

void Transaction::commit_and_start()
{
	check_started();
	internal_commit();
	internal_start();
}

void Transaction::rollback()
{
	check_started();
	internal_rollback();
}

void Transaction::rollback_and_start()
{
	check_started();
	internal_rollback();
	internal_start();
}

TransactionState Transaction::get_state() const
{
	return state_;
}

void Transaction::set_state(TransactionState new_state)
{
	state_ = new_state;
}

void Transaction::check_not_started()
{
	if (state_ == TransactionState::Started)
		throw WrongSeqException("Transaction is already started");
}

void Transaction::check_started()
{
	if (state_ != TransactionState::Started)
		throw WrongSeqException("Transaction is not started");
}


/* struct Date */

static bool dates_equal(const Date& date1, const Date& date2)
{
	return
		(date1.year == date2.year) &&
		(date1.month == date2.month) &&
		(date1.day == date2.day);
}

bool operator == (const Date& date1, const Date& date2)
{
	return dates_equal(date1, date2);
}

bool operator != (const Date& date1, const Date& date2)
{
	return !dates_equal(date1, date2);
}


/* struct Time */

static bool times_equal(const Time& time1, const Time& time2)
{
	return
		(time1.hour == time2.hour) &&
		(time1.min == time2.min) &&
		(time1.sec == time2.sec) &&
		(time1.msec == time2.msec);
}

bool operator == (const Time& time1, const Time& time2)
{
	return times_equal(time1, time2);
}

bool operator != (const Time& time1, const Time& time2)
{
	return !times_equal(time1, time2);
}


/* struct TimeStamp */

static bool ts_equal(const TimeStamp& ts1, const TimeStamp& ts2)
{
	return
		dates_equal(ts1.date, ts2.date) &&
		times_equal(ts1.time, ts2.time);
}

bool operator == (const TimeStamp& ts1, const TimeStamp& ts2)
{
	return ts_equal(ts1, ts2);
}

bool operator != (const TimeStamp& ts1, const TimeStamp& ts2)
{
	return !ts_equal(ts1, ts2);
}


/* class IndexOrName */

IndexOrName::IndexOrName(size_t index) :
	value_(index)
{}

IndexOrName::IndexOrName(std::string_view name) :
	value_(name)
{}

IndexOrName::IndexOrName(const char* name) :
	value_(name)
{}

IndexOrName::IndexOrName(const std::string& name) :
	value_(name)
{}


IndexOrNameType IndexOrName::get_type() const
{
	return 
		std::holds_alternative<size_t>(value_) ? 
		IndexOrNameType::Index : 
		IndexOrNameType::Name;
}

size_t IndexOrName::get_index() const
{
	assert(std::holds_alternative<size_t>(value_));
	return std::get<size_t>(value_);
}

std::string_view IndexOrName::get_name() const
{
	assert(std::holds_alternative<std::string_view>(value_));
	return std::get<std::string_view>(value_);
}


/* class Statement */

Statement::~Statement()
{}


void Statement::set_int32(const IndexOrName& param, int32_t value)
{
	set_int32_opt(param, value);
}

void Statement::set(const IndexOrName& param, Int32Opt value)
{
	set_int32_opt(param, value);
}

void Statement::set(const IndexOrName& param, int32_t value)
{
	set_int32_opt(param, value);
}

void Statement::set_int64(const IndexOrName& param, int64_t value)
{
	set_int64_opt(param, value);
}

void Statement::set(const IndexOrName& param, Int64Opt value)
{
	set_int64_opt(param, value);
}

void Statement::set(const IndexOrName& param, int64_t value)
{
	set_int64_opt(param, value);
}

void Statement::set_float(const IndexOrName& param, float value)
{
	set_float_opt(param, value);
}

void Statement::set(const IndexOrName& param, FloatOpt value)
{
	set_float_opt(param, value);
}

void Statement::set(const IndexOrName& param, float value)
{
	set_float_opt(param, value);
}

void Statement::set_double(const IndexOrName& param, double value)
{
	set_double_opt(param, value);
}

void Statement::set(const IndexOrName& param, DoubleOpt value)
{
	set_double_opt(param, value);
}

void Statement::set(const IndexOrName& param, double value)
{
	set_double_opt(param, value);
}

void Statement::set_u8str(const IndexOrName& param, const std::string& text)
{
	set_u8str_opt(param, text);
}

void Statement::set(const IndexOrName& param, const StringOpt& text)
{
	set_u8str_opt(param, text);
}

void Statement::set(const IndexOrName& param, const std::string& text)
{
	set_u8str_opt(param, text);
}

void Statement::set_wstr(const IndexOrName& param, const std::wstring& text)
{
	set_wstr_opt(param, text);
}

void Statement::set(const IndexOrName& param, const WStringOpt& text)
{
	set_wstr_opt(param, text);
}

void Statement::set(const IndexOrName& param, const std::wstring& text)
{
	set_wstr_opt(param, text);
}

void Statement::set_date(const IndexOrName& param, const Date& date)
{
	set_date_opt(param, date);
}

void Statement::set(const IndexOrName& param, const DateOpt& date)
{
	set_date_opt(param, date);
}

void Statement::set(const IndexOrName& param, const Date& date)
{
	set_date_opt(param, date);
}

void Statement::set_time(const IndexOrName& param, const Time& time)
{
	set_time_opt(param, time);
}

void Statement::set(const IndexOrName& param, const TimeOpt& time)
{
	set_time_opt(param, time);
}

void Statement::set(const IndexOrName& param, const Time& time)
{
	set_time_opt(param, time);
}

void Statement::set_timestamp(const IndexOrName& param, const TimeStamp& ts)
{
	set_timestamp_opt(param, ts);
}

void Statement::set(const IndexOrName& param, const TimeStampOpt& ts)
{
	set_timestamp_opt(param, ts);
}

void Statement::set(const IndexOrName& param, const TimeStamp& ts)
{
	set_timestamp_opt(param, ts);
}


int32_t Statement::get_int32(const IndexOrName& column)
{
	auto result = get_int32_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

int32_t Statement::get_int32_or(const IndexOrName& column, int32_t value_if_null)
{
	auto result = get_int32_opt(column);
	if (!result) return value_if_null;
	return *result;
}

int64_t Statement::get_int64(const IndexOrName& column)
{
	auto result = get_int64_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

int64_t Statement::get_int64_or(const IndexOrName& column, int64_t value_if_null)
{
	auto result = get_int64_opt(column);
	if (!result) return value_if_null;
	return *result;
}

float Statement::get_float(const IndexOrName& column)
{
	auto result = get_float_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

float Statement::get_float_or(const IndexOrName& column, float value_if_null)
{
	auto result = get_float_opt(column);
	if (!result) return value_if_null;
	return *result;
}

double Statement::get_double(const IndexOrName& column)
{
	auto result = get_double_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

double Statement::get_double_or(const IndexOrName& column, double value_if_null)
{
	auto result = get_double_opt(column);
	if (!result) return value_if_null;
	return *result;
}

std::string Statement::get_str_utf8(const IndexOrName& column)
{
	auto result = get_str_utf8_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

std::string Statement::get_str_utf8_or(const IndexOrName& column, std::string_view value_if_null)
{
	auto result = get_str_utf8_opt(column);
	if (!result) return std::string{ value_if_null };
	return *result;
}

std::wstring Statement::get_wstr(const IndexOrName& column)
{
	auto result = get_wstr_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

std::wstring Statement::get_wstr_or(const IndexOrName& column, std::wstring_view value_if_null)
{
	auto result = get_wstr_opt(column);
	if (!result) return std::wstring{ value_if_null };
	return *result;
}

Date Statement::get_date(const IndexOrName& column)
{
	auto result = get_date_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

Date Statement::get_date_or(const IndexOrName& column, const Date& value_if_null)
{
	auto result = get_date_opt(column);
	if (!result) return value_if_null;
	return *result;
}

Time Statement::get_time(const IndexOrName& column)
{
	auto result = get_time_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

Time Statement::get_time_or(const IndexOrName& column, const Time& value_if_null)
{
	auto result = get_time_opt(column);
	if (!result) return value_if_null;
	return *result;
}

TimeStamp Statement::get_timestamp(const IndexOrName& column)
{
	auto result = get_timestamp_opt(column);
	if (!result) throw ColumnValueIsNullException();
	return *result;
}

TimeStamp Statement::get_timestamp_or(const IndexOrName& column, const TimeStamp& value_if_null)
{
	auto result = get_timestamp_opt(column);
	if (!result) return value_if_null;
	return *result;
}

void Statement::start_transaction()
{
	get_transaction()->start();
}

void Statement::commit_transaction()
{
	get_transaction()->commit();
}

void Statement::commit_and_start_transaction()
{
	get_transaction()->commit_and_start();
}

void Statement::rollback_transaction()
{
	get_transaction()->rollback();
}

void Statement::rollback_and_start_transaction()
{
	get_transaction()->rollback_and_start();
}



} // namespace dblib
