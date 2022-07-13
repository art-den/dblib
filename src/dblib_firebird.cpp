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

#include <assert.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <vector>

#include "../include/dblib/dblib_exception.hpp"
#include "../include/dblib/dblib_firebird.hpp"
#include "../include/dblib/dblib_cvt_utils.hpp"

#include "dblib_type_cvt.hpp"
#include "dblib_dyn.hpp"
#include "dblib_stmt_tools.hpp"

namespace dblib {

namespace
{
	const unsigned SQLDADefSize = 10;
	const unsigned StatusLen = 20;
	const unsigned short DaVersion = 1;
}

/* class BinaryBuffer */

class BinaryBuffer
{
public:
	short size() const
	{
		return (short)buffer_.size();
	}

	const char* data() const
	{
		return buffer_.size() ? buffer_.data() : nullptr;
	}

	void add_uint8(uint8_t key)
	{
		buffer_.push_back(key);
		assert(buffer_.size() <= SHRT_MAX);
	}

	void add_uint8(uint8_t key, uint8_t value)
	{
		buffer_.push_back(key);
		buffer_.push_back(value);
		assert(buffer_.size() <= SHRT_MAX);
	}

	void add_uint16_with_len(uint8_t key, uint16_t value)
	{
		add_int_with_len(key, value);
		assert(buffer_.size() <= SHRT_MAX);
	}

	void add_uint32_with_len(uint8_t key, uint32_t value)
	{
		add_int_with_len(key, value);
		assert(buffer_.size() <= SHRT_MAX);
	}

	void add_str(uint8_t key, const std::string &value)
	{
		buffer_.push_back(key);
		assert(value.size() <= UCHAR_MAX);
		buffer_.push_back((uint8_t)value.size());
		buffer_.insert(buffer_.end(), value.begin(), value.end());
		assert(buffer_.size() <= SHRT_MAX);
	}

	void add_str2(uint8_t key, const std::string &value)
	{
		buffer_.push_back(key);
		assert(value.size() <= USHRT_MAX);
		buffer_.push_back((uint8_t)value.size());
		buffer_.push_back((uint8_t)(value.size() >> 8));
		buffer_.insert(buffer_.end(), value.begin(), value.end());
		assert(buffer_.size() <= SHRT_MAX);
	}

protected:
	typedef std::vector<char> Buffer;
	Buffer buffer_;

	template <typename T>
	void add_int_with_len(uint8_t key, T value)
	{
		buffer_.push_back(key);
		buffer_.push_back(sizeof(T));

		for (unsigned i = 0; i < sizeof(T); i++)
		{
			buffer_.push_back(value & 0xFF);
			value >>= 8;
		}
	}
};

/* class TLRes */

template <unsigned Size>
class TLRes
{
public:
	TLRes()
	{
		std::fill(items_, items_ + Size, 0);
	}

	char* data() { return items_; }
	size_t size() const { return Size; }

	int get_int(const FbApi& api, int type, int def_value = -1) const
	{
		for (const char *p = items_; *p != isc_info_end;)
		{
			char item = *p++;
			int length = (int)api.f_isc_portable_integer((const ISC_UCHAR*)p, 2);
			p += 2;
			if (item == type)
				return (int)api.f_isc_portable_integer((const ISC_UCHAR*)p, length);
			p += length;
		}
		return def_value;
	}

private:
	char items_[Size] = {0};
};


/* struct FbLibData */

struct FbLibData
{
	FbApi api;
	DynLib module;
};

using FbLibDataPtr = std::shared_ptr<FbLibData>;


/* class FbLibImpl */

class FbLibImpl : public FbLib
{
public:
	FbLibImpl();
	~FbLibImpl() {}

	void load(const FileName &dyn_lib_file_name) override;
	bool is_loaded() const override;

	const FbApi& get_api() override;

	FbConnectionPtr create_connection(const FbConnectParams& connect_params, const FbDbCreateParams* create_params = nullptr) override;
	FbServicesPtr create_services() override;

private:
	FbLibDataPtr data_;
};


/* class FbServicesImpl */

class FbServicesImpl : public FbServices
{
public:
	FbServicesImpl(const FbLibDataPtr& lib);
	~FbServicesImpl();

	void attach(const FbServicesConnectParams& params) override;
	void detach() override;

	void add_user(const std::string &user, const std::string &password) override;
	void add_user(const std::string &user, const std::string &password, const std::string &firstname, const std::string &middlename, const std::string &lastname) override;

	void modify_user(const std::string &user, const std::string *password, const std::string *firstname, const std::string *middlename, const std::string *lastname) override;
	void delete_user(const std::string &user) override;

private:
	FbLibDataPtr lib_;
	isc_svc_handle handle_ = 0;

	void check_is_not_attached();
	void check_is_attached();

	void detach_internal(bool check_error);
	void start_service_and_check_status(const BinaryBuffer &data);
};


/* class FbConnectionImpl */

class FbConnectionImpl :
	public FbConnection,
	public std::enable_shared_from_this<FbConnectionImpl>
{
public:
	FbConnectionImpl(const FbLibDataPtr& lib, const FbConnectParams& connect_params, const FbDbCreateParams* create_params);
	~FbConnectionImpl();

	void connect() override;
	void disconnect() override;
	bool is_connected() const override;
	bool supports_sequences() const override;
	std::shared_ptr<Transaction> create_transaction(const TransactionParams& transaction_params) override;

	void set_default_transaction_level(TransactionLevel level) override;
	TransactionLevel get_default_transaction_level() const override;

	void direct_execute(std::string_view sql) override;

	std::string get_driver_name() const override;

	isc_db_handle& get_handle() override;
	short get_dialect() const override;

	FbTransactionPtr create_fb_transaction(const TransactionParams& transaction_params) override;

private:
	FbLibDataPtr lib;
	FbConnectParams connect_params_;
	FbDbCreateParams create_params_;
	const bool create_params_defined_;
	short dialect_ = -1;
	isc_db_handle db_handle_ = 0;
	TransactionLevel default_transaction_level_ = DefaultTransactionLevel;

	void internal_disconnect(bool throw_exception);
	void check_is_connected();
	void check_is_disconnected();
	void try_to_create_database();
};

using FbConnectionImplPtr = std::shared_ptr<FbConnectionImpl>;


/* class FbTransactionImpl */

class FbTransactionImpl :
	public FbTransaction,
	public std::enable_shared_from_this<FbTransactionImpl>
{
public:
	FbTransactionImpl(const FbLibDataPtr& lib, const FbConnectionImplPtr& conn, const TransactionParams& transaction_params);
	~FbTransactionImpl();

	ConnectionPtr get_connection() override;
	StatementPtr create_statement() override;

	isc_tr_handle& get_handle() override;
	FbStatementPtr create_fb_statement() override;

protected:
	void internal_start() override;
	void internal_commit() override;
	void internal_rollback() override;

private:
	FbLibDataPtr lib_;
	FbConnectionImplPtr conn_;

	BinaryBuffer tpb_;
	isc_tr_handle tran_ = 0;

	bool commit_on_destroy_ = true;
};

using FbTransactionImplPtr = std::shared_ptr<FbTransactionImpl>;


/* class SqlDA */

class SqlDA
{
public:
	SqlDA(int size);

	size_t get_size() const;

	void alloc_fields();
	void check_size(const FbApi& api, bool in, isc_stmt_handle stmt);
	XSQLDA* data() { return data_; }

	void close_blob_handles(const FbApi& api, bool throw_exception);
	void clear_null_flags();
	void clear_buffers();
	void check_index(size_t index) const;

	ValueType get_column_type(size_t index) const;
	std::string get_column_name(size_t index) const;

protected:
	XSQLVAR* get_var(size_t index);
	const XSQLVAR* get_var(size_t index) const;
	template <typename T> static T& as(XSQLVAR* var);
	short& null(size_t index);
	const short& null(size_t index) const;
	isc_blob_handle& blob_handle(size_t index);

	struct SqlDAItem
	{
		typedef std::vector<char> Buffer;

		Buffer          buffer;
		short           null_flag = 0;
		isc_blob_handle blob_handle = 0;
	};

private:
	typedef std::vector<SqlDAItem> Items;
	typedef std::vector<char> Buffer;

	XSQLDA* data_ = nullptr;
	Buffer data_buffer_;
	Items items_;

	void allocate(int size);
};


/* class class InSqlDA */

class InSqlDA : public SqlDA
{
public:
	InSqlDA(int size) : SqlDA(size) {}

	void set_null(size_t index, bool is_null);

	template <typename T>
	void set_param(size_t index, T value, int sql_type);
	void string_param(size_t index, const std::string& value);
	void wstring_param(size_t index, const std::wstring& value);
	void set_date(size_t index, const Date& date);
	void set_time(size_t index, const Time& time);
	void set_timestamp(size_t index, const TimeStamp& ts);
	void blob_param(const FbApi& api, size_t index, const char* blob_data, size_t blob_size, FbConnectionImpl& conn, FbTransactionImpl& tran);
};


/* class class OutSqlDA */

class OutSqlDA : public SqlDA
{
public:
	OutSqlDA(int size) : SqlDA(size) {}

	bool is_null(size_t index) const;
	template <typename T>
	T get_value(size_t index, int type);
	std::string get_string(const FbApi& api, size_t index, FbConnectionImpl& conn, FbTransactionImpl& tran);
	std::wstring get_wstring(size_t index);
	void get_date(size_t index, Date& date);
	void get_time(size_t index, Time& time);
	void get_timestamp(size_t index, TimeStamp& ts);
	size_t get_blob_size(const FbApi& api, size_t index, FbConnectionImpl& conn, FbTransactionImpl& tran);
	template<typename I>
	void read_blob(const FbApi& api, size_t index, I dst_from, I dst_to, FbConnectionImpl& conn, FbTransactionImpl& tran);

private:
	void prepare_blob_handle(const FbApi& api, size_t index, FbConnectionImpl& conn, FbTransactionImpl& tran);

	std::string tmp_buffer_;
};


/* class FbStatementImpl */

class FbStatementImpl :
	public FbStatement,
	public IParameterSetterWithTypeCvt,
	public IResultGetterWithTypeCvt
{
public:
	FbStatementImpl(const FbLibDataPtr& lib, const FbConnectionImplPtr& conn, const FbTransactionImplPtr& tran);
	~FbStatementImpl();

	TransactionPtr get_transaction() override;

	void prepare(std::string_view sql, bool use_native_parameters_syntax) override;
	void prepare(std::wstring_view sql, bool use_native_parameters_syntax) override;

	void execute(std::string_view sql) override;
	void execute(std::wstring_view sql) override;

	StatementType get_type() override;

	void execute() override;

	size_t get_changes_count() override;

	int64_t get_last_row_id() override;

	std::string get_last_sql() const override;

	bool fetch() override;

	size_t get_params_count() const override;
	ValueType get_param_type(const IndexOrName& param) override;

	void set_null(const IndexOrName& param) override;

	void set_int32_opt(const IndexOrName& param, Int32Opt value) override;
	void set_int64_opt(const IndexOrName& param, Int64Opt value) override;
	void set_float_opt(const IndexOrName& param, FloatOpt value) override;
	void set_double_opt(const IndexOrName& param, DoubleOpt value) override;
	void set_u8str_opt(const IndexOrName& param, const StringOpt& text) override;
	void set_wstr_opt(const IndexOrName& param, const WStringOpt& text) override;

	void set_date_opt(const IndexOrName& param, const DateOpt& date) override;
	void set_time_opt(const IndexOrName& param, const TimeOpt& time) override;
	void set_timestamp_opt(const IndexOrName& param, const TimeStampOpt& ts) override;

	void set_blob(const IndexOrName& param, const char* blob_data, size_t blob_size) override;

	size_t get_columns_count() override;
	ValueType get_column_type(const IndexOrName& column) override;
	std::string get_column_name(size_t index) override;

	bool is_null(const IndexOrName& column) override;
	Int32Opt get_int32_opt(const IndexOrName& column) override;
	Int64Opt get_int64_opt(const IndexOrName& column) override;
	FloatOpt get_float_opt(const IndexOrName& column) override;
	DoubleOpt get_double_opt(const IndexOrName& column) override;
	StringOpt get_str_utf8_opt(const IndexOrName& column) override;
	WStringOpt get_wstr_opt(const IndexOrName& column) override;
	DateOpt get_date_opt(const IndexOrName& column) override;
	TimeOpt get_time_opt(const IndexOrName& column) override;
	TimeStampOpt get_timestamp_opt(const IndexOrName& column) override;
	size_t get_blob_size(const IndexOrName& column) override;
	void get_blob_data(const IndexOrName& column, char* dst, size_t size) override;

	isc_stmt_handle& get_handle() override;

	// IParameterSetterWithTypeCvt impl.
	void set_int16_impl(size_t index, int16_t value) override;
	void set_int32_impl(size_t index, int32_t value) override;
	void set_int64_impl(size_t index, int64_t value) override;
	void set_float_impl(size_t index, float value) override;
	void set_double_impl(size_t index, double value) override;
	void set_u8str_impl(size_t index, const std::string& text) override;
	void set_wstr_impl(size_t index, const std::wstring& text) override;

	// IResultGetterWithTypeCvt impl.
	int16_t get_int16_impl(size_t index) override;
	int32_t get_int32_impl(size_t index) override;
	int64_t get_int64_impl(size_t index) override;
	float get_float_impl(size_t index) override;
	double get_double_impl(size_t index) override;
	std::string get_str_utf8_impl(size_t index) override;
	std::wstring get_wstr_impl(size_t index) override;

private:
	typedef std::vector<char> Buffer;
	typedef std::vector<Buffer> Buffers;

	FbConnectionImplPtr conn_;
	FbTransactionImplPtr tran_;
	FbLibDataPtr lib_;

	SqlPreprocessor sql_preprocessor_;
	isc_stmt_handle stmt_ = 0;
	InSqlDA in_sqlda_;
	OutSqlDA out_sqlda_;
	StatementType type_ = StatementType::Unknown;
	bool cursor_opened_ = false;
	std::string last_sql_;
	std::string preprocessed_sql_;
	bool sql_preprocessed_flag_ = false;
	ColumnsHelper columns_helper_;
	bool has_data_ = false;
	std::string utf8_sql_buffer_;

	void prepare_impl(std::string_view sql);
	StatementType get_type_internal();
	void check_is_prepared() const;
	void check_has_data() const;
	void close(bool throw_exception);
	void close_cursor();
	void internal_execute();

	template<typename T>
	void set_param_opt_impl(const IndexOrName& param, const std::optional<T>& value);

	template<typename T>
	std::optional<T> get_value_opt_impl(const IndexOrName& column);
};

/* class FbSqlPreprocessorActions */

class FbSqlPreprocessorActions : public SqlPreprocessorActions
{
protected:
	void append_index_param_to_sql(const std::string&, int, std::string& sql) const override
	{
		sql.append("?");
	}

	void append_named_param_to_sql(const std::string& parameter, int, std::string& sql) const override
	{
		sql.append("?");
	}

	void append_if_seq_data(const std::string& data, const std::string& other, std::string& sql) const override
	{
		sql.append(data);
		sql.append(other);
	}

	void append_seq_generator(const std::string& seq_name, const std::string& other, std::string& sql) const override
	{
		sql.append("gen_id(");
		sql.append(seq_name);
		sql.append(", 1)");
		sql.append(other);
	}
};


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename V, typename M>
void check_not_greater(V value, M max, const char *error_text)
{
	if (value <= (V)max) return;
	throw WrongArgumentException(error_text);
}

static void trim_right(std::string& str)
{
	while (!str.empty() && (str.back() == ' '))
		str.pop_back();
}

static void get_str_var(const XSQLVAR *var, std::string &result)
{
	switch (var->sqltype & ~1)
	{
	case SQL_TEXT:
		result.assign(var->sqldata, var->sqldata + var->sqllen);
		trim_right(result);
		break;

	case SQL_VARYING:
		{
			unsigned short len = *(unsigned short*)var->sqldata;
			result.assign(var->sqldata + 2, var->sqldata + 2 + len);
			break;
		}

	default:
		throw InternalException("Is not string field", -1, -1);
	}
}

static void set_str_var(const XSQLVAR *var, const std::string &str)
{
	if (str.size() > SHRT_MAX)
		throw WrongArgumentException("Len of varchar field can't exceed 32K!");

	if (str.size() > (unsigned)var->sqllen)
		throw WrongArgumentException("String it too long for parameter");

	switch (var->sqltype & ~1)
	{
	case SQL_TEXT:
		std::copy(str.begin(), str.end(), var->sqldata);
		std::fill(var->sqldata + str.size(), var->sqldata + var->sqllen, ' ');
		break;

	case SQL_VARYING:
		*(unsigned short*)var->sqldata = (unsigned short)str.size();
		std::copy(str.begin(), str.end(), var->sqldata + 2);
		break;

	default:
		throw InternalException("Is not string field", -1, -1);
	}
}

static ValueType cvt_fb_type_to_lib_type(ISC_SHORT fb_type)
{
	switch (fb_type)
	{
	case SQL_TEXT:
		return ValueType::Char;

	case SQL_VARYING:
		return ValueType::Varchar;

	case SQL_SHORT:
		return ValueType::Short;

	case SQL_LONG:
		return ValueType::Integer;

	case SQL_FLOAT:
		return ValueType::Float;

	case SQL_DOUBLE:
		return ValueType::Double;

	case SQL_TIMESTAMP:
		return ValueType::Timestamp;

	case SQL_BLOB:
		return ValueType::Blob;

	case SQL_TYPE_TIME:
		return ValueType::Time;

	case SQL_TYPE_DATE:
		return ValueType::Date;

	case SQL_INT64:
		return ValueType::BigInt;
	}

	throw InternalException("Field of this type is not supported", fb_type, 0);
}

static void fb_date_to_dblib_date(ISC_DATE fb_date, Date &dblib_d)
{
	// (idea from IBPP)
	int RataDie = fb_date + (693595 - 15019);
	int Z = RataDie + 306;
	int H = 100 * Z - 25;
	int A = H / 3652425;
	int B = A - A / 4;
	dblib_d.year = (100 * B + H) / 36525;
	int C = B + Z - 365 * dblib_d.year - dblib_d.year / 4;
	dblib_d.month = (5 * C + 456) / 153;
	dblib_d.day = C - (153 * dblib_d.month - 457) / 5;
	if (dblib_d.month > 12)
	{
		dblib_d.year += 1;
		dblib_d.month -= 12;
	}
}

static ISC_DATE dblib_date_to_fb_date(const Date &date)
{
	// (idea from IBPP)
	int d = date.day;
	int m = date.month;
	int y = date.year;
	if (m < 3)
	{
		m += 12;
		y -= 1;
	}
	int RataDie = d + (153 * m - 457) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 306;
	return RataDie - (693595 - 15019);
}

static void fb_time_to_dblib_time(ISC_TIME time, Time &dblib_t)
{
	dblib_t.hour = time / 36000000;
	time -= dblib_t.hour * 36000000;

	dblib_t.min = time / 600000;
	time -= dblib_t.min * 600000;

	dblib_t.sec = time / 10000;
	time -= dblib_t.sec * 10000;

	dblib_t.msec = time / 10;
}

static ISC_TIME dblib_time_to_fb_time(const Time &time)
{
	return
		time.hour * 36000000 +
		time.min * 600000 +
		time.sec * 10000 +
		time.msec * 10;
}

static void fb_timestamp_to_dblib_timestamp(TimeStamp &dblib_ts, const ISC_TIMESTAMP &fb_ts)
{
	fb_date_to_dblib_date(fb_ts.timestamp_date, dblib_ts.date);
	fb_time_to_dblib_time(fb_ts.timestamp_time, dblib_ts.time);
}

static ISC_TIMESTAMP dblib_timestamp_to_fb_timestamp(const TimeStamp &dblib_ts)
{
	ISC_TIMESTAMP result = {};
	result.timestamp_date = dblib_date_to_fb_date(dblib_ts.date);
	result.timestamp_time = dblib_time_to_fb_time(dblib_ts.time);
	return result;
}

static bool is_status_vector_ok(const ISC_STATUS *status_vect)
{
	return ((status_vect[0] != 1) || (status_vect[1] <= 0));
}

bool find_error_code(const ISC_STATUS* status_vect, ISC_STATUS error_code)
{
	for (size_t i = 0; (i < StatusLen) && (status_vect[i] != 0); i += 2)
		if (status_vect[i] == 1)
			if (status_vect[i + 1] == error_code) return true;

	return false;
}

static void check_status_vector(
	const FbApi       &api,
	const char        *fun_name,
	const ISC_STATUS  *status_vect,
	std::string_view  sql)
{
	if (is_status_vector_ok(status_vect)) return;

	// error code
	ISC_LONG sql_code = api.f_isc_sqlcode(status_vect);

	// error code explanation
	char sql_msg[2048] = { 0 };
	api.f_isc_sql_interprete((ISC_SHORT)sql_code, sql_msg, sizeof(sql_msg));

	// error text
	std::string whole_error_text;
	const ISC_STATUS *status_vector_to_call = status_vect;
	for (;;)
	{
		char fb_msg[1024] = { 0 };
		auto res = api.f_fb_interpret(fb_msg, sizeof(fb_msg), &status_vector_to_call);
		if (!res) break;
		whole_error_text.append(fb_msg);
		whole_error_text.append("\n");
		if (!status_vector_to_call) break;
	}

	// can't connect?
	bool cant_connect = find_error_code(status_vect, isc_net_connect_err);

	// lost connection?
	bool lost_conn =
		find_error_code(status_vect, isc_net_read_err) ||
		find_error_code(status_vect, isc_net_write_err);

	// deadlock?
	bool update_conflict =
		find_error_code(status_vect, isc_deadlock) ||
		find_error_code(status_vect, isc_update_conflict) ||
		find_error_code(status_vect, isc_lock_conflict);

	// TODO: exception for transactions

	ErrorType error_type =
		cant_connect    ? ErrorType::Connection :
		lost_conn       ? ErrorType::LostConnection :
		update_conflict ? ErrorType::Lock :
		ErrorType::Normal;

	// throw
	throw_exception(
		fun_name,
		sql_code,
		-1,
		sql_msg,
		std::to_string(sql_code),
		whole_error_text,
		sql,
		error_type
	);
}


/* class FbLib */

FbLib::~FbLib()
{}


/* class FbLibImpl */

FbLibImpl::FbLibImpl() :
	data_(std::make_shared<FbLibData>())
{}

void FbLibImpl::load(const FileName &dyn_lib_file_name)
{
	if (data_->module.is_loaded()) return;

	auto file_name = dyn_lib_file_name;

#if defined(DBLIB_WINDOWS)
	if (file_name.empty())
		file_name = L"fbclient.dll";
#elif defined(DBLIB_LINUX)
	if (file_name.empty())
		file_name = "fbclient.so";
#endif

	data_->module.load(file_name);

	auto& module = data_->module;
	auto& api = data_->api;

	module.load_func(api.f_isc_attach_database,         "isc_attach_database");
	module.load_func(api.f_isc_database_info,           "isc_database_info");
	module.load_func(api.f_fb_interpret,                "fb_interpret");
	module.load_func(api.f_isc_sql_interprete,          "isc_sql_interprete");
	module.load_func(api.f_isc_detach_database,         "isc_detach_database");
	module.load_func(api.f_isc_create_database,         "isc_create_database");
	module.load_func(api.f_isc_blob_info,               "isc_blob_info");
	module.load_func(api.f_isc_close_blob,              "isc_close_blob");
	module.load_func(api.f_isc_commit_transaction,      "isc_commit_transaction");
	module.load_func(api.f_isc_create_blob2,            "isc_create_blob2");
	module.load_func(api.f_isc_dsql_allocate_statement, "isc_dsql_allocate_statement");
	module.load_func(api.f_isc_dsql_describe,           "isc_dsql_describe");
	module.load_func(api.f_isc_dsql_describe_bind,      "isc_dsql_describe_bind");
	module.load_func(api.f_isc_dsql_execute2,           "isc_dsql_execute2");
	module.load_func(api.f_isc_dsql_fetch,              "isc_dsql_fetch");
	module.load_func(api.f_isc_dsql_free_statement,     "isc_dsql_free_statement");
	module.load_func(api.f_isc_dsql_prepare,            "isc_dsql_prepare");
	module.load_func(api.f_isc_dsql_sql_info,           "isc_dsql_sql_info");
	module.load_func(api.f_isc_get_segment,             "isc_get_segment");
	module.load_func(api.f_isc_open_blob2,              "isc_open_blob2");
	module.load_func(api.f_isc_put_segment,             "isc_put_segment");
	module.load_func(api.f_isc_rollback_transaction,    "isc_rollback_transaction");
	module.load_func(api.f_isc_start_transaction,       "isc_start_transaction");
	module.load_func(api.f_isc_sqlcode,                 "isc_sqlcode");
	module.load_func(api.f_isc_portable_integer,        "isc_portable_integer");
	module.load_func(api.f_isc_service_attach,          "isc_service_attach");
	module.load_func(api.f_isc_service_detach,          "isc_service_detach");
	module.load_func(api.f_isc_service_start,           "isc_service_start");
}

bool FbLibImpl::is_loaded() const
{
	return data_->module.is_loaded();
}

const FbApi& FbLibImpl::get_api()
{
	return data_->api;
}

FbConnectionPtr FbLibImpl::create_connection(
	const FbConnectParams&  connect_params,
	const FbDbCreateParams* create_params)
{
	return std::make_shared<FbConnectionImpl>(data_, connect_params, create_params);
}

FbServicesPtr FbLibImpl::create_services()
{
	return std::make_shared<FbServicesImpl>(data_);
}


/* class FbServices */

FbServices::~FbServices()
{}


/* class FbServicesImpl */

FbServicesImpl::FbServicesImpl(const FbLibDataPtr& lib) :
	lib_(lib)
{}

FbServicesImpl::~FbServicesImpl()
{
	if (handle_ != 0)
		detach_internal(false);
}

void FbServicesImpl::attach(const FbServicesConnectParams &params)
{
	check_is_not_attached();

	std::string host_and_name;

	auto host = params.host;
	if (!host.empty())
	{
		host_and_name.append(host);
		host_and_name.append(":");
	}

	auto name = params.name;
	host_and_name.append(name);

	BinaryBuffer spb;
	spb.add_uint8(isc_spb_version, isc_spb_current_version);
	spb.add_str(isc_spb_user_name, params.user);
	spb.add_str(isc_spb_password, params.password);

	ISC_STATUS status[StatusLen] = {};

	lib_->api.f_isc_service_attach(status, 0, (char*)name.c_str(), &handle_, spb.size(), spb.data());
	check_status_vector(lib_->api, "isc_service_attach", status, {});
}

void FbServicesImpl::detach()
{
	check_is_attached();
	detach_internal(true);
}

void FbServicesImpl::add_user(const std::string &user, const std::string &password)
{
	check_is_attached();
	BinaryBuffer args;
	args.add_uint8(isc_action_svc_add_user);
	args.add_str2(isc_spb_sec_username, user);
	args.add_str2(isc_spb_sec_password, password);
	start_service_and_check_status(args);
}

void FbServicesImpl::add_user(
	const std::string &user,
	const std::string &password,
	const std::string &firstname,
	const std::string &middlename,
	const std::string &lastname)
{
	check_is_attached();
	BinaryBuffer args;
	args.add_uint8(isc_action_svc_add_user);
	args.add_str2(isc_spb_sec_username, user);
	args.add_str2(isc_spb_sec_password, password);
	if (!firstname.empty()) args.add_str2(isc_spb_sec_firstname, firstname);
	if (!middlename.empty()) args.add_str2(isc_spb_sec_middlename, middlename);
	if (!lastname.empty()) args.add_str2(isc_spb_sec_lastname, lastname);
	start_service_and_check_status(args);
}

void FbServicesImpl::modify_user(
	const std::string &user,
	const std::string *password,
	const std::string *firstname,
	const std::string *middlename,
	const std::string *lastname)
{
	check_is_attached();
	BinaryBuffer args;
	args.add_uint8(isc_action_svc_modify_user);
	args.add_str2(isc_spb_sec_username, user);
	if (password  ) args.add_str2(isc_spb_sec_password,   *password);
	if (firstname ) args.add_str2(isc_spb_sec_firstname,  *firstname);
	if (middlename) args.add_str2(isc_spb_sec_middlename, *middlename);
	if (lastname  ) args.add_str2(isc_spb_sec_lastname,   *lastname);
	start_service_and_check_status(args);
}

void FbServicesImpl::delete_user(const std::string &user)
{
	check_is_attached();
	BinaryBuffer args;
	args.add_uint8(isc_action_svc_delete_user);
	args.add_str2(isc_spb_sec_username, user);
	start_service_and_check_status(args);
}

void FbServicesImpl::check_is_not_attached()
{
	if (handle_ != 0)
		throw WrongSeqException("Service is already attached");
}

void FbServicesImpl::check_is_attached()
{
	if (handle_ == 0)
		throw WrongSeqException("Service is not attached");
}

void FbServicesImpl::detach_internal(bool check_error)
{
	ISC_STATUS status[StatusLen] = {};
	lib_->api.f_isc_service_detach(status, &handle_);
	if (check_error)
		check_status_vector(lib_->api, "isc_service_detach", status, {});
}

void FbServicesImpl::start_service_and_check_status(const BinaryBuffer &data)
{
	ISC_STATUS status[StatusLen] = {};
	lib_->api.f_isc_service_start(status, &handle_, nullptr, data.size(), data.data());
	check_status_vector(lib_->api, "isc_service_start", status, {});
}


/* class FbConnectionImpl */

FbConnectionImpl::FbConnectionImpl(
	const FbLibDataPtr&     lib,
	const FbConnectParams&  connect_params,
	const FbDbCreateParams* create_params
) :
	connect_params_(connect_params),
	create_params_defined_(create_params != 0),
	lib(lib)
{
	if (create_params)
		create_params_ = *create_params;
}

FbConnectionImpl::~FbConnectionImpl()
{
	if (db_handle_ != 0)
		internal_disconnect(false);
}

void FbConnectionImpl::check_is_connected()
{
	if (db_handle_ == 0)
		throw WrongSeqException("Database is not connected");
}

void FbConnectionImpl::check_is_disconnected()
{
	if (db_handle_ != 0)
		throw WrongSeqException("Database is already connected");
}

void FbConnectionImpl::connect()
{
	BinaryBuffer dpb;

	dpb.add_uint8(isc_dpb_version1);
	dpb.add_uint8(isc_dpb_utf8_filename); dpb.add_uint8(0);
	dpb.add_str(isc_dpb_user_name, connect_params_.user);
	dpb.add_str(isc_dpb_password, connect_params_.password);
	if (!connect_params_.role.empty())
		dpb.add_str(isc_dpb_sql_role_name, connect_params_.role);

	dpb.add_str(isc_dpb_lc_ctype, connect_params_.charset);

	std::string server_and_path;
	if (!connect_params_.host.empty())
	{
		server_and_path.append(connect_params_.host);
		server_and_path.append(":");
	}
	server_and_path.append(file_name_to_utf8(connect_params_.database));

	check_not_greater(
		server_and_path.size(),
		SHRT_MAX,
		"Length of host and database string too long (>SHRT_MAX)"
	);

	check_is_disconnected();

	ISC_STATUS status_vect[StatusLen] = {};

	assert(server_and_path.size() <= SHRT_MAX);

	lib->api.f_isc_attach_database(
		status_vect,
		(short)server_and_path.size(),
		server_and_path.c_str(),
		&db_handle_,
		dpb.size(),
		dpb.data()
	);

	// if it's error and create params are defined
	if (create_params_defined_ && !is_status_vector_ok(status_vect) && (status_vect[1] == isc_io_error))
	{
		// try to create new database
		try_to_create_database();
		disconnect();

		// connect to new created database
		lib->api.f_isc_attach_database(
			status_vect,
			(short)server_and_path.size(),
			server_and_path.c_str(),
			&db_handle_,
			dpb.size(),
			dpb.data()
		);
	}

	check_status_vector(lib->api, "isc_attach_database", status_vect, {});


	// get info about database

	TLRes<100> res_buffer;
	char db_items[] = { isc_info_db_SQL_dialect, isc_info_end };

	assert(res_buffer.size() <= SHRT_MAX);
	lib->api.f_isc_database_info(
		status_vect,
		&db_handle_,
		sizeof(db_items),
		db_items,
		(short)res_buffer.size(),
		res_buffer.data()
	);
	check_status_vector(lib->api, "isc_database_info", status_vect, {});

	dialect_ = res_buffer.get_int(lib->api, isc_info_db_SQL_dialect);
	assert(dialect_ != -1);
}

void FbConnectionImpl::disconnect()
{
	internal_disconnect(true);
}

void FbConnectionImpl::internal_disconnect(bool throw_exception)
{
	check_is_connected();
	ISC_STATUS status_vect[StatusLen] = {};
	lib->api.f_isc_detach_database(status_vect, &db_handle_);
	if (throw_exception)
		check_status_vector(lib->api, "isc_detach_database", status_vect, {});
	db_handle_ = 0;
}

bool FbConnectionImpl::is_connected() const
{
	return (db_handle_ != 0);
}

bool FbConnectionImpl::supports_sequences() const
{
	return true;
}

void FbConnectionImpl::try_to_create_database()
{
	check_is_disconnected();

	BinaryBuffer dpb;

	dpb.add_uint8(isc_dpb_version1);
	dpb.add_uint8(isc_dpb_utf8_filename); dpb.add_uint8(0);
	dpb.add_str(isc_dpb_set_db_charset, create_params_.charset);
	dpb.add_str(isc_dpb_lc_ctype, connect_params_.charset);
	dpb.add_str(isc_dpb_user_name, create_params_.user);
	dpb.add_str(isc_dpb_password, create_params_.password);
	dpb.add_uint32_with_len(isc_dpb_sql_dialect, create_params_.dialect);
	dpb.add_uint32_with_len(isc_dpb_force_write, create_params_.force_write ? 1 : 0);

	if (create_params_.page_size > 0)
		dpb.add_uint32_with_len(isc_dpb_page_size, create_params_.page_size);

	std::string database_utf8 = file_name_to_utf8(connect_params_.database);

	ISC_STATUS status_vect[StatusLen] = {};
	db_handle_ = 0;

	check_not_greater(database_utf8.size(), SHRT_MAX, "Length of file is too long (>SHRT_MAX)");

	lib->api.f_isc_create_database(
		status_vect,
		(short)database_utf8.size(),
		database_utf8.c_str(),
		&db_handle_,
		dpb.size(),
		dpb.data(),
		0
	);

	check_status_vector(lib->api, "isc_create_database", status_vect, {});
}

TransactionPtr FbConnectionImpl::create_transaction(const TransactionParams& transaction_params)
{
	return create_fb_transaction(transaction_params);
}

void FbConnectionImpl::set_default_transaction_level(TransactionLevel level)
{
	default_transaction_level_ = level;
}

TransactionLevel FbConnectionImpl::get_default_transaction_level() const
{
	return default_transaction_level_;
}

void FbConnectionImpl::direct_execute(std::string_view sql)
{
	throw FunctionalityNotSupported();
}

std::string FbConnectionImpl::get_driver_name() const
{
	return "firebird";
}

isc_db_handle& FbConnectionImpl::get_handle()
{
	return db_handle_;
}

short FbConnectionImpl::get_dialect() const
{
	return dialect_;
}

FbTransactionPtr FbConnectionImpl::create_fb_transaction(const TransactionParams& transaction_params)
{
	check_is_connected();
	auto tran = std::make_shared<FbTransactionImpl>(lib, shared_from_this(), transaction_params);
	if (transaction_params.autostart) tran->start();
	return tran;
}

/* class FbTransactionImpl */

FbTransactionImpl::FbTransactionImpl(
	const FbLibDataPtr&        lib,
	const FbConnectionImplPtr& conn,
	const TransactionParams&   transaction_params

) :
	lib_(lib),
	conn_(conn)

{
	tran_ = 0;

	commit_on_destroy_ = transaction_params.auto_commit_on_destroy;

	tpb_.add_uint8(isc_tpb_version3);

	switch (transaction_params.access)
	{
	case TransactionAccess::Read:
		tpb_.add_uint8(isc_tpb_read);
		break;

	case TransactionAccess::ReadAndWrite:
		tpb_.add_uint8(isc_tpb_write);
		break;
	}

	auto level = transaction_params.level;
	if (level == TransactionLevel::Default)
		level = conn_->get_default_transaction_level();

	switch (level)
	{
	case TransactionLevel::Serializable:
		tpb_.add_uint8(isc_tpb_consistency);
		break;

	case TransactionLevel::RepeatableRead:
		tpb_.add_uint8(isc_tpb_concurrency);
		break;

	case TransactionLevel::ReadCommitted:
		tpb_.add_uint8(isc_tpb_read_committed);
		tpb_.add_uint8(isc_tpb_no_rec_version);
		break;

	case TransactionLevel::DirtyRead:
		tpb_.add_uint8(isc_tpb_read_committed);
		tpb_.add_uint8(isc_tpb_rec_version);
		break;

	default:
		throw TransactionLevelNotSupportedException(level);
	}

	switch (transaction_params.lock_resolution)
	{
	case LockResolution::Wait:
		{
			auto lock_time_out = transaction_params.lock_time_out;
			if (lock_time_out == -1)
				lock_time_out = conn->get_default_transaction_lock_timeout();

			tpb_.add_uint8(isc_tpb_wait);
			if (lock_time_out != -1)
				tpb_.add_uint32_with_len(isc_tpb_lock_timeout, lock_time_out);
		}
		break;

	case LockResolution::Nowait:
		tpb_.add_uint8(isc_tpb_nowait);
		break;
	}

	tran_ = 0;
}

FbTransactionImpl::~FbTransactionImpl()
{
	if (get_state() == TransactionState::Started)
	{
		if (commit_on_destroy_)
			internal_commit();
		else
			internal_rollback();
	}
}

ConnectionPtr FbTransactionImpl::get_connection()
{
	return conn_;
}

StatementPtr FbTransactionImpl::create_statement()
{
	return create_fb_statement();
}

isc_tr_handle& FbTransactionImpl::get_handle()
{
	return tran_;
}

FbStatementPtr FbTransactionImpl::create_fb_statement()
{
	check_started();
	return std::make_shared<FbStatementImpl>(lib_, conn_, shared_from_this());
}

void FbTransactionImpl::internal_start()
{
	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_start_transaction(
		status_vect,
		&tran_,
		1,
		&conn_->get_handle(),
		tpb_.size(),
		tpb_.data()
	);
	check_status_vector(lib_->api, "isc_start_transaction", status_vect, {});
}

void FbTransactionImpl::internal_commit()
{
	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_commit_transaction(status_vect, &tran_);
	check_status_vector(lib_->api, "isc_commit_transaction", status_vect, {});
	tran_ = 0;
}

void FbTransactionImpl::internal_rollback()
{
	check_started();
	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_rollback_transaction(status_vect, &tran_);
	check_status_vector(lib_->api, "isc_rollback_transaction", status_vect, {});
	tran_ = 0;
}

/* class SqlDA */

SqlDA::SqlDA(int size) :
	data_(nullptr)
{
	allocate(size);
}


void SqlDA::allocate(int size)
{
	int n = XSQLDA_LENGTH(size);
	data_buffer_.resize(n);
	items_.resize(size);

	data_ = (XSQLDA*)data_buffer_.data();
	data_->sqln = size;
	data_->version = SQLDA_VERSION1;
}


void SqlDA::alloc_fields()
{
	if (data_->sqld == 0) return;
	XSQLVAR* var = data_->sqlvar;
	auto it = items_.begin();
	for (short i = 0; i < data_->sqld; i++, var++, it++)
	{
		short type = var->sqltype & ~1;
		unsigned len_to_alloc = 0;
		switch (type)
		{
		case SQL_VARYING:
			len_to_alloc = var->sqllen + 2;
			break;

		default:
			len_to_alloc = var->sqllen;
			break;
		}

		it->buffer.resize(len_to_alloc);
		var->sqldata = var->sqllen ? it->buffer.data() : nullptr;
		var->sqlind = (var->sqltype & 1) ? &it->null_flag : nullptr;
	}
}

size_t SqlDA::get_size() const
{
	return data_->sqld;
}


void SqlDA::check_size(const FbApi& api, bool in, isc_stmt_handle stmt)
{
	if (data_->sqld > data_->sqln)
	{
		allocate(data_->sqld);

		ISC_STATUS status_vect[StatusLen] = {};

		if (in)
		{
			api.f_isc_dsql_describe_bind(status_vect, &stmt, DaVersion, data_);
			check_status_vector(api, "isc_dsql_describe_bind", status_vect, {});
		}
		else
		{
			api.f_isc_dsql_describe(status_vect, &stmt, DaVersion, data_);
			check_status_vector(api, "isc_dsql_describe", status_vect, {});
		}
	}
}


void SqlDA::close_blob_handles(const FbApi& api, bool throw_exception)
{
	for (auto& item : items_)
	{
		if (item.blob_handle == 0) continue;
		ISC_STATUS status_vect[StatusLen] = {};
		api.f_isc_close_blob(status_vect, &item.blob_handle);
		item.blob_handle = 0;
		if (throw_exception)
			check_status_vector(api, "isc_close_blob", status_vect, {});
	}
}


void SqlDA::clear_null_flags()
{
	for (auto& item : items_)
		item.null_flag = 0;
}


void SqlDA::clear_buffers()
{
	for (auto& item : items_)
		item.buffer.clear();

	for (int i = 0; i < data_->sqld; i++)
	{
		XSQLVAR* sqlvar = &data_->sqlvar[i];
		memset(sqlvar, 0, sizeof(XSQLVAR));
	}
}


void SqlDA::check_index(size_t index) const
{
	if ((index < 1) || (index > (size_t)data_->sqld))
		throw WrongArgumentException("Variable index is out of bounds!");
}


ValueType SqlDA::get_column_type(size_t index) const
{
	const XSQLVAR* var = get_var(index);
	return cvt_fb_type_to_lib_type(var->sqltype & ~1);
}


std::string SqlDA::get_column_name(size_t index) const
{
	const XSQLVAR* var = get_var(index);
	return var->aliasname;
}


XSQLVAR* SqlDA::get_var(size_t index)
{
	return &data_->sqlvar[index - 1];
}


const XSQLVAR* SqlDA::get_var(size_t index) const
{
	return &data_->sqlvar[index - 1];
}


template <typename T> T& SqlDA::as(XSQLVAR* var)
{
	assert(var->sqllen == sizeof(T));
	return *(T*)var->sqldata;
}


short& SqlDA::null(size_t index)
{
	return items_[index - 1].null_flag;
}


const short& SqlDA::null(size_t index) const
{
	return items_[index - 1].null_flag;
}


isc_blob_handle& SqlDA::blob_handle(size_t index)
{
	return items_[index - 1].blob_handle;
}


/* class InSqlDA */

void InSqlDA::set_null(size_t index, bool is_null)
{
	null(index) = is_null ? -1 : 0;
}

template <typename T>
void InSqlDA::set_param(size_t index, T value, int sql_type)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != sql_type)
		throw WrongParameterType();
	assert(var->sqllen == sizeof(T));
	as<T>(var) = value;
}


void InSqlDA::string_param(size_t index, const std::string& value)
{
	XSQLVAR* var = get_var(index);
	if (((var->sqltype & ~1) != SQL_TEXT) && ((var->sqltype & ~1) != SQL_VARYING))
		throw WrongParameterType();
	set_str_var(var, value);
}


void InSqlDA::wstring_param(size_t index, const std::wstring& value)
{
	string_param(index, utf16_to_utf8(value));
}


void InSqlDA::set_date(size_t index, const Date& date)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TYPE_DATE)
		throw WrongParameterType();
	as<ISC_DATE>(var) = dblib_date_to_fb_date(date);
}


void InSqlDA::set_time(size_t index, const Time& time)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TYPE_TIME)
		throw WrongParameterType();
	as<ISC_TIME>(var) = dblib_time_to_fb_time(time);
}

void InSqlDA::set_timestamp(size_t index, const TimeStamp& ts)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TIMESTAMP)
		throw WrongParameterType();
	as<ISC_TIMESTAMP>(var) = dblib_timestamp_to_fb_timestamp(ts);
}


void InSqlDA::blob_param(
	const FbApi&       api,
	size_t             index,
	const char*        blob_data,
	size_t             blob_size,
	FbConnectionImpl&  conn,
	FbTransactionImpl& tran)
{
	XSQLVAR* var = get_var(index);

	if ((var->sqltype & ~1) != SQL_BLOB)
		throw WrongParameterType();

	auto& bl_handle = blob_handle(index);
	if (bl_handle != 0)
		throw WrongArgumentException("Blob is already stored for parameter");

	ISC_QUAD& blob_id = as<ISC_QUAD>(var);

	ISC_STATUS status_vect[StatusLen] = {};
	api.f_isc_create_blob2(
		status_vect,
		&conn.get_handle(),
		&tran.get_handle(),
		&bl_handle,
		&blob_id,
		0,
		nullptr
	);

	check_status_vector(api, "isc_create_blob2", status_vect, {});

	while (blob_size != 0)
	{
		uint16_t len = (blob_size > SHRT_MAX) ? SHRT_MAX : (short)blob_size;
		api.f_isc_put_segment(status_vect, &bl_handle, len, blob_data);
		check_status_vector(api, "isc_put_segment", status_vect, {});
		blob_data += len;
		blob_size -= len;
	}

	api.f_isc_close_blob(status_vect, &bl_handle);
	check_status_vector(api, "isc_close_blob", status_vect, {});
}


bool OutSqlDA::is_null(size_t index) const
{
	return null(index) == -1;
}


/* class OutSqlDA */

template <typename T>
T OutSqlDA::get_value(size_t index, int type)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != type)
		throw WrongColumnType{};
	assert(var->sqllen == sizeof(T));
	return as<T>(var);
}


std::string OutSqlDA::get_string(
	const FbApi&       api,
	size_t             index,
	FbConnectionImpl&  conn,
	FbTransactionImpl& tran)
{
	XSQLVAR* var = get_var(index);
	switch (var->sqltype & ~1)
	{
	case SQL_TEXT:
	case SQL_VARYING:
		get_str_var(var, tmp_buffer_);
		return tmp_buffer_;

	case SQL_BLOB:
		tmp_buffer_.resize(get_blob_size(api, index, conn, tran));
		read_blob(api, index, tmp_buffer_.begin(), tmp_buffer_.end(), conn, tran);
		return tmp_buffer_;

	}
	throw WrongColumnType{};
}


std::wstring OutSqlDA::get_wstring(size_t index)
{
	XSQLVAR* var = get_var(index);
	switch (var->sqltype & ~1)
	{
	case SQL_TEXT:
	case SQL_VARYING:
		get_str_var(var, tmp_buffer_);
		return utf8_to_utf16(tmp_buffer_);
	}
	throw WrongColumnType{};
}


void OutSqlDA::get_date(size_t index, Date& date)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TYPE_DATE)
		throw WrongColumnType{};
	fb_date_to_dblib_date(as<ISC_DATE>(var), date);
}


void OutSqlDA::get_time(size_t index, Time& time)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TYPE_TIME)
		throw WrongColumnType{};
	fb_time_to_dblib_time(as<ISC_TIME>(var), time);
}

void OutSqlDA::get_timestamp(size_t index, TimeStamp& ts)
{
	XSQLVAR* var = get_var(index);
	if ((var->sqltype & ~1) != SQL_TIMESTAMP)
		throw WrongColumnType{};
	fb_timestamp_to_dblib_timestamp(ts, as<ISC_TIMESTAMP>(var));
}

void OutSqlDA::prepare_blob_handle(
	const FbApi&       api,
	size_t             index,
	FbConnectionImpl&  conn,
	FbTransactionImpl& tran)
{
	auto& bl_handle = blob_handle(index);
	if (bl_handle != 0) return;

	XSQLVAR* var = get_var(index);
	ISC_STATUS status_vect[StatusLen] = {};
	ISC_QUAD& blob_id = as<ISC_QUAD>(var);

	api.f_isc_open_blob2(
		status_vect,
		&conn.get_handle(),
		&tran.get_handle(),
		&bl_handle,
		&blob_id,
		0,
		nullptr
	);

	check_status_vector(api, "isc_open_blob2", status_vect, {});
}


size_t OutSqlDA::get_blob_size(
	const FbApi&       api,
	size_t             index,
	FbConnectionImpl&  conn,
	FbTransactionImpl& tran)
{
	prepare_blob_handle(api, index, conn, tran);
	auto& bl_handle = blob_handle(index);

	char req_items[] = { isc_info_blob_total_length, isc_info_end };

	TLRes<100> res_buffer;
	ISC_STATUS status_vect[StatusLen] = {};

	assert(res_buffer.size() <= SHRT_MAX);
	api.f_isc_blob_info(
		status_vect,
		&bl_handle,
		sizeof(req_items),
		req_items,
		(short)res_buffer.size(),
		res_buffer.data()
	);

	check_status_vector(api, "isc_blob_info", status_vect, {});

	return res_buffer.get_int(api, isc_info_blob_total_length);
}

template<typename I>
void OutSqlDA::read_blob(
	const FbApi&       api,
	size_t             index,
	I                  dst_from,
	I                  dst_to,
	FbConnectionImpl&  conn,
	FbTransactionImpl& tran)
{
	if (dst_from == dst_to) return;
	size_t size = std::distance(dst_from, dst_to);

	prepare_blob_handle(api, index, conn, tran);
	auto& bl_handle = blob_handle(index);

	std::vector<char> buffer(SHRT_MAX);

	for (;;)
	{
		ISC_STATUS status_vect[StatusLen] = {};

		unsigned short bytes_read = 0;
		unsigned short to_read = (size < SHRT_MAX) ? (unsigned short)size : SHRT_MAX;

		auto res = api.f_isc_get_segment(
			status_vect,
			&bl_handle,
			&bytes_read,
			to_read,
			buffer.data()
		);

		check_status_vector(api, "isc_get_segment", status_vect,{});

		std::copy(buffer.begin(), buffer.begin() + bytes_read, dst_from);

		dst_from += bytes_read;
		size -= bytes_read;

		if (res == isc_segstr_eof) break;
		if (size == 0) break;
	}
}


/* class FbStatementImpl */

FbStatementImpl::FbStatementImpl(
	const FbLibDataPtr&         lib,
	const FbConnectionImplPtr&  conn,
	const FbTransactionImplPtr& tran

) :
	lib_(lib),
	conn_(conn),
	tran_(tran),
	stmt_(0),
	in_sqlda_(SQLDADefSize),
	out_sqlda_(SQLDADefSize),
	columns_helper_(*this)
{}

FbStatementImpl::~FbStatementImpl()
{
	close(false);
}

TransactionPtr FbStatementImpl::get_transaction()
{
	return tran_;
}

void FbStatementImpl::check_is_prepared() const
{
	if (stmt_ == 0)
		throw WrongSeqException("Statement is not prepared");
}

void FbStatementImpl::check_has_data() const
{
	if (!has_data_)
		throw WrongSeqException("Statement does not have data");
}

void FbStatementImpl::close(bool throw_exception)
{
	if (stmt_ == 0) return;

	out_sqlda_.close_blob_handles(lib_->api, throw_exception);
	in_sqlda_.close_blob_handles(lib_->api, throw_exception);

	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_dsql_free_statement(status_vect, &stmt_, DSQL_drop);

	if (throw_exception)
		check_status_vector(lib_->api, "isc_dsql_free_statement", status_vect, {});

	out_sqlda_.clear_null_flags();
	out_sqlda_.clear_buffers();
	in_sqlda_.clear_null_flags();
	in_sqlda_.clear_buffers();

	stmt_ = 0;

	cursor_opened_ = false;
}

void FbStatementImpl::close_cursor()
{
	if (!cursor_opened_ || !stmt_) return;
	cursor_opened_ = false;
	has_data_ = false;

	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_dsql_free_statement(status_vect, &stmt_, DSQL_close);
	check_status_vector(lib_->api, "isc_dsql_free_statement", status_vect, {});
}

void FbStatementImpl::prepare_impl(std::string_view sql)
{
	columns_helper_.clear();
	close(true);
	has_data_ = false;

	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_dsql_allocate_statement(
		status_vect,
		&conn_->get_handle(),
		&stmt_
	);
	check_status_vector(lib_->api, "isc_dsql_allocate_statement", status_vect, {});

	assert(sql.size() <= USHRT_MAX);
	lib_->api.f_isc_dsql_prepare(
		status_vect,
		&tran_->get_handle(),
		&stmt_,
		(unsigned short)sql.size(),
		sql.data(),
		conn_->get_dialect(),
		out_sqlda_.data()
	);
	check_status_vector(lib_->api, "isc_dsql_prepare", status_vect, sql);

	type_ = get_type_internal();

	lib_->api.f_isc_dsql_describe_bind(
		status_vect,
		&stmt_,
		DaVersion,
		in_sqlda_.data()
	);
	check_status_vector(lib_->api, "isc_dsql_describe_bind", status_vect, {});

	in_sqlda_.check_size(lib_->api, true, stmt_);
	in_sqlda_.alloc_fields();

	out_sqlda_.check_size(lib_->api, false, stmt_);
	out_sqlda_.alloc_fields();
}

StatementType FbStatementImpl::get_type()
{
	check_is_prepared();
	return get_type_internal();
}

StatementType FbStatementImpl::get_type_internal()
{
	ISC_STATUS status_vect[StatusLen] = {};

	char type_item[] = { isc_info_sql_stmt_type };
	char res_buffer[128] = {};
	lib_->api.f_isc_dsql_sql_info(
		status_vect,
		&stmt_,
		sizeof(type_item),
		type_item,
		sizeof(res_buffer),
		res_buffer
	);
	check_status_vector(lib_->api, "isc_dsql_sql_info", status_vect, {});

	int stmt_type_len = (int)lib_->api.f_isc_portable_integer((const ISC_UCHAR*)&res_buffer[1], 2);
	int stmt_type = (int)lib_->api.f_isc_portable_integer((const ISC_UCHAR*)&res_buffer[3], stmt_type_len);

	switch (stmt_type)
	{
	case isc_info_sql_stmt_select:
		return StatementType::Select;

	case isc_info_sql_stmt_insert:
		return StatementType::Insert;

	case isc_info_sql_stmt_update:
		return StatementType::Update;

	case isc_info_sql_stmt_delete:
		return StatementType::Delete;
	}

	return StatementType::Other;
}

void FbStatementImpl::execute()
{
	internal_execute();
}

size_t FbStatementImpl::get_changes_count()
{
	ISC_STATUS status_vect[StatusLen] = {};

	char type_item[] = { isc_info_sql_records, isc_info_end };
	char res_buffer[128] = {};
	lib_->api.f_isc_dsql_sql_info(
		status_vect,
		&stmt_,
		sizeof(type_item),
		type_item,
		sizeof(res_buffer),
		res_buffer
	);
	check_status_vector(lib_->api, "isc_dsql_sql_info", status_vect, {});

	if (res_buffer[0] != isc_info_sql_records) return 0;

	size_t result = 0;
	for (const char* ptr = res_buffer + 3; *ptr != isc_info_end; )
	{
		char type = *ptr++;
		int len = (int)lib_->api.f_isc_portable_integer((const ISC_UCHAR*)ptr, 2);
		ptr += 2;
		int count = (int)lib_->api.f_isc_portable_integer((const ISC_UCHAR*)ptr, len);
		ptr += len;

		if ((type == isc_info_req_update_count) ||
			(type == isc_info_req_delete_count) ||
			(type == isc_info_req_insert_count))
			result += count;
	}

	return result;
}

int64_t FbStatementImpl::get_last_row_id()
{
	throw FunctionalityNotSupported();
}

std::string FbStatementImpl::get_last_sql() const
{
	return last_sql_;
}

void FbStatementImpl::internal_execute()
{
	check_is_prepared();

	close_cursor();

	ISC_STATUS status_vect[StatusLen] = {};
	lib_->api.f_isc_dsql_execute2(
		status_vect,
		&tran_->get_handle(),
		&stmt_,
		DaVersion,
		in_sqlda_.data(),
		nullptr
	);

	check_status_vector(lib_->api, "isc_dsql_execute2", status_vect, last_sql_);

	if (type_ == StatementType::Select)
		cursor_opened_ = true;

	in_sqlda_.close_blob_handles(lib_->api, true);
}

void FbStatementImpl::prepare(std::string_view sql, bool use_native_parameters_syntax)
{
	last_sql_ = sql;

	sql_preprocessor_.preprocess(
		sql,
		use_native_parameters_syntax,
		false,
		FbSqlPreprocessorActions()
	);

	prepare_impl(sql_preprocessor_.get_preprocessed_sql());
}

void FbStatementImpl::prepare(std::wstring_view sql, bool use_native_parameters_syntax)
{
	utf16_to_utf8(sql, utf8_sql_buffer_);
	prepare(utf8_sql_buffer_, use_native_parameters_syntax);
}

void FbStatementImpl::execute(std::string_view sql)
{
	prepare(sql, true);
	internal_execute();
}

void FbStatementImpl::execute(std::wstring_view sql)
{
	utf16_to_utf8(sql, utf8_sql_buffer_);
	execute(utf8_sql_buffer_);
}

bool FbStatementImpl::fetch()
{
	check_is_prepared();

	has_data_ = false;

	out_sqlda_.close_blob_handles(lib_->api, true);
	out_sqlda_.clear_null_flags();

	ISC_STATUS status_vect[StatusLen] = {};
	ISC_STATUS status = lib_->api.f_isc_dsql_fetch(status_vect, &stmt_, DaVersion, out_sqlda_.data());
	switch (status)
	{
	case 0:
		cursor_opened_ = true;
		has_data_ = true;
		return true;

	case 100:
		cursor_opened_ = true;
		return false;

	case isc_req_sync:
		throw WrongSeqException("Can't fetch data");
	}

	check_status_vector(lib_->api, "isc_dsql_fetch", status_vect, {});
	return false;
}


size_t FbStatementImpl::get_params_count() const
{
	check_is_prepared();
	return in_sqlda_.get_size();
}

ValueType FbStatementImpl::get_param_type(const IndexOrName& param)
{
	check_is_prepared();

	ValueType result = ValueType::None;

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&result, this](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			ValueType internal_type = in_sqlda_.get_column_type(internal_index);
			if (result == ValueType::None)
				result = internal_type;
			else if (result != internal_type)
				result = ValueType::Any;
		}
	);

	return result;
}

void FbStatementImpl::set_null(const IndexOrName& param)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			in_sqlda_.set_null(internal_index, true);
		}
	);
}

template<typename T>
void FbStatementImpl::set_param_opt_impl(const IndexOrName& param, const std::optional<T>& value)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this, &value](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);

			if (value.has_value()) set_param_with_type_cvt(
				*this,
				in_sqlda_.get_column_type(internal_index),
				internal_index,
				*value
			);
			in_sqlda_.set_null(internal_index, !value.has_value());
		}
	);
}

void FbStatementImpl::set_int32_opt(const IndexOrName& param, Int32Opt value)
{
	set_param_opt_impl(param, value);
}

void FbStatementImpl::set_int64_opt(const IndexOrName& param, Int64Opt value)
{
	set_param_opt_impl(param, value);
}

void FbStatementImpl::set_float_opt(const IndexOrName& param, FloatOpt value)
{
	set_param_opt_impl(param, value);
}

void FbStatementImpl::set_double_opt(const IndexOrName& param, DoubleOpt value)
{
	set_param_opt_impl(param, value);
}

void FbStatementImpl::set_u8str_opt(const IndexOrName& param, const StringOpt& text)
{
	set_param_opt_impl(param, text);
}

void FbStatementImpl::set_wstr_opt(const IndexOrName& param, const WStringOpt& text)
{
	set_param_opt_impl(param, text);
}

void FbStatementImpl::set_date_opt(const IndexOrName& param, const DateOpt& date)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this, &date](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			if (date.has_value()) in_sqlda_.set_date(internal_index, *date);
			in_sqlda_.set_null(internal_index, !date.has_value());
		}
	);
}

void FbStatementImpl::set_time_opt(const IndexOrName& param, const TimeOpt& time)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this, &time](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			if (time.has_value()) in_sqlda_.set_time(internal_index, *time);
			in_sqlda_.set_null(internal_index, !time.has_value());
		}
	);
}

void FbStatementImpl::set_timestamp_opt(const IndexOrName& param, const TimeStampOpt& ts)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this, &ts](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			if (ts.has_value()) in_sqlda_.set_timestamp(internal_index, *ts);
			in_sqlda_.set_null(internal_index, !ts.has_value());
		}
	);
}

void FbStatementImpl::set_blob(const IndexOrName& param, const char* blob_data, size_t blob_size)
{
	check_is_prepared();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[this, blob_data, blob_size](size_t internal_index)
		{
			in_sqlda_.check_index(internal_index);
			in_sqlda_.blob_param(
				lib_->api,
				internal_index,
				blob_data,
				blob_size,
				*conn_,
				*tran_
			);
			in_sqlda_.set_null(internal_index, false);
		}
	);
}


void FbStatementImpl::set_int16_impl(size_t index, int16_t value)
{
	in_sqlda_.set_param<int16_t>(index, value, SQL_SHORT);
}

void FbStatementImpl::set_int32_impl(size_t index, int32_t value)
{
	in_sqlda_.set_param<int32_t>(index, value, SQL_LONG);
}

void FbStatementImpl::set_int64_impl(size_t index, int64_t value)
{
	in_sqlda_.set_param<int64_t>(index, value, SQL_INT64);
}

void FbStatementImpl::set_float_impl(size_t index, float value)
{
	in_sqlda_.set_param<float>(index, value, SQL_FLOAT);
}

void FbStatementImpl::set_double_impl(size_t index, double value)
{
	in_sqlda_.set_param<double>(index, value, SQL_DOUBLE);
}

void FbStatementImpl::set_u8str_impl(size_t index, const std::string& text)
{
	in_sqlda_.string_param(index, text);
}

void FbStatementImpl::set_wstr_impl(size_t index, const std::wstring& text)
{
	in_sqlda_.wstring_param(index, text);
}

size_t FbStatementImpl::get_columns_count()
{
	check_is_prepared();
	return out_sqlda_.get_size();
}

ValueType FbStatementImpl::get_column_type(const IndexOrName& column)
{
	check_is_prepared();
	auto index = columns_helper_.get_column_index(column);
	out_sqlda_.check_index(index);
	return out_sqlda_.get_column_type(index);
}

std::string FbStatementImpl::get_column_name(size_t index)
{
	check_is_prepared();
	out_sqlda_.check_index(index);
	return out_sqlda_.get_column_name(index);
}


bool FbStatementImpl::is_null(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);
	out_sqlda_.check_index(index);
	return out_sqlda_.is_null(index);
}

template<typename T>
std::optional<T> FbStatementImpl::get_value_opt_impl(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);
	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index)) return {};
	return get_with_type_cvt<T>(*this, out_sqlda_.get_column_type(index), index);
}

Int32Opt FbStatementImpl::get_int32_opt(const IndexOrName& column)
{
	return get_value_opt_impl<int32_t>(column);
}

Int64Opt FbStatementImpl::get_int64_opt(const IndexOrName& column)
{
	return get_value_opt_impl<int64_t>(column);
}

FloatOpt FbStatementImpl::get_float_opt(const IndexOrName& column)
{
	return get_value_opt_impl<float>(column);
}

DoubleOpt FbStatementImpl::get_double_opt(const IndexOrName& column)
{
	return get_value_opt_impl<double>(column);
}

StringOpt FbStatementImpl::get_str_utf8_opt(const IndexOrName& column)
{
	return get_value_opt_impl<std::string>(column);
}

WStringOpt FbStatementImpl::get_wstr_opt(const IndexOrName& column)
{
	return get_value_opt_impl<std::wstring>(column);
}

DateOpt FbStatementImpl::get_date_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);

	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index)) return {};

	Date result;
	out_sqlda_.get_date(index, result);
	return result;
}

TimeOpt FbStatementImpl::get_time_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);

	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index)) return {};

	Time result;
	out_sqlda_.get_time(index, result);
	return result;
}

TimeStampOpt FbStatementImpl::get_timestamp_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);

	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index)) return {};

	TimeStamp result;
	out_sqlda_.get_timestamp(index, result);
	return result;
}

size_t FbStatementImpl::get_blob_size(const IndexOrName& column)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);
	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index))
		throw ColumnValueIsNullException(column.to_str());

	return out_sqlda_.get_blob_size(lib_->api, index, *conn_, *tran_);
}

void FbStatementImpl::get_blob_data(const IndexOrName& column, char* dst, size_t size)
{
	check_is_prepared();
	check_has_data();
	auto index = columns_helper_.get_column_index(column);
	out_sqlda_.check_index(index);
	if (out_sqlda_.is_null(index)) throw ColumnValueIsNullException(column.to_str());
	out_sqlda_.read_blob(lib_->api, index, dst, dst + size, *conn_, *tran_);
}

int16_t FbStatementImpl::get_int16_impl(size_t index)
{
	return out_sqlda_.get_value<short>(index, SQL_SHORT);
}

int32_t FbStatementImpl::get_int32_impl(size_t index)
{
	return out_sqlda_.get_value<long>(index, SQL_LONG);
}

int64_t FbStatementImpl::get_int64_impl(size_t index)
{
	return out_sqlda_.get_value<int64_t>(index, SQL_INT64);
}

float FbStatementImpl::get_float_impl(size_t index)
{
	return out_sqlda_.get_value<float>(index, SQL_FLOAT);
}

double FbStatementImpl::get_double_impl(size_t index)
{
	return out_sqlda_.get_value<double>(index, SQL_DOUBLE);
}

std::string FbStatementImpl::get_str_utf8_impl(size_t index)
{
	return out_sqlda_.get_string(lib_->api, index, *conn_, *tran_);
}

std::wstring FbStatementImpl::get_wstr_impl(size_t index)
{
	return out_sqlda_.get_wstring(index);
}

isc_stmt_handle& FbStatementImpl::get_handle()
{
	return stmt_;
}

FbLibPtr create_fb_lib()
{
	return std::make_shared<FbLibImpl>();
}

} // namespace dblib
