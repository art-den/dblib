/*

Copyright (c) 2015-2017 Artyomov Denis (denis.artyomov@gmail.com)

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

#include <vector>
#include "dblib/dblib_postgresql.hpp"
#include "dblib/dblib_exception.hpp"
#include "dblib/dblib_cvt_utils.hpp"
#include "dblib_stmt_tools.hpp"
#include "dblib_type_cvt.hpp"
#include "dblib_dyn.hpp"

namespace dblib {

constexpr Oid INT2OID = 21;
constexpr Oid INT4OID = 23;
constexpr Oid INT8OID = 20;
constexpr Oid FLOAT4OID = 700;
constexpr Oid FLOAT8OID = 701;
constexpr Oid VARCHAROID = 1043;
constexpr Oid BPCHAROID = 1042;
constexpr Oid DATEOID = 1082;
constexpr Oid TIMEOID = 1083;
constexpr Oid TIMESTAMPOID = 1114;
constexpr Oid BYTEAOID = 17;
constexpr Oid NAMEOID = 19;
constexpr Oid TEXTOID = 25;


/* class PGresultHandler */

class PGresultHandler
{
public:
	PGresultHandler(const PgApi& api, PGresult* res = nullptr) :
		api_(api),
		res_(res)
	{}

	~PGresultHandler()
	{
		if (res_)
			api_.PQclear(res_);
	}

	PGresult* get()
	{
		return res_;
	}

	void set(PGresult* value)
	{
		if (res_)
			api_.PQclear(res_);

		res_ = value;
	}

private:
	const PgApi& api_;
	PGresult* res_;
};


/* struct PgLibData */

struct PgLibData
{
	DynLib module;
	PgApi api;
};

using PgLibDataPtr = std::shared_ptr<PgLibData>;


/* class PgLibImpl */

class PgLibImpl : public PgLib
{
public:
	PgLibImpl();

	void load(const std::wstring_view dyn_lib_file_name) override;
	bool is_loaded() const override;
	const PgApi& get_api() override;
	PgConnectionPtr create_connection(const PgConnectParams& connect_params) override;

private:
	PgLibDataPtr lib_;
};


/* class PgConnectionImpl */

class PgConnectionImpl : 
	public PgConnection, 
	public std::enable_shared_from_this<PgConnectionImpl>
{
public:
	PgConnectionImpl(const PgLibDataPtr &lib, const PgConnectParams& params);
	~PgConnectionImpl();

	void connect() override;
	void disconnect() override;
	bool is_connected() const override;
	bool supports_sequences() const override;
	TransactionPtr create_transaction(const TransactionParams& transaction_params) override;
	void set_default_transaction_level(TransactionLevel level) override;
	TransactionLevel get_default_transaction_level() const override;
	void direct_execute(std::string_view sql) override;
	std::string get_driver_name() const override;

	PGconn* get_connection() override;
	PgTransactionPtr create_pg_transaction(const TransactionParams& transaction_params) override;

	void skip_previous_data();

private:
	PgLibDataPtr lib_;
	PgConnectParams conn_params_;
	PGconn* conn_ = nullptr;
	TransactionLevel default_transaction_level_ = DefaultTransactionLevel;
	std::string direct_execute_buffer_;

	void disconnect_impl();
	void check_is_connected();
};

using PgConnectionImplPtr = std::shared_ptr<PgConnectionImpl>;


/* class PgTransactionImpl */

class PgTransactionImpl : 
	public PgTransaction,
	public std::enable_shared_from_this<PgTransactionImpl>
{
public:
	PgTransactionImpl(const PgLibDataPtr &lib, const PgConnectionImplPtr &conn, const TransactionParams& transaction_params);
	~PgTransactionImpl();

	ConnectionPtr get_connection() override;
	StatementPtr create_statement() override;
	PgStatementPtr create_pg_statement() override;

protected:
	void internal_start() override;
	void internal_commit() override;
	void internal_rollback() override;

private:
	PgLibDataPtr lib_;
	PgConnectionImplPtr conn_;
	TransactionParams params_;

	void exec(const char *sql);
};

using PgTransactionImplPtr = std::shared_ptr<PgTransactionImpl>;


/* class PgStatementImpl */

class PgStatementImpl :
	public PgStatement,
	public IParameterSetterWithTypeCvt,
	public IResultGetterWithTypeCvt
{
public:
	PgStatementImpl(const PgLibDataPtr &lib, const PgConnectionImplPtr& conn, const PgTransactionImplPtr& tran);

	// impl. Statement
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
	ValueType get_column_type(const IndexOrName& colum) override;
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

	// impl. IParameterSetterWithTypeCvt
	void set_int16_impl(size_t index, int16_t value) override;
	void set_int32_impl(size_t index, int32_t value) override;
	void set_int64_impl(size_t index, int64_t value) override;
	void set_float_impl(size_t index, float value) override;
	void set_double_impl(size_t index, double value) override;
	void set_u8str_impl(size_t index, const std::string& text) override;
	void set_wstr_impl(size_t index, const std::wstring& text) override;

	// impl. IResultGetterWithTypeCvt
	int16_t get_int16_impl(size_t index) override;
	int32_t get_int32_impl(size_t index) override;
	int64_t get_int64_impl(size_t index) override;
	float get_float_impl(size_t index) override;
	double get_double_impl(size_t index) override;
	std::string get_str_utf8_impl(size_t index) override;
	std::wstring get_wstr_impl(size_t index) override;

private:
	struct ParamValue
	{
		uint16_t i16 = 0;
		uint32_t i32 = 0;
		uint64_t i64 = 0;
		std::vector<char> str;
	};

	using ParamValues = std::vector<ParamValue>;

	PgLibDataPtr lib_;
	PgConnectionImplPtr conn_;
	PgTransactionImplPtr tran_;
	std::string sql_buffer_;
	SqlPreprocessor sql_preprocessor_;
	PGresultHandler result_;
	StmtState state_ = StmtState::Undef;
	bool result_contains_first_row_data_ = false;
	bool contains_data_ = false;
	std::vector<Oid> param_types_;
	ParamValues param_data_;
	std::vector <const char*> param_values_;
	std::vector<int> param_lengths_;
	std::vector<int> param_formats_;
	std::string utf16_to_utf8_buffer_;
	std::wstring utf8_to_utf16_buffer_;
	ColumnsHelper columns_helper_;

	void fetch_and_check_if_result_is_end_of_tuples();

	template <typename T>
	void set_value_parameter(size_t param_index, T value);

	template <typename T>
	void set_param_opt_impl(const IndexOrName& param, const std::optional<T>& value);

	void check_is_in_executed_state() const;
	void check_is_in_prepared_or_executed_state() const;
	void check_contains_data() const;

	bool is_null_impl(size_t col_index);

	template<typename T>
	std::optional<T> get_value_opt_impl(const IndexOrName& column);

	template<typename T, typename F>
	std::optional<T> get_dt_opt_impl(
		const IndexOrName& column,
		Oid col_oid,
		const char *error_text_if_oid_not_match,
		const F &ret_fun
	);

	template<typename T>
	T get_value_impl(size_t col_index);

};

// ":aaa ?3 :aaa" -> "$1 $2 $1"

class PgPreprocessorActions : public SqlPreprocessorActions
{
public:
	void append_index_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
	{
		add_indexed_param(param_index, sql);
	}

	void append_named_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
	{
		add_indexed_param(param_index, sql);
	}

	void append_if_seq_data(const std::string& data, const std::string& other, std::string& sql) const override
	{
		sql.append(data);
		sql.append(other);
	}

	void append_seq_generator(const std::string& seq_name, const std::string& other, std::string& sql) const override
	{
		sql.append("nextval('");
		sql.append(seq_name);
		sql.append("')");
		sql.append(other);
	}

private:
	void add_indexed_param(int param_index, std::string& sql) const
	{
		char buffer[12] = {};
		sprintf_s(buffer, "%d", param_index);
		sql.append("$");
		sql.append(buffer);
	}
};

using Statuses = std::initializer_list<ExecStatusType>;

static void check_result_status(
	const PgApi      &api,
	const PGresult   *res, 
	const char       *fun_name, 
	Statuses         ok_statuses,
	std::string_view sql,
	ErrorType        error_type)
{
	if (res == nullptr) return;

	auto status = api.PQresultStatus(res);

	auto it = std::find(ok_statuses.begin(), ok_statuses.end(), status);
	if (it != ok_statuses.end()) return;

	std::string sql_code = api.PQresultErrorField(res, PG_DIAG_SQLSTATE);

	if ((sql_code == "55P03") || (sql_code == "40P01") || (sql_code == "40001"))
		error_type = ErrorType::Lock;

	throw_exception(
		fun_name,
		(int)status,
		-1,
		api.PQresStatus(status),
		sql_code,
		api.PQresultVerboseErrorMessage(res, PQERRORS_VERBOSE, PQSHOW_CONTEXT_ALWAYS),
		sql,
		error_type
	);
}

using OkCodes = std::initializer_list<int>;

static void check_ret_code(
	const PgApi      &api,
	const PGconn     *conn,
	int              ret_code,
	const char       *fun_name,
	OkCodes          ok_codes,
	std::string_view sql,
	ErrorType        error_type)
{
	auto it = std::find(ok_codes.begin(), ok_codes.end(), ret_code);
	if (it != ok_codes.end()) return;

	throw_exception(
		fun_name,
		(int)ret_code,
		-1,
		{},
		{},
		api.PQerrorMessage(conn),
		sql,
		error_type
	);
}

static ValueType oid_to_value_type(Oid uid)
{
	switch (uid)
	{
	case INT2OID:
		return ValueType::Short;

	case INT4OID:
		return ValueType::Integer;

	case INT8OID:
		return ValueType::BigInt;

	case FLOAT4OID:
		return ValueType::Float;

	case FLOAT8OID:
		return ValueType::Double;

	case VARCHAROID:
	case NAMEOID:
	case TEXTOID:
		return ValueType::Varchar;

	case BPCHAROID:
		return ValueType::Char;

	case DATEOID:
		return ValueType::Date;

	case TIMEOID:
		return ValueType::Time;

	case TIMESTAMPOID:
		return ValueType::Timestamp;
	}

	throw InternalException(
		"Type for oid=" + std::to_string(uid) +
		" is not supported in oid_to_value_type",
		0, 0
	);
}

const int64_t USecsInDay = 24LL * 60LL * 60LL * 1000LL * 1000LL;
const int DaysBetweenJDayAnd2000Year = 2451545;

int32_t dblib_date_to_pg_date(const Date &date)
{
	return date_to_julianday_integer(date) - DaysBetweenJDayAnd2000Year;
}

int64_t dblib_time_to_pg_time(const Time& time)
{
	int64_t result = time.hour;
	result *= 60;
	result += time.min;
	result *= 60;
	result += time.sec;
	result *= 1000;
	result += time.msec;
	result *= 1000;
	result += time.usec;
	return result;
}

int64_t dblib_timestamp_to_pg_timestamp(const TimeStamp& ts)
{
	return 
		(int64_t)dblib_date_to_pg_date(ts.date) * USecsInDay +
		dblib_time_to_pg_time(ts.time);
}

Date pg_date_to_dblib_date(int32_t pg_date)
{
	return julianday_integer_to_date(pg_date + DaysBetweenJDayAnd2000Year);
}

Time pg_time_to_dblib_time(int64_t pg_time, int32_t* date_rest)
{
	Time result = {};

	int64_t positiv_time = [&]
	{
		if (pg_time < 0)
		{
			int64_t day = pg_time / USecsInDay;
			return pg_time + (-day + 1) * USecsInDay;
		}
		return pg_time;
	} ();

	result.usec = positiv_time % 1000;
	positiv_time /= 1000;
	pg_time -= result.usec;
	pg_time /= 1000;

	result.msec = positiv_time % 1000;
	positiv_time /= 1000;
	pg_time -= result.msec;
	pg_time /= 1000;

	result.sec = positiv_time % 60;
	positiv_time /= 60;
	pg_time -= result.sec;
	pg_time /= 60;

	result.min = positiv_time % 60;
	positiv_time /= 60;
	pg_time -= result.min;
	pg_time /= 60;

	result.hour = positiv_time % 24;
	pg_time -= result.hour;
	pg_time /= 24;

	if (date_rest) *date_rest = (int32_t)pg_time;

	return result;
}

TimeStamp pg_ts_to_dblib_ts(int64_t pg_ts)
{
	TimeStamp result;

	int date_rest = 0;

	result.time = pg_time_to_dblib_time(pg_ts, &date_rest);
	result.date = pg_date_to_dblib_date(date_rest);

	return result;
}

/* class PgLib */

PgLib::~PgLib()
{}


/* class PgLibImpl */

PgLibImpl::PgLibImpl()
{
	lib_ = std::make_shared<PgLibData>();
}

void PgLibImpl::load(const std::wstring_view dyn_lib_file_name)
{
	auto& module = lib_->module;
	auto& api = lib_->api;

	if (module.is_loaded()) return;

	std::wstring file_name_to_load =
		dyn_lib_file_name.empty()
		? L"libpq.dll"
		: std::wstring(dyn_lib_file_name);

	module.load(file_name_to_load);

	module.load_func(api.PQconnectdbParams,           "PQconnectdbParams");
	module.load_func(api.PQfinish,                    "PQfinish");
	module.load_func(api.PQstatus,                    "PQstatus");
	module.load_func(api.PQerrorMessage,              "PQerrorMessage");
	module.load_func(api.PQexec,                      "PQexec");
	module.load_func(api.PQprepare,                   "PQprepare");
	module.load_func(api.PQsendQueryParams,           "PQsendQueryParams");
	module.load_func(api.PQsendQueryPrepared,         "PQsendQueryPrepared");
	module.load_func(api.PQsetSingleRowMode,          "PQsetSingleRowMode");
	module.load_func(api.PQgetResult,                 "PQgetResult");
	module.load_func(api.PQresultStatus,              "PQresultStatus");
	module.load_func(api.PQresStatus,                 "PQresStatus");
	module.load_func(api.PQresultErrorMessage,        "PQresultErrorMessage");
	module.load_func(api.PQresultVerboseErrorMessage, "PQresultVerboseErrorMessage");
	module.load_func(api.PQresultErrorField,          "PQresultErrorField");
	module.load_func(api.PQntuples,                   "PQntuples");
	module.load_func(api.PQnfields,                   "PQnfields");
	module.load_func(api.PQbinaryTuples,              "PQbinaryTuples");
	module.load_func(api.PQfname,                     "PQfname");
	module.load_func(api.PQftype,                     "PQftype");
	module.load_func(api.PQfsize,                     "PQfsize");
	module.load_func(api.PQcmdTuples,                 "PQcmdTuples");
	module.load_func(api.PQgetvalue,                  "PQgetvalue");
	module.load_func(api.PQgetlength,                 "PQgetlength");
	module.load_func(api.PQgetisnull,                 "PQgetisnull");
	module.load_func(api.PQnparams,                   "PQnparams");
	module.load_func(api.PQparamtype,                 "PQparamtype");
	module.load_func(api.PQdescribePrepared,          "PQdescribePrepared");
	module.load_func(api.PQclear,                     "PQclear");
}

bool PgLibImpl::is_loaded() const
{
	return lib_->module.is_loaded();
}

const PgApi& PgLibImpl::get_api()
{
	return lib_->api;
}

PgConnectionPtr PgLibImpl::create_connection(const PgConnectParams& connect_params)
{
	return std::make_shared<PgConnectionImpl>(lib_, connect_params);
}


/* class PgConnectionImpl */

PgConnectionImpl::PgConnectionImpl(const PgLibDataPtr& lib, const PgConnectParams& params) :
	lib_(lib),
	conn_params_(params)
{}

PgConnectionImpl::~PgConnectionImpl()
{
	if (is_connected())
	{
		skip_previous_data();
		disconnect_impl();
	}
}

void PgConnectionImpl::connect()
{
	if (is_connected())
		throw WrongSeqException("Database is already connected");

	std::map<std::string, std::string> values_map = conn_params_.other_items;

	if (!conn_params_.host.empty())
		values_map["host"] = conn_params_.host;

	if (conn_params_.port != -1)
		values_map["port"] = std::to_string(conn_params_.port);

	values_map["dbname"] = conn_params_.db_name;

	if (!conn_params_.user.empty())
		values_map["user"] = conn_params_.user;

	if (!conn_params_.password.empty())
		values_map["password"] = conn_params_.password;

	if (conn_params_.connect_timeout != -1)
		values_map["connect_timeout"] = std::to_string(conn_params_.connect_timeout);

	if (!conn_params_.encoding.empty())
		values_map["client_encoding"] = conn_params_.encoding;

	std::vector<const char*> keywords, values;
	for (auto& item : values_map)
	{
		keywords.push_back(item.first.c_str());
		values.push_back(item.second.c_str());
	}
	keywords.push_back(nullptr);
	values.push_back(nullptr);

	conn_ = lib_->api.PQconnectdbParams(keywords.data(), values.data(), 0);

	auto status = lib_->api.PQstatus(conn_);

	try
	{
		check_ret_code(
			lib_->api,
			conn_,
			status,
			"PQconnectdbParams",
			{ CONNECTION_OK },
			{},
			ErrorType::Connection
		);
	}
	catch (const Exception&)
	{
		disconnect_impl();
		throw;
	}
}

void PgConnectionImpl::disconnect()
{
	check_is_connected();
	disconnect_impl();
}

void PgConnectionImpl::disconnect_impl()
{
	lib_->api.PQfinish(conn_);
	conn_ = nullptr;
}

bool PgConnectionImpl::is_connected() const
{
	return conn_ != nullptr;
}

bool PgConnectionImpl::supports_sequences() const
{
	return true;
}

TransactionPtr PgConnectionImpl::create_transaction(const TransactionParams& transaction_params)
{
	return create_pg_transaction(transaction_params);
}

void PgConnectionImpl::set_default_transaction_level(TransactionLevel level)
{
	default_transaction_level_ = level;
}

TransactionLevel PgConnectionImpl::get_default_transaction_level() const
{
	return default_transaction_level_;
}

void PgConnectionImpl::direct_execute(std::string_view sql)
{
	check_is_connected();
	direct_execute_buffer_ = sql;

	skip_previous_data();

	auto exec_impl = [this](const char* sql)
	{
		auto result = lib_->api.PQexec(conn_, sql);
		check_result_status(lib_->api, result, "PQexec", { PGRES_COMMAND_OK }, sql, ErrorType::Normal);
	};

	exec_impl(direct_execute_buffer_.c_str());
	exec_impl("COMMIT");
}

std::string PgConnectionImpl::get_driver_name() const
{
	return "postgresql";
}

PGconn* PgConnectionImpl::get_connection()
{
	check_is_connected();
	return conn_;
}

PgTransactionPtr PgConnectionImpl::create_pg_transaction(const TransactionParams& transaction_params)
{
	check_is_connected();
	auto tran = std::make_shared<PgTransactionImpl>(
		lib_,
		shared_from_this(),
		transaction_params
	);
	if (transaction_params.autostart) tran->start();
	return tran;
}

void PgConnectionImpl::skip_previous_data()
{
	for (;;) 
	{
		PGresultHandler res(lib_->api, lib_->api.PQgetResult(conn_));
		if (res.get() == nullptr) break;
	}
}

void PgConnectionImpl::check_is_connected()
{
	if (!is_connected())
		throw WrongSeqException("Database is disconnected");
}


/* class PgTransactionImpl */

PgTransactionImpl::PgTransactionImpl(
	const PgLibDataPtr&        lib,
	const PgConnectionImplPtr& conn, 
	const TransactionParams&   transaction_params
) :
	lib_(lib),
	conn_(conn),
	params_(transaction_params)
{}

PgTransactionImpl::~PgTransactionImpl()
{
	if (get_state() == TransactionState::Started)
	{
		if (params_.auto_commit_on_destroy)
			internal_commit();
		else
			internal_rollback();
	}
}

ConnectionPtr PgTransactionImpl::get_connection()
{
	return conn_;
}

StatementPtr PgTransactionImpl::create_statement()
{
	return create_pg_statement();
}

void PgTransactionImpl::internal_start()
{
	std::string sql = "BEGIN TRANSACTION";

	switch (params_.level)
	{
	case TransactionLevel::Serializable:
		sql.append(" ISOLATION LEVEL SERIALIZABLE");
		break;

	case TransactionLevel::RepeatableRead:
		sql.append(" ISOLATION LEVEL REPEATABLE READ");
		break;

	case TransactionLevel::ReadCommitted:
		sql.append(" ISOLATION LEVEL READ COMMITTED");
		break;

	case TransactionLevel::DirtyRead:
		sql.append(" ISOLATION LEVEL READ UNCOMMITTED");
		break;
	}

	switch (params_.access)
	{
	case TransactionAccess::Read:
		sql.append(" READ ONLY");
		break;

	case TransactionAccess::ReadAndWrite:
		sql.append(" READ WRITE");
		break;
	}

	// TODO: DEFERRABLE

	exec(sql.c_str());

	sql = "SET LOCAL lock_timeout = '" + std::to_string(params_.lock_time_out) + "s';";
	exec(sql.c_str());
}

void PgTransactionImpl::internal_commit()
{
	exec("COMMIT");
}

void PgTransactionImpl::internal_rollback()
{
	exec("ROLLBACK");
}

void PgTransactionImpl::exec(const char* sql)
{
	conn_->skip_previous_data();
	auto result = lib_->api.PQexec(conn_->get_connection(), sql);
	check_result_status(lib_->api, result, "PQexec", { PGRES_COMMAND_OK }, sql, ErrorType::Transaction);
}

PgStatementPtr PgTransactionImpl::create_pg_statement()
{
	return std::make_shared<PgStatementImpl>(
		lib_,
		conn_,
		shared_from_this()
	);
}


/* class PgStatementImpl */

PgStatementImpl::PgStatementImpl(
	const PgLibDataPtr&         lib,
	const PgConnectionImplPtr&  conn, 
	const PgTransactionImplPtr& tran
) : 
	lib_(lib),
	conn_(conn), 
	tran_(tran),
	result_(lib->api),
	columns_helper_(*this)
{
}

TransactionPtr PgStatementImpl::get_transaction()
{
	return tran_;
}

void PgStatementImpl::prepare(
	std::string_view sql, 
	bool             use_native_parameters_syntax)
{
	columns_helper_.clear();

	result_.set(nullptr);
	conn_->skip_previous_data();

	sql_preprocessor_.preprocess(
		sql,
		use_native_parameters_syntax,
		true,
		PgPreprocessorActions()
	);

	sql_buffer_ = sql_preprocessor_.get_preprocessed_sql();

	PGresultHandler tmp_result(lib_->api, lib_->api.PQprepare(
		conn_->get_connection(),
		"",
		sql_buffer_.c_str(),
		0,
		nullptr
	));

	check_result_status(
		lib_->api,
		tmp_result.get(),
		"PQprepare",
		{ PGRES_COMMAND_OK },
		sql,
		ErrorType::Normal
	);

	result_.set(lib_->api.PQdescribePrepared(
		conn_->get_connection(),
		""
	));

	check_result_status(
		lib_->api,
		result_.get(),
		"PQdescribePrepared",
		{ PGRES_COMMAND_OK },
		sql,
		ErrorType::Normal
	);

	int params_count = lib_->api.PQnparams(result_.get());

	param_types_.clear();
	for (int i = 0; i < params_count; i++)
		param_types_.push_back(lib_->api.PQparamtype(result_.get(), i));

	param_data_.clear();
	param_data_.resize(params_count);
	param_values_.clear();
	param_values_.resize(params_count);
	param_lengths_.clear();
	param_lengths_.resize(params_count);
	param_formats_.clear();
	param_formats_.resize(params_count, 1); // all binary format

	state_ = StmtState::Prepared;
}

void PgStatementImpl::prepare(
	std::wstring_view sql, 
	bool              use_native_parameters_syntax)
{
	utf16_to_utf8(sql, utf16_to_utf8_buffer_);
	prepare(utf16_to_utf8_buffer_, use_native_parameters_syntax);
}

void PgStatementImpl::execute(std::string_view sql)
{
	columns_helper_.clear();

	result_contains_first_row_data_ = false;
	contains_data_ = false;

	result_.set(nullptr);
	conn_->skip_previous_data();

	sql_preprocessor_.preprocess(
		sql,
		true,
		true,
		PgPreprocessorActions()
	);

	sql_buffer_ = sql_preprocessor_.get_preprocessed_sql();

	int res = lib_->api.PQsendQueryParams(
		conn_->get_connection(), 
		sql_buffer_.c_str(),
		0,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		1 // 1 - result in binary format
	);

	check_ret_code(
		lib_->api,
		conn_->get_connection(),
		res,
		"PQsendQuery",
		{ 1 },
		sql,
		ErrorType::Normal
	);

	res = lib_->api.PQsetSingleRowMode(conn_->get_connection());

	check_ret_code(
		lib_->api,
		conn_->get_connection(),
		res,
		"PQsetSingleRowMode",
		{ 1 },
		{},
		ErrorType::Normal
	);

	fetch_and_check_if_result_is_end_of_tuples();

	result_contains_first_row_data_ = true;
	state_ = StmtState::Executed;
}

void PgStatementImpl::fetch_and_check_if_result_is_end_of_tuples()
{
	result_.set(lib_->api.PQgetResult(conn_->get_connection()));
	if (!result_.get()) return;

	check_result_status(
		lib_->api,
		result_.get(),
		"PQgetResult",
		{ PGRES_SINGLE_TUPLE, PGRES_COMMAND_OK, PGRES_TUPLES_OK },
		{},
		ErrorType::Normal
	);

	auto status = lib_->api.PQresultStatus(result_.get());
	if ((status == PGRES_TUPLES_OK) || (status == PGRES_COMMAND_OK))
	{
		contains_data_ = false;
		return;
	}

	int rows_count = lib_->api.PQntuples(result_.get());
	if (rows_count != 1)
		throw InternalException("Number on tuples in result != 1", rows_count, 0);

	int bin_format = lib_->api.PQbinaryTuples(result_.get());
	if (bin_format != 1)
		throw InternalException("Result format is not binary", rows_count, 0);

	contains_data_ = true;
}

void PgStatementImpl::execute(std::wstring_view sql)
{
	execute(utf16_to_utf8(sql));
}

StatementType PgStatementImpl::get_type()
{
	return StatementType::Other;
}

void PgStatementImpl::execute()
{
	result_contains_first_row_data_ = false;
	contains_data_ = false;

	result_.set(nullptr);
	conn_->skip_previous_data();

	int params_count = (int)param_data_.size();

	int res = lib_->api.PQsendQueryPrepared(
		conn_->get_connection(),
		"",
		params_count,
		params_count ? param_values_.data() : nullptr,
		params_count ?param_lengths_.data() : nullptr,
		params_count ? param_formats_.data() : nullptr,
		1
	);

	check_ret_code(
		lib_->api,
		conn_->get_connection(),
		res,
		"PQsendQueryPrepared",
		{ 1 },
		{},
		ErrorType::Normal
	);

	res = lib_->api.PQsetSingleRowMode(conn_->get_connection());

	check_ret_code(
		lib_->api,
		conn_->get_connection(),
		res,
		"PQsetSingleRowMode",
		{ 1 },
		{},
		ErrorType::Normal
	);

	fetch_and_check_if_result_is_end_of_tuples();

	result_contains_first_row_data_ = true;
	state_ = StmtState::Executed;
}

size_t PgStatementImpl::get_changes_count()
{
	check_is_in_executed_state();
	auto* changes_text = lib_->api.PQcmdTuples(result_.get());
	return (size_t)atoi(changes_text);
}

int64_t PgStatementImpl::get_last_row_id()
{
	throw FunctionalityNotSupported();
}

std::string PgStatementImpl::get_last_sql() const
{
	return sql_buffer_;
}

bool PgStatementImpl::fetch()
{
	if (result_contains_first_row_data_)
	{
		result_contains_first_row_data_ = false;
		return contains_data_;
	}

	check_contains_data();

	fetch_and_check_if_result_is_end_of_tuples();

	return contains_data_;
}

template <typename T>
void PgStatementImpl::set_value_parameter(size_t param_index, T value)
{
	if constexpr (sizeof(T) == 2)
	{
		char* data = (char*)&param_data_.at(param_index - 1).i16;
		write_value_into_bytes_be(value, data);
		param_values_.at(param_index - 1) = data;
	}
	else if constexpr (sizeof(T) == 4)
	{
		char* data = (char*)&param_data_.at(param_index - 1).i32;
		write_value_into_bytes_be(value, data);
		param_values_.at(param_index - 1) = data;
	}
	else if constexpr (sizeof(T) == 8)
	{
		char* data = (char*)&param_data_.at(param_index - 1).i64;
		write_value_into_bytes_be(value, data);
		param_values_.at(param_index - 1) = data;
	}
	else
	{
		static_assert(false, "Wrong type of parameter");
	}

	param_lengths_.at(param_index - 1) = sizeof(T);
}

template <typename T>
void PgStatementImpl::set_param_opt_impl(const IndexOrName& param, const std::optional<T>& value)
{
	check_is_in_prepared_or_executed_state();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index) {
			if (value.has_value())
			{
				set_param_with_type_cvt(
					*this,
					oid_to_value_type(param_types_.at(param_index - 1)),
					param_index,
					*value
				);
			}
			else
				param_values_.at(param_index - 1) = nullptr;
		}
	);
}

void PgStatementImpl::check_is_in_executed_state() const
{
	if (state_ != StmtState::Executed)
		throw WrongSeqException("Statement is not executed");
}

void PgStatementImpl::check_is_in_prepared_or_executed_state() const
{
	if ((state_ != StmtState::Executed) && (state_ != StmtState::Prepared))
		throw WrongSeqException("Statement is not prepared or executed");
}

void PgStatementImpl::check_contains_data() const
{
	// if result_contains_first_row_data_ == true, fetch has not called
	if (!contains_data_ || result_contains_first_row_data_)
		throw WrongSeqException("Statement doesn't contain data");
}

size_t PgStatementImpl::get_params_count() const
{
	check_is_in_prepared_or_executed_state();
	return param_values_.size();
}

ValueType PgStatementImpl::get_param_type(const IndexOrName& param)
{
	check_is_in_prepared_or_executed_state();

	ValueType result = ValueType::None;

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index) 
		{ 
			ValueType param_type = oid_to_value_type(param_types_.at(param_index));
			if (result == ValueType::None)
				result = param_type;
			else if (result != param_type)
				result = ValueType::Any;
		}
	);

	return result;
}

void PgStatementImpl::set_null(const IndexOrName& param)
{
	check_is_in_prepared_or_executed_state();
	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index) { param_values_.at(param_index - 1) = nullptr; }
	);
}

void PgStatementImpl::set_int32_opt(const IndexOrName& param, Int32Opt value)
{
	set_param_opt_impl(param, value);
}

void PgStatementImpl::set_int64_opt(const IndexOrName& param, Int64Opt value)
{
	set_param_opt_impl(param, value);
}

void PgStatementImpl::set_float_opt(const IndexOrName& param, FloatOpt value)
{
	set_param_opt_impl(param, value);
}

void PgStatementImpl::set_double_opt(const IndexOrName& param, DoubleOpt value)
{
	set_param_opt_impl(param, value);
}

void PgStatementImpl::set_u8str_opt(const IndexOrName& param, const StringOpt& text)
{
	set_param_opt_impl(param, text);
}

void PgStatementImpl::set_wstr_opt(const IndexOrName& param, const WStringOpt& text)
{
	set_param_opt_impl(param, text);
}

void PgStatementImpl::set_date_opt(const IndexOrName& param, const DateOpt& date)
{
	check_is_in_prepared_or_executed_state();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index) 
		{
			if (date.has_value())
				set_value_parameter(param_index, dblib_date_to_pg_date(*date));
			else
				param_values_.at(param_index - 1) = nullptr;
		}
	);
}

void PgStatementImpl::set_time_opt(const IndexOrName& param, const TimeOpt& time)
{
	check_is_in_prepared_or_executed_state();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index)
		{
			if (time.has_value())
				set_value_parameter(param_index, dblib_time_to_pg_time(*time));
			else
				param_values_.at(param_index - 1) = nullptr;
		}
	);
}

void PgStatementImpl::set_timestamp_opt(const IndexOrName& param, const TimeStampOpt& ts)
{
	check_is_in_prepared_or_executed_state();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index)
		{
			if (ts.has_value())
				set_value_parameter(param_index, dblib_timestamp_to_pg_timestamp(*ts));
			else
				param_values_.at(param_index - 1) = nullptr;
		}
	);
}

void PgStatementImpl::set_blob(const IndexOrName& param, const char* blob_data, size_t blob_size)
{
	check_is_in_prepared_or_executed_state();

	sql_preprocessor_.do_for_param_indexes(
		param,
		[&](size_t param_index)
		{
			auto& str_buf = param_data_.at(param_index - 1).str;
			str_buf.assign(blob_data, blob_data + blob_size);
			str_buf.push_back(0);

			param_values_.at(param_index - 1) = str_buf.data();
			param_lengths_.at(param_index - 1) = (int)blob_size;
		}
	);
}

size_t PgStatementImpl::get_columns_count()
{
	check_is_in_prepared_or_executed_state();
	return lib_->api.PQnfields(result_.get());
}

ValueType PgStatementImpl::get_column_type(const IndexOrName& colum)
{
	check_is_in_prepared_or_executed_state();
	size_t index = columns_helper_.get_column_index(colum);
	return oid_to_value_type(lib_->api.PQftype(result_.get(), (int)index - 1));
}

std::string PgStatementImpl::get_column_name(size_t index)
{
	check_is_in_prepared_or_executed_state();
	return lib_->api.PQfname(result_.get(), (int)index - 1);
}

bool PgStatementImpl::is_null(const IndexOrName& column)
{
	check_contains_data();
	size_t index = columns_helper_.get_column_index(column);
	return is_null_impl(index);
}

bool PgStatementImpl::is_null_impl(size_t col_index)
{
	return lib_->api.PQgetisnull(result_.get(), 0, (int)col_index - 1) != 0;
}

template<typename T>
std::optional<T> PgStatementImpl::get_value_opt_impl(const IndexOrName& column)
{
	check_contains_data();
	size_t index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	Oid col_oid = lib_->api.PQftype(result_.get(), (int)index - 1);
	ValueType col_type = oid_to_value_type(col_oid);
	return get_with_type_cvt<T>(*this, col_type, index);
}

Int32Opt PgStatementImpl::get_int32_opt(const IndexOrName& column)
{
	return get_value_opt_impl<int32_t>(column);
}

Int64Opt PgStatementImpl::get_int64_opt(const IndexOrName& column)
{
	return get_value_opt_impl<int64_t>(column);
}

FloatOpt PgStatementImpl::get_float_opt(const IndexOrName& column)
{
	return get_value_opt_impl<float>(column);
}

DoubleOpt PgStatementImpl::get_double_opt(const IndexOrName& column)
{
	return get_value_opt_impl<double>(column);
}

StringOpt PgStatementImpl::get_str_utf8_opt(const IndexOrName& column)
{
	return get_value_opt_impl<std::string>(column);
}

WStringOpt PgStatementImpl::get_wstr_opt(const IndexOrName& column)
{
	return get_value_opt_impl<std::wstring>(column);
}

template<typename T, typename F>
std::optional<T> PgStatementImpl::get_dt_opt_impl(
	const IndexOrName& column,
	Oid                col_oid,
	const char*        error_text_if_oid_not_match,
	const F&           ret_fun)
{
	check_contains_data();

	size_t index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};

	Oid real_col_oid = lib_->api.PQftype(result_.get(), (int)index - 1);
	
	if (real_col_oid != col_oid)
		throw WrongTypeConvException(error_text_if_oid_not_match);

	return ret_fun(index);
}

DateOpt PgStatementImpl::get_date_opt(const IndexOrName& column)
{
	return get_dt_opt_impl<Date>(
		column,
		DATEOID,
		"Result is not in date format",
		[this](size_t index) {
			int date_int_value = get_value_impl<int32_t>(index);
			return pg_date_to_dblib_date(date_int_value);
		}
	);
}

TimeOpt PgStatementImpl::get_time_opt(const IndexOrName& column)
{
	return get_dt_opt_impl<Time>(
		column,
		TIMEOID,
		"Result is not in time format",
		[this](size_t index) {
			int64_t time_int_value = get_value_impl<int64_t>(index);
			return pg_time_to_dblib_time(time_int_value);
		}
	);
}

TimeStampOpt PgStatementImpl::get_timestamp_opt(const IndexOrName& column)
{
	return get_dt_opt_impl<TimeStamp>(
		column,
		TIMESTAMPOID,
		"Result is not in timestamp format",
		[this](size_t index) {
			int64_t ts_int_value = get_value_impl<int64_t>(index);
			return pg_ts_to_dblib_ts(ts_int_value);
		}
	);
}

size_t PgStatementImpl::get_blob_size(const IndexOrName& column)
{
	check_contains_data();

	size_t index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return 0;

	if (lib_->api.PQftype(result_.get(), (int)index - 1) != BYTEAOID)
		throw WrongTypeConvException("Result is not in bytea format");

	return lib_->api.PQgetlength(result_.get(), 0, (int)index - 1);
}

void PgStatementImpl::get_blob_data(const IndexOrName& column, char* dst, size_t size)
{
	check_contains_data();

	size_t index = columns_helper_.get_column_index(column);

	if (is_null_impl(index)) return;

	if (lib_->api.PQftype(result_.get(), (int)index - 1) != BYTEAOID)
		throw WrongTypeConvException("Result is not in bytea format");

	const char* value = lib_->api.PQgetvalue(result_.get(), 0, (int)index - 1);

	size_t real_len = lib_->api.PQgetlength(result_.get(), 0, (int)index - 1);
	if (size > real_len) size = real_len;

	memcpy(dst, value, size);
}

void PgStatementImpl::set_int16_impl(size_t index, int16_t value)
{
	set_value_parameter(index, value);
}

void PgStatementImpl::set_int32_impl(size_t index, int32_t value)
{
	set_value_parameter(index, value);
}

void PgStatementImpl::set_int64_impl(size_t index, int64_t value)
{
	set_value_parameter(index, value);
}

void PgStatementImpl::set_float_impl(size_t index, float value)
{
	set_value_parameter(index, value);
}

void PgStatementImpl::set_double_impl(size_t index, double value)
{
	set_value_parameter(index, value);
}

void PgStatementImpl::set_u8str_impl(size_t index, const std::string& text)
{
	auto& str_buf = param_data_.at(index - 1).str;
	str_buf.assign(text.begin(), text.end());
	str_buf.push_back(0);

	param_values_.at(index - 1) = str_buf.data();
	param_lengths_.at(index - 1) = (int)text.length();
}

void PgStatementImpl::set_wstr_impl(size_t index, const std::wstring& text)
{
	utf16_to_utf8(text, utf16_to_utf8_buffer_);
	set_u8str_impl(index, utf16_to_utf8_buffer_);
}

template<typename T>
T PgStatementImpl::get_value_impl(size_t col_index)
{
	T result {};
	const char* value = lib_->api.PQgetvalue(result_.get(), 0, (int)col_index - 1);
	
	if constexpr (std::is_same_v<T, std::string>)
	{
		int len = lib_->api.PQgetlength(result_.get(), 0, (int)col_index - 1);
		result.assign(value, value + len);
	}
	else if constexpr (std::is_same_v<T, std::wstring>)
	{
		int len = lib_->api.PQgetlength(result_.get(), 0, (int)col_index - 1);
		utf8_to_utf16(std::string_view{ value , (size_t)len }, utf8_to_utf16_buffer_);
		result = utf8_to_utf16_buffer_;
	}
	else
	{
		int len = lib_->api.PQfsize(result_.get(), (int)col_index - 1);
		if (len != sizeof(T))
			throw InternalException("Real value size and size of type doesn't match", -1, -1);

		result = read_value_from_bytes_be<T>(value);
	}

	return result;
}


int16_t PgStatementImpl::get_int16_impl(size_t index)
{
	return get_value_impl<int16_t>(index);
}

int32_t PgStatementImpl::get_int32_impl(size_t index)
{
	return get_value_impl<int32_t>(index);
}

int64_t PgStatementImpl::get_int64_impl(size_t index)
{
	return get_value_impl<int64_t>(index);
}

float PgStatementImpl::get_float_impl(size_t index)
{
	return get_value_impl<float>(index);
}

double PgStatementImpl::get_double_impl(size_t index)
{
	return get_value_impl<double>(index);
}

std::string PgStatementImpl::get_str_utf8_impl(size_t index)
{
	return get_value_impl<std::string>(index);
}

std::wstring PgStatementImpl::get_wstr_impl(size_t index)
{
	return get_value_impl<std::wstring>(index);
}

PgLibPtr create_pg_lib()
{
	return std::make_shared<PgLibImpl>();
}

} // namespace dblib
