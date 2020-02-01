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
#include "dblib_dyn.hpp"
#include "dblib/dblib_sqlite.hpp"
#include "dblib/dblib_cvt_utils.hpp"
#include "dblib_stmt_tools.hpp"

namespace dblib {


/* struct SqliteLibData */

struct SqliteLibData
{
	DynLib module;
	SqliteApi api;
};

using SqliteLibImplPtr = std::shared_ptr<SqliteLibData>;


/* class SqliteLibImpl */

class SqliteLibImpl : public SqliteLib
{
public:
	SqliteLibImpl();
	~SqliteLibImpl() override {}

	void load(const std::wstring_view &dyn_lib_file_name) override;
	bool is_loaded() const override;

	const SqliteApi& get_api() override;

	SqliteConnectionPtr create_connection(const std::wstring &file_name, const SqliteConfig &config) override;
	SqliteConnectionPtr create_connection(const std::string &file_name_utf8, const SqliteConfig &config) override;

private:
	SqliteLibImplPtr lib_;
};


/* class SqliteConnectionImpl */

class SqliteConnectionImpl :
	public SqliteConnection,
	public std::enable_shared_from_this<SqliteConnectionImpl>
{
public:
	SqliteConnectionImpl(
		const SqliteLibImplPtr &lib,
		const std::string      &file_name_utf8,
		const SqliteConfig     &config
	);

	~SqliteConnectionImpl();

	void connect() override;
	void disconnect() override;
	bool is_connected() const override;
	bool supports_sequences() const override;
	TransactionPtr create_transaction(const TransactionParams &transaction_params) override;

	void set_default_transaction_level(TransactionLevel level) override;
	TransactionLevel get_default_transaction_level() const override;

	void direct_execute(std::string_view sql) override;

	sqlite3* get_instance() override;
	SQLiteTransactionPtr create_sqlite_transaction(const TransactionParams &transaction_params) override;

	void set_transaction_is_active(bool value);
	bool is_transaction_active() const;

private:
	SqliteLibImplPtr lib_;

	std::string file_name_utf8_;
	sqlite3 *db_ = nullptr;
	SqliteConfig config_;
	std::string tmp_sql_text_;
	bool transaction_is_active_ = false;

	void check_is_not_connected();
	void check_is_connected();
	void disconnect_internal(bool check_ret_code);
};

using SqliteConnectionImplPtr = std::shared_ptr<SqliteConnectionImpl>;


/* class SQLiteTransactionImpl */

class SQLiteTransactionImpl :
	public SQLiteTransaction,
	public std::enable_shared_from_this<SQLiteTransactionImpl>
{
public:
	SQLiteTransactionImpl(const SqliteLibImplPtr& lib, const SqliteConnectionImplPtr &conn, const TransactionParams& transaction_params);
	~SQLiteTransactionImpl();

	ConnectionPtr get_connection() override;
	StatementPtr create_statement() override;
	SQLiteStatementPtr create_sqlite_statement() override;

protected:
	void internal_start() override;
	void internal_commit() override;
	void internal_rollback() override;

private:
	SqliteLibImplPtr lib_;
	SqliteConnectionImplPtr conn_;

	bool commit_on_destroy_ = true;
	uint32_t busy_time_out_ = 0;
};

using SQLiteTransactionImplPtr = std::shared_ptr<SQLiteTransactionImpl>;


/* class SQLiteSqlPreprocessorActions */

class SQLiteSqlPreprocessorActions : public SqlPreprocessorActions
{
protected:
	void append_index_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
	{
		sql.append("?");
		sql.append(parameter);
	}

	void append_named_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
	{
		sql.append(parameter);
	}

	void append_if_seq_data(const std::string& data, const std::string& other, std::string& sql) const override
	{
	}

	void append_seq_generator(const std::string& seq_name, const std::string& other, std::string& sql) const override
	{
	}
};


/* class SQLiteStatementImpl */

class SQLiteStatementImpl : public SQLiteStatement
{
public:
	SQLiteStatementImpl(const SqliteLibImplPtr& lib, const SqliteConnectionImplPtr &conn, const SQLiteTransactionImplPtr &tran);
	~SQLiteStatementImpl();

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
	void set_u8str_opt(const IndexOrName& param, const StringOpt &text) override;
	void set_wstr_opt(const IndexOrName& param, const WStringOpt &text) override;
	void set_date_opt(const IndexOrName& param, const DateOpt &date) override;
	void set_time_opt(const IndexOrName& param, const TimeOpt&time) override;
	void set_timestamp_opt(const IndexOrName& param, const TimeStampOpt&ts) override;

	void set_blob(const IndexOrName& param, const char *blob_data, size_t blob_size) override;

	size_t get_columns_count() override;
	ValueType get_column_type(const IndexOrName& colum) override;
	std::string get_column_name(size_t index) override;

	bool is_null(const IndexOrName& column) override;
	bool is_null_impl(size_t index) const;

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
	void get_blob_data(const IndexOrName& column, char *dst, size_t size) override;

	sqlite3_stmt* get_stmt() override;

private:
	SqliteLibImplPtr lib_;
	SqliteConnectionImplPtr conn_;
	SQLiteTransactionImplPtr tran_;

	sqlite3_stmt *stmt_ = nullptr;
	mutable bool must_be_reseted_ = false;
	bool step_called_ = false;
	int last_step_result_ = -1;
	mutable ColumnsHelper columns_helper_;
	std::string parameter_name_tmp_;
	bool contains_data_ = false;
	std::string last_sql_;
	SqlPreprocessor sql_preprocessor_;

	void close(bool check_ret_code);
	void check_is_prepared() const;
	void check_contains_data() const;
	void internal_execute(bool do_reset_if_needed);
	void reset_statement() const;

	int get_param_index(const IndexOrName& param);
	void set_null_impl(int index);
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static void check_sqlite_ret_code(
	const SqliteApi  &api, 
	int              ret_code, 
	const char       *fun_name, 
	sqlite3          *db, 
	std::string_view sql,
	ErrorType        error_type)
{
	if (ret_code == SQLITE_OK) return;

	if (ret_code == SQLITE_BUSY)
		error_type = ErrorType::Deadlock;

	throw_exception(
		fun_name,
		ret_code,
		api.sqlite3_extended_errcode(db),
		api.sqlite3_errstr(ret_code),
		{},
		api.sqlite3_errmsg(db),
		sql,
		error_type
	);
}

static ValueType cvt_sqlite_type_to_lib_type(int sqlite_type)
{
	switch (sqlite_type)
	{
	case SQLITE_TEXT:
		return ValueType::Varchar;

	case SQLITE_FLOAT:
		return ValueType::Double;

	case SQLITE_INTEGER:
		return ValueType::Integer;

	case SQLITE_BLOB:
		return ValueType::Blob;

	case SQLITE_NULL:
		return ValueType::Null;
	}

	throw InternalException("Field of this type is not supported", 0, 0);
}

static const char* journal_mode_to_str(SqliteJournalMode mode)
{
	switch (mode)
	{
	case SqliteJournalMode::Delete:   return "DELETE";
	case SqliteJournalMode::Truncate: return "TRUNCATE";
	case SqliteJournalMode::Persist:  return "PERSIST";
	case SqliteJournalMode::Memory:   return "MEMORY";
	case SqliteJournalMode::WAL:      return "WAL";
	case SqliteJournalMode::Off:      return "OFF";
	}
	throw InternalException("Field of this type is not supported", 0, 0);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/* class SqliteLib */

SqliteLib::~SqliteLib()
{}

/* class SqliteLibImpl */

SqliteLibImpl::SqliteLibImpl() : 
	lib_(std::make_shared<SqliteLibData>())
{}

void SqliteLibImpl::load(const std::wstring_view &dyn_lib_file_name)
{
	auto &module = lib_->module;
	auto &api = lib_->api;

	if (module.is_loaded()) return;

	std::wstring file_name_to_load(dyn_lib_file_name.begin(), dyn_lib_file_name.end());
	if (file_name_to_load.empty())
		file_name_to_load = L"sqlite3.dll";

	module.load(file_name_to_load);

	module.load_func(api.sqlite3_close,                "sqlite3_close");
	module.load_func(api.sqlite3_exec,                 "sqlite3_exec");
	module.load_func(api.sqlite3_open_v2,              "sqlite3_open_v2");
	module.load_func(api.sqlite3_errmsg,               "sqlite3_errmsg");
	module.load_func(api.sqlite3_errstr,               "sqlite3_errstr");
	module.load_func(api.sqlite3_prepare_v2,           "sqlite3_prepare_v2");
	module.load_func(api.sqlite3_prepare16_v2,         "sqlite3_prepare16_v2");
	module.load_func(api.sqlite3_bind_parameter_count, "sqlite3_bind_parameter_count");
	module.load_func(api.sqlite3_bind_blob,            "sqlite3_bind_blob");
	module.load_func(api.sqlite3_bind_double,          "sqlite3_bind_double");
	module.load_func(api.sqlite3_bind_int,             "sqlite3_bind_int");
	module.load_func(api.sqlite3_bind_int64,           "sqlite3_bind_int64");
	module.load_func(api.sqlite3_bind_null,            "sqlite3_bind_null");
	module.load_func(api.sqlite3_bind_text,            "sqlite3_bind_text");
	module.load_func(api.sqlite3_column_count,         "sqlite3_column_count");
	module.load_func(api.sqlite3_column_name,          "sqlite3_column_name");
	module.load_func(api.sqlite3_step,                 "sqlite3_step");
	module.load_func(api.sqlite3_column_blob,          "sqlite3_column_blob");
	module.load_func(api.sqlite3_column_bytes,         "sqlite3_column_bytes");
	module.load_func(api.sqlite3_column_double,        "sqlite3_column_double");
	module.load_func(api.sqlite3_column_int,           "sqlite3_column_int");
	module.load_func(api.sqlite3_column_int64,         "sqlite3_column_int64");
	module.load_func(api.sqlite3_column_text,          "sqlite3_column_text");
	module.load_func(api.sqlite3_column_text16,        "sqlite3_column_text16");
	module.load_func(api.sqlite3_column_type,          "sqlite3_column_type");
	module.load_func(api.sqlite3_finalize,             "sqlite3_finalize");
	module.load_func(api.sqlite3_reset,                "sqlite3_reset");
	module.load_func(api.sqlite3_bind_parameter_index, "sqlite3_bind_parameter_index");
	module.load_func(api.sqlite3_changes,              "sqlite3_changes");
	module.load_func(api.sqlite3_last_insert_rowid,    "sqlite3_last_insert_rowid");
	module.load_func(api.sqlite3_create_function_v2,   "sqlite3_create_function_v2");
	module.load_func(api.sqlite3_value_type,           "sqlite3_value_type");
	module.load_func(api.sqlite3_value_text16,         "sqlite3_value_text16");
	module.load_func(api.sqlite3_value_text,           "sqlite3_value_text");
	module.load_func(api.sqlite3_user_data,            "sqlite3_user_data");
	module.load_func(api.sqlite3_result_text16,        "sqlite3_result_text16");
	module.load_func(api.sqlite3_result_value,         "sqlite3_result_value");
	module.load_func(api.sqlite3_result_text,          "sqlite3_result_text");
	module.load_func(api.sqlite3_busy_timeout,         "sqlite3_busy_timeout");
	module.load_func(api.sqlite3_extended_errcode,     "sqlite3_extended_errcode");
}

bool SqliteLibImpl::is_loaded() const
{
	return lib_->module.is_loaded();
}

const SqliteApi& SqliteLibImpl::get_api()
{
	assert(lib_->module.is_loaded());
	return lib_->api;
}

SqliteConnectionPtr SqliteLibImpl::create_connection(
	const std::wstring &file_name, 
	const SqliteConfig &config)
{
	assert(lib_->module.is_loaded());
	return std::make_shared<SqliteConnectionImpl>(lib_, utf16_to_utf8(file_name), config);
}

SqliteConnectionPtr SqliteLibImpl::create_connection(
	const std::string  &file_name_utf8, 
	const SqliteConfig &config)
{
	assert(lib_->module.is_loaded());
	return std::make_shared<SqliteConnectionImpl>(lib_, file_name_utf8, config);
}


/* class SqliteConnectionImpl */

SqliteConnectionImpl::SqliteConnectionImpl(
	const SqliteLibImplPtr &lib,
	const std::string      &file_name_utf8, 
	const SqliteConfig     &config)
{
	this->file_name_utf8_ = file_name_utf8;
	this->config_ = config;
	this->lib_ = lib;
}

SqliteConnectionImpl::~SqliteConnectionImpl()
{
	if (is_connected())
		disconnect_internal(false);
}

void SqliteConnectionImpl::connect()
{
	check_is_not_connected();

	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	switch (config_.multi_thread_mode)
	{
	case SqliteMultiThreadMode::FullMutex:
		flags |= SQLITE_OPEN_FULLMUTEX;
		break;

	case SqliteMultiThreadMode::NoMutex:
		flags |= SQLITE_OPEN_NOMUTEX;
		break;
	}

	int res = lib_->api.sqlite3_open_v2(
		file_name_utf8_.c_str(),
		&db_,
		flags,
		nullptr
	);

	check_sqlite_ret_code(lib_->api, res, "sqlite3_open", db_, {}, ErrorType::Connection);

	auto direct_execute_helper = [this](const char *format, auto arg)
	{
		char pragma_buffer[256] = {};
		sprintf_s(pragma_buffer, format, arg);
		direct_execute(pragma_buffer);
	};

	direct_execute_helper("PRAGMA encoding = \"%s\";", "UTF-8");

	if (config_.page_size != 0)
		direct_execute_helper("PRAGMA page_size = %d;", config_.page_size);

	if (config_.auto_vacuum != SqliteAutoVacuum::Default)
		direct_execute_helper("PRAGMA auto_vacuum = %i;", int(config_.auto_vacuum));

	if (config_.foreign_keys != SqliteForignKey::Default)
		direct_execute_helper(
			"PRAGMA foreign_keys = %s;", 
			(config_.foreign_keys == SqliteForignKey::True) ? "true" : "false"
		);

	if (config_.case_sensitive_like != SqliteCILike::Default)
		direct_execute_helper(
			"PRAGMA case_sensitive_like = %s;", 
			(config_.case_sensitive_like == SqliteCILike::True) ? "true" : "false"
		);

	if (config_.cache_size != 0)
		direct_execute_helper("PRAGMA cache_size = %i;", config_.cache_size);

	if (config_.journal_mode != SqliteJournalMode::Default)
		direct_execute_helper(
			"PRAGMA journal_mode = %s;", 
			journal_mode_to_str(config_.journal_mode)
		);
}

void SqliteConnectionImpl::disconnect()
{
	check_is_connected();
	disconnect_internal(true);
}

bool SqliteConnectionImpl::is_connected() const
{
	return db_ != nullptr;
}

bool SqliteConnectionImpl::supports_sequences() const
{
	return false;
}

TransactionPtr SqliteConnectionImpl::create_transaction(const TransactionParams &transaction_params)
{
	return create_sqlite_transaction(transaction_params);
}

void SqliteConnectionImpl::set_default_transaction_level(TransactionLevel level)
{}

TransactionLevel SqliteConnectionImpl::get_default_transaction_level() const
{
	return TransactionLevel::Default;
}

void SqliteConnectionImpl::direct_execute(std::string_view sql)
{
	check_is_connected();
	tmp_sql_text_ = sql;
	int res = lib_->api.sqlite3_exec(db_, tmp_sql_text_.c_str(), nullptr, nullptr, nullptr);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_exec", db_, sql, ErrorType::Normal);
}

void SqliteConnectionImpl::check_is_not_connected()
{
	if (db_ == nullptr) return;
	throw WrongSeqException("Database is connected");
}

void SqliteConnectionImpl::check_is_connected()
{
	if (db_ != nullptr) return;
	throw WrongSeqException("Database is not connected");
}

void SqliteConnectionImpl::disconnect_internal(bool check_ret_code)
{
	int res = lib_->api.sqlite3_close(db_);
	if (check_ret_code) 
		check_sqlite_ret_code(lib_->api, res, "sqlite3_close", db_, {}, ErrorType::Connection);
	db_ = nullptr;
}

sqlite3* SqliteConnectionImpl::get_instance()
{
	return db_;
}

SQLiteTransactionPtr 
SqliteConnectionImpl::create_sqlite_transaction(const TransactionParams &transaction_params)
{
	check_is_connected();

	auto tran = std::make_shared<SQLiteTransactionImpl>(lib_, shared_from_this(), transaction_params);
	if (transaction_params.autostart)
		tran->start();
	return tran;
}

void SqliteConnectionImpl::set_transaction_is_active(bool value)
{
	transaction_is_active_ = value;
}

bool SqliteConnectionImpl::is_transaction_active() const
{
	return transaction_is_active_;
}


/* class SQLiteTransactionImpl */

SQLiteTransactionImpl::SQLiteTransactionImpl(
	const SqliteLibImplPtr&       lib,
	const SqliteConnectionImplPtr &conn,
	const TransactionParams       &transaction_params
) :
	lib_(lib),
	conn_(conn)
{
	commit_on_destroy_ = transaction_params.auto_commit_on_destroy;
	busy_time_out_ = 1000 * transaction_params.lock_time_out;
}

SQLiteTransactionImpl::~SQLiteTransactionImpl()
{
	if (get_state() == TransactionState::Started)
	{
		if (commit_on_destroy_)
			internal_commit();
		else
			internal_rollback();
	}
}

StatementPtr SQLiteTransactionImpl::create_statement()
{
	return create_sqlite_statement();
}

SQLiteStatementPtr SQLiteTransactionImpl::create_sqlite_statement()
{
	return std::make_shared<SQLiteStatementImpl>(lib_, conn_, shared_from_this());
}

ConnectionPtr SQLiteTransactionImpl::get_connection()
{
	return conn_;
}

void SQLiteTransactionImpl::internal_start()
{
	if (conn_->is_transaction_active())
		throw WrongSeqException("Only one transaction is allowed per one SQLite connection");

	if (busy_time_out_ != 0)
		lib_->api.sqlite3_busy_timeout(conn_->get_instance(), busy_time_out_);

	const char* sql = "begin";
	int res = lib_->api.sqlite3_exec(conn_->get_instance(), sql, nullptr, nullptr, nullptr);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_exec", conn_->get_instance(), sql, ErrorType::Transaction);

	conn_->set_transaction_is_active(true);
}


void SQLiteTransactionImpl::internal_commit()
{
	conn_->set_transaction_is_active(false);

	const char* sql = "commit";
	int res = lib_->api.sqlite3_exec(conn_->get_instance(), sql, nullptr, nullptr, nullptr);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_exec", conn_->get_instance(), sql, ErrorType::Transaction);
}


void SQLiteTransactionImpl::internal_rollback()
{
	conn_->set_transaction_is_active(false);

	const char* sql = "rollback";
	int res = lib_->api.sqlite3_exec(conn_->get_instance(), sql, nullptr, nullptr, nullptr);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_exec", conn_->get_instance(), sql, ErrorType::Transaction);
}


/* class SQLiteStatementImpl */

SQLiteStatementImpl::SQLiteStatementImpl(
	const SqliteLibImplPtr         &lib,
	const SqliteConnectionImplPtr  &conn, 
	const SQLiteTransactionImplPtr &tran
) :
	lib_(lib),
	conn_(conn),
	tran_(tran),
	columns_helper_(*this)
{}

SQLiteStatementImpl::~SQLiteStatementImpl()
{
	close(false);
}

TransactionPtr SQLiteStatementImpl::get_transaction()
{
	return tran_;
}

void SQLiteStatementImpl::close(bool check_ret_code)
{
	if (stmt_ == nullptr) return;
	int res = lib_->api.sqlite3_finalize(stmt_);
	if (check_ret_code)
		check_sqlite_ret_code(lib_->api, res, "sqlite3_finalize", conn_->get_instance(), {}, ErrorType::Normal);
	stmt_ = nullptr;
	must_be_reseted_ = false;
	step_called_ = false;
	contains_data_ = false;
}

void SQLiteStatementImpl::check_is_prepared() const
{
	if (stmt_ == nullptr)
		throw WrongSeqException("Statement is not prepared");
}

void SQLiteStatementImpl::check_contains_data() const
{
	if (!contains_data_)
		throw WrongSeqException("Statement does not have data");
}

void SQLiteStatementImpl::prepare(std::string_view sql, bool use_native_parameters_syntax)
{
	last_sql_ = sql;

	sql_preprocessor_.preprocess(
		sql, 
		use_native_parameters_syntax, 
		false,
		SQLiteSqlPreprocessorActions()
	);

	columns_helper_.clear();

	close(false);

	auto& preprocessed_sql = sql_preprocessor_.get_preprocessed_sql();

	int res = lib_->api.sqlite3_prepare_v2(
		conn_->get_instance(),
		preprocessed_sql.data(),
		(int)preprocessed_sql.size(),
		&stmt_,
		nullptr
	);

	check_sqlite_ret_code(lib_->api, res, "sqlite3_prepare", conn_->get_instance(), sql, ErrorType::Normal);
}

void SQLiteStatementImpl::prepare(std::wstring_view sql, bool use_native_parameters_syntax)
{
	prepare(utf16_to_utf8(sql), use_native_parameters_syntax);
}

StatementType SQLiteStatementImpl::get_type()
{
	check_is_prepared();
	return StatementType::Unknown;
}

void SQLiteStatementImpl::internal_execute(bool do_reset_if_needed)
{
	if (do_reset_if_needed) reset_statement();

	last_step_result_ = lib_->api.sqlite3_step(stmt_);

	if ((last_step_result_ == SQLITE_DONE) || (last_step_result_ == SQLITE_ROW))
		must_be_reseted_ = true;

	else
		check_sqlite_ret_code(
			lib_->api, 
			last_step_result_, 
			"sqlite3_step", 
			conn_->get_instance(),
			last_sql_,
			ErrorType::Normal
		);
}

void SQLiteStatementImpl::reset_statement() const
{
	if (!must_be_reseted_) return;
	int res = lib_->api.sqlite3_reset(stmt_);
	must_be_reseted_ = false;
	check_sqlite_ret_code(lib_->api, res, "sqlite3_reset", conn_->get_instance(), {}, ErrorType::Normal);
}

void SQLiteStatementImpl::execute()
{
	check_is_prepared();
	internal_execute(true);
	step_called_ = true;
}

size_t SQLiteStatementImpl::get_changes_count()
{
	return lib_->api.sqlite3_changes(conn_->get_instance());
}

int64_t SQLiteStatementImpl::get_last_row_id()
{
	return lib_->api.sqlite3_last_insert_rowid(conn_->get_instance());
}

std::string SQLiteStatementImpl::get_last_sql() const
{
	return last_sql_;
}

void SQLiteStatementImpl::execute(std::string_view sql)
{
	prepare(sql, true);
	internal_execute(true);
	step_called_ = true;
}

void SQLiteStatementImpl::execute(std::wstring_view sql)
{
	prepare(sql, true);
	internal_execute(true);
	step_called_ = true;
}

bool SQLiteStatementImpl::fetch()
{
	check_is_prepared();

	if (step_called_)
		step_called_ = false;
	else
	{
		if (last_step_result_ == SQLITE_DONE)
			throw WrongSeqException("Fetch after data end");

		internal_execute(false);
	}

	contains_data_ = (last_step_result_ == SQLITE_ROW);
	return contains_data_;
}

size_t SQLiteStatementImpl::get_params_count() const
{
	check_is_prepared();
	return lib_->api.sqlite3_bind_parameter_count(stmt_);
}

ValueType SQLiteStatementImpl::get_param_type(const IndexOrName& param)
{
	check_is_prepared();
	return ValueType::Any;
}

int SQLiteStatementImpl::get_param_index(const IndexOrName& param)
{
	if (param.get_type() == IndexOrNameType::Index) 
		return (int)param.get_index();

	parameter_name_tmp_ = param.get_name();

	int result = lib_->api.sqlite3_bind_parameter_index(stmt_, parameter_name_tmp_.c_str());
	if (result == 0)
		throw ParameterNotFoundException();

	return result;
}

void SQLiteStatementImpl::set_null(const IndexOrName& param)
{
	check_is_prepared();
	reset_statement();
	set_null_impl(get_param_index(param));
}

void SQLiteStatementImpl::set_null_impl(int index)
{
	int res = lib_->api.sqlite3_bind_null(stmt_, index);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_null", conn_->get_instance(), {}, ErrorType::Normal);
}

void SQLiteStatementImpl::set_int32_opt(const IndexOrName& param, Int32Opt value)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (value.has_value())
	{
		int res = lib_->api.sqlite3_bind_int(stmt_, index, *value);
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_int", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_int64_opt(const IndexOrName& param, Int64Opt value)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (value.has_value())
	{
		int res = lib_->api.sqlite3_bind_int64(stmt_, index, *value);
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_int64", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_float_opt(const IndexOrName& param, FloatOpt value)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (value.has_value())
	{
		int res = lib_->api.sqlite3_bind_double(stmt_, index, *value);
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_double", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_double_opt(const IndexOrName& param, DoubleOpt value)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (value.has_value())
	{
		int res = lib_->api.sqlite3_bind_double(stmt_, index, *value);
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_double", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_u8str_opt(const IndexOrName& param, const StringOpt &text)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (text.has_value())
	{
		int res = lib_->api.sqlite3_bind_text(
			stmt_, 
			index, 
			text->data(), 
			(int)text->size(), 
			SQLITE_TRANSIENT
		);
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_text", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_wstr_opt(const IndexOrName& param, const WStringOpt &text)
{
	set_u8str_opt(param, text.has_value() ? utf16_to_utf8(*text) : StringOpt{});
}

void SQLiteStatementImpl::set_date_opt(const IndexOrName& param, const DateOpt &date)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (date.has_value())
	{
		int res = lib_->api.sqlite3_bind_double(stmt_, index, date_to_julianday(*date));
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_int64", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_time_opt(const IndexOrName& param, const TimeOpt &time)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (time.has_value())
	{
		int res = lib_->api.sqlite3_bind_double(stmt_, index, time_to_days(*time));
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_int", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_timestamp_opt(const IndexOrName& param, const TimeStampOpt &ts)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	if (ts.has_value())
	{
		int res = lib_->api.sqlite3_bind_double(stmt_, index, timestamp_to_julianday(*ts));
		check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_int64", conn_->get_instance(), {}, ErrorType::Normal);
	}
	else
		set_null_impl(index);
}

void SQLiteStatementImpl::set_blob(
	const IndexOrName& param, 
	const char*        blob_data, 
	size_t             blob_size)
{
	check_is_prepared();
	reset_statement();
	auto index = get_param_index(param);
	int res = lib_->api.sqlite3_bind_blob(stmt_, index, blob_data, (int)blob_size, nullptr);
	check_sqlite_ret_code(lib_->api, res, "sqlite3_bind_blob", conn_->get_instance(), {}, ErrorType::Normal);
}

size_t SQLiteStatementImpl::get_columns_count()
{
	check_is_prepared();
	return lib_->api.sqlite3_column_count(stmt_);
}

ValueType SQLiteStatementImpl::get_column_type(const IndexOrName& column)
{
	check_is_prepared();
	auto index = columns_helper_.get_column_index(column);
	int sqlite_type = lib_->api.sqlite3_column_type(stmt_, (int)index - 1);
	return cvt_sqlite_type_to_lib_type(sqlite_type);
}

std::string SQLiteStatementImpl::get_column_name(size_t index)
{
	check_is_prepared();
	return lib_->api.sqlite3_column_name(stmt_, (int)index - 1);
}

bool SQLiteStatementImpl::is_null(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	return is_null_impl(columns_helper_.get_column_index(column));
}

bool SQLiteStatementImpl::is_null_impl(size_t index) const
{
	return lib_->api.sqlite3_column_type(stmt_, (int)index - 1) == SQLITE_NULL;
}

Int32Opt SQLiteStatementImpl::get_int32_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return lib_->api.sqlite3_column_int(stmt_, (int)index - 1);
}


Int64Opt SQLiteStatementImpl::get_int64_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return lib_->api.sqlite3_column_int64(stmt_, (int)index - 1);
}

FloatOpt SQLiteStatementImpl::get_float_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	double result = lib_->api.sqlite3_column_double(stmt_, (int)index - 1);
	if ((result > FLT_MAX) || (result < -FLT_MAX))
		throw WrongTypeConvException("Value of column exceeds range for float type");
	return static_cast<float>(result);
}

DoubleOpt SQLiteStatementImpl::get_double_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return lib_->api.sqlite3_column_double(stmt_, (int)index - 1);
}

StringOpt SQLiteStatementImpl::get_str_utf8_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	const char *result = (const char*)lib_->api.sqlite3_column_text(stmt_, (int)index - 1);
	return result ? result : std::string();
}

WStringOpt SQLiteStatementImpl::get_wstr_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	const wchar_t *result = (const wchar_t*)lib_->api.sqlite3_column_text16(stmt_, (int)index - 1);
	return result ? std::wstring(result) : std::wstring();
}

DateOpt SQLiteStatementImpl::get_date_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return julianday_to_date(lib_->api.sqlite3_column_double(stmt_, (int)index - 1));
}

TimeOpt SQLiteStatementImpl::get_time_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return days_to_time(lib_->api.sqlite3_column_double(stmt_, (int)index - 1));
}

TimeStampOpt SQLiteStatementImpl::get_timestamp_opt(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) return {};
	return julianday_to_timestamp(lib_->api.sqlite3_column_double(stmt_, (int)index - 1));
}

size_t SQLiteStatementImpl::get_blob_size(const IndexOrName& column)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) throw ColumnValueIsNullException();
	return lib_->api.sqlite3_column_bytes(stmt_, (int)index - 1);
}

void SQLiteStatementImpl::get_blob_data(const IndexOrName& column, char *dst, size_t size)
{
	check_is_prepared();
	check_contains_data();
	auto index = columns_helper_.get_column_index(column);
	if (is_null_impl(index)) throw ColumnValueIsNullException();
	int blob_size = lib_->api.sqlite3_column_bytes(stmt_, (int)index - 1);

	if (size > (size_t)blob_size)
		throw WrongTypeConvException("Buffer size is larger than blob size");

	auto src = lib_->api.sqlite3_column_blob(stmt_, (int)index - 1);
	memcpy(dst, src, size);
}

sqlite3_stmt* SQLiteStatementImpl::get_stmt()
{
	return stmt_;
}

SqliteLibPtr create_sqlite_lib()
{
	return std::make_shared<SqliteLibImpl>();
}

} // namespace dblib
