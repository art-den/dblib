#define BOOST_TEST_MODULE DbLibTest

#define NOMINMAX

#include <stdint.h>
#include <stdio.h>

#include <stdexcept>
#include <vector>
#include <mutex>
#include <memory>
#include <functional>

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>

#include "../src/dblib_type_cvt.hpp"
#include "../src/dblib_stmt_tools.hpp"

#include "dblib/dblib.hpp"
#include "dblib/dblib_firebird.hpp"
#include "dblib/dblib_sqlite.hpp"
#include "dblib/dblib_postgresql.hpp"
#include "dblib/dblib_cvt_utils.hpp"


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#if defined(__linux__)
#define DBLIB_LINUX
#elif defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)
#define DBLIB_WINDOWS
#include <Windows.h>
#else
#error "Unsupported platform"
#endif

using namespace dblib;

static std::mutex create_conn_mutex;
static FbLibPtr fb_lib;
static SqliteLibPtr sqlite_lib;
static PgLibPtr pg_lib;

static std::wstring get_executable_path()
{
#if defined (DBLIB_LINUX)
	char path[1024];
	int path_len = readlink("/proc/self/exe", path, sizeof(path) - 1);
	assert(path_len != -1);
	path[path_len] = 0;
	return path;

#elif defined (DBLIB_WINDOWS)
	wchar_t path[MAX_PATH];
	GetModuleFileNameW(NULL, path, MAX_PATH);
#endif
	return path;
}

static FbConnectionPtr get_firebird_connection()
{
	auto lock = std::lock_guard { create_conn_mutex };

	if (!fb_lib)
	{
		fb_lib = create_fb_lib();
		fb_lib->load();
	}

#if defined (DBLIB_LINUX)
	std::wstring fb_database = L"/tmp/dblib_test.fdb";
#elif defined (DBLIB_WINDOWS)
	std::wstring fb_database = get_executable_path() + L".fbd";
#endif

	std::string role = "";

	FbConnectParams connect_params;
	FbDbCreateParams create_params;

	connect_params.host = "localhost";
	connect_params.database = fb_database;
	connect_params.user = "sysdba";
	connect_params.password = "masterkey";
	connect_params.role = role;

	create_params.dialect = 3;
	create_params.user = connect_params.user;
	create_params.password = connect_params.password;

	return fb_lib->create_connection(connect_params, &create_params);
}

static FbServicesPtr get_firebird_services()
{
	auto lock = std::lock_guard{ create_conn_mutex };

	if (!fb_lib)
	{
		fb_lib = create_fb_lib();
		fb_lib->load();
	}

	return fb_lib->create_services();
}

static SqliteConnectionPtr get_sqlite_connection()
{
	auto lock = std::lock_guard{ create_conn_mutex };

	if (!sqlite_lib)
	{
		sqlite_lib = create_sqlite_lib();
		sqlite_lib->load();
	}

	std::wstring sqlite_database = get_executable_path() + L".sqlite";

	return sqlite_lib->create_connection(sqlite_database, {});
}

static PgConnectionPtr get_postgresql_connection()
{
	auto lock = std::lock_guard{ create_conn_mutex };
	if (!pg_lib)
	{
		pg_lib = create_pg_lib();
		pg_lib->load();
	}

	PgConnectParams params;

	params.connect_timeout = 10;
	params.host = "localhost";
	params.db_name = "dblib_test";
	params.user = "dblib_test";
	params.password = "dblib_test";

	return pg_lib->create_connection(params); 
}

using Connections = std::vector<ConnectionPtr>;

static void for_all_connections_do(size_t conn_count, const std::function<void (const Connections&)> &fun)
{
	if (true)
	{
		Connections fb_conns;

		for (size_t i = 0; i < conn_count; i++)
			fb_conns.push_back(get_firebird_connection());

		fun(fb_conns);
	}

	if (true)
	{
		Connections sqlite_conns;

		for (size_t i = 0; i < conn_count; i++)
			sqlite_conns.push_back(get_sqlite_connection());

		fun(sqlite_conns);
	}

	if (true)
	{
		Connections pb_conns;

		for (size_t i = 0; i < conn_count; i++)
			pb_conns.push_back(get_postgresql_connection());

		fun(pb_conns);
	}
}

static void exec_no_throw(Connection &connection, std::initializer_list<std::string> sql_list)
{
	for (auto &sql : sql_list)
	{
		auto tran = connection.create_transaction();
		auto st = tran->create_statement();
		try
		{
			st->execute(sql);
			tran->commit();
		}
		catch (...) 
		{
			tran->rollback();
		}
	}
}

static void exec(Connection &connection, std::initializer_list<std::string_view> sql_list)
{
	for (auto &sql : sql_list)
	{
		auto tran = connection.create_transaction();
		auto st = tran->create_statement();
		st->execute(sql);
		tran->commit();
	}
}

static unsigned get_table_rows_count(Connection &connection, const char *table)
{
	std::string sql = "select * from ";
	sql.append(table);

	auto tran = connection.create_transaction();
	auto st = tran->create_statement();
	st->execute(sql);

	unsigned result = 0;
	while (st->fetch()) result++;
	return result;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


BOOST_AUTO_TEST_SUITE(HelperTests)

BOOST_AUTO_TEST_CASE(preprocess_sql_test)
{
	class TestSqlPreprocessorActions : public SqlPreprocessorActions
	{
	protected:
		void append_index_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
		{
			sql.append("$I");
			sql.append(parameter);
		}

		void append_named_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const override
		{
			sql.append("$N");
			sql.append(parameter);
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

	TestSqlPreprocessorActions actions;
	SqlPreprocessor preprocessor;

	preprocessor.preprocess("test", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test");

	preprocessor.preprocess("test 'aaa'", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test 'aaa'");

	preprocessor.preprocess("test ?1", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $I1");

	preprocessor.preprocess("?1", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "$I1");

	preprocessor.preprocess("?1?2", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "$I1$I2");

	preprocessor.preprocess("?1aaa", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "$I1aaa");

	preprocessor.preprocess("test @aaa", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $N@aaa");

	preprocessor.preprocess("test @aaa%", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $N@aaa%");

	preprocessor.preprocess("test ?1 ?2", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $I1 $I2");

	preprocessor.preprocess("test ?1 ?2 @aaa% @bbb%", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $I1 $I2 $N@aaa% $N@bbb%");

	preprocessor.preprocess("test ?1 ?2 @aaa @bbb", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test $I1 $I2 $N@aaa $N@bbb");

	preprocessor.preprocess("test '?1 ?2'", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test '?1 ?2'");

	preprocessor.preprocess("'?1 ?2'", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "'?1 ?2'");

	preprocessor.preprocess("test 'text?1 ?2text'", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test 'text?1 ?2text'");

	preprocessor.preprocess("test 'aaa''aaa ?1'", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test 'aaa''aaa ?1'");

	preprocessor.preprocess("test 'aaa''aaa ?1' ?1", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "test 'aaa''aaa ?1' $I1");

	preprocessor.preprocess("insert into tbl(text, {if_seq id}) values('aaaa', {next id_gen})", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "insert into tbl(text, id) values('aaaa', gen_id(id_gen, 1))");

	preprocessor.preprocess("insert into tbl({if_seq id,} text) values({next id_gen,} 'aaaa')", false, false, actions);
	BOOST_CHECK(preprocessor.get_preprocessed_sql() == "insert into tbl(id, text) values(gen_id(id_gen, 1), 'aaaa')");
}

BOOST_AUTO_TEST_CASE(type_cvt_tests)
{
	auto test_fail = [](const auto &fun)
	{
		try
		{
			fun();
			return false;
		}
		catch (const TypeRangeExceeds&)
		{
			return true;
		}
	};

	// int -> int32_t
	BOOST_CHECK(int_to<int32_t>(10) == 10);
	BOOST_CHECK(int_to<int32_t>(-10) == -10);
	BOOST_CHECK(int_to<int32_t>(INT32_MAX) == INT32_MAX);
	BOOST_CHECK(int_to<int32_t>(INT32_MIN) == INT32_MIN);
	BOOST_CHECK(test_fail([] { int_to<int32_t>(uint64_t(INT32_MAX) + 1); }));
	BOOST_CHECK(test_fail([] { int_to<int32_t>(uint64_t(INT32_MIN) - 1); }));

	// int -> int64_t
	BOOST_CHECK(int_to<int64_t>(10) == 10);
	BOOST_CHECK(int_to<int64_t>(-10) == -10);
	BOOST_CHECK(int_to<int64_t>(INT64_MAX) == INT64_MAX);
	BOOST_CHECK(int_to<int64_t>(INT64_MIN) == INT64_MIN);

	// int -> float
	BOOST_CHECK(int_to<float>(10) == 10.0);
	BOOST_CHECK(int_to<float>(-10) == -10.0);
	BOOST_CHECK(int_to<float>(INT32_MAX) == INT32_MAX);
	BOOST_CHECK(int_to<float>(INT64_MAX) == INT64_MAX);

	// int -> double
	BOOST_CHECK(int_to<double>(10) == 10.0);
	BOOST_CHECK(int_to<double>(-10) == -10.0);
	BOOST_CHECK(int_to<double>(INT32_MAX) == INT32_MAX);
	BOOST_CHECK(int_to<double>(INT64_MAX) == INT64_MAX);

	// int -> string
	BOOST_CHECK(int_to<std::string>(10) == "10");
	BOOST_CHECK(int_to<std::string>(-20000) == "-20000");

	// int -> wstring
	BOOST_CHECK(int_to<std::wstring>(10) == L"10");
	BOOST_CHECK(int_to<std::wstring>(-20000) == L"-20000");

	// float -> int
	BOOST_CHECK(float_to<int32_t>(10.1) == 10);
	BOOST_CHECK(float_to<int32_t>(9.9) == 10);
	BOOST_CHECK(float_to<int32_t>(-10.1) == -10);
	BOOST_CHECK(float_to<int32_t>(-9.9) == -10);

	BOOST_CHECK(test_fail([] { float_to<int32_t>(uint64_t(INT32_MIN) - 1); }));
}

BOOST_AUTO_TEST_CASE(timestamp_cvt_test)
{
	TimeStamp ts;

	ts.date.year = 2020;
	ts.date.month = 1;
	ts.date.day = 5;

	ts.time.hour = 12;
	ts.time.min = 57;

	for (int sec = 0; sec < 60; sec++)
	{
		ts.time.sec = sec;
		for (int msec = 0; msec < 1000; msec++)
		{
			ts.time.msec = msec;
			double jd = timestamp_to_julianday(ts);
			TimeStamp converted_ts = julianday_to_timestamp(jd);
			BOOST_CHECK(ts == converted_ts);
		}
	}
}

BOOST_AUTO_TEST_SUITE_END()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(ConnectionTests)

// Connect/disconnect test

BOOST_AUTO_TEST_CASE(connect_disconnect_test)
{
	for_all_connections_do(1, [] (const Connections &connections)
	{
		auto &connection = *connections[0];

		connection.connect();
		BOOST_CHECK(connection.is_connected());
		connection.disconnect();
		BOOST_CHECK(!connection.is_connected());

		connection.connect();
		BOOST_CHECK(connection.is_connected());
		connection.disconnect();
		BOOST_CHECK(!connection.is_connected());
	});
}

BOOST_AUTO_TEST_SUITE_END()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(TransactionTests)

BOOST_AUTO_TEST_CASE(trasaction_create_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		auto trans1 = connection.create_transaction();
		BOOST_CHECK(trans1.get() != nullptr);
		if (trans1->get_state() == TransactionState::Started) trans1->commit();

		TransactionParams tp2;
		tp2.autostart = true;
		auto trans2 = connection.create_transaction(tp2);
		BOOST_CHECK(trans2->get_state() == TransactionState::Started);
		trans2->commit();

		TransactionParams tp3;
		tp3.autostart = false;
		auto trans3 = connection.create_transaction(tp3);
		BOOST_CHECK(trans3->get_state() == TransactionState::Undefined);

		connection.disconnect();
		try
		{
			auto tisconnected_db_trans = connection.create_transaction();
			BOOST_CHECK_MESSAGE(false, "Transaction created for disconnected database");
		}
		catch (const WrongSeqException&) {}
	});
}

BOOST_AUTO_TEST_CASE(trasaction_start_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		auto tran = connection.create_transaction();
		BOOST_CHECK(tran->get_state() == TransactionState::Started);

		tran->commit();
		BOOST_CHECK(tran->get_state() == TransactionState::Commited);

		tran->start();
		BOOST_CHECK(tran->get_state() == TransactionState::Started);

		tran->rollback();
		BOOST_CHECK(tran->get_state() == TransactionState::Rollbacked);

		tran->start();
		BOOST_CHECK(tran->get_state() == TransactionState::Started);

		tran->rollback_and_start();
		BOOST_CHECK(tran->get_state() == TransactionState::Started);

		tran->commit_and_start();
		BOOST_CHECK(tran->get_state() == TransactionState::Started);
	});
}

BOOST_AUTO_TEST_CASE(transaction_test_simple)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table transaction_simple" });
		exec(connection, { "create table transaction_simple (int_fld integer)" });

		// rollbacked transaction
		auto tran_to_roll_back = connection.create_transaction();

		auto st_rb = tran_to_roll_back->create_statement();
		st_rb->execute("insert into transaction_simple(int_fld) values(1)");
		st_rb->execute("insert into transaction_simple(int_fld) values(2)");
		tran_to_roll_back->rollback();

		BOOST_CHECK(get_table_rows_count(connection, "transaction_simple") == 0);

		// commited transaction
		auto tran_to_commit = connection.create_transaction();
		auto st_cm = tran_to_commit->create_statement();
		st_cm->execute("insert into transaction_simple(int_fld) values(1)");
		st_cm->execute("insert into transaction_simple(int_fld) values(2)");
		tran_to_commit->commit();

		BOOST_CHECK(get_table_rows_count(connection, "transaction_simple") == 2);
	});
}

BOOST_AUTO_TEST_CASE(transaction_and_statament_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table tr_and_st_test" });
		exec(connection, { "create table tr_and_st_test (int_fld integer)" });

		auto tr = connection.create_transaction();

		auto st = tr->create_statement();

		st->execute("insert into tr_and_st_test(int_fld) values (1)");

		st->execute("select * from tr_and_st_test");

		tr->commit_and_start();

		st->execute("select * from tr_and_st_test");

		tr->commit();

		try
		{
			st->execute("select * from tr_and_st_test");
		}
		catch (const Exception&)
		{
			BOOST_CHECK(true);
		}

	});
}

BOOST_AUTO_TEST_CASE(transaction_deadlock_test)
{
	for_all_connections_do(2, [](const Connections &connections)
	{
		auto &conn1 = *connections[0];
		auto &conn2 = *connections[1];

		conn1.connect();
		exec_no_throw(conn1, { "drop table tr_deadlock_test" });
		exec(conn1, { 
			"create table tr_deadlock_test (int_fld1 integer, int_fld2 integer)",
			"insert into tr_deadlock_test (int_fld1, int_fld2) values (1, 2)",
			"insert into tr_deadlock_test (int_fld1, int_fld2) values (2, 3)",
		});

		conn2.connect();

		TransactionParams tr_params;
		tr_params.lock_time_out = 1; // 1 sec

		auto tr1 = conn1.create_transaction(tr_params);
		auto tr2 = conn2.create_transaction(tr_params);

		try
		{
			auto st1 = tr1->create_statement();
			st1->execute("update tr_deadlock_test set int_fld2 = 10 where int_fld1 = 1");
			BOOST_CHECK(true);
		}
		catch (const Exception&)
		{
			BOOST_CHECK(false);
		}
		
		try
		{
			auto st2 = tr2->create_statement();
			st2->execute("update tr_deadlock_test set int_fld2 = 20 where int_fld1 = 1");
			BOOST_CHECK(false);
		}
		catch (const LockException&)
		{
			BOOST_CHECK(true);
		}

		tr1->commit();

		tr2->rollback();

		auto tr3 = conn2.create_transaction();
		auto st3 = tr3->create_statement();
		st3->execute("select int_fld2 from tr_deadlock_test where int_fld1 = 1");
		BOOST_CHECK(st3->fetch());
		BOOST_CHECK(st3->get_int32(1) == 10);
		tr3->commit();

		tr2->start();
		auto st2 = tr2->create_statement();
		st2->execute("select int_fld2 from tr_deadlock_test where int_fld1 = 1");
		BOOST_CHECK(st2->fetch());
		BOOST_CHECK(st2->get_int32(1) == 10);
	});
}

BOOST_AUTO_TEST_SUITE_END()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(CommonTests)

BOOST_AUTO_TEST_CASE(changes_count_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table changes_count_test" });
		exec(connection, { "create table changes_count_test (int_fld integer)" });

		auto tr = connection.create_transaction();
		auto st = tr->create_statement();

		st->execute("insert into changes_count_test(int_fld) values (1)");
		BOOST_CHECK(st->get_changes_count() == 1);

		st->execute("insert into changes_count_test(int_fld) values (2)");
		BOOST_CHECK(st->get_changes_count() == 1);

		st->execute("update changes_count_test set int_fld = 10 where int_fld = 10");
		BOOST_CHECK(st->get_changes_count() == 0);

		st->execute("update changes_count_test set int_fld = 10 where int_fld = 1");
		BOOST_CHECK(st->get_changes_count() == 1);

		st->execute("delete from changes_count_test");
		BOOST_CHECK(st->get_changes_count() == 2);
	});
}

// Tests for fetching

BOOST_AUTO_TEST_CASE(fetch_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table fetch_test" });
		exec(connection, { "create table fetch_test (int_fld integer)" });

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		auto insert_row = [&](int value) {
			st->execute("insert into fetch_test(int_fld) values(" + std::to_string(value) + ")");
		};

		auto test_row_after_data = [&]
		{
			try
			{
				st->fetch();
				BOOST_CHECK_MESSAGE(false, "fetch doesn't throw after data end");
			}
			catch (...) {}
		};

		// 0 rows
		st->execute("select * from fetch_test");
		BOOST_CHECK(!st->fetch());
		test_row_after_data();


		// 1 row
		insert_row(1);
		st->execute("select * from fetch_test");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		// 2 rows
		insert_row(2);
		st->execute("select * from fetch_test");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		// 3 rows
		insert_row(3);
		st->execute("select * from fetch_test");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		// partially fetch
		st->execute("select int_fld from fetch_test where int_fld < 3 order by int_fld");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 1);
		st->execute("select int_fld from fetch_test where int_fld = 3 order by int_fld");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 3);
		BOOST_CHECK(!st->fetch());

		// prepare + fetch
		st->prepare("select * from fetch_test where int_fld = ?1");
		st->set_int32(1, 1);
		st->execute();
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		st->set_int32(1, 2);
		st->execute();
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		st->set_int32(1, 3);
		st->execute();
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		st->set_int32(1, 4);
		st->execute();
		BOOST_CHECK(!st->fetch());
		test_row_after_data();

		st->execute("select 1 from fetch_test where int_fld = 33333");
		BOOST_CHECK(!st->fetch());
		test_row_after_data();
	});
}


// Binds (result values) test

BOOST_AUTO_TEST_CASE(binds_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table binds_test" });
		exec(connection, { 
			"create table binds_test (" 
				"n          integer, "
				"big_fld    bigint, "
				"int_fld    integer, "
				"flt_fld    float, "
				"vchar_fld1 varchar(256), "
				"vchar_fld2 varchar(256) "
			")"
		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->execute(
			"insert into binds_test(int_fld, flt_fld, vchar_fld1, vchar_fld2) "
			"values(1, 1.75, '-2', 'the text')"
		);

		st->execute("select * from binds_test t where int_fld = 100");
		BOOST_CHECK(!st->fetch());

		st->execute(
			"select t.int_fld, t.flt_fld, t.vchar_fld1, t.vchar_fld2 "
			"from binds_test t"
		);

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 1);
		BOOST_CHECK(st->get_int64(1) == 1);
		BOOST_CHECK(st->get_float(1) == 1.0f);
		BOOST_CHECK(st->get_double(1) == 1.0);

		BOOST_CHECK(st->get_float(2) == 1.75f);
		BOOST_CHECK(st->get_double(2) == 1.75);

		BOOST_CHECK(st->get_str_utf8(3) == "-2");
		BOOST_CHECK(st->get_wstr(3) == L"-2");
		BOOST_CHECK(st->get_str_utf8(4) == "the text");
		BOOST_CHECK(st->get_wstr(4) == L"the text");
		BOOST_CHECK(!st->fetch());
	});
}


// Paramaters test

BOOST_AUTO_TEST_CASE(parameters_test_num)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table parameters_test" });
		exec(connection, {
			"create table parameters_test ("
				"n          integer, "
				"int_fld    integer, "
				"flt_fld    float, "
				"vchar_fld1 varchar(256), "
				"vchar_fld2 char(256) "
			")"
		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare(
			"insert into parameters_test(int_fld, flt_fld, vchar_fld1, vchar_fld2) "
			"values(?1, ?2, ?3, ?4)"
		);

		st->set_int32(1, 11);
		st->set_float(2, 12);
		st->set_u8str(3, "aaa");
		st->set_wstr(4, L"Юникодный текст");

		st->execute();

		st->commit_and_start_transaction();

		st->execute(
			"select t.int_fld, t.flt_fld, t.vchar_fld1, t.vchar_fld2 "
			"from parameters_test t"
		);
		BOOST_CHECK(st->fetch());

		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_float(2) == 12);
		BOOST_CHECK(st->get_str_utf8(3) == "aaa");
		BOOST_CHECK(st->get_wstr(4) == L"Юникодный текст");

		st->execute("delete from parameters_test");
		st->prepare(
			"insert into parameters_test(n, int_fld, flt_fld) "
			"values(?1, ?2, ?3)"
		);
		st->set_int32(1, 1);
		st->set_float(2, 11);
		st->set_null(3);
		st->execute();

		st->execute("select flt_fld from parameters_test");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->is_null(1));
	});
}

BOOST_AUTO_TEST_CASE(parameters_test_name)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table parameters_test" });

		exec(connection, {
			"create table parameters_test ("
				"n          integer, "
				"int_fld    integer, "
				"flt_fld    float, "
				"vchar_fld1 varchar(256), "
				"vchar_fld2 varchar(256) "
			")"
		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare(
			"insert into parameters_test(int_fld, flt_fld, vchar_fld1, vchar_fld2) "
			"values(@int_value, @float_value, @vchar1, @vchar2)"
		);

		st->set_int32("@int_value",   11);
		st->set_float("@float_value", 12);
		st->set_u8str("@vchar1",      "aaa");
		st->set_wstr ("@vchar2",      L"Юникодный текст");
		st->execute();

		st->set_int32("@int_value",   222);
		st->set_float("@float_value", 333.0);
		st->set_u8str("@vchar1",      "text");
		st->set_wstr ("@vchar2",      L"lalala");
		st->execute();

		st->set_int32("@int_value",   222);
		st->set_float("@float_value", 999.0);
		st->set_u8str("@vchar1",      "text-2");
		st->set_wstr ("@vchar2",      L"lalala-3");
		st->execute();

		st->prepare(
			"select t.int_fld, t.flt_fld, t.vchar_fld1, t.vchar_fld2 "
			"from parameters_test t "
			"order by t.int_fld"
		);

		st->execute();
		BOOST_CHECK(st->fetch());

		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_float(2) == 12);
		BOOST_CHECK(st->get_str_utf8(3) == "aaa");
		BOOST_CHECK(st->get_wstr(4) == L"Юникодный текст");

		BOOST_CHECK(st->fetch());

		BOOST_CHECK(st->get_int32(1) == 222);
		BOOST_CHECK(st->get_float(2) == 333);
		BOOST_CHECK(st->get_str_utf8(3) == "text");

		BOOST_CHECK(st->fetch());

		BOOST_CHECK(st->get_int32(1) == 222);
		BOOST_CHECK(st->get_float(2) == 999);
		BOOST_CHECK(st->get_str_utf8(3) == "text-2");

		BOOST_CHECK(!st->fetch());
	});
}


BOOST_AUTO_TEST_CASE(parameters_and_fetch)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table p_and_f_test" });
		exec(connection, {
			"create table p_and_f_test (n integer, int_fld integer)",
			"insert into p_and_f_test(n, int_fld) values(10, 100)",
			"insert into p_and_f_test(n, int_fld) values(10, 101)",
			"insert into p_and_f_test(n, int_fld) values(11, 110)",
			"insert into p_and_f_test(n, int_fld) values(11, 111)",
			"insert into p_and_f_test(n, int_fld) values(11, 112)",
			"insert into p_and_f_test(n, int_fld) values(11, 113)",
		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare("select n, int_fld from p_and_f_test where n=?1 order by int_fld");

		st->set_int32(1, 10);
		st->execute();

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 10);
		BOOST_CHECK(st->get_int32(2) == 100);

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 10);
		BOOST_CHECK(st->get_int32(2) == 101);

		BOOST_CHECK(!st->fetch());

		st->set_int32(1, 11);
		st->execute();

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_int32(2) == 110);

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_int32(2) == 111);

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_int32(2) == 112);

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(st->get_int32(2) == 113);

		BOOST_CHECK(!st->fetch());

		st->set_int32(1, 999);
		st->execute();

		BOOST_CHECK(!st->fetch());

		st->prepare("select n, int_fld from p_and_f_test where int_fld=?1");

		st->set_int32(1, 112);
		st->execute();
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 11);
		BOOST_CHECK(!st->fetch());

		st->execute("select n, int_fld from p_and_f_test where int_fld=100");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_int32(1) == 10);
		BOOST_CHECK(!st->fetch());
	});
}


BOOST_AUTO_TEST_CASE(wrong_parameters_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table wrong_parameters_test" });
		exec(connection, { "create table wrong_parameters_test (n integer, int_fld integer)" });

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare("insert into wrong_parameters_test(n) values(@blabla)");

		try
		{
			st->set_int32("@blabla2", 3);
			BOOST_CHECK(false);
		}
		catch (const ParameterNotFoundException&)
		{
			BOOST_CHECK(true);
		}
	});
}


BOOST_AUTO_TEST_CASE(query_result_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table query_result_test" });
		exec(connection, {
			"create table query_result_test ("
				"n          integer, "
				"big_fld    bigint, "
				"int_fld    integer, "
				"flt_fld    float, "
				"vchar_fld1 varchar(256), "
				"vchar_fld2 varchar(256) "
			")"
		});
		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		// is_null

		st->execute("delete from query_result_test");
		st->execute("insert into query_result_test(n, int_fld) values(1, 10)");
		st->execute("insert into query_result_test(n, flt_fld) values(2, 20)");
		st->execute("insert into query_result_test(n, big_fld) values(3, 30)");
		st->execute("insert into query_result_test(n, int_fld) values(4, 40)");
		st->execute("insert into query_result_test(n, flt_fld) values(5, 50)");
		st->execute("insert into query_result_test(n, big_fld) values(6, 60)");

		st->execute("select int_fld, flt_fld, big_fld from query_result_test order by n");

		BOOST_CHECK(st->fetch());

		BOOST_CHECK(!st->is_null(1));
		BOOST_CHECK(st->is_null(2));
		BOOST_CHECK(st->is_null(3));

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->is_null(1));
		BOOST_CHECK(!st->is_null(2));
		BOOST_CHECK(st->is_null(3));

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->is_null(1));
		BOOST_CHECK(st->is_null(2));
		BOOST_CHECK(!st->is_null(3));

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(!st->is_null(1));
		BOOST_CHECK(st->is_null(2));
		BOOST_CHECK(st->is_null(3));

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->is_null(1));
		BOOST_CHECK(!st->is_null(2));
		BOOST_CHECK(st->is_null(3));

		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->is_null(1));
		BOOST_CHECK(st->is_null(2));
		BOOST_CHECK(!st->is_null(3));

		BOOST_CHECK(!st->fetch());

		// ColumnValueIsNullException and default values

		st->execute("delete from query_result_test");
		st->execute("insert into query_result_test(n, int_fld) values(1, 10)");
		st->execute("select big_fld, int_fld from query_result_test");

		BOOST_CHECK(st->fetch());

		// Tests for null result

		BOOST_CHECK(st->is_null(1));

		try
		{
			BOOST_CHECK(st->get_int64(1));
			BOOST_CHECK(false);
		}
		catch (const ColumnValueIsNullException&)
		{
			BOOST_CHECK(true);
		}

		BOOST_CHECK(!st->get_int64_opt(1)); // optional returns {}

		BOOST_CHECK(st->get_int64_or(1, 333) == 333);

		// Tests for not null results

		BOOST_CHECK(!st->is_null(2));
		BOOST_CHECK(st->get_int64(2) == 10);
		BOOST_CHECK(st->get_int64_opt(2) == 10);
		BOOST_CHECK(st->get_int64_or(2, 333) == 10);
	});
}


BOOST_AUTO_TEST_CASE(col_names_test)
{
	using boost::iequals;

	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { 
			"drop table col_names_test",
			"drop table col_names_test2"
		});

		exec(connection, {
			"create table col_names_test ("
				"int_fld    integer, "
				"flt_fld    float, "
				"vchar_fld1 varchar(256), "
				"vchar_fld2 varchar(256) "
			")",

			"insert into col_names_test(int_fld, flt_fld, vchar_fld1, vchar_fld2) "
			"values(22, 33.0, 'wow', 'blabla')",

			"create table col_names_test2 ("
				"vchar_fld2 varchar(256) "
			")",

			"insert into col_names_test2(vchar_fld2) "
			"values('blabla-2')",

		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->execute(
			"select t.int_fld, t.flt_fld, t.vchar_fld1, t.vchar_fld2 "
			"from col_names_test t"
		);

		BOOST_CHECK(st->get_columns_count() == 4);
		BOOST_CHECK(iequals(st->get_column_name(1), "int_fld"));
		BOOST_CHECK(iequals(st->get_column_name(2), "flt_fld"));
		BOOST_CHECK(iequals(st->get_column_name(3), "vchar_fld1"));
		BOOST_CHECK(iequals(st->get_column_name(4), "vchar_fld2"));

		st->fetch();

		try
		{
			st->get_int32("int_fld_not_exist");
			BOOST_CHECK(false);
		}
		catch (const ColumnNotFoundException&)
		{
			BOOST_CHECK(true);
		}

		BOOST_CHECK(st->get_columns_count() == 4);
		BOOST_CHECK(iequals(st->get_column_name(1), "int_fld"));
		BOOST_CHECK(iequals(st->get_column_name(2), "flt_fld"));
		BOOST_CHECK(iequals(st->get_column_name(3), "vchar_fld1"));
		BOOST_CHECK(iequals(st->get_column_name(4), "vchar_fld2"));

		BOOST_CHECK(st->get_int32   ("int_fld"   ) == 22);
		BOOST_CHECK(st->get_float   ("flt_fld"   ) == 33.0f);
		BOOST_CHECK(st->get_str_utf8("vchar_fld1") == "wow");
		BOOST_CHECK(st->get_str_utf8("vchar_fld2") == "blabla");

		st->prepare(
			"select t.vchar_fld2, t.int_fld, t.flt_fld, t.vchar_fld1, t.vchar_fld2 "
			"from col_names_test t "
			"where t.int_fld = ?1"
		);

		BOOST_CHECK(st->get_columns_count() == 5);
		BOOST_CHECK(iequals(st->get_column_name(1), "vchar_fld2"));
		BOOST_CHECK(iequals(st->get_column_name(2), "int_fld"));
		BOOST_CHECK(iequals(st->get_column_name(3), "flt_fld"));
		BOOST_CHECK(iequals(st->get_column_name(4), "vchar_fld1"));
		BOOST_CHECK(iequals(st->get_column_name(5), "vchar_fld2"));

		st->execute("select * from col_names_test2");
		BOOST_CHECK(st->get_columns_count() == 1);
		st->fetch();
		BOOST_CHECK(st->get_str_utf8("vchar_fld2") == "blabla-2");
	});
}


BOOST_AUTO_TEST_CASE(date_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table date_test" });
		exec(connection, { "create table date_test (date_fld date)" });

		Date date (1924, 12, 22);

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare("insert into date_test(date_fld) values(?1)");
		st->set_date(1, date);
		st->execute();
		st->execute("select date_fld from date_test");
		st->fetch();
		Date fetched_date = st->get_date(1);
		BOOST_CHECK(fetched_date == date);
	});
}


BOOST_AUTO_TEST_CASE(time_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table time_test" });
		exec(connection, { "create table time_test (time_fld time)" });

		Time time(10, 22, 33, 555);

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare("insert into time_test(time_fld) values(?1)");
		st->set_time(1, time);
		st->execute();
		st->execute("select time_fld from time_test");
		st->fetch();

		Time fetched_time = st->get_time(1);

		BOOST_CHECK(fetched_time == time);
	});
}


BOOST_AUTO_TEST_CASE(timestamp_db_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table timestamp_test" });
		exec(connection, { 
			"create table timestamp_test (ts_fld timestamp)",
			"insert into timestamp_test(ts_fld) values(null)"
		});

		TimeStamp ts({ 1924, 12, 2 }, {10, 22, 33, 555});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->prepare("update timestamp_test set ts_fld = ?1");
		st->set_timestamp(1, ts);
		st->execute();

		st->execute("select ts_fld from timestamp_test");
		st->fetch();

		TimeStamp fetched_ts = st->get_timestamp(1);
		BOOST_CHECK(fetched_ts == ts);
	});
}


// Blobs

BOOST_AUTO_TEST_CASE(blobs_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table test_blobs" });

		std::string blob_type_name = 
			(connection.get_driver_name() == "postgresql") 
			? "bytea" 
			: "blob";

		std::string create_sql = 
			"create table test_blobs ( "
				"n integer, "
				"blob_fld1 " + blob_type_name + ", " +
				"blob_fld2 " + blob_type_name + " "
			")";

		exec(connection, { create_sql });

		auto tran = connection.create_transaction();

		auto test_blob = [&](const std::vector<char> &test_blob)
		{
			auto st = tran->create_statement();

			st->execute("delete from test_blobs");
			st->prepare("insert into test_blobs(blob_fld1) values(?1)");

			static int RepeatCount = 10;

			std::vector<char> blob = test_blob;
			for (int i = 0; i < RepeatCount; i++)
			{
				std::rotate(blob.begin(), blob.begin() + 1, blob.end());
				st->set_blob(1, blob.data(), blob.size());
				st->execute();
			}

			st->execute("select blob_fld1 from test_blobs");

			blob = test_blob;
			for (int i = 0; i < RepeatCount; i++)
			{
				st->fetch();

				size_t blob_size = st->get_blob_size(1);
				std::vector<char> fetched_blob(blob_size);
				st->get_blob_data(1, fetched_blob.data(), blob_size);

				std::rotate(blob.begin(), blob.begin() + 1, blob.end());
				BOOST_CHECK(fetched_blob == blob);
			}
		};

		// testing with small blob
		std::vector<char> small_blob = { 1, 2, 3, 4, 10, 20, 30, 40, 100, 120 };
		test_blob(small_blob);

		// testing with large blob
		std::vector<char> large_blob(1000000);
		for (auto &item : large_blob) item = rand();
		test_blob(large_blob);

		tran->commit();
	});
}


BOOST_AUTO_TEST_CASE(correct_seq_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table test_seq" });
		exec(connection, { 
			"create table test_seq (n integer)",
			"insert into test_seq(n) values(777)"
		});

		auto test = [](const std::function<void()>& fun)
		{
			try { fun(); }
			catch (const WrongSeqException&) { return true; }
			return false;
		};

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		BOOST_CHECK(test([&] { st->fetch(); })); 

		BOOST_CHECK(test([&] { st->get_columns_count(); }));
		BOOST_CHECK(test([&] { st->get_column_type(1); }));
		BOOST_CHECK(test([&] { st->get_column_name(1); }));

		BOOST_CHECK(test([&] { st->is_null(1); }));
		BOOST_CHECK(test([&] { st->get_int32(1); }));
		BOOST_CHECK(test([&] { st->get_int64(1); }));
		BOOST_CHECK(test([&] { st->get_float(1); }));
		BOOST_CHECK(test([&] { st->get_double(1); }));
		BOOST_CHECK(test([&] { st->get_str_utf8(1); }));
		BOOST_CHECK(test([&] { st->get_wstr(1); }));
		BOOST_CHECK(test([&] { st->get_date(1); }));
		BOOST_CHECK(test([&] { st->get_time(1); }));
		BOOST_CHECK(test([&] { st->get_timestamp(1); }));
		BOOST_CHECK(test([&] { st->get_blob_size(1); }));
		BOOST_CHECK(test([&] { st->get_blob_data(1, nullptr, 0); }));

		BOOST_CHECK(test([&] { st->get_params_count(); }));
		BOOST_CHECK(test([&] { st->get_param_type(2); }));
		BOOST_CHECK(test([&] { st->set_null(1); }));
		BOOST_CHECK(test([&] { st->set_int32(1, 1); }));
		BOOST_CHECK(test([&] { st->set_int64(1, 1); }));
		BOOST_CHECK(test([&] { st->set_float(1, 1.0f); }));
		BOOST_CHECK(test([&] { st->set_double(1, 1.0); }));
		BOOST_CHECK(test([&] { st->set_u8str(1, ""); }));
		BOOST_CHECK(test([&] { st->set_wstr(1, L""); }));
		BOOST_CHECK(test([&] { st->set_date(1, {}); }));
		BOOST_CHECK(test([&] { st->set_time(1, {}); }));
		BOOST_CHECK(test([&] { st->set_timestamp(1, {}); }));
		BOOST_CHECK(test([&] { st->set_blob(1, nullptr, 0); }));

		st->execute("select * from test_seq");
		BOOST_CHECK(!test([&] { st->get_columns_count(); }));
		BOOST_CHECK(!test([&] { st->get_column_type(1); }));
		BOOST_CHECK(!test([&] { st->get_column_name(1); }));

		BOOST_CHECK(test([&] { st->is_null(1); }));
		BOOST_CHECK(test([&] { st->get_int32(1); }));
		BOOST_CHECK(test([&] { st->get_int64(1); }));
		BOOST_CHECK(test([&] { st->get_float(1); }));
		BOOST_CHECK(test([&] { st->get_double(1); }));
		BOOST_CHECK(test([&] { st->get_str_utf8(1); }));
		BOOST_CHECK(test([&] { st->get_wstr(1); }));
		BOOST_CHECK(test([&] { st->get_date(1); }));
		BOOST_CHECK(test([&] { st->get_time(1); }));
		BOOST_CHECK(test([&] { st->get_timestamp(1); }));
		BOOST_CHECK(test([&] { st->get_blob_size(1); }));
		BOOST_CHECK(test([&] { st->get_blob_data(1, nullptr, 1); }));

		BOOST_CHECK(st->fetch());

		BOOST_CHECK(!test([&] { st->get_columns_count(); }));
		BOOST_CHECK(!test([&] { st->get_column_type(1); }));
		BOOST_CHECK(!test([&] { st->get_column_name(1); }));

		BOOST_CHECK(!test([&] { st->is_null(1); }));
		BOOST_CHECK(!test([&] { st->get_int32(1); }));
		BOOST_CHECK(!test([&] { st->get_int64(1); }));
		BOOST_CHECK(!test([&] { st->get_float(1); }));
		BOOST_CHECK(!test([&] { st->get_double(1); }));
		BOOST_CHECK(!test([&] { st->get_str_utf8(1); }));
		BOOST_CHECK(!test([&] { st->get_wstr(1); }));

		BOOST_CHECK(!st->fetch());
		BOOST_CHECK(test([&] { st->is_null(1); }));
		BOOST_CHECK(test([&] { st->get_int32(1); }));
		BOOST_CHECK(test([&] { st->get_int64(1); }));
		BOOST_CHECK(test([&] { st->get_float(1); }));
		BOOST_CHECK(test([&] { st->get_double(1); }));
		BOOST_CHECK(test([&] { st->get_str_utf8(1); }));
		BOOST_CHECK(test([&] { st->get_wstr(1); }));

		BOOST_CHECK(test([&] { st->fetch(); }));
	});
}

BOOST_AUTO_TEST_CASE(unicode_test)
{
	for_all_connections_do(1, [](const Connections &connections)
	{
		auto &connection = *connections[0];
		connection.connect();

		exec_no_throw(connection, { "drop table unicode_test" });
		exec(connection, {
			u8"create table unicode_test (fld varchar(256))",
			u8"insert into unicode_test(fld) values('Лалала')",
			u8"insert into unicode_test(fld) values('Гыгыгы')"
		});

		auto tran = connection.create_transaction();
		auto st = tran->create_statement();

		st->execute(u8"select * from unicode_test where fld like 'Лал%'");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_str_utf8("fld") == u8"Лалала");
		BOOST_CHECK(st->get_wstr("fld") == L"Лалала");
		BOOST_CHECK(!st->fetch());

		st->execute(L"select * from unicode_test where fld like 'Гыг%'");
		BOOST_CHECK(st->fetch());
		BOOST_CHECK(st->get_str_utf8("fld") == u8"Гыгыгы");
		BOOST_CHECK(st->get_wstr("fld") == L"Гыгыгы");
		BOOST_CHECK(!st->fetch());
	});
}

BOOST_AUTO_TEST_SUITE_END()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(FirebirdServices)

BOOST_AUTO_TEST_CASE(test_fb_services_users)
{
	FbServicesConnectParams params;
	params.name = "service_mgr";
	params.host = "localhost";
	params.user = "sysdba";
	params.password = "masterkey";

	auto services = get_firebird_services();
	services->attach(params);

	services->add_user("new_user", "password");
	services->add_user("new_user2", "password", "first name", "middle name", "last name");

	std::string new_password = "pass";
	services->modify_user("new_user", &new_password, nullptr, nullptr, nullptr);

	std::string new_middle_name = "mn";
	services->modify_user("new_user2", nullptr, nullptr, &new_middle_name, nullptr);

	services->delete_user("new_user2");
	services->delete_user("new_user");
}

BOOST_AUTO_TEST_SUITE_END()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(PosgreSQLMisc)

BOOST_AUTO_TEST_CASE(pg_time)
{
	auto check = [&](const Time& time)
	{
		auto pg_time_value = dblib_time_to_pg_time(time);
		auto result_time = pg_time_to_dblib_time(pg_time_value);
		BOOST_CHECK(time == result_time);
	};

	check({ 22, 12, 44, 555, 777 });
	check({ 10, 11, 12,  13,  14 });
	check({  0,  0,  0,   0,   0 });
	check({ 23, 59, 59, 999, 999 });
}

BOOST_AUTO_TEST_CASE(pg_date)
{
	auto check = [&](const Date& date)
	{
		auto pg_date_value = dblib_date_to_pg_date(date);
		auto result_date = pg_date_to_dblib_date(pg_date_value);
		BOOST_CHECK(date == result_date);
	};

	check({   0,   1, 22 });
	check({ 999,   1, 31 });
	check({ 1917,  1,  1 });
	check({ 1954,  1,  1 });
	check({ 2021, 11, 21 });
	check({ 2050, 12, 31 });
}

BOOST_AUTO_TEST_CASE(pg_timestamp)
{
	auto check = [&](const Date& date, const Time& time)
	{
		TimeStamp ts;
		ts.date = date;
		ts.time = time;
		auto pg_ts_value = dblib_timestamp_to_pg_timestamp(ts);
		auto result_ts = pg_ts_to_dblib_ts(pg_ts_value);
		BOOST_CHECK(ts == result_ts);
	};

	check({    0,  1,  1 }, {  0,  0,  0,   0,   1 });
	check({  999,  1,  1 }, {  0,  0,  0,   0,   0 });
	check({ 1917,  1,  1 }, { 13, 12, 44, 555, 777 });
	check({ 1954,  1,  1 }, { 11,  0,  0,   0,  11 });
	check({ 2021, 11, 21 }, { 22, 12, 33, 555, 777 });
	check({ 2055, 12, 31 }, { 22, 12, 33, 555, 777 });
}


BOOST_AUTO_TEST_SUITE_END()
