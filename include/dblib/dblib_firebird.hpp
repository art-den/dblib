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

#include "dblib_conf.hpp"
#include "dblib.hpp"
#include "firebird_c_api/ibase.h"

namespace dblib {

class FbConnection; typedef std::shared_ptr<FbConnection> FbConnectionPtr;
class FbServices; typedef std::shared_ptr<FbServices> FbServicesPtr;
class FbTransaction; typedef std::shared_ptr<FbTransaction> FbTransactionPtr;
class FbStatement; typedef std::shared_ptr<FbStatement> FbStatementPtr;

struct DBLIB_API FbApi
{
	decltype(isc_attach_database)         *isc_attach_database = nullptr;
	decltype(isc_database_info)           *isc_database_info = nullptr;
	decltype(fb_interpret)                *fb_interpret = nullptr;
	decltype(isc_sql_interprete)          *isc_sql_interprete = nullptr;
	decltype(isc_detach_database)         *isc_detach_database = nullptr;
	decltype(isc_create_database)         *isc_create_database = nullptr;
	decltype(isc_blob_info)               *isc_blob_info = nullptr;
	decltype(isc_close_blob)              *isc_close_blob = nullptr;
	decltype(isc_commit_transaction)      *isc_commit_transaction = nullptr;
	decltype(isc_create_blob2)            *isc_create_blob2 = nullptr;
	decltype(isc_dsql_allocate_statement) *isc_dsql_allocate_statement = nullptr;
	decltype(isc_dsql_describe)           *isc_dsql_describe = nullptr;
	decltype(isc_dsql_describe_bind)      *isc_dsql_describe_bind = nullptr;
	decltype(isc_dsql_execute2)           *isc_dsql_execute2 = nullptr;
	decltype(isc_dsql_fetch)              *isc_dsql_fetch = nullptr;
	decltype(isc_dsql_free_statement)     *isc_dsql_free_statement = nullptr;
	decltype(isc_dsql_prepare)            *isc_dsql_prepare = nullptr;
	decltype(isc_dsql_sql_info)           *isc_dsql_sql_info = nullptr;
	decltype(isc_get_segment)             *isc_get_segment = nullptr;
	decltype(isc_open_blob2)              *isc_open_blob2 = nullptr;
	decltype(isc_put_segment)             *isc_put_segment = nullptr;
	decltype(isc_rollback_transaction)    *isc_rollback_transaction = nullptr;
	decltype(isc_start_transaction)       *isc_start_transaction = nullptr;
	decltype(isc_sqlcode)                 *isc_sqlcode = nullptr;
	decltype(isc_portable_integer)        *isc_portable_integer = nullptr;
	decltype(isc_service_attach)          *isc_service_attach = nullptr;
	decltype(isc_service_detach)          *isc_service_detach = nullptr;
	decltype(isc_service_start)           *isc_service_start = nullptr;
};

struct DBLIB_API FbConnectParams
{
	std::string  host;
	std::wstring database;
	std::string  user = "SYSDBA";
	std::string  password = "masterkey";
	std::string  role;
	std::string  charset = "UTF8";
};

struct DBLIB_API FbDbCreateParams
{
	int         dialect = 3;
	int         page_size = 0; // 0 - use default value
	std::string charset = "UTF8";
	bool        force_write = true;
	std::string user = "SYSDBA";
	std::string password = "masterkey";
};

class DBLIB_API FbLib
{
public:
	virtual ~FbLib();

	virtual void load(const std::wstring_view dyn_lib_file_name = {}) = 0;
	virtual bool is_loaded() const = 0;

	virtual const FbApi& get_api() = 0;

	virtual FbConnectionPtr create_connection(const FbConnectParams& connect_params, const FbDbCreateParams* create_params = nullptr) = 0;
	virtual FbServicesPtr create_services() = 0;
};

using FbLibPtr = std::shared_ptr<FbLib>;

struct DBLIB_API FbServicesConnectParams
{
	std::string name = "service_mgr";
	std::string host;
	std::string user = "SYSDBA";
	std::string password = "masterkey";
};

class DBLIB_API FbServices
{
public:
	virtual ~FbServices();

	virtual void attach(const FbServicesConnectParams& params) = 0;
	virtual void detach() = 0;
	virtual void add_user(const std::string &user, const std::string &password) = 0;
	virtual void add_user(const std::string &user, const std::string &password, const std::string &firstname, const std::string &middlename, const std::string &lastname) = 0;
	virtual void modify_user(const std::string &user, const std::string *password, const std::string *firstname, const std::string *middlename, const std::string *lastname) = 0;
	virtual void delete_user(const std::string &user) = 0;
};

class DBLIB_API FbConnection : public Connection
{
public:
	virtual isc_db_handle& get_handle() = 0;
	virtual short get_dialect() const = 0;
	virtual FbTransactionPtr create_fb_transaction(const TransactionParams& transaction_params = {}) = 0;
};

class DBLIB_API FbTransaction : public Transaction
{
public:
	virtual isc_tr_handle& get_handle() = 0;
	virtual FbStatementPtr create_fb_statement() = 0;
};

class DBLIB_API FbStatement : public Statement
{
public:
	virtual isc_stmt_handle& get_handle() = 0;
};

DBLIB_API FbLibPtr create_fb_lib();

} // namespace dblib
