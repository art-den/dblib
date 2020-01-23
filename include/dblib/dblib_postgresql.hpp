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

#include "dblib_conf.hpp"
#include "dblib.hpp"
#include "libpq-fe.h"

namespace dblib {

class PgConnection; typedef std::shared_ptr<PgConnection> PgConnectionPtr;
class PgTransaction; typedef std::shared_ptr<PgTransaction> PgTransactionPtr;
class PgStatement; typedef std::shared_ptr<PgStatement> PgStatementPtr;

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
};


DBLIB_API PgConnectionPtr create_pg_connection(const PgConnectParams &params);

} // namespace dblib
