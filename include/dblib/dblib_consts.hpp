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

#include <string>

#include "dblib_conf.hpp"

namespace dblib {

enum class StatementType
{
	Unknown,
	Insert,
	Select,
	Update,
	Delete,
	Other
};

enum class ValueType
{
	Any,
	None,
	Integer,
	Short,
	BigInt,
	Char,
	Varchar,
	Boolean,
	Float,
	Double,
	Date,
	Time,
	Timestamp,
	Blob,
	Null
};

enum class TransactionLevel
{
	Default,
	Serializable,
	RepeatableRead,
	ReadCommitted,
	DirtyRead
};


enum class TransactionAccess
{
	Read,
	ReadAndWrite
};


enum class LockResolution
{
	Wait,
	Nowait
};


enum class TransactionState
{
	Undefined,
	Started,
	Commited,
	Rollbacked
};

DBLIB_API std::string field_type_to_string(const ValueType &field_type);

DBLIB_API std::string get_transaction_level_string(TransactionLevel level);

} // namespace dblib