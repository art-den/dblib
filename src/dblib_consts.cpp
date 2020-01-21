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


#include "dblib/dblib_consts.hpp"

namespace dblib {

std::string field_type_to_string(const ValueType &field_type)
{
	std::string result;
	switch(field_type)
	{
	case ValueType::Any:       result = "Any"; break;
	case ValueType::None:      result = "None"; break;
	case ValueType::Integer:   result = "Integer"; break;
	case ValueType::Short:     result = "Short"; break;
	case ValueType::BigInt:    result = "BigInt"; break;
	case ValueType::Char:      result = "Char"; break;
	case ValueType::Varchar:   result = "Varchar"; break;
	case ValueType::Boolean:   result = "Boolean"; break;
	case ValueType::Float:     result = "Float"; break;
	case ValueType::Double:    result = "Double"; break;
	case ValueType::Date:      result = "Date"; break;
	case ValueType::Time:      result = "Time"; break;
	case ValueType::Timestamp: result = "Timestamp"; break;
	case ValueType::Blob:      result = "Blob"; break;
	case ValueType::Null:      result = "Null"; break;
	default:                   result = "Unknown (" + std::to_string(int(field_type)) + ")"; break;
	}
	return result;
}

std::string get_transaction_level_string(TransactionLevel level)
{
	switch (level)
	{
	case TransactionLevel::Default:
		return "Default";

	case TransactionLevel::Serializable:
		return "Serializable";

	case TransactionLevel::RepeatableRead:
		return "RepeatableRead";

	case TransactionLevel::ReadCommitted:
		return "ReadCommitted";

	case TransactionLevel::DirtyRead:
		return "DirtyRead";
	}

	return "Wrong (" + std::to_string((int)level) + ")";
}


} // namespace dblib

