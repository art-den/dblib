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

#include <string>
#include <functional>
#include <map>

#include "../include/dblib/dblib.hpp"

namespace dblib {


enum class StmtState
{
	Undef,
	Error,
	Prepared,
	Executed,
	FinishedFetching
};

struct CaseInsensitiveComparer
{
	using is_transparent = std::true_type;
	bool operator () (std::string_view left, std::string_view right) const;
};


class SqlPreprocessorActions
{
public:
	virtual void append_index_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const = 0;
	virtual void append_named_param_to_sql(const std::string& parameter, int param_index, std::string& sql) const = 0;
	virtual void append_if_seq_data(const std::string& data, const std::string& other, std::string& sql) const = 0;
	virtual void append_seq_generator(const std::string& seq_name, const std::string& other, std::string& sql) const = 0;
};

class SqlPreprocessor
{
public:
	void preprocess(
		std::string_view sql,
		bool use_native_parameters_syntax,
		bool supports_indexed_params,
		const SqlPreprocessorActions &actions
	);

	const std::string& get_preprocessed_sql() const;

	void do_for_param_indexes(const IndexOrName& param, const std::function<void(size_t)>& fun);

	size_t get_parameters_count() const;

private:
	using NamedParams = std::map<std::string, std::vector<size_t>, CaseInsensitiveComparer>;
	using IndexedParams = std::map<size_t, std::vector<size_t>>;

	std::string preprocessed_sql_;
	NamedParams named_params_;
	IndexedParams indexed_params_;
	bool use_native_parameters_syntax_ = false;

	static void preprocess_internal(
		std::string_view             sql,
		std::string                  &preprocessed_sql,
		const SqlPreprocessorActions &actions,
		NamedParams                  &named_params,
		IndexedParams                &indexed_params,
		int                          &param_index,
		bool                         use_native_parameters_syntax,
		bool                         supports_indexed_params
	);
};

class ColumnsHelper
{
public:
	ColumnsHelper(Statement& statement);
	void clear();
	size_t get_column_index(const IndexOrName& column);

private:
	using ColumnIndexByName = std::map<std::string, size_t, CaseInsensitiveComparer>;

	ColumnIndexByName index_by_name_;
	Statement& statement_;
	bool initialized_ = false;
};

enum class ErrorType
{
	Normal,
	Transaction,
	Lock,
	Connection,
	LostConnection
};

void throw_exception(
	const char       *fun_name,
	int              code,
	int              extended_code,
	std::string_view code_expl,
	std::string_view sql_state,
	std::string_view err_msg,
	std::string_view sql,
	ErrorType        error_type
);

} // namespace dblib
