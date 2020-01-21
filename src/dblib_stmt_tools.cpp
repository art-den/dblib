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

#include <assert.h>

#ifdef DBLIB_BOOST_REGEX
#include <boost/regex.hpp>
#else
#include <regex>
#endif

#include "dblib_stmt_tools.hpp"
#include "dblib/dblib_exception.hpp"


namespace dblib {

/* struct CaseInsensitiveComparer */

bool CaseInsensitiveComparer::operator() (std::string_view left, std::string_view right) const
{
	if (left.size() < right.size()) return true;
	if (left.size() > right.size()) return false;

	assert(left.size() == right.size());

	auto it1 = left.begin();
	auto it2 = right.begin();
	size_t size = left.size();
	for (size_t i = 0; i < size; i++)
		if (tolower(*it1++) < tolower(*it2++)) return true;

	return false;
}

/* class SqlPreprocessor */

void SqlPreprocessor::preprocess(std::string_view sql, bool use_native_parameters_syntax, const SqlPreprocessorActions &actions)
{
	use_native_parameters_syntax_ = use_native_parameters_syntax;

	preprocessed_sql_.clear();
	indexed_params_.clear();
	named_params_.clear();

	int param_index = 1;

	preprocess_internal(
		sql, 
		preprocessed_sql_,
		actions, 
		named_params_, 
		indexed_params_, 
		param_index,
		use_native_parameters_syntax_
	);
}

const std::string& SqlPreprocessor::get_preprocessed_sql() const
{
	return preprocessed_sql_;
}

void SqlPreprocessor::do_for_param_indexes(const IndexOrName& param, const std::function<void(size_t)>& fun)
{
	if (!use_native_parameters_syntax_)
	{
		std::vector<size_t>* param_indices = nullptr;

		if (param.get_type() == IndexOrNameType::Index)
		{
			auto it = indexed_params_.find(param.get_index());
			if (it != indexed_params_.end())
				param_indices = &it->second;
		}
		else
		{
			auto it = named_params_.find(param.get_name());
			if (it != named_params_.end())
				param_indices = &it->second;
		}

		if (!param_indices)
			throw WrongArgumentException("Parameter not found!");

		for (auto param_index : *param_indices)
			fun(param_index);
	}
	else
		fun(param.get_index());
}

#ifdef DBLIB_BOOST_REGEX
namespace regex_ns = boost;
#else
namespace regex_ns = std;
#endif

void SqlPreprocessor::preprocess_internal(
	std::string_view             sql, 
	std::string                  &preprocessed_sql,
	const SqlPreprocessorActions &actions, 
	NamedParams                  &named_params, 
	IndexedParams                &indexed_params, 
	int                          &param_index,
	bool                         use_native_parameters_syntax)
{
	static regex_ns::regex item_regex{
		R"--((\?)\d+)--" // (gr 1) indexed param ?1, ?2 etc
		"|"
		R"--(([@:$])\w+)--" // (gr 2) named param :lala, @param1, $blabla
		"|"
		R"--((")(?:\\.|""|\\"|[^"])*")--" // (gr 3) "string"
		"|"
		R"--((')(?:\\.|''|\\'|[^'])*')--" // (gr 4) 'string'
		"|"
		R"--(\{(if_seq)\s*([^,}]+)(.*?)\})--" // (gr 5,6,7) if_seq placeholder {if_seq seq_name }
		"|"
		R"--(\{(next)\s*?(\w+)(.*?)\})--" // (gr 8,9,10) next placeholder {next seq_name }
		"|"
		R"--((\/\*)[\s\S]*?\*\/)--" // (gr 11) multi line comment /* comment */
		"|"
		R"--((\/\/).*?$)--" // (gr 12) single line comment
	};


	using SVI = std::string_view::const_iterator;
	using Match = regex_ns::match_results<SVI>;
	
	Match m;
	std::string parameter, seq_name, other_str, in_placeholder_parsed_text;
	for (SVI begin = sql.begin();;)
	{
		bool matched = regex_ns::regex_search(begin, sql.end(), m, item_regex);
		if (!matched)
		{
			preprocessed_sql.insert(preprocessed_sql.end(), begin, sql.end());
			break;
		}

		// indexed params
		if (m[1].length())
		{
			if (!use_native_parameters_syntax)
			{
				preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].first);
				parameter.assign(m[0].first + 1, m[0].second); // + 1 to skip ?
				actions.append_index_param_to_sql(parameter, preprocessed_sql);
				size_t user_index = atoi(parameter.c_str());
				indexed_params[user_index].push_back(param_index);
				param_index++;
			}
			else
				preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].second);
		}

		// named param @param1, $param2 or :param3
		else if (m[2].length())
		{
			if (!use_native_parameters_syntax)
			{
				parameter.assign(m[0].first, m[0].second);
				preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].first);
				actions.append_named_param_to_sql(parameter, preprocessed_sql);
				named_params[parameter].push_back(param_index);
				param_index++;
			}
			else
				preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].second);
		}

		// strings or multi line comment
		else if (m[3].length() || m[4].length() || m[11].length())
		{
			preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].second);
		}

		// seq placeholder {if_seq some, }
		else if (m[5].length())
		{
			preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].first);

			in_placeholder_parsed_text.clear();
			preprocess_internal(
				std::string_view(&*m[6].first, m[6].length()),
				in_placeholder_parsed_text,
				actions,
				named_params,
				indexed_params,
				param_index,
				use_native_parameters_syntax
			);

			other_str.assign(m[7].first, m[7].second);
			actions.append_if_seq_data(in_placeholder_parsed_text, other_str, preprocessed_sql);
		}

		// seq value generator placeholder {next seq_name, }
		else if (m[8].length())
		{
			preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].first);
			seq_name.assign(m[9].first, m[9].second);
			other_str.assign(m[10].first, m[10].second);
			actions.append_seq_generator(seq_name, other_str, preprocessed_sql);
		}

		// single line comment
		else if (m[12].length())
		{
			preprocessed_sql.insert(preprocessed_sql.end(), begin, m[0].second);
			preprocessed_sql.append("\n");
		}

		begin = m[0].second;
	}
}


/* class ColumnsHelper */

ColumnsHelper::ColumnsHelper(Statement& statement) : 
	statement_(statement)
{}

void ColumnsHelper::clear()
{
	index_by_name_.clear();
	initialized_ = false;
}

size_t ColumnsHelper::get_column_index(const IndexOrName& column)
{
	if (column.get_type() == IndexOrNameType::Index)
		return column.get_index();

	if (!initialized_)
	{
		auto col_count = statement_.get_columns_count();
		for (size_t i = 1; i <= col_count; i++)
			index_by_name_.insert(std::pair{ statement_.get_column_name(i), i });

		initialized_ = true;
	}

	auto it = index_by_name_.find(column.get_name());
	if (it == index_by_name_.end())
		throw ColumnNotFoundException();

	return it->second;
}


} // namespace dblib