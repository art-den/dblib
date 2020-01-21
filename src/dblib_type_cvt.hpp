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

#include <stdint.h>
#include <type_traits>
#include <limits>
#include <string_view>
#include <string>

#include "dblib/dblib_exception.hpp"
#include "dblib/dblib_consts.hpp"

namespace dblib {

template <typename T>
constexpr const char *get_std_type_name()
{
	if constexpr (std::is_same_v<T, long>)
		return "long";
	else if constexpr (std::is_same_v<T, int16_t>)
		return "int16_t";
	else if constexpr (std::is_same_v<T, uint16_t>)
		return "uint16_t";
	else if constexpr (std::is_same_v<T, int32_t>)
		return "int32_t";
	else if constexpr (std::is_same_v<T, uint32_t>)
		return "uint32_t";
	else if constexpr (std::is_same_v<T, int64_t>)
		return "int64_t";
	else if constexpr (std::is_same_v<T, uint64_t>)
		return "uint64_t";
	else if constexpr (std::is_same_v<T, float>)
		return "float";
	else if constexpr (std::is_same_v<T, double>)
		return "double";
	else if constexpr (std::is_same_v<T, std::string>)
		return "std::string";
	else if constexpr (std::is_same_v<T, std::wstring>)
		return "std::wstring";
	else
		static_assert(false, "Unsupported type in get_std_type_name");
}

template <typename T1, typename T2>
bool check_cvt_range(T2 value, bool raize_exception)
{
	using limits = std::numeric_limits<T1>;
	using GeneralType = decltype(value + limits::lowest());

	if ((GeneralType(value) < GeneralType(limits::lowest()))
		|| (GeneralType(value) > GeneralType(limits::max())))
	{
		if (raize_exception)
		{
			std::string err_text;
			err_text
				.append("Value ")
				.append(std::to_string(value))
				.append(" of type ")
				.append(get_std_type_name<T2>())
				.append(" exceeds range for type ")
				.append(get_std_type_name<T1>())
				.append(" (")
				.append(std::to_string(limits::lowest()))
				.append(" ... ")
				.append(std::to_string(limits::max()))
				.append(")");
			throw TypeRangeExceeds(err_text);
		}
		else
			return false;
	}

	return true;
}

template <typename R, typename A> 
R int_to(A arg)
{
	if constexpr (std::is_same_v<R, A>)
		return arg;

	else if constexpr (std::is_same_v<R, std::string>)
		return std::to_string(arg);

	else if constexpr (std::is_same_v<R, std::wstring>)
		return std::to_wstring(arg);

	else
	{
		check_cvt_range<R>(arg, true);
		return R(arg);
	}
}

template <typename R, typename A>
R float_to(A arg)
{
	if constexpr (std::is_same_v<R, A>)
		return arg;

	else if constexpr (std::is_same_v<R, std::string>)
		return std::to_string(arg);

	else if constexpr (std::is_same_v<R, std::wstring>)
		return std::to_wstring(arg);

	else if constexpr (std::is_same_v<R, float> || std::is_same_v<R, double>)
	{
		check_cvt_range<R>(arg, true);
		return R(arg);
	}

	else if constexpr (std::is_integral_v<R>)
	{
		check_cvt_range<R>(arg, true);
		return static_cast<R>((arg < 0) ? arg - 0.5 : arg + 0.5);
	}

	else
		static_assert(false, "Unsupported type in float_to");
}

template <typename R, typename A>
R str_to(const A &arg)
{
	if constexpr (std::is_same_v<R, A>)
		return arg;

	else if constexpr (std::is_integral_v<R>)
	{
		auto value = std::stoll(arg);
		check_cvt_range<R>(value, true);
		return R(value);
	}

	else if constexpr (std::is_same_v<R, float>)
	{
		return std::stof(arg);
	}

	else if constexpr (std::is_same_v<R, double>)
	{
		return std::stod(arg);
	}

	else
		static_assert(false, "Unsupported type in str_to");
}

class TypeConverterDataProvider
{
public:
	virtual void set_int16_impl(size_t index, int16_t value) = 0;
	virtual void set_int32_impl(size_t index, int32_t value) = 0;
	virtual void set_int64_impl(size_t index, int64_t value) = 0;
	virtual void set_float_impl(size_t index, float value) = 0;
	virtual void set_double_impl(size_t index, double value) = 0;
	virtual void set_u8str_impl(size_t index, const std::string &text) = 0;
	virtual void set_wstr_impl(size_t index, const std::wstring &text) = 0;

	virtual int16_t get_int16_impl(size_t index) = 0;
	virtual int32_t get_int32_impl(size_t index) = 0;
	virtual int64_t get_int64_impl(size_t index) = 0;
	virtual float get_float_impl(size_t index) = 0;
	virtual double get_double_impl(size_t index) = 0;
	virtual std::string get_str_utf8_impl(size_t index) = 0;
	virtual std::wstring get_wstr_impl(size_t index) = 0;
};

template <typename T> 
void set_int_with_cvt(TypeConverterDataProvider &dp, ValueType param_type, size_t index, T value)
{
	switch (param_type)
	{
	case ValueType::Short:
		dp.set_int16_impl(index, int_to<int16_t>(value));
		break;

	case ValueType::Integer:
		dp.set_int32_impl(index, int_to<int32_t>(value));
		break;

	case ValueType::BigInt:
		dp.set_int64_impl(index, int_to<int64_t>(value));
		break;

	case ValueType::Float:
		dp.set_float_impl(index, int_to<float>(value));
		break;

	case ValueType::Double:
		dp.set_double_impl(index, int_to<double>(value));
		break;

	case ValueType::Char:
	case ValueType::Varchar:
		dp.set_u8str_impl(index, int_to<std::string>(value));
		break;

	default:
		throw WrongTypeConvException(
			"Can't convert from from " + std::string(get_std_type_name<T>()) +
			" to " + field_type_to_string(param_type)
		);
	}
}

template <typename T> 
void set_float_with_cvt(TypeConverterDataProvider &dp, ValueType param_type, size_t index, T value)
{
	switch (param_type)
	{
	case ValueType::Short:
		dp.set_int16_impl(index, float_to<int16_t>(value));
		break;

	case ValueType::Integer:
		dp.set_int32_impl(index, float_to<int32_t>(value));
		break;

	case ValueType::BigInt:
		dp.set_int64_impl(index, float_to<int64_t>(value));
		break;

	case ValueType::Float:
		dp.set_float_impl(index, float_to<float>(value));
		break;

	case ValueType::Double:
		dp.set_double_impl(index, float_to<double>(value));
		break;

	case ValueType::Char:
	case ValueType::Varchar:
		dp.set_u8str_impl(index, float_to<std::string>(value));
		break;

	default:
		throw WrongTypeConvException(
			"Can't convert from from " + std::string(get_std_type_name<T>()) +
			" to " + field_type_to_string(param_type)
		);
	}
}

template <typename T> 
void set_str_with_cvt(TypeConverterDataProvider &dp, ValueType param_type, size_t index, const T &text)
{
	static_assert(
		std::is_same_v<T, std::string> | std::is_same_v<T, std::wstring>,
		"Type must be std::string or std::wstring"
		);

	switch (param_type)
	{
	case ValueType::Char:
	case ValueType::Varchar:
		if constexpr (std::is_same_v<T, std::string>)
			dp.set_u8str_impl(index, text);
		else
			dp.set_wstr_impl(index, text);
		break;

	case ValueType::Short:
		dp.set_int16_impl(index, str_to<int16_t>(text));
		break;

	case ValueType::Integer:
		dp.set_int32_impl(index, str_to<int32_t>(text));
		break;

	case ValueType::BigInt:
		dp.set_int64_impl(index, str_to<int64_t>(text));
		break;

	case ValueType::Float:
		dp.set_float_impl(index, str_to<float>(text));
		break;

	case ValueType::Double:
		dp.set_float_impl(index, str_to<float>(text));
		break;

	default:
		throw WrongTypeConvException(
			"Can't convert from from " + std::string(get_std_type_name<T>()) +
			" to " + field_type_to_string(param_type)
		);
	}
}

template <typename T> T 
get_with_type_cvt(TypeConverterDataProvider &dp, ValueType fld_type, size_t index)
{
	switch (fld_type)
	{
	case ValueType::Integer:
		return int_to<T>(dp.get_int32_impl(index));

	case ValueType::Short:
		return int_to<T>(dp.get_int16_impl(index));

	case ValueType::BigInt:
		return int_to<T>(dp.get_int64_impl(index));

	case ValueType::Char:
	case ValueType::Varchar:
		if constexpr (std::is_same_v<T, std::string>)
			return str_to<T>(dp.get_str_utf8_impl(index));
		else
			return str_to<T>(dp.get_wstr_impl(index));;

	case ValueType::Float:
		return float_to<T>(dp.get_float_impl(index));

	case ValueType::Double:
		return float_to<T>(dp.get_double_impl(index));
	}

	throw WrongTypeConvException(
		"Can't convert from from " + field_type_to_string(fld_type) +
		" to " + get_std_type_name<T>()
	);
}


} // namespace dblib
