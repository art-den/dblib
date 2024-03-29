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

#pragma once

#include <stdint.h>
#include <type_traits>
#include <limits>
#include <string_view>
#include <string>

#include "../include/dblib/dblib_exception.hpp"
#include "../include/dblib/dblib_consts.hpp"

namespace dblib {

class IParameterSetterWithTypeCvt
{
public:
	virtual void set_int16_impl(size_t index, int16_t value) = 0;
	virtual void set_int32_impl(size_t index, int32_t value) = 0;
	virtual void set_int64_impl(size_t index, int64_t value) = 0;
	virtual void set_float_impl(size_t index, float value) = 0;
	virtual void set_double_impl(size_t index, double value) = 0;
	virtual void set_u8str_impl(size_t index, const std::string& text) = 0;
	virtual void set_wstr_impl(size_t index, const std::wstring& text) = 0;
};

class IResultGetterWithTypeCvt
{
public:
	virtual int16_t get_int16_impl(size_t index) = 0;
	virtual int32_t get_int32_impl(size_t index) = 0;
	virtual int64_t get_int64_impl(size_t index) = 0;
	virtual float get_float_impl(size_t index) = 0;
	virtual double get_double_impl(size_t index) = 0;
	virtual std::string get_str_utf8_impl(size_t index) = 0;
	virtual std::wstring get_wstr_impl(size_t index) = 0;
};

template <typename T>
constexpr const char *get_std_type_name()
{
	bool constexpr is_int16_t = std::is_same_v<T, int16_t>;
	bool constexpr is_uint16_t = std::is_same_v<T, uint16_t>;
	bool constexpr is_int32_t = std::is_same_v<T, int32_t>;
	bool constexpr is_uint32_t = std::is_same_v<T, uint32_t>;
	bool constexpr is_int64_t = std::is_same_v<T, int64_t>;
	bool constexpr is_uint64_t = std::is_same_v<T, uint64_t>;
	bool constexpr is_int = std::is_same_v<T, int>;
	bool constexpr is_long = std::is_same_v<T, long>;
	bool constexpr is_long_long_int = std::is_same_v<T, long long int>;
	bool constexpr is_float = std::is_same_v<T, float>;
	bool constexpr is_double = std::is_same_v<T, double>;
	bool constexpr is_string = std::is_same_v<T, std::string>;
	bool constexpr is_wstring = std::is_same_v<T, std::wstring>;

	static_assert(
		is_long||is_int16_t||is_uint16_t||is_int32_t||
		is_uint32_t||is_int64_t||is_uint64_t||is_int||
		is_long_long_int||is_float||is_double||is_string||
		is_wstring,
		"Unsupported type in get_std_type_name"
	);

	if constexpr (is_int16_t)
		return "int16_t";
	else if constexpr (is_uint16_t)
		return "uint16_t";
	else if constexpr (is_int32_t)
		return "int32_t";
	else if constexpr (is_uint32_t)
		return "uint32_t";
	else if constexpr (is_int64_t)
		return "int64_t";
	else if constexpr (is_uint64_t)
		return "uint64_t";
	else if constexpr (is_int)
		return "int";
	else if constexpr (is_long)
		return "long";
	else if constexpr (is_long_long_int)
		return "long long int";
	else if constexpr (is_float)
		return "float";
	else if constexpr (is_double)
		return "double";
	else if constexpr (is_string)
		return "std::string";
	else if constexpr (is_wstring)
		return "std::wstring";
	else
		return nullptr;
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
	bool constexpr is_same = std::is_same_v<R, A>;
	bool constexpr is_string = std::is_same_v<R, std::string>;
	bool constexpr is_wstring = std::is_same_v<R, std::wstring>;
	bool constexpr is_float = std::is_same_v<R, float>;
	bool constexpr is_double = std::is_same_v<R, double>;
	bool constexpr is_integral = std::is_integral_v<R>;

	static_assert(
		is_same||is_string||is_wstring||
		is_float||is_double||is_integral,
		"Unsupported type in float_to"
	);

	if constexpr (is_same)
		return arg;

	else if constexpr (is_string)
		return std::to_string(arg);

	else if constexpr (is_wstring)
		return std::to_wstring(arg);

	else if constexpr (is_float || is_double)
	{
		check_cvt_range<R>(arg, true);
		return R(arg);
	}

	else if constexpr (is_integral)
	{
		check_cvt_range<R>(arg, true);
		return static_cast<R>((arg < 0) ? arg - 0.5 : arg + 0.5);
	}
}

template <typename R, typename A>
R str_to(A &&arg)
{
	bool constexpr is_same = std::is_same_v<R, A>;
	bool constexpr is_integral = std::is_integral_v<R>;
	bool constexpr is_float = std::is_same_v<R, float>;
	bool constexpr is_double = std::is_same_v<R, double>;

	static_assert(
		is_same||is_integral||is_float||is_double,
		"Unsupported type in str_to"
	);

	if constexpr (is_same)
		return arg;

	else if constexpr (is_integral)
	{
		auto value = std::stoll(arg);
		check_cvt_range<R>(value, true);
		return R(value);
	}

	else if constexpr (is_float)
	{
		return std::stof(arg);
	}

	else if constexpr (is_double)
	{
		return std::stod(arg);
	}
}

template <typename T>
void set_int_param_with_type_cvt(IParameterSetterWithTypeCvt &dp, ValueType param_type, size_t index, T value)
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
			get_std_type_name<T>(),
			field_type_to_string(param_type)
		);
	}
}

template <typename T>
void set_float_param_with_type_cvt(IParameterSetterWithTypeCvt &dp, ValueType param_type, size_t index, T value)
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
			get_std_type_name<T>(),
			field_type_to_string(param_type)
		);
	}
}

template <typename T>
void set_str_param_with_type_cvt(IParameterSetterWithTypeCvt &dp, ValueType param_type, size_t index, const T &text)
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
			get_std_type_name<T>(),
			field_type_to_string(param_type)
		);
	}
}

template <typename T>
void set_param_with_type_cvt(IParameterSetterWithTypeCvt& dp, ValueType param_type, size_t index, T&& param_value)
{
	using Type = std::remove_const_t<std::remove_reference_t<T> >;

	bool constexpr is_int16_t = std::is_same_v<Type, int16_t>;
	bool constexpr is_int32_t = std::is_same_v<Type, int32_t>;
	bool constexpr is_int64_t = std::is_same_v<Type, int64_t>;
	bool constexpr is_float = std::is_same_v<Type, float>;
	bool constexpr is_double = std::is_same_v<Type, double>;
	bool constexpr is_string = std::is_same_v<Type, std::string>;
	bool constexpr is_wstring = std::is_same_v<Type, std::wstring>;

	static_assert(
		is_int16_t||is_int32_t||is_int64_t||
		is_float||is_double||
		is_string || is_wstring,
		"Type is not supported in set_param_with_type_cvt"
	);

	if constexpr (is_int16_t || is_int32_t || is_int64_t)
		set_int_param_with_type_cvt(dp, param_type, index, param_value);

	else if constexpr (is_float || is_double)
		set_float_param_with_type_cvt(dp, param_type, index, param_value);

	else if constexpr (is_string || is_wstring)
		set_str_param_with_type_cvt(dp, param_type, index, param_value);
}


template <typename T>
T get_with_type_cvt(IResultGetterWithTypeCvt &dp, ValueType fld_type, size_t index)
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
		if constexpr (std::is_same_v<T, std::string>)
		{
			auto str = dp.get_str_utf8_impl(index);
			while (!str.empty() && (str.back() == ' ')) str.pop_back(); // trim right
			return str_to<T>(std::move(str));
		}
		else
		{
			auto str = dp.get_wstr_impl(index);
			while (!str.empty() && (str.back() == ' ')) str.pop_back(); // trim right
			return str_to<T>(std::move(str));
		}

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
		field_type_to_string(fld_type),
		get_std_type_name<T>()
	);
}

// integer -> big endian area
template <typename T>
void write_value_into_bytes_be(T value, char* dest_it)
{
	static_assert(std::is_integral_v<T>);

	dest_it += sizeof(T) - 1;
	for (size_t i = 0; i < sizeof(T); i++)
	{
		*dest_it-- = value & 0xFF;
		value >>= 8;
	}
}

// float -> big endian area
template <>
inline void write_value_into_bytes_be<float>(float value, char *dest_it)
{
	write_value_into_bytes_be(
		*(uint32_t*)&value,
		dest_it
	);
}

// double -> big endian area
template <>
inline void write_value_into_bytes_be(double value, char* dest_it)
{
	write_value_into_bytes_be(
		*(uint64_t*)&value,
		dest_it
	);
}

// big endian area -> integer
template <typename T>
T read_value_from_bytes_be(const char* src_it)
{
	static_assert(std::is_integral_v<T>);

	T result = 0;
	for (size_t i = 0; i < sizeof(T); i++)
	{
		result <<= 8;
		result |= (unsigned char)(*src_it++);
	}
	return result;
}

// big endian area -> float
template <>
inline float read_value_from_bytes_be<float>(const char* src_it)
{
	auto tmp = read_value_from_bytes_be<uint32_t>(src_it);
	return *(float*)&tmp;
}

// big endian area -> double
template <>
inline double read_value_from_bytes_be<double>(const char* src_it)
{
	auto tmp = read_value_from_bytes_be<uint64_t>(src_it);
	return *(double*)&tmp;
}


} // namespace dblib
