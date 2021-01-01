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


#include "dblib_type_cvt.hpp"
#include "dblib/dblib_exception.hpp"
#include "dblib/dblib_cvt_utils.hpp"

namespace dblib {

// ExceptionEx

ExceptionEx::ExceptionEx(const std::wstring &text, int code, int ext_code) :
	code_(code),
	ext_code_(ext_code)
{
	auto utf8_text = utf16_to_utf8(text);
	text_buffer_.assign(utf8_text.begin(), utf8_text.end());
	text_buffer_.push_back(0);
}

ExceptionEx::ExceptionEx(const std::string &text, int code, int ext_code) :
	code_(code),
	ext_code_(ext_code)
{
	text_buffer_.assign(text.begin(), text.end());
	text_buffer_.push_back(0);
}

int ExceptionEx::get_code() const 
{ 
	return code_; 
}

int ExceptionEx::get_ext_code() const 
{ 
	return ext_code_; 
}

char const* ExceptionEx::what() const
{
	return text_buffer_.data();
}


// TypeRangeExceeds

TypeRangeExceeds::TypeRangeExceeds(const std::string &text) :
	ExceptionEx(text, 0, 0) 
{}


// WrongTypeConvException

WrongTypeConvException::WrongTypeConvException(const std::string &text) :
	ExceptionEx(text, 0, 0) 
{}

WrongTypeConvException::WrongTypeConvException(
	std::string_view from_type_name, 
	std::string_view to_type_name
) : 
	ExceptionEx(
		"Can't convert from from " + std::string(from_type_name) + " to " + std::string(to_type_name),
		0, 
		0
	)
{}


// InternalException

InternalException::InternalException(const std::string &text, int code, int ext_code) :
	ExceptionEx(text, code, ext_code) 
{}


// TypeNotSupportedException

TypeNotSupportedException::TypeNotSupportedException(const std::string &type) :
	InternalException("Type '" + type + "' is not supported", -1, -1) 
{}


// TransactionException

TransactionException::TransactionException(const std::string &text, int code, int ext_code) :
	ExceptionEx(text, code, ext_code) {}


// TransactionLevelNotSupportedException

TransactionLevelNotSupportedException::TransactionLevelNotSupportedException(TransactionLevel level) :
	TransactionException(
		"Transaction level '" +
		get_transaction_level_string(level) +
		"' is not supported!",
		0,
		0
	)
{}


// WrongSeqException

WrongSeqException::WrongSeqException(const std::string &text) :
	ExceptionEx(text, 0, 0) 
{}


// WrongArgumentException

WrongArgumentException::WrongArgumentException(const std::string &text) :
	ExceptionEx(text, 0, 0) 
{}


// WrongParameterType

char const* WrongParameterType::what() const
{
	return "Wrong parameter type";
}


// WrongColumnType

char const* WrongColumnType::what() const
{
	return "Wrong column type";
}


// ColumnNotFoundException

ColumnNotFoundException::ColumnNotFoundException(std::string_view column_name)
{
	error_text_.append("Column ");
	error_text_.append(column_name);
	error_text_.append(" not found");
}

char const* ColumnNotFoundException::what() const
{
	return error_text_.c_str();
}


// ParameterNotFoundException

ParameterNotFoundException::ParameterNotFoundException(std::string_view param_name)
{
	error_text_.append("Parameter ");
	error_text_.append(param_name);
	error_text_.append(" not found");
}

char const* ParameterNotFoundException::what() const
{
	return error_text_.c_str();
}


// FunctionalityNotSupported

char const* FunctionalityNotSupported::what() const
{
	return "Functionality is not supported";
}


// ColumnValueIsNullException

ColumnValueIsNullException::ColumnValueIsNullException(std::string_view column_name)
{
	error_text_.append("Value of column ");
	error_text_.append(column_name);
	error_text_.append(" is NULL");
}

char const* ColumnValueIsNullException::what() const
{
	return error_text_.c_str();
}


// EmptyParameterException

char const* EmptyParameterNameException::what() const
{
	return "Empty parameter name";
}


// SharedLibLoadError

SharedLibLoadError::SharedLibLoadError(const std::wstring& lib_file_name, int os_err_code) :
	ExceptionEx(L"Fail to load " + lib_file_name, os_err_code, -1)
{}


// SharedLibProcNotFoundError

SharedLibProcNotFoundError::SharedLibProcNotFoundError(const char* proc_name) :
	ExceptionEx("Procedure " + std::string(proc_name) + " not found in shared library", -1, -1)
{}



} // namespace dblib