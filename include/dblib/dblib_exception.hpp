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

#include <exception>
#include <vector>
#include <string_view>

#include "dblib_conf.hpp"
#include "dblib_consts.hpp"

namespace dblib {

class DBLIB_API Exception : public std::exception {};


class DBLIB_API ExceptionEx : public Exception
{
public:
	ExceptionEx(const std::wstring &text, int code, int ext_code);
	ExceptionEx(const std::string &text, int code, int ext_code);
	int get_code() const;
	int get_ext_code() const;
	char const* what() const noexcept override;

private:
	int code_;
	int ext_code_;
	std::vector<char> text_buffer_;
};

class DBLIB_API ConnectException : public ExceptionEx
{
public:
	using ExceptionEx::ExceptionEx;
};

class DBLIB_API TypeRangeExceeds : public ExceptionEx
{
public:
	TypeRangeExceeds(const std::string &text);
};


class DBLIB_API WrongTypeConvException : public ExceptionEx
{
public:
	WrongTypeConvException(const std::string &text);
	WrongTypeConvException(std::string_view from_type_name, std::string_view to_type_name);
};


class DBLIB_API InternalException : public ExceptionEx
{
public:
	InternalException(const std::string &text, int code, int ext_code);
};


class DBLIB_API TypeNotSupportedException : public InternalException
{
public:
	TypeNotSupportedException(const std::string &type);
};


class DBLIB_API TransactionException : public ExceptionEx
{
public:
	TransactionException(const std::string &text, int code, int ext_code);
};


class DBLIB_API TransactionLevelNotSupportedException : public TransactionException
{
public:
	TransactionLevelNotSupportedException(TransactionLevel level);
};


class DBLIB_API WrongSeqException : public ExceptionEx
{
public:
	WrongSeqException(const std::string &text);
};


class DBLIB_API WrongArgumentException : public ExceptionEx
{
public:
	WrongArgumentException(const std::string &text);
};

class DBLIB_API WrongParameterType : public Exception
{
public:
	char const* what() const noexcept override;
};

class DBLIB_API WrongColumnType : public Exception
{
public:
	char const* what() const noexcept override;
};

class DBLIB_API ColumnNotFoundException : public Exception
{
public:
	ColumnNotFoundException(std::string_view column_name);
	char const* what() const noexcept override;

private:
	std::string error_text_;
};

class DBLIB_API ParameterNotFoundException : public Exception
{
public:
	ParameterNotFoundException(std::string_view param_name);
	char const* what() const noexcept override;

private:
	std::string error_text_;
};

class DBLIB_API FunctionalityNotSupported : public Exception
{
public:
	char const* what() const noexcept override;
};

class DBLIB_API ConnectionLostException : public ExceptionEx
{
public:
	using ExceptionEx::ExceptionEx;
};

class DBLIB_API ColumnValueIsNullException : public Exception
{
public:
	ColumnValueIsNullException(std::string_view column_name);
	char const* what() const noexcept override;

private:
	std::string error_text_;
};

class DBLIB_API EmptyParameterNameException : public Exception
{
public:
	char const* what() const noexcept override;
};

class DBLIB_API SharedLibLoadError : public ExceptionEx
{
public:
#if defined(DBLIB_WINDOWS)
	SharedLibLoadError(const std::wstring& lib_file_name, int os_err_code);
#elif defined(DBLIB_LINUX)
	SharedLibLoadError(const std::string& lib_file_name, const char *os_err_text);
#endif
};

class DBLIB_API SharedLibProcNotFoundError : public ExceptionEx
{
public:
	SharedLibProcNotFoundError(const char* proc_name);
};

class DBLIB_API LockException : public TransactionException
{
public:
	using TransactionException::TransactionException;
};

} // namespace dblib
