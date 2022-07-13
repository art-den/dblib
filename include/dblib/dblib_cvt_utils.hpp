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

#include <string_view>
#include <string>

#include "dblib_conf.hpp"
#include "dblib.hpp"

namespace dblib {

DBLIB_API std::string file_name_to_utf8(FileName file_name);

// UTF8 -> UTF16

DBLIB_API void utf8_to_utf16(std::string_view utf8, std::wstring& result, char err_char = '?');
DBLIB_API std::wstring utf8_to_utf16(std::string_view utf8, char err_char = '?');

// UTF16 -> UTF8

DBLIB_API void utf16_to_utf8(std::wstring_view wstr, std::string& result, char err_char = '?');
DBLIB_API std::string utf16_to_utf8(std::wstring_view wstr, char err_char = '?');

// Time -> days

DBLIB_API double time_to_days(int hour, int min, int sec, int msec);
DBLIB_API double time_to_days(const Time &time);

// Date -> julianday

DBLIB_API double date_to_julianday(int year, int mon, int day);
DBLIB_API int32_t date_to_julianday_integer(int year, int mon, int day);

DBLIB_API double date_to_julianday(const Date &date);
DBLIB_API int32_t date_to_julianday_integer(const Date& date);

// TimeStamp -> julianday

DBLIB_API double timestamp_to_julianday(int year, int mon, int day, int hour, int min, int sec, int msec);
DBLIB_API double timestamp_to_julianday(const TimeStamp &ts);

// days -> Time

DBLIB_API Time days_to_time(double days);

// julianday -> Date

DBLIB_API Date julianday_to_date(double julianday);
DBLIB_API Date julianday_integer_to_date(int32_t julianday);

// julianday -> TimeStamp

DBLIB_API TimeStamp julianday_to_timestamp(double julianday);


} // namespace dblib