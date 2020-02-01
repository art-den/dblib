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

#include <iterator>
#include "dblib/dblib_cvt_utils.hpp"

namespace dblib {

template <typename SrcIt>
uint32_t get_utf8_char(SrcIt &it, SrcIt end, char err_char)
{
	uint32_t result = 0;

	uint8_t first_char = *it++;
	uint8_t first_mask = 0;
	uint8_t count = 0;
	if ((first_char & 0b10000000) == 0)
	{
		first_mask = 0b01111111;
		count = 1;
	}
	else if ((first_char & 0b11100000) == 0b11000000)
	{
		first_mask = 0b00011111;
		count = 2;
	}
	else if ((first_char & 0b11110000) == 0b11100000)
	{
		first_mask = 0b00001111;
		count = 3;
	}
	else if ((first_char & 0b11111000) == 0b11110000)
	{
		first_mask = 0b00000111;
		count = 4;
	}
	else
		return err_char;

	result = (first_char & first_mask);

	for (uint8_t i = 1; i < count; i++)
	{
		if (it >= end)
			return err_char;

		uint8_t next_char = *it++;
		if ((next_char & 0b11000000) != 0b10000000)
			return err_char;

		result <<= 6;
		result |= (next_char & 0b00111111);
	}

	return result;
}

template <typename SrcIt>
uint32_t get_utf16_char(SrcIt &it, SrcIt end, char err_char)
{
	if ((0xD800 <= *it) && (*it <= 0xDBFF))
	{
		uint32_t value = ((uint32_t)*it & 0b1111111111) << 10;
		++it;
		if (it >= end)
			return err_char;

		if ((0xDC00 <= *it) && (*it <= 0xDFFF))
		{
			value |= (uint32_t)*it & 0b1111111111;
			++it;
			return value + 0x10000;
		}
		else
			return err_char;
	}

	if ((0xD800 <= *it) && (*it <= 0xDFFF))
	{
		++it;
		return err_char;
	}

	return *it++;
}


template <typename SrcChar, typename SrcIt>
uint32_t get_char(SrcIt &it, SrcIt end, char err_char)
{
	if constexpr (std::is_same_v<SrcChar, char>)
		return get_utf8_char(it, end, err_char);

	else if constexpr (std::is_same_v<SrcChar, wchar_t>)
		return get_utf16_char(it, end, err_char);

	else
		static_assert(false && "Wrong type in get_char");
}

template <typename DstIt>
void put_char_utf8(DstIt &it, uint32_t value, char err_char)
{
	if (value <= 0x7F)
	{
		*it++ = (char)value;
	}
	else if (value <= 0x7FF)
	{
		*it++ = (char)((value & 0b11111000000) >> 6) & 0xFF | 0b11000000;
		*it++ = (char)((value & 0b00000111111) >> 0) & 0xFF | 0b10000000;
	}
	else if (value <= 0xFFFF)
	{
		*it++ = (char)((value & 0b1111000000000000) >> 12) & 0xFF | 0b11100000;
		*it++ = (char)((value & 0b0000111111000000) >> 6) & 0xFF | 0b10000000;
		*it++ = (char)((value & 0b0000000000111111) >> 0) & 0xFF | 0b10000000;
	}
	else if (value <= 0x10FFFF)
	{
		*it++ = (char)((value & 0b111000000000000000000) >> 18) & 0xFF | 0b11110000;
		*it++ = (char)((value & 0b000111111000000000000) >> 12) & 0xFF | 0b10000000;
		*it++ = (char)((value & 0b000000000111111000000) >> 6) & 0xFF | 0b10000000;
		*it++ = (char)((value & 0b000000000000000111111) >> 0) & 0xFF | 0b10000000;
	}
	else
		*it++ = err_char;
}


template <typename DstIt>
void put_char_utf16(DstIt &it, uint32_t value, char err_char)
{
	if ((0xD800 <= value) && (value <= 0xDFFF))
	{
		*it++ = err_char;
	}

	else if (value < 0x10000)
	{
		*it++ = (wchar_t)value;
	}

	else
	{
		value -= 0x10000;
		*it++ = (wchar_t)(((value & 0b11111111110000000000) >> 10) & 0xFFFF | 0xD800);
		*it++ = (wchar_t)(((value & 0b00000000001111111111) >> 0) & 0xFFFF | 0xDC00);
	}
}

template <typename DstChar, typename DstIt>
void put_char(DstIt &it, uint32_t value, char err_char)
{
	if constexpr (std::is_same_v<DstChar, wchar_t>)
		put_char_utf16(it, value, err_char);
	else if constexpr (std::is_same_v<DstChar, char>)
		put_char_utf8(it, value, err_char);
	else
		static_assert(false && "Wrong type in put_char");
}

template <typename SrcChar, typename DstChar, typename SrcIt, typename DstIt>
void utf_to_utf_impl(SrcIt src_begin, SrcIt src_end, DstIt dst_begin, char err_char)
{
	while (src_begin < src_end)
	{
		uint32_t char_value = get_char<SrcChar>(src_begin, src_end, err_char);
		put_char<DstChar>(dst_begin, char_value, err_char);
	}
}


void utf8_to_utf16(std::string_view utf8, std::wstring& result, char err_char)
{
	result.clear();
	utf_to_utf_impl<char, wchar_t>(utf8.begin(), utf8.end(), std::back_inserter(result), err_char);
}

std::wstring utf8_to_utf16(std::string_view utf8, char err_char)
{
	std::wstring result;
	utf8_to_utf16(utf8, result, err_char);
	return result;
}

void utf16_to_utf8(std::wstring_view wstr, std::string& result, char err_char)
{
	result.clear();
	utf_to_utf_impl<wchar_t, char>(wstr.begin(), wstr.end(), std::back_inserter(result), err_char);
}

std::string utf16_to_utf8(std::wstring_view wstr, char err_char)
{
	std::string result;
	utf16_to_utf8(wstr, result, err_char);
	return result;
}

double time_to_days(int hour, int min, int sec, int msec)
{
	return
		hour / 24.0 +
		min / (24.0 * 60.0) +
		sec / (24.0 * 60.0 * 60.0) +
		msec / (24.0 * 60.0 * 60.0 * 1000.0);
}

double time_to_days(const Time &time)
{
	return time_to_days(time.hour, time.min, time.sec, time.msec);
}

double date_to_julianday(int year, int mon, int day)
{
	return date_to_julianday_integer(year, mon, day);
}

int32_t date_to_julianday_integer(int year, int mon, int day)
{
	int64_t a = (14 - (int64_t)mon) / 12;
	int64_t y = (int64_t)year + 4800 - a;
	int64_t m = (int64_t)mon + 12 * a - 3;
	return (int)((int64_t)day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045);
}

double date_to_julianday(const Date &date)
{
	return date_to_julianday(date.year, date.month, date.day);
}

int32_t date_to_julianday_integer(const Date& date)
{
	return date_to_julianday_integer(date.year, date.month, date.day);
}

double timestamp_to_julianday(int year, int mon, int day, int hour, int min, int sec, int msec)
{
	return 
		date_to_julianday(year, mon, day) 
		+ time_to_days(hour, min, sec, msec) 
		- 0.5;
}

double timestamp_to_julianday(const TimeStamp &ts)
{
	return 
		date_to_julianday(ts.date) 
		+ time_to_days(ts.time) 
		- 0.5;
}

Time days_to_time(double days)
{
	days -= (int)days;

	days += 1.0 / (24.0 * 60.0 * 60.0 * 1000.0 * 10.0); // + 0.1 msec to compensate double accuracy

	Time result;

	days *= 24;
	result.hour = (int)days;
	days -= result.hour;

	days *= 60;
	result.min = (int)days;
	days -= result.min;

	days *= 60;
	result.sec = (int)days;
	days -= result.sec;

	days *= 1000;
	result.msec = (int)days;
	days -= result.msec;

	return result;
}

Date julianday_to_date(double julianday)
{
	return julianday_integer_to_date(static_cast<int>(julianday));
}

Date julianday_integer_to_date(int32_t julianday)
{
	Date result;

	int a = julianday + 32044;
	int b = (4 * a + 3) / 146097;
	int c = a - (146097 * b) / 4;
	int d = (4 * c + 3) / 1461;
	int e = c - (1461 * d) / 4;
	int m = (5 * e + 2) / 153;
	result.year = 100 * b + d - 4800 + m / 10;
	result.month = m + 3 - 12 * (m / 10);
	result.day = e - (153 * m + 2) / 5 + 1;

	return result;
}

TimeStamp julianday_to_timestamp(double julianday)
{
	TimeStamp result;

	julianday += 0.5;

	result.time = days_to_time(julianday);
	result.date = julianday_to_date(julianday);

	return result;
}


} // namespace dblib
