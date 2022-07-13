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

#include <string>

#if defined(__linux__)
	#define DBLIB_LINUX
	namespace dblib {
		using FileName = std::string;
	}
#elif defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)
	namespace dblib {
		using FileName = std::wstring;
	}
	#define DBLIB_WINDOWS
#else
	#error "Unsupported platform"
#endif

// building
#if defined(DBLIB_BUILDING) && defined(DBLIB_WINDOWS)
	#define DBLIB_API __declspec(dllexport)
	#pragma warning(disable: 4251)
	#pragma warning(disable: 4275)

// using
#else
	#define DBLIB_API
#endif

#define DB_LIB_UNIQUE_PIMPL(CLASS, OBJ) struct CLASS; std::unique_ptr<CLASS> OBJ;

