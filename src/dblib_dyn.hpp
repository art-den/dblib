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
#include "../include/dblib/dblib_conf.hpp"

#if defined (DBLIB_WINDOWS)
	#define NOMINMAX
	#include <Windows.h>
	using DllType = HMODULE;
#elif defined (DBLIB_LINUX)
	#include <dlfcn.h>
	using DllType = void*;
#endif

#include <string>
#include "../include/dblib/dblib_exception.hpp"

namespace dblib {

class DynLib
{
public:
	~DynLib();

#if defined (DBLIB_WINDOWS)
	void load(const std::wstring &file_name);
#elif defined (DBLIB_LINUX)
	void load(const std::string &file_name);
#endif

	void close();
	bool is_loaded() const;

	template <typename Fun>
	void load_func(Fun &fun, const char *name)
	{
#if defined (DBLIB_WINDOWS)
		fun = (Fun)GetProcAddress(dll_, name);
#elif defined (DBLIB_LINUX)
		fun = (Fun)dlsym(dll_, name);
#endif
		if (fun == nullptr)
			throw SharedLibProcNotFoundError{ name };
	}

private:
	DllType dll_ = nullptr;
};

} // namespace dblib