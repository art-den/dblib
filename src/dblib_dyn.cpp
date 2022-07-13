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

#include <assert.h>
#include "dblib_dyn.hpp"

namespace dblib {

DynLib::~DynLib()
{
	close();
}

#if defined (DBLIB_WINDOWS)
static std::wstring get_path_for_file_name(const std::wstring &file_name)
{
	auto last_del = file_name.find_last_of(L"\\/");
	if (last_del == std::wstring::npos) return {};
	return file_name.substr(0, last_del);
}

static std::wstring get_cur_dir()
{
	size_t size = GetCurrentDirectoryW(0, nullptr);
	std::wstring str;
	str.resize(size);
	GetCurrentDirectoryW((DWORD)str.size(), &str.front());
	return str;
}

void DynLib::load(const std::wstring &file_name)
{
	assert(dll_ == nullptr);

	std::wstring prev_cur_dir;
	auto path = get_path_for_file_name(file_name);
	if (!path.empty())
	{
		prev_cur_dir = get_cur_dir();
		SetCurrentDirectoryW(path.c_str());
	}

	dll_ = LoadLibraryW(file_name.c_str());

	if (!prev_cur_dir.empty())
		SetCurrentDirectoryW(prev_cur_dir.c_str());

	if (dll_ == nullptr)
	{
		int err = GetLastError();
		throw SharedLibLoadError{ file_name, err };
	}
}
#elif defined (DBLIB_LINUX)
void DynLib::load(const std::string &file_name)
{
	assert(dll_ == nullptr);

	dll_ = dlopen (file_name.c_str(), RTLD_NOW);

	if (dll_ == nullptr)
		throw SharedLibLoadError{ file_name, dlerror() };
}
#endif

void DynLib::close()
{
	if (dll_ != nullptr)
	{
#if defined (DBLIB_WINDOWS)
		FreeLibrary(dll_);
#elif defined (DBLIB_LINUX)
		dlclose(dll_);
#endif
		dll_ = nullptr;
	}
}

bool DynLib::is_loaded() const
{
	return dll_ != nullptr;
}

} // namespace dblib
