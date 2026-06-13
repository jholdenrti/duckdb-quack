#pragma once

namespace duckdb {

class TableFunction;

class QuackPoolStatusFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
