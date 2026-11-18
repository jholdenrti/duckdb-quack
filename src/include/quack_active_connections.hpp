#pragma once

namespace duckdb {

class TableFunction;

class QuackActiveConnectionsFunction {
public:
	static TableFunction GetFunction();
};

//! Client side: the connection pool of every attached quack catalog
class QuackPoolStatusFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
