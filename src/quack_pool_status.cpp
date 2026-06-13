#include "quack_pool_status.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "storage/quack_catalog.hpp"

namespace duckdb {

struct QuackPoolStatusData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackPoolStatusData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuackPoolStatusBind(ClientContext &, TableFunctionBindInput &,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::BIGINT};
	names = {"catalog", "pool_size", "live", "in_use", "idle", "checkout_wait_us", "checkout_timeouts"};
	return make_uniq<QuackPoolStatusData>();
}

static void QuackPoolStatusScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackPoolStatusData>();
	if (data.finished) {
		return;
	}

	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);

	idx_t row = 0;
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "quack") {
			continue;
		}
		auto &quack_catalog = catalog.Cast<QuackCatalog>();
		auto stats = quack_catalog.GetPoolStats();
		output.SetValue(0, row, Value(db->GetName()));
		output.SetValue(1, row, Value::BIGINT(static_cast<int64_t>(stats.pool_size)));
		output.SetValue(2, row, Value::BIGINT(static_cast<int64_t>(stats.live)));
		output.SetValue(3, row, Value::BIGINT(static_cast<int64_t>(stats.in_use)));
		output.SetValue(4, row, Value::BIGINT(static_cast<int64_t>(stats.idle)));
		output.SetValue(5, row, Value::BIGINT(static_cast<int64_t>(stats.checkout_wait_us)));
		output.SetValue(6, row, Value::BIGINT(static_cast<int64_t>(stats.checkout_timeouts)));
		row++;
	}
	output.SetCardinality(row);
	data.finished = true;
}

TableFunction QuackPoolStatusFunction::GetFunction() {
	return TableFunction("quack_pool_status", {}, QuackPoolStatusScan, QuackPoolStatusBind);
}

} // namespace duckdb
