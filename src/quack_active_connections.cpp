#include "quack_active_connections.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

namespace duckdb {

static string QueryStateToString(QuackQueryState state) {
	switch (state) {
	case QuackQueryState::IDLE:
		return "idle";
	case QuackQueryState::ACTIVE:
		return "active";
	case QuackQueryState::FINISHED:
		return "finished";
	case QuackQueryState::CANCELLED:
		return "cancelled";
	default:
		return "unknown";
	}
}

struct QuackActiveConnectionsData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackActiveConnectionsData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuackActiveConnectionsBind(ClientContext &, TableFunctionBindInput &,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR,   LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR,   LogicalType::TIMESTAMP, LogicalType::TIMESTAMP,
	                LogicalType::BOOLEAN,   LogicalType::BIGINT};
	names = {"server_id",       "connection_id", "query",          "state",
	         "query_started_at", "last_activity", "in_transaction", "idle_seconds"};
	return make_uniq<QuackActiveConnectionsData>();
}

static void QuackActiveConnectionsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackActiveConnectionsData>();
	if (data.finished) {
		return;
	}

	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();

	auto now = Timestamp::GetCurrentTimestamp();
	idx_t row = 0;
	for (auto &snap : snapshots) {
		output.SetValue(0, row, snap.server_id);
		output.SetValue(1, row, snap.session_id);
		output.SetValue(2, row, snap.sql_query);
		output.SetValue(3, row, Value(QueryStateToString(snap.query_state)));
		if (snap.query_state == QuackQueryState::IDLE) {
			output.SetValue(4, row, Value(LogicalType::TIMESTAMP));
		} else {
			output.SetValue(4, row, Value::TIMESTAMP(snap.query_started_at));
		}
		if (snap.last_activity.value == 0) {
			output.SetValue(5, row, Value(LogicalType::TIMESTAMP));
			output.SetValue(7, row, Value(LogicalType::BIGINT));
		} else {
			output.SetValue(5, row, Value::TIMESTAMP(snap.last_activity));
			output.SetValue(7, row,
			                Value::BIGINT((now.value - snap.last_activity.value) / Interval::MICROS_PER_SEC));
		}
		output.SetValue(6, row, Value::BOOLEAN(snap.in_transaction));
		row++;
	}
	output.SetCardinality(row);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quack_active_connections", {}, QuackActiveConnectionsScan, QuackActiveConnectionsBind);
}

} // namespace duckdb
