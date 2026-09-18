#pragma once

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	CONNECTION_REQUEST = 1,
	CONNECTION_RESPONSE = 2,
	PREPARE_REQUEST = 3,
	PREPARE_RESPONSE = 4,
	FETCH_REQUEST = 7,
	FETCH_RESPONSE = 8,
	APPEND_REQUEST = 9,
	SUCCESS_RESPONSE = 10,
	DISCONNECT_MESSAGE = 11,
	ERROR_RESPONSE = 100
};

template <>
const char *EnumUtil::ToChars<MessageType>(MessageType value);
template <>
MessageType EnumUtil::FromString<MessageType>(const char *value);

// workaround for wrong serialization functions signature on DataChunk :/
// TODO: remove in 2.0
class DataChunkWrapper {
public:
	explicit DataChunkWrapper(DataChunk &chunk_p) {
		chunk.InitializeEmpty(chunk_p.GetTypes());
		chunk.Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return chunk;
	}
	void Serialize(Serializer &serializer) const;
	static unique_ptr<DataChunkWrapper> Deserialize(Deserializer &deserializer);

private:
	DataChunk chunk;
};

string MessageTypeToString(MessageType type);

struct MessageHeader {
	MessageHeader(MessageType type_p, string connection_id_p)
	    : type(type_p), connection_id(std::move(connection_id_p)) {
	}

	MessageType type = MessageType::INVALID;
	string connection_id;
	optional_idx client_query_id;

	void Serialize(Serializer &serializer) const;
	static MessageHeader Deserialize(Deserializer &deserializer);
};

class QuackMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<QuackMessage> FromMemoryStream(MemoryStream &read_stream);

	template <class TARGET>
	TARGET &Cast() {
		if (header.type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (header.type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const = 0;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer, MessageType message_type);
	static MessageHeader DeserializeHeader(BinaryDeserializer &deserializer);
	static unique_ptr<QuackMessage> DeserializeMessage(BinaryDeserializer &deserializer, MessageHeader header);

	const MessageType &Type() const {
		return header.type;
	}

	optional_idx ClientQueryId() const {
		return header.client_query_id;
	}

	void SetClientQueryId(optional_idx query_id) {
		header.client_query_id = query_id;
	}

	virtual ~QuackMessage() {
	}

	const string &ConnectionId() const {
		return header.connection_id;
	}

	void SetHeader(MessageHeader header_p) {
		header = std::move(header_p);
	}

protected:
	explicit QuackMessage(MessageType type);
	explicit QuackMessage(MessageType type, string connection_id_p);

private:
	MessageHeader header;
};

class PrepareRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(string connection_id_p, string sql_query_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), sql_query(std::move(sql_query_p)) {
	}

public:
	const string &Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<PrepareRequestMessage> Deserialize(Deserializer &deserializer);

protected:
	PrepareRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string sql_query;
};

class PrepareResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;

	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       vector<unique_ptr<DataChunkWrapper>> results_p, bool needs_more_fetch_p,
	                       hugeint_t result_uuid)
	    : QuackMessage(TYPE), result_types(types_p), result_names(names_p), results(std::move(results_p)),
	      needs_more_fetch(needs_more_fetch_p), result_uuid(result_uuid) {
	}

public:
	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
		return results;
	}

	bool NeedsMoreFetch() const {
		return needs_more_fetch;
	}
	hugeint_t ResultUUID() const {
		return result_uuid;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<PrepareResponseMessage> Deserialize(Deserializer &deserializer);

protected:
	PrepareResponseMessage() : QuackMessage(TYPE) {
	}

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	vector<unique_ptr<DataChunkWrapper>> results;
	bool needs_more_fetch = false;
	hugeint_t result_uuid;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	explicit ConnectionRequestMessage(const string &auth_string_p);

public:
	const string &AuthString() const {
		return auth_string;
	}
	const string &ClientVersion() const {
		return client_duckdb_version;
	}
	const string &ClientPlatform() const {
		return client_platform;
	}
	const idx_t MinimumSupportedQuackVersion() const {
		return min_supported_quack_version;
	}
	const idx_t MaximumSupportedQuackVersion() const {
		return max_supported_quack_version;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ConnectionRequestMessage> Deserialize(Deserializer &deserializer);

protected:
	ConnectionRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string auth_string;
	string client_duckdb_version;
	string client_platform;
	idx_t min_supported_quack_version;
	idx_t max_supported_quack_version;
};

class ConnectionResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(string connection_id_p);

protected:
	ConnectionResponseMessage() : QuackMessage(TYPE) {
	}

public:
	const string &ServerVersion() const {
		return server_duckdb_version;
	}
	const string &ServerPlatform() const {
		return server_platform;
	}
	idx_t QuackVersion() const {
		return quack_version;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ConnectionResponseMessage> Deserialize(Deserializer &deserializer);

private:
	string server_duckdb_version;
	string server_platform;
	idx_t quack_version;
};

class FetchRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	explicit FetchRequestMessage(string connection_id_p, hugeint_t uuid)
	    : QuackMessage(TYPE, std::move(connection_id_p)), uuid(uuid) {
	}

protected:
	FetchRequestMessage() : QuackMessage(TYPE) {
	}

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchRequestMessage> Deserialize(Deserializer &deserializer);

	hugeint_t uuid;
};

class FetchResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage() : QuackMessage(TYPE) {};
	explicit FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p)
	    : QuackMessage(TYPE), results(std::move(results_p)) {};
	FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p, optional_idx batch_index_p)
	    : QuackMessage(TYPE), results(std::move(results_p)), batch_index(batch_index_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchResponseMessage> Deserialize(Deserializer &deserializer);

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
		return results;
	}

	optional_idx BatchIndex() const {
		return batch_index;
	}

private:
	vector<unique_ptr<DataChunkWrapper>> results;
	optional_idx batch_index;
};

class AppendRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_REQUEST;

	explicit AppendRequestMessage(string connection_id_p, string schema_name_p, string table_name_p,
	                              unique_ptr<DataChunkWrapper> append_chunk_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), schema_name(std::move(schema_name_p)),
	      table_name(std::move(table_name_p)), append_chunk(std::move(append_chunk_p)) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AppendRequestMessage> Deserialize(Deserializer &deserializer);

	DataChunk &AppendChunk() const {
		return append_chunk->Chunk();
	}
	const string &SchemaName() const {
		return schema_name;
	}
	const string &TableName() const {
		return table_name;
	}

protected:
	AppendRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string schema_name;
	string table_name;
	unique_ptr<DataChunkWrapper> append_chunk;
};

class DisconnectMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::DISCONNECT_MESSAGE;

	explicit DisconnectMessage(string connection_id_p) : QuackMessage(TYPE, std::move(connection_id_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<DisconnectMessage> Deserialize(Deserializer &deserializer);

protected:
	DisconnectMessage() : QuackMessage(TYPE) {
	}
};

class SuccessResponse : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::SUCCESS_RESPONSE;

	explicit SuccessResponse() : QuackMessage(TYPE) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<SuccessResponse> Deserialize(Deserializer &deserializer);
};

//! Keys quack adds to an error's extra info. They live in the ErrorData, not in ErrorResponse, so that they
//! survive relaying: a quack server can be the client of another one, and there the error is caught and
//! re-raised through DuckDB code that has never heard of quack. Extra info makes that hop, message fields do
//! not.
namespace QuackErrorInfo {
//! The type the SERVER raised, when it was too severe to rethrow as-is (see ErrorResponse::FromWire)
constexpr const char *ORIGINAL_EXCEPTION_TYPE = "original_exception_type";
} // namespace QuackErrorInfo

//! An error raised while handling a request. Type and extra info travel with the message, so the client
//! rethrows the exception the server raised (a Catalog Error stays a Catalog Error).
class ErrorResponse : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::ERROR_RESPONSE;
	explicit ErrorResponse(ErrorData error_p) : QuackMessage(TYPE), error(std::move(error_p)) {
	}
	explicit ErrorResponse(const string &error_p) : QuackMessage(TYPE), error(ExceptionType::INVALID_INPUT, error_p) {
	}
	template <typename... ARGS>
	explicit ErrorResponse(const string &msg, ARGS &&...params)
	    : ErrorResponse(Exception::ConstructMessage(msg, std::forward<ARGS>(params)...)) {
	}
	const ErrorData &Error() const {
		return error;
	}
	const string &ErrorMessage() const {
		return error.Message();
	}
	//! The exception type in its wire form (e.g. "Catalog"), empty if this response holds no error
	string ExceptionTypeName() const;
	//! The extra info minus the entries that only mean something in the process that raised the error
	unordered_map<string, string> TransferableExtraInfo() const;

	//! Whether the SENDER's database is now invalidated, i.e. the receiver should stop using this connection.
	//! A bare flag rather than the sender's identity: a server behind a proxy does not know the address it is
	//! reachable at, while the receiver always knows which server it dialed.
	bool MustInvalidate() const {
		return must_invalidate;
	}
	void SetMustInvalidate(bool must_invalidate_p) {
		must_invalidate = must_invalidate_p;
	}
	//! Rebuild an error response from the wire fields - the inverse of Serialize
	static unique_ptr<ErrorResponse> FromWire(const string &message, const string &exception_type,
	                                          const unordered_map<string, string> &extra_info, bool must_invalidate);

	//! Request header a client sends to say it can decode fields 3 (extra_info) and 4 (must_invalidate).
	//! DuckDB's binary deserializer rejects a field id it does not know, so a peer that predates those
	//! fields (v1.5.4) would fail to decode the error instead of reading it. Sent on every request so the
	//! server needs no per-connection state and connection-less errors are covered too.
	static constexpr const char *EXTRA_FIELDS_HEADER = "X-Quack-Error-Extra";

	//! The receiver did not send EXTRA_FIELDS_HEADER: stop serializing after field 2.
	void SetLegacyPeer() {
		legacy_peer = true;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ErrorResponse> Deserialize(Deserializer &deserializer);

protected:
	ErrorResponse() : QuackMessage(TYPE) {
	}

private:
	ErrorData error;
	//! Set by the server when its OWN database died with this error - see MustInvalidate()
	bool must_invalidate = false;
	//! See SetLegacyPeer
	bool legacy_peer = false;
};

} // namespace duckdb
