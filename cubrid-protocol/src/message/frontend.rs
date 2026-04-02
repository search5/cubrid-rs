//! Client-to-server request message serialization.
//!
//! This module provides free functions that serialize CUBRID protocol request
//! messages directly into a [`BytesMut`] buffer. Each function corresponds
//! to a CAS function code and produces a complete, framed message ready for
//! transmission over the wire.
//!
//! # Design
//!
//! Following the rust-postgres pattern, messages are serialized via free
//! functions rather than an intermediate message enum. This avoids extra
//! allocations and allows the caller to write directly into a shared buffer.
//!
//! Every request message has the same header structure:
//!
//! ```text
//! [4 bytes] Payload length (big-endian, excludes these 4 bytes and CAS info)
//! [4 bytes] CAS info (echoed from previous response)
//! [1 byte]  Function code
//! [N bytes] Function-specific parameters
//! ```

use bytes::{BufMut, BytesMut};
use byteorder::{BigEndian, ByteOrder};

use crate::cas_info::CasInfo;
use crate::types::{FunctionCode, SchemaType, TransactionOp, XaOp, Xid};
use crate::{
    write_cubrid_string, write_param_byte, write_param_int, write_param_int64, write_param_null,
    NET_SIZE_CAS_INFO, NET_SIZE_INT,
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Write the common request header and return the position of the length field.
///
/// The caller should write the function-specific payload after this, then
/// call [`patch_payload_length`] to fill in the correct length.
fn write_header(cas_info: &CasInfo, func: FunctionCode, buf: &mut BytesMut) -> usize {
    let len_pos = buf.len();

    // Placeholder for payload length (will be patched later)
    buf.put_i32(0);

    // CAS info echoed from the last server response
    buf.put_slice(cas_info.as_bytes());

    // Function code identifying this request
    buf.put_u8(func as u8);

    len_pos
}

/// Patch the payload length field at the position recorded by [`write_header`].
///
/// The payload length is the total bytes written after the length field,
/// minus the length field itself (4 bytes) and the CAS info (4 bytes).
/// Writes directly into the buffer without cloning.
fn patch_payload_length(buf: &mut BytesMut, len_pos: usize) {
    // Payload = everything after (len_pos + 4 bytes length + 4 bytes cas_info)
    let payload_len = (buf.len() - len_pos - NET_SIZE_INT - NET_SIZE_CAS_INFO) as i32;
    BigEndian::write_i32(&mut buf[len_pos..], payload_len);
}

/// Build a complete request message in a single step.
///
/// This helper creates a new buffer, writes the header, calls the body
/// writer closure, patches the length, and returns the finished buffer.
fn build_message(
    cas_info: &CasInfo,
    func: FunctionCode,
    body: impl FnOnce(&mut BytesMut),
) -> BytesMut {
    let mut buf = BytesMut::new();
    let len_pos = write_header(cas_info, func, &mut buf);
    body(&mut buf);
    patch_payload_length(&mut buf, len_pos);
    buf
}

// ---------------------------------------------------------------------------
// Public message builders
// ---------------------------------------------------------------------------

/// Build a PREPARE request message.
///
/// Prepares a SQL statement for execution. The server returns a query handle
/// that can be used with [`execute`] to run the statement with parameters.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `sql`: The SQL statement to prepare (may contain `?` placeholders)
/// - `prepare_flag`: Preparation options (see [`PrepareFlag`](crate::types::PrepareFlag))
/// - `auto_commit`: Whether auto-commit is enabled
pub fn prepare(
    cas_info: &CasInfo,
    sql: &str,
    prepare_flag: u8,
    auto_commit: bool,
) -> BytesMut {
    build_message(cas_info, FunctionCode::Prepare, |buf| {
        // SQL statement as null-terminated string
        write_cubrid_string(sql, buf);
        // Prepare option flag
        write_param_byte(prepare_flag, buf);
        // Auto-commit mode
        write_param_byte(auto_commit as u8, buf);
    })
}

/// Build an EXECUTE request message.
///
/// Executes a previously prepared statement. For SELECT statements, the
/// response may include an initial batch of fetched rows.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `query_handle`: Handle returned by a previous PREPARE
/// - `execute_flag`: Execution options (see [`ExecuteFlag`](crate::types::ExecuteFlag))
/// - `auto_commit`: Whether auto-commit is enabled
/// - `params`: Serialized bind parameter values (pre-encoded by the caller)
/// - `is_select`: Whether this is a SELECT statement (affects fetch flag)
pub fn execute(
    cas_info: &CasInfo,
    query_handle: i32,
    execute_flag: u8,
    auto_commit: bool,
    params: &[u8],
    is_select: bool,
) -> BytesMut {
    execute_with_timeout(cas_info, query_handle, execute_flag, auto_commit, params, is_select, 0)
}

/// Build an EXECUTE request message with a query timeout.
///
/// Like [`execute`] but allows specifying a query timeout in milliseconds.
/// A timeout of 0 means no timeout.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `query_handle`: Handle returned by a previous PREPARE
/// - `execute_flag`: Execution options
/// - `auto_commit`: Whether auto-commit is enabled
/// - `params`: Serialized bind parameter values (pre-encoded by the caller)
/// - `is_select`: Whether this is a SELECT statement (affects fetch flag)
/// - `query_timeout_ms`: Query timeout in milliseconds (0 = no timeout)
pub fn execute_with_timeout(
    cas_info: &CasInfo,
    query_handle: i32,
    execute_flag: u8,
    auto_commit: bool,
    params: &[u8],
    is_select: bool,
    query_timeout_ms: i32,
) -> BytesMut {
    build_message(cas_info, FunctionCode::Execute, |buf| {
        // Query handle from prepare
        write_param_int(query_handle, buf);
        // Execute flag
        write_param_byte(execute_flag, buf);
        // Max column size (0 = unlimited)
        write_param_int(0, buf);
        // Max row size (0 = unlimited)
        write_param_int(0, buf);
        // NULL parameter (reserved)
        write_param_null(buf);
        // Fetch flag: 1 for SELECT (fetch first batch), 0 otherwise
        write_param_byte(if is_select { 1 } else { 0 }, buf);
        // Auto-commit mode
        write_param_byte(auto_commit as u8, buf);
        // Forward-only cursor
        write_param_byte(1, buf);
        // Cache time: 8 bytes = seconds(4) + microseconds(4), both zero
        buf.put_i32(NET_SIZE_INT as i32 * 2);
        buf.put_i32(0); // seconds
        buf.put_i32(0); // microseconds
        // Query timeout (PROTOCOL_V1+)
        write_param_int(query_timeout_ms, buf);
        // Bind parameters (pre-serialized by the caller)
        if !params.is_empty() {
            buf.put_slice(params);
        }
    })
}

/// Build a FETCH request message.
///
/// Fetches the next batch of rows from an open result set.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `query_handle`: Handle of the result set to fetch from
/// - `start_position`: 1-based position of the first row to fetch
/// - `fetch_size`: Maximum number of rows to fetch in this batch
pub fn fetch(
    cas_info: &CasInfo,
    query_handle: i32,
    start_position: i32,
    fetch_size: i32,
) -> BytesMut {
    build_message(cas_info, FunctionCode::Fetch, |buf| {
        // Query handle
        write_param_int(query_handle, buf);
        // Start position (1-based cursor position)
        write_param_int(start_position, buf);
        // Fetch size (number of rows)
        write_param_int(fetch_size, buf);
        // Case-sensitive flag (0 = case insensitive)
        write_param_byte(0, buf);
        // Result set index (0 = default)
        write_param_int(0, buf);
    })
}

/// Build an END_TRAN (commit or rollback) request message.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `op`: Transaction operation (commit or rollback)
pub fn end_tran(cas_info: &CasInfo, op: TransactionOp) -> BytesMut {
    build_message(cas_info, FunctionCode::EndTran, |buf| {
        write_param_byte(op as u8, buf);
    })
}

/// Build a CON_CLOSE request message.
///
/// Closes the CAS connection. After sending this, the client should
/// close the TCP socket.
pub fn con_close(cas_info: &CasInfo) -> BytesMut {
    build_message(cas_info, FunctionCode::ConClose, |_buf| {
        // No additional parameters
    })
}

/// Build a GET_DB_VERSION request message.
///
/// Retrieves the database server version string, which is used for
/// runtime dialect detection.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `auto_commit`: Whether auto-commit is enabled
pub fn get_db_version(cas_info: &CasInfo, auto_commit: bool) -> BytesMut {
    build_message(cas_info, FunctionCode::GetDbVersion, |buf| {
        write_param_byte(auto_commit as u8, buf);
    })
}

/// Build a CLOSE_REQ_HANDLE request message.
///
/// Releases a prepared statement handle on the server, freeing
/// associated resources.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `query_handle`: The statement handle to release
/// - `auto_commit`: Whether auto-commit is enabled
pub fn close_req_handle(cas_info: &CasInfo, query_handle: i32, auto_commit: bool) -> BytesMut {
    build_message(cas_info, FunctionCode::CloseReqHandle, |buf| {
        write_param_int(query_handle, buf);
        write_param_byte(auto_commit as u8, buf);
    })
}

/// Build a SCHEMA_INFO request message.
///
/// Queries database schema metadata (tables, columns, constraints, etc.).
/// The response is a result set that must be fetched like a query result.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `schema_type`: The category of schema information to retrieve
/// - `table_name`: Filter by table name (empty string for all)
/// - `column_name`: Filter by column name (empty string for all)
/// - `flag`: Pattern match flag. 0 = exact match, 1 = class name pattern,
///   2 = attribute name pattern, 3 = both. Typically 0.
pub fn schema_info(
    cas_info: &CasInfo,
    schema_type: SchemaType,
    table_name: &str,
    column_name: &str,
    flag: u8,
) -> BytesMut {
    build_message(cas_info, FunctionCode::SchemaInfo, |buf| {
        write_param_int(schema_type as u8 as i32, buf);
        write_cubrid_string(table_name, buf);
        write_cubrid_string(column_name, buf);
        write_param_byte(flag, buf);
    })
}

/// Build a SET_DB_PARAMETER request message.
///
/// Sets a database session parameter (e.g., isolation level, auto-commit).
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `param_id`: The parameter identifier (see [`DbParameter`](crate::types::DbParameter))
/// - `param_value`: The value to set
pub fn set_db_parameter(cas_info: &CasInfo, param_id: i32, param_value: i32) -> BytesMut {
    build_message(cas_info, FunctionCode::SetDbParameter, |buf| {
        write_param_int(param_id, buf);
        write_param_int(param_value, buf);
    })
}

/// Build a GET_DB_PARAMETER request message.
///
/// Retrieves the current value of a database session parameter.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `param_id`: The parameter identifier (see [`DbParameter`](crate::types::DbParameter))
pub fn get_db_parameter(cas_info: &CasInfo, param_id: i32) -> BytesMut {
    build_message(cas_info, FunctionCode::GetDbParameter, |buf| {
        write_param_int(param_id, buf);
    })
}

/// Build an END_SESSION request message.
///
/// Ends the current database session without closing the CAS connection.
pub fn end_session(cas_info: &CasInfo) -> BytesMut {
    build_message(cas_info, FunctionCode::EndSession, |_buf| {
        // No additional parameters
    })
}

/// Build a PREPARE_AND_EXECUTE request message.
///
/// Combines prepare and execute into a single round trip, which is more
/// efficient for one-shot queries. This is function code 41.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `sql`: The SQL statement to prepare and execute
/// - `prepare_flag`: Preparation options
/// - `auto_commit`: Whether auto-commit is enabled
/// - `execute_flag`: Execution options
/// - `is_select`: Whether this is a SELECT statement (affects fetch flag)
/// - `query_timeout_ms`: Query timeout in milliseconds (0 = no timeout)
/// - `params`: Serialized bind parameter values (pre-encoded)
pub fn prepare_and_execute(
    cas_info: &CasInfo,
    sql: &str,
    prepare_flag: u8,
    auto_commit: bool,
    execute_flag: u8,
    is_select: bool,
    query_timeout_ms: i32,
    params: &[u8],
) -> BytesMut {
    build_message(cas_info, FunctionCode::PrepareAndExecute, |buf| {
        // argv[0]: prepare_argc_count — number of prepare-phase arguments.
        // CAS server reads this first to know where prepare args end and
        // execute args begin (see cas_function.c fn_prepare_and_execute).
        write_param_int(3, buf); // 3 args: sql + prepare_flag + auto_commit

        // Prepare phase arguments (argv[1..3])
        write_cubrid_string(sql, buf);
        write_param_byte(prepare_flag, buf);
        write_param_byte(auto_commit as u8, buf);

        // Execute phase arguments (argv[4..], must match execute_with_timeout layout)
        write_param_byte(execute_flag, buf);
        write_param_int(0, buf); // max column size
        write_param_int(0, buf); // max row size
        write_param_null(buf); // reserved
        // Fetch flag: 1 for SELECT (fetch first batch), 0 otherwise
        write_param_byte(if is_select { 1 } else { 0 }, buf);
        // Auto-commit mode (for execute phase)
        write_param_byte(auto_commit as u8, buf);
        // Forward-only cursor
        write_param_byte(1, buf);

        // Cache time: 8 bytes (seconds + microseconds)
        buf.put_i32(NET_SIZE_INT as i32 * 2);
        buf.put_i32(0);
        buf.put_i32(0);

        // Query timeout
        write_param_int(query_timeout_ms, buf);

        // Bind parameters
        if !params.is_empty() {
            buf.put_slice(params);
        }
    })
}

/// Build an EXECUTE_BATCH request message.
///
/// Executes multiple SQL statements in a single request. Each statement
/// is executed independently; the response contains individual results.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `sqls`: The SQL statements to execute
/// - `auto_commit`: Whether auto-commit is enabled
pub fn execute_batch(
    cas_info: &CasInfo,
    sqls: &[&str],
    auto_commit: bool,
    query_timeout_ms: i32,
) -> BytesMut {
    build_message(cas_info, FunctionCode::ExecuteBatch, |buf| {
        write_param_byte(auto_commit as u8, buf);
        // Query timeout (PROTOCOL_V4+, required for all supported versions)
        write_param_int(query_timeout_ms, buf);
        // Each SQL as a null-terminated string parameter
        for sql in sqls {
            write_cubrid_string(sql, buf);
        }
    })
}

/// Build a SAVEPOINT request message.
///
/// Creates or releases a transaction savepoint.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `op`: Savepoint operation (1 = create, 2 = release)
/// - `name`: The savepoint name
pub fn savepoint(cas_info: &CasInfo, op: u8, name: &str) -> BytesMut {
    build_message(cas_info, FunctionCode::Savepoint, |buf| {
        write_param_byte(op, buf);
        write_cubrid_string(name, buf);
    })
}

/// Build a GET_ROW_COUNT request message.
///
/// Retrieves the number of rows affected by the last executed statement.
pub fn get_row_count(cas_info: &CasInfo) -> BytesMut {
    build_message(cas_info, FunctionCode::GetRowCount, |_buf| {
        // No additional parameters
    })
}

/// Build a GET_LAST_INSERT_ID request message.
///
/// Retrieves the last auto-generated insert ID.
pub fn get_last_insert_id(cas_info: &CasInfo) -> BytesMut {
    build_message(cas_info, FunctionCode::GetLastInsertId, |_buf| {
        // No additional parameters
    })
}

/// Build a CURSOR_CLOSE request message.
///
/// Closes a server-side cursor.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `query_handle`: The cursor handle to close
pub fn cursor_close(cas_info: &CasInfo, query_handle: i32) -> BytesMut {
    build_message(cas_info, FunctionCode::CursorClose, |buf| {
        write_param_int(query_handle, buf);
    })
}

// ---------------------------------------------------------------------------
// LOB operations (H6)
// ---------------------------------------------------------------------------

/// Build a LOB_NEW request to create a new LOB handle.
///
/// Creates an empty LOB (BLOB or CLOB) on the server and returns a
/// LOB handle that can be used with [`lob_write`] and [`lob_read`].
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `lob_type`: LOB type code (23 = BLOB, 24 = CLOB)
pub fn lob_new(cas_info: &CasInfo, lob_type: u8) -> BytesMut {
    build_message(cas_info, FunctionCode::LobNew, |buf| {
        write_param_int(lob_type as i32, buf);
    })
}

/// Build a LOB_WRITE request to write data to a LOB.
///
/// Writes a chunk of data to an existing LOB at the specified offset.
/// For large LOBs, multiple write calls may be needed.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `lob_handle`: The LOB handle bytes (obtained from LOB_NEW or a query result)
/// - `offset`: Byte offset within the LOB to start writing
/// - `data`: The data to write
pub fn lob_write(cas_info: &CasInfo, lob_handle: &[u8], offset: i64, data: &[u8]) -> BytesMut {
    assert!(
        lob_handle.len() <= i32::MAX as usize,
        "LOB handle exceeds maximum size"
    );
    assert!(
        data.len() <= i32::MAX as usize,
        "LOB data exceeds maximum size"
    );
    build_message(cas_info, FunctionCode::LobWrite, |buf| {
        // LOB handle as a length-prefixed blob
        buf.put_i32(lob_handle.len() as i32);
        buf.put_slice(lob_handle);
        // Offset within the LOB
        write_param_int64(offset, buf);
        // Data to write, length-prefixed
        buf.put_i32(data.len() as i32);
        buf.put_slice(data);
    })
}

/// Build a LOB_READ request to read data from a LOB.
///
/// Reads a chunk of data from an existing LOB at the specified offset.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `lob_handle`: The LOB handle bytes
/// - `offset`: Byte offset within the LOB to start reading
/// - `length`: Maximum number of bytes to read
pub fn lob_read(cas_info: &CasInfo, lob_handle: &[u8], offset: i64, length: i32) -> BytesMut {
    assert!(
        lob_handle.len() <= i32::MAX as usize,
        "LOB handle exceeds maximum size"
    );
    build_message(cas_info, FunctionCode::LobRead, |buf| {
        // LOB handle as a length-prefixed blob
        buf.put_i32(lob_handle.len() as i32);
        buf.put_slice(lob_handle);
        // Offset within the LOB
        write_param_int64(offset, buf);
        // Length to read
        write_param_int(length, buf);
    })
}

// ---------------------------------------------------------------------------
// OID operations (H7)
// ---------------------------------------------------------------------------

/// Build an OID_GET request to retrieve an object by its OID.
///
/// Retrieves attribute values for the object identified by the given OID.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `oid`: The 8-byte object identifier (pageId + slotId + volId)
/// - `attrs`: List of attribute names to retrieve (empty for all)
pub fn oid_get(cas_info: &CasInfo, oid: &[u8; 8], attrs: &[&str]) -> BytesMut {
    build_message(cas_info, FunctionCode::OidGet, |buf| {
        // OID (8 bytes, length-prefixed)
        buf.put_i32(8);
        buf.put_slice(oid);
        // Attribute names as a comma-separated null-terminated string
        let attrs_str = attrs.join(",");
        write_cubrid_string(&attrs_str, buf);
    })
}

/// Build an OID_PUT request to update an object by its OID.
///
/// Updates attribute values for the object identified by the given OID.
///
/// # Parameters
///
/// - `cas_info`: Current session CAS info
/// - `oid`: The 8-byte object identifier
/// - `attrs`: List of attribute names to update
/// - `values`: Pre-serialized attribute values
pub fn oid_put(cas_info: &CasInfo, oid: &[u8; 8], attrs: &[&str], values: &[u8]) -> BytesMut {
    build_message(cas_info, FunctionCode::OidPut, |buf| {
        // OID (8 bytes, length-prefixed)
        buf.put_i32(8);
        buf.put_slice(oid);
        // Attribute count
        write_param_int(attrs.len() as i32, buf);
        // Attribute names
        for attr in attrs {
            write_cubrid_string(attr, buf);
        }
        // Pre-serialized values
        if !values.is_empty() {
            buf.put_slice(values);
        }
    })
}

// ---------------------------------------------------------------------------
// NEXT_RESULT (FC 19)
// ---------------------------------------------------------------------------

/// Build a NEXT_RESULT request to advance to the next result set.
///
/// Used with multi-result queries. Returns the number of affected rows
/// in the next result via the response code.
pub fn next_result(cas_info: &CasInfo, query_handle: i32) -> BytesMut {
    build_message(cas_info, FunctionCode::NextResult, |buf| {
        write_param_int(query_handle, buf);
    })
}

// ---------------------------------------------------------------------------
// CURSOR_UPDATE (FC 22)
// ---------------------------------------------------------------------------

/// Build a CURSOR_UPDATE request to update a row at the given cursor position.
///
/// The `values` parameter contains pre-serialized bind parameter data
/// for each column to update (type code + value per column).
pub fn cursor_update(
    cas_info: &CasInfo,
    query_handle: i32,
    cursor_pos: i32,
    values: &[u8],
) -> BytesMut {
    build_message(cas_info, FunctionCode::CursorUpdate, |buf| {
        write_param_int(query_handle, buf);
        write_param_int(cursor_pos, buf);
        if !values.is_empty() {
            buf.put_slice(values);
        }
    })
}

// ---------------------------------------------------------------------------
// GET_GENERATED_KEYS (FC 34)
// ---------------------------------------------------------------------------

/// Build a GET_GENERATED_KEYS request to retrieve auto-generated keys
/// from the last INSERT statement.
pub fn get_generated_keys(cas_info: &CasInfo, query_handle: i32) -> BytesMut {
    build_message(cas_info, FunctionCode::GetGeneratedKeys, |buf| {
        write_param_int(query_handle, buf);
    })
}

// ---------------------------------------------------------------------------
// XA_PREPARE (FC 28)
// ---------------------------------------------------------------------------

/// Build an XA_PREPARE request for the first phase of two-phase commit.
pub fn xa_prepare(cas_info: &CasInfo, xid: &Xid) -> BytesMut {
    build_message(cas_info, FunctionCode::XaPrepare, |buf| {
        let xid_data = xid.encode();
        buf.put_i32(xid_data.len() as i32);
        buf.put_slice(&xid_data);
    })
}

// ---------------------------------------------------------------------------
// XA_RECOVER (FC 29)
// ---------------------------------------------------------------------------

/// Build an XA_RECOVER request to list in-doubt (prepared) XA transactions.
pub fn xa_recover(cas_info: &CasInfo) -> BytesMut {
    build_message(cas_info, FunctionCode::XaRecover, |_buf| {
        // No additional parameters.
    })
}

// ---------------------------------------------------------------------------
// XA_END_TRAN (FC 30)
// ---------------------------------------------------------------------------

/// Build an XA_END_TRAN request to commit or rollback a prepared XA
/// transaction (second phase of 2PC).
pub fn xa_end_tran(cas_info: &CasInfo, xid: &Xid, op: XaOp) -> BytesMut {
    build_message(cas_info, FunctionCode::XaEndTran, |buf| {
        let xid_data = xid.encode();
        buf.put_i32(xid_data.len() as i32);
        buf.put_slice(&xid_data);
        write_param_byte(op as u8, buf);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PrepareFlag;

    /// Default CAS info for testing.
    fn test_cas_info() -> CasInfo {
        CasInfo::from_bytes([0x01, 0x02, 0x03, 0x04])
    }

    /// Verify the common header structure of a message.
    fn verify_header(buf: &[u8], expected_func: FunctionCode, cas_info: &CasInfo) {
        // First 4 bytes: payload length
        let payload_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        // Payload = total - length_field(4) - cas_info(4)
        let expected_payload = buf.len() as i32 - 8;
        assert_eq!(
            payload_len, expected_payload,
            "payload length mismatch: header says {}, actual is {}",
            payload_len, expected_payload
        );

        // Bytes 4-7: CAS info
        assert_eq!(&buf[4..8], cas_info.as_bytes());

        // Byte 8: function code
        assert_eq!(buf[8], expected_func as u8);
    }

    // -----------------------------------------------------------------------
    // PREPARE
    // -----------------------------------------------------------------------

    #[test]
    fn test_prepare_basic() {
        let ci = test_cas_info();
        let buf = prepare(&ci, "SELECT 1", 0x00, true);

        verify_header(&buf, FunctionCode::Prepare, &ci);

        // After header (9 bytes), SQL string:
        // length(4) = 9 ("SELECT 1" + null), data(8), null(1)
        let sql_len = i32::from_be_bytes([buf[9], buf[10], buf[11], buf[12]]);
        assert_eq!(sql_len, 9); // "SELECT 1" (8) + null (1)
        assert_eq!(&buf[13..21], b"SELECT 1");
        assert_eq!(buf[21], 0); // null terminator

        // Prepare flag: length(4)=1, value(1)=0x00
        let flag_offset = 22;
        assert_eq!(
            i32::from_be_bytes([
                buf[flag_offset],
                buf[flag_offset + 1],
                buf[flag_offset + 2],
                buf[flag_offset + 3]
            ]),
            1
        );
        assert_eq!(buf[flag_offset + 4], 0x00);

        // Auto-commit: length(4)=1, value(1)=1
        let ac_offset = flag_offset + 5;
        assert_eq!(buf[ac_offset + 4], 1); // auto_commit = true
    }

    #[test]
    fn test_prepare_with_flags() {
        let ci = test_cas_info();
        let flag = PrepareFlag::HOLDABLE.union(PrepareFlag::INCLUDE_OID);
        let buf = prepare(&ci, "INSERT INTO t VALUES (?)", flag.as_raw(), false);

        verify_header(&buf, FunctionCode::Prepare, &ci);

        // Verify prepare flag value
        let sql_str = "INSERT INTO t VALUES (?)";
        let flag_offset = 9 + 4 + sql_str.len() + 1; // header + len + sql + null
        assert_eq!(buf[flag_offset + 4], 0x09); // HOLDABLE(0x08) | INCLUDE_OID(0x01)

        // Auto-commit = false
        let ac_offset = flag_offset + 5;
        assert_eq!(buf[ac_offset + 4], 0);
    }

    // -----------------------------------------------------------------------
    // EXECUTE
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_select() {
        let ci = test_cas_info();
        let buf = execute(&ci, 42, 0x00, true, &[], true);

        verify_header(&buf, FunctionCode::Execute, &ci);

        // Query handle
        let offset = 9; // after header
        assert_eq!(
            i32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]),
            4 // length prefix
        );
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7]
            ]),
            42 // query handle value
        );
    }

    #[test]
    fn test_execute_with_params() {
        let ci = test_cas_info();
        // Pre-encoded parameter: type byte + value
        let params = vec![0x08, 0x00, 0x00, 0x00, 0x07]; // INT type, value = 7
        let buf = execute(&ci, 1, 0x02, false, &params, false);

        verify_header(&buf, FunctionCode::Execute, &ci);
        // The params should appear at the end of the message
        assert!(buf.len() > params.len());
        assert_eq!(&buf[buf.len() - params.len()..], &params);
    }

    #[test]
    fn test_execute_fetch_flag() {
        let ci = test_cas_info();

        // SELECT: fetch flag = 1
        let buf_select = execute(&ci, 1, 0x00, false, &[], true);
        // Non-SELECT: fetch flag = 0
        let buf_insert = execute(&ci, 1, 0x00, false, &[], false);

        // Fetch flag is at a fixed position after header + handle + exec_flag +
        // max_col + max_row + null
        // header(9) + handle(8) + exec_flag(5) + max_col(8) + max_row(8) + null(4) = 42
        let fetch_flag_offset = 42;
        // length prefix (4 bytes) + value (1 byte)
        assert_eq!(buf_select[fetch_flag_offset + 4], 1); // SELECT
        assert_eq!(buf_insert[fetch_flag_offset + 4], 0); // non-SELECT
    }

    // -----------------------------------------------------------------------
    // H9: EXECUTE with query timeout
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_with_timeout_zero() {
        let ci = test_cas_info();
        // Default execute uses timeout 0
        let buf = execute(&ci, 1, 0x00, false, &[], false);
        verify_header(&buf, FunctionCode::Execute, &ci);

        // Query timeout is the last param before bind params.
        // header(9) + handle(8) + exec_flag(5) + max_col(8) + max_row(8) +
        // null(4) + fetch_flag(5) + auto_commit(5) + fwd_cursor(5) +
        // cache_time(12) = 69
        let timeout_offset = 69;
        let timeout_val = i32::from_be_bytes([
            buf[timeout_offset + 4],
            buf[timeout_offset + 5],
            buf[timeout_offset + 6],
            buf[timeout_offset + 7],
        ]);
        assert_eq!(timeout_val, 0);
    }

    #[test]
    fn test_execute_with_timeout_nonzero() {
        let ci = test_cas_info();
        let buf = execute_with_timeout(&ci, 1, 0x00, false, &[], false, 5000);
        verify_header(&buf, FunctionCode::Execute, &ci);

        // Same offset as above
        let timeout_offset = 69;
        let timeout_val = i32::from_be_bytes([
            buf[timeout_offset + 4],
            buf[timeout_offset + 5],
            buf[timeout_offset + 6],
            buf[timeout_offset + 7],
        ]);
        assert_eq!(timeout_val, 5000);
    }

    // -----------------------------------------------------------------------
    // FETCH
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_basic() {
        let ci = test_cas_info();
        let buf = fetch(&ci, 10, 1, 100);

        verify_header(&buf, FunctionCode::Fetch, &ci);

        let offset = 9;
        // Query handle = 10
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7]
            ]),
            10
        );
        // Start position = 1
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 12],
                buf[offset + 13],
                buf[offset + 14],
                buf[offset + 15]
            ]),
            1
        );
        // Fetch size = 100
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 20],
                buf[offset + 21],
                buf[offset + 22],
                buf[offset + 23]
            ]),
            100
        );
    }

    #[test]
    fn test_fetch_different_positions() {
        let ci = test_cas_info();
        let buf = fetch(&ci, 5, 101, 50);

        // start_position should be 101
        let offset = 9 + 8; // after header + handle param
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7]
            ]),
            101
        );
    }

    // -----------------------------------------------------------------------
    // END_TRAN
    // -----------------------------------------------------------------------

    #[test]
    fn test_end_tran_commit() {
        let ci = test_cas_info();
        let buf = end_tran(&ci, TransactionOp::Commit);

        verify_header(&buf, FunctionCode::EndTran, &ci);

        // Commit flag: length(4)=1, value(1)=1
        assert_eq!(buf[9 + 4], 1); // Commit = 1
    }

    #[test]
    fn test_end_tran_rollback() {
        let ci = test_cas_info();
        let buf = end_tran(&ci, TransactionOp::Rollback);

        verify_header(&buf, FunctionCode::EndTran, &ci);

        // Rollback flag: length(4)=1, value(1)=2
        assert_eq!(buf[9 + 4], 2); // Rollback = 2
    }

    // -----------------------------------------------------------------------
    // CON_CLOSE
    // -----------------------------------------------------------------------

    #[test]
    fn test_con_close() {
        let ci = test_cas_info();
        let buf = con_close(&ci);

        verify_header(&buf, FunctionCode::ConClose, &ci);

        // No additional parameters: total = header(9) only
        assert_eq!(buf.len(), 9);
    }

    // -----------------------------------------------------------------------
    // GET_DB_VERSION
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_db_version() {
        let ci = test_cas_info();
        let buf = get_db_version(&ci, true);

        verify_header(&buf, FunctionCode::GetDbVersion, &ci);

        // Auto-commit param: length(4)=1, value(1)=1
        assert_eq!(buf[9 + 4], 1);
    }

    // -----------------------------------------------------------------------
    // CLOSE_REQ_HANDLE
    // -----------------------------------------------------------------------

    #[test]
    fn test_close_req_handle() {
        let ci = test_cas_info();
        let buf = close_req_handle(&ci, 7, false);

        verify_header(&buf, FunctionCode::CloseReqHandle, &ci);

        // Query handle = 7
        assert_eq!(
            i32::from_be_bytes([buf[9 + 4], buf[9 + 5], buf[9 + 6], buf[9 + 7]]),
            7
        );
        // Auto-commit = false
        assert_eq!(buf[9 + 8 + 4], 0);
    }

    // -----------------------------------------------------------------------
    // SCHEMA_INFO
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_info_primary_key() {
        let ci = test_cas_info();
        let buf = schema_info(&ci, SchemaType::PrimaryKey, "my_table", "", 0);

        verify_header(&buf, FunctionCode::SchemaInfo, &ci);

        // Schema type = PrimaryKey (16)
        assert_eq!(
            i32::from_be_bytes([buf[9 + 4], buf[9 + 5], buf[9 + 6], buf[9 + 7]]),
            16
        );
    }

    // -----------------------------------------------------------------------
    // SET_DB_PARAMETER / GET_DB_PARAMETER
    // -----------------------------------------------------------------------

    #[test]
    fn test_set_db_parameter() {
        let ci = test_cas_info();
        let buf = set_db_parameter(&ci, 4, 1); // AutoCommit = on

        verify_header(&buf, FunctionCode::SetDbParameter, &ci);

        // param_id = 4
        assert_eq!(
            i32::from_be_bytes([buf[9 + 4], buf[9 + 5], buf[9 + 6], buf[9 + 7]]),
            4
        );
        // param_value = 1
        assert_eq!(
            i32::from_be_bytes([buf[9 + 12], buf[9 + 13], buf[9 + 14], buf[9 + 15]]),
            1
        );
    }

    #[test]
    fn test_get_db_parameter() {
        let ci = test_cas_info();
        let buf = get_db_parameter(&ci, 1); // IsolationLevel

        verify_header(&buf, FunctionCode::GetDbParameter, &ci);

        assert_eq!(
            i32::from_be_bytes([buf[9 + 4], buf[9 + 5], buf[9 + 6], buf[9 + 7]]),
            1
        );
    }

    // -----------------------------------------------------------------------
    // END_SESSION
    // -----------------------------------------------------------------------

    #[test]
    fn test_end_session() {
        let ci = test_cas_info();
        let buf = end_session(&ci);

        verify_header(&buf, FunctionCode::EndSession, &ci);
        assert_eq!(buf.len(), 9);
    }

    // -----------------------------------------------------------------------
    // PREPARE_AND_EXECUTE
    // -----------------------------------------------------------------------

    #[test]
    fn test_prepare_and_execute() {
        let ci = test_cas_info();
        let buf = prepare_and_execute(&ci, "SELECT 1", 0x00, true, 0x02, true, 0, &[]);

        verify_header(&buf, FunctionCode::PrepareAndExecute, &ci);

        // argv[0]: prepare_argc_count — length prefix (4) + value (3)
        let argc_len = i32::from_be_bytes([buf[9], buf[10], buf[11], buf[12]]);
        assert_eq!(argc_len, 4); // length prefix = 4 bytes
        let argc_val = i32::from_be_bytes([buf[13], buf[14], buf[15], buf[16]]);
        assert_eq!(argc_val, 3); // 3 prepare args: sql + flag + auto_commit

        // argv[1]: SQL string should follow
        let sql_len = i32::from_be_bytes([buf[17], buf[18], buf[19], buf[20]]);
        assert_eq!(sql_len, 9); // "SELECT 1" + null
    }

    // -----------------------------------------------------------------------
    // EXECUTE_BATCH
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_batch() {
        let ci = test_cas_info();
        let sqls = &["INSERT INTO t1 VALUES (1)", "INSERT INTO t1 VALUES (2)"];
        let buf = execute_batch(&ci, sqls, true, 0);

        verify_header(&buf, FunctionCode::ExecuteBatch, &ci);

        // Auto-commit flag should be first param
        assert_eq!(buf[9 + 4], 1); // auto_commit = true
    }

    // -----------------------------------------------------------------------
    // SAVEPOINT
    // -----------------------------------------------------------------------

    #[test]
    fn test_savepoint_create() {
        let ci = test_cas_info();
        let buf = savepoint(&ci, 1, "sp1");

        verify_header(&buf, FunctionCode::Savepoint, &ci);

        // Op = 1 (create)
        assert_eq!(buf[9 + 4], 1);
    }

    // -----------------------------------------------------------------------
    // GET_ROW_COUNT
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_row_count() {
        let ci = test_cas_info();
        let buf = get_row_count(&ci);

        verify_header(&buf, FunctionCode::GetRowCount, &ci);
        assert_eq!(buf.len(), 9);
    }

    // -----------------------------------------------------------------------
    // GET_LAST_INSERT_ID
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_last_insert_id() {
        let ci = test_cas_info();
        let buf = get_last_insert_id(&ci);

        verify_header(&buf, FunctionCode::GetLastInsertId, &ci);
        assert_eq!(buf.len(), 9);
    }

    // -----------------------------------------------------------------------
    // CURSOR_CLOSE
    // -----------------------------------------------------------------------

    #[test]
    fn test_cursor_close() {
        let ci = test_cas_info();
        let buf = cursor_close(&ci, 99);

        verify_header(&buf, FunctionCode::CursorClose, &ci);

        assert_eq!(
            i32::from_be_bytes([buf[9 + 4], buf[9 + 5], buf[9 + 6], buf[9 + 7]]),
            99
        );
    }

    // -----------------------------------------------------------------------
    // H6: LOB operations
    // -----------------------------------------------------------------------

    #[test]
    fn test_lob_new_blob() {
        let ci = test_cas_info();
        let buf = lob_new(&ci, 23); // BLOB type code

        verify_header(&buf, FunctionCode::LobNew, &ci);

        // LOB type param: length(4)=4, value(4)=23
        let offset = 9;
        assert_eq!(
            i32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3]
            ]),
            4
        );
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7]
            ]),
            23
        );
    }

    #[test]
    fn test_lob_new_clob() {
        let ci = test_cas_info();
        let buf = lob_new(&ci, 24); // CLOB type code

        verify_header(&buf, FunctionCode::LobNew, &ci);

        let offset = 9;
        assert_eq!(
            i32::from_be_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7]
            ]),
            24
        );
    }

    #[test]
    fn test_lob_write_message_structure() {
        let ci = test_cas_info();
        let handle = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let data = b"hello LOB";
        let buf = lob_write(&ci, &handle, 0, data);

        verify_header(&buf, FunctionCode::LobWrite, &ci);

        // After header(9): handle_len(4) + handle(8) + offset_len(4) + offset(8) +
        // data_len(4) + data(9)
        let offset = 9;
        // Handle length
        let handle_len = i32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        assert_eq!(handle_len, 8);
        // Handle bytes
        assert_eq!(&buf[offset + 4..offset + 12], &handle);
    }

    #[test]
    fn test_lob_write_with_offset() {
        let ci = test_cas_info();
        let handle = [0u8; 16];
        let data = b"data";
        let buf = lob_write(&ci, &handle, 1024, data);

        verify_header(&buf, FunctionCode::LobWrite, &ci);

        // After header(9) + handle_len(4) + handle(16) = 29
        // offset param: length(4)=8, value(8)=1024
        let off_pos = 9 + 4 + 16;
        let off_len = i32::from_be_bytes([
            buf[off_pos],
            buf[off_pos + 1],
            buf[off_pos + 2],
            buf[off_pos + 3],
        ]);
        assert_eq!(off_len, 8);
        let off_val = i64::from_be_bytes([
            buf[off_pos + 4],
            buf[off_pos + 5],
            buf[off_pos + 6],
            buf[off_pos + 7],
            buf[off_pos + 8],
            buf[off_pos + 9],
            buf[off_pos + 10],
            buf[off_pos + 11],
        ]);
        assert_eq!(off_val, 1024);
    }

    #[test]
    fn test_lob_read_message_structure() {
        let ci = test_cas_info();
        let handle = [0xAA; 12];
        let buf = lob_read(&ci, &handle, 256, 4096);

        verify_header(&buf, FunctionCode::LobRead, &ci);

        // After header(9): handle_len(4) + handle(12) = 25
        let offset = 9;
        let handle_len = i32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        assert_eq!(handle_len, 12);

        // Offset param at 9 + 4 + 12 = 25
        let off_pos = 25;
        let off_val = i64::from_be_bytes([
            buf[off_pos + 4],
            buf[off_pos + 5],
            buf[off_pos + 6],
            buf[off_pos + 7],
            buf[off_pos + 8],
            buf[off_pos + 9],
            buf[off_pos + 10],
            buf[off_pos + 11],
        ]);
        assert_eq!(off_val, 256);

        // Length param at 25 + 12 = 37
        let len_pos = 37;
        let read_len = i32::from_be_bytes([
            buf[len_pos + 4],
            buf[len_pos + 5],
            buf[len_pos + 6],
            buf[len_pos + 7],
        ]);
        assert_eq!(read_len, 4096);
    }

    // -----------------------------------------------------------------------
    // H7: OID operations
    // -----------------------------------------------------------------------

    #[test]
    fn test_oid_get_message_structure() {
        let ci = test_cas_info();
        let oid: [u8; 8] = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let buf = oid_get(&ci, &oid, &["name", "age"]);

        verify_header(&buf, FunctionCode::OidGet, &ci);

        // After header(9): oid_len(4)=8 + oid(8) = 21
        let offset = 9;
        let oid_len = i32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        assert_eq!(oid_len, 8);
        assert_eq!(&buf[offset + 4..offset + 12], &oid);

        // Attributes string: "name,age\0"
        let attrs_offset = offset + 12;
        let attrs_len = i32::from_be_bytes([
            buf[attrs_offset],
            buf[attrs_offset + 1],
            buf[attrs_offset + 2],
            buf[attrs_offset + 3],
        ]);
        assert_eq!(attrs_len, 9); // "name,age" (8) + null (1)
    }

    #[test]
    fn test_oid_get_empty_attrs() {
        let ci = test_cas_info();
        let oid = [0u8; 8];
        let buf = oid_get(&ci, &oid, &[]);

        verify_header(&buf, FunctionCode::OidGet, &ci);

        // Empty attrs -> empty string ""
        let attrs_offset = 9 + 4 + 8;
        let attrs_len = i32::from_be_bytes([
            buf[attrs_offset],
            buf[attrs_offset + 1],
            buf[attrs_offset + 2],
            buf[attrs_offset + 3],
        ]);
        assert_eq!(attrs_len, 1); // just null terminator
    }

    #[test]
    fn test_oid_put_message_structure() {
        let ci = test_cas_info();
        let oid: [u8; 8] = [0x10; 8];
        let values = vec![0x01, 0x02, 0x03]; // dummy pre-serialized values
        let buf = oid_put(&ci, &oid, &["col1"], &values);

        verify_header(&buf, FunctionCode::OidPut, &ci);

        // After header(9): oid_len(4)=8 + oid(8) = 21
        let offset = 9;
        assert_eq!(
            i32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3]
            ]),
            8
        );
        assert_eq!(&buf[offset + 4..offset + 12], &[0x10; 8]);

        // Attr count param: length(4)=4, value(4)=1
        let count_offset = offset + 12;
        let count = i32::from_be_bytes([
            buf[count_offset + 4],
            buf[count_offset + 5],
            buf[count_offset + 6],
            buf[count_offset + 7],
        ]);
        assert_eq!(count, 1);

        // Values should appear at the end
        assert_eq!(&buf[buf.len() - 3..], &values);
    }

    // -----------------------------------------------------------------------
    // Header validation across all message types
    // -----------------------------------------------------------------------

    #[test]
    fn test_all_messages_have_valid_headers() {
        let ci = test_cas_info();
        let oid = [0u8; 8];

        // Collect all message builders and their expected function codes
        let messages: Vec<(BytesMut, FunctionCode)> = vec![
            (prepare(&ci, "SQL", 0, false), FunctionCode::Prepare),
            (
                execute(&ci, 1, 0, false, &[], false),
                FunctionCode::Execute,
            ),
            (fetch(&ci, 1, 1, 100), FunctionCode::Fetch),
            (
                end_tran(&ci, TransactionOp::Commit),
                FunctionCode::EndTran,
            ),
            (con_close(&ci), FunctionCode::ConClose),
            (get_db_version(&ci, false), FunctionCode::GetDbVersion),
            (
                close_req_handle(&ci, 1, false),
                FunctionCode::CloseReqHandle,
            ),
            (
                schema_info(&ci, SchemaType::Class, "", "", 0),
                FunctionCode::SchemaInfo,
            ),
            (
                set_db_parameter(&ci, 1, 1),
                FunctionCode::SetDbParameter,
            ),
            (get_db_parameter(&ci, 1), FunctionCode::GetDbParameter),
            (end_session(&ci), FunctionCode::EndSession),
            (
                prepare_and_execute(&ci, "SQL", 0, false, 0, false, 0, &[]),
                FunctionCode::PrepareAndExecute,
            ),
            (
                execute_batch(&ci, &["SQL"], false, 0),
                FunctionCode::ExecuteBatch,
            ),
            (savepoint(&ci, 1, "sp"), FunctionCode::Savepoint),
            (get_row_count(&ci), FunctionCode::GetRowCount),
            (get_last_insert_id(&ci), FunctionCode::GetLastInsertId),
            (cursor_close(&ci, 1), FunctionCode::CursorClose),
            // H6: LOB operations
            (lob_new(&ci, 23), FunctionCode::LobNew),
            (lob_write(&ci, &[0u8; 8], 0, b"data"), FunctionCode::LobWrite),
            (lob_read(&ci, &[0u8; 8], 0, 100), FunctionCode::LobRead),
            // H7: OID operations
            (oid_get(&ci, &oid, &["a"]), FunctionCode::OidGet),
            (oid_put(&ci, &oid, &["a"], &[]), FunctionCode::OidPut),
            // New Group B builders
            (next_result(&ci, 1), FunctionCode::NextResult),
            (
                cursor_update(&ci, 1, 1, &[]),
                FunctionCode::CursorUpdate,
            ),
            (get_generated_keys(&ci, 1), FunctionCode::GetGeneratedKeys),
            (
                xa_prepare(&ci, &Xid::new(1, b"g".to_vec(), b"b".to_vec())),
                FunctionCode::XaPrepare,
            ),
            (xa_recover(&ci), FunctionCode::XaRecover),
            (
                xa_end_tran(&ci, &Xid::new(1, b"g".to_vec(), b"b".to_vec()), XaOp::Commit),
                FunctionCode::XaEndTran,
            ),
        ];

        for (buf, expected_func) in &messages {
            verify_header(buf, *expected_func, &ci);
        }
    }

    // -----------------------------------------------------------------------
    // C3: patch_payload_length must not clone the buffer
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_message_returns_same_buffer_no_clone() {
        // After the fix, build_message works in-place on a single BytesMut.
        // Verify the result has the correct payload length patched in.
        let ci = test_cas_info();
        let buf = build_message(&ci, FunctionCode::ConClose, |_buf| {
            // No body
        });
        // Total = 9 bytes (header). Payload = 9 - 4 (len) - 4 (cas_info) = 1
        let payload_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(payload_len, 1);
    }

    #[test]
    fn test_patch_payload_length_in_place() {
        // Ensure large messages also get correct in-place patching.
        let ci = test_cas_info();
        let long_sql = "SELECT ".to_string() + &"x".repeat(10000);
        let buf = prepare(&ci, &long_sql, 0x00, false);
        verify_header(&buf, FunctionCode::Prepare, &ci);
    }
}
