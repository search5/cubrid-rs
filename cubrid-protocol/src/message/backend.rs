//! Server-to-client response message parsing.
//!
//! Unlike PostgreSQL, CUBRID does not tag response messages with a type byte.
//! Instead, the client knows which response format to expect based on the
//! request it sent. This module provides individual parsing functions for
//! each response type.
//!
//! # Response structure
//!
//! Every CAS response follows this layout:
//!
//! ```text
//! [4 bytes] Response length (big-endian)
//! [4 bytes] CAS info (updated session state)
//! [4 bytes] Response code (>= 0 success, < 0 error)
//! [N bytes] Function-specific payload
//! ```

use bytes::{Buf, Bytes};

use crate::cas_info::CasInfo;
use crate::error::{Error, ErrorIndicator};
use crate::types::{CubridDataType, StatementType};
use crate::{NET_SIZE_CAS_INFO, NET_SIZE_INT};

// ---------------------------------------------------------------------------
// ResponseFrame
// ---------------------------------------------------------------------------

/// A parsed response frame containing the common header fields.
///
/// This is the first stage of response parsing: extracting the CAS info
/// and response code from the raw bytes. The remaining payload is kept
/// as raw [`Bytes`] for function-specific parsing.
#[derive(Debug, Clone)]
pub struct ResponseFrame {
    /// Updated CAS info to use for the next request.
    pub cas_info: CasInfo,
    /// Response code: >= 0 for success (often a handle ID or count),
    /// < 0 for errors.
    pub response_code: i32,
    /// The remaining payload after the response code.
    pub payload: Bytes,
}

impl ResponseFrame {
    /// Parse a response frame from raw bytes.
    ///
    /// The input should be the payload after the 4-byte length prefix has
    /// been stripped by the codec. It must contain at least 8 bytes
    /// (CAS info + response code).
    pub fn parse(mut data: Bytes) -> Result<Self, Error> {
        // Need at least cas_info(4) + response_code(4)
        if data.len() < NET_SIZE_CAS_INFO + NET_SIZE_INT {
            return Err(Error::InvalidMessage(format!(
                "response frame too short: {} bytes (need at least {})",
                data.len(),
                NET_SIZE_CAS_INFO + NET_SIZE_INT
            )));
        }

        // Read CAS info (4 bytes)
        let mut cas_info_bytes = [0u8; 4];
        cas_info_bytes.copy_from_slice(&data[..4]);
        data.advance(4);
        let cas_info = CasInfo::from_bytes(cas_info_bytes);

        // Read response code (4 bytes)
        let response_code = (&data[..4]).get_i32();
        data.advance(4);

        Ok(ResponseFrame {
            cas_info,
            response_code,
            payload: data,
        })
    }

    /// Returns `true` if this response indicates an error.
    pub fn is_error(&self) -> bool {
        self.response_code < 0
    }

    /// Parse the payload as a CAS/DBMS error.
    ///
    /// Should only be called when [`is_error`](Self::is_error) returns `true`.
    /// The error payload format is:
    ///
    /// ```text
    /// [4 bytes] Error indicator (-1 = CAS, -2 = DBMS)
    /// [4 bytes] Error code
    /// [N bytes] Error message (null-terminated)
    /// ```
    /// Parse the error from this response frame.
    ///
    /// Supports two error formats:
    ///
    /// - **Renewed error code** (`BROKER_RENEWED_ERROR_CODE` flag): the
    ///   `response_code` itself carries the error indicator (-1 = CAS,
    ///   -2 = DBMS). The payload starts directly with `error_code` (4 bytes)
    ///   + null-terminated message.
    ///
    /// - **Legacy format**: the payload starts with an `error_indicator`
    ///   (4 bytes), then `error_code` (4 bytes), then the message.
    ///
    /// The driver always requests `BROKER_RENEWED_ERROR_CODE` during
    /// handshake, so the renewed format is the expected path.
    pub fn parse_error(&self) -> Error {
        let mut cursor = &self.payload[..];

        // When BROKER_RENEWED_ERROR_CODE is active, response_code is the
        // error indicator: -1 = CAS, -2 = DBMS.
        let indicator = ErrorIndicator::from_wire(self.response_code);

        if indicator.is_some() {
            // Renewed format: payload = [4] error_code + [null-terminated] message
            let error_code = if cursor.remaining() >= 4 {
                cursor.get_i32()
            } else {
                // Payload too short for error_code; use 0 (unknown).
                // Don't use response_code here — it's the indicator (-1/-2),
                // not an error code.
                0
            };

            let msg_bytes: Vec<u8> = cursor.iter().take_while(|&&b| b != 0).copied().collect();
            let message = String::from_utf8_lossy(&msg_bytes).to_string();

            match indicator {
                Some(ErrorIndicator::Dbms) => Error::Dbms {
                    code: error_code,
                    message,
                },
                _ => Error::Cas {
                    code: error_code,
                    message,
                },
            }
        } else {
            // Legacy format: payload = [4] indicator + [4] error_code + message
            if cursor.remaining() < 4 {
                return Error::Cas {
                    code: self.response_code,
                    message: String::new(),
                };
            }

            let indicator_val = cursor.get_i32();
            let legacy_indicator = ErrorIndicator::from_wire(indicator_val);

            let error_code = if cursor.remaining() >= 4 {
                cursor.get_i32()
            } else {
                self.response_code
            };

            let msg_bytes: Vec<u8> = cursor.iter().take_while(|&&b| b != 0).copied().collect();
            let message = String::from_utf8_lossy(&msg_bytes).to_string();

            match legacy_indicator {
                Some(ErrorIndicator::Dbms) => Error::Dbms {
                    code: error_code,
                    message,
                },
                _ => Error::Cas {
                    code: error_code,
                    message,
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Column metadata
// ---------------------------------------------------------------------------

/// Metadata for a single column in a result set.
///
/// This information is returned by PREPARE and describes the structure
/// of each column in the query result.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// The CUBRID data type of this column.
    pub column_type: CubridDataType,
    /// For collection types (SET, MULTISET, SEQUENCE), the element type.
    pub collection_element_type: Option<CubridDataType>,
    /// Decimal scale (number of digits after the decimal point).
    pub scale: i16,
    /// Numeric precision (total number of digits).
    pub precision: i32,
    /// Column name (may be an alias from the query).
    pub name: String,
    /// Real column name as defined in the table schema.
    pub real_name: String,
    /// Name of the table this column belongs to.
    pub table_name: String,
    /// Whether this column can contain NULL values.
    pub is_nullable: bool,
    /// Default value expression, if any.
    pub default_value: Option<String>,
    /// Whether this column is auto-incremented.
    pub is_auto_increment: bool,
    /// Whether this column has a unique constraint.
    pub is_unique_key: bool,
    /// Whether this column is part of the primary key.
    pub is_primary_key: bool,
    /// Whether this column has a reverse index.
    pub is_reverse_index: bool,
    /// Whether this column has a reverse unique constraint.
    pub is_reverse_unique: bool,
    /// Whether this column has a foreign key constraint.
    pub is_foreign_key: bool,
    /// Whether this column is shared.
    pub is_shared: bool,
}

/// Parse a null-terminated string from a cursor, consuming the length prefix.
///
/// Format: `[4-byte length (includes null)] [string bytes] [0x00]`
/// Read a length-prefixed, null-terminated string from the cursor.
///
/// The wire format is `[4-byte length (including null)][UTF-8 bytes][0x00]`.
///
/// # Note on embedded null bytes (M9)
///
/// The returned string may contain embedded null bytes if the server sends
/// binary data in a string-typed column. Only the trailing null terminator
/// is stripped; interior null bytes are preserved via `from_utf8_lossy`.
/// Callers should be aware of this when processing string values from
/// columns that may hold binary content.
fn read_string(cursor: &mut &[u8]) -> Result<String, Error> {
    if cursor.remaining() < NET_SIZE_INT {
        return Err(Error::InvalidMessage(
            "truncated string length".to_string(),
        ));
    }
    let len_i32 = cursor.get_i32();
    if len_i32 < 0 {
        return Err(Error::InvalidMessage(format!(
            "negative string length: {}",
            len_i32
        )));
    }
    let len = len_i32 as usize;
    if len == 0 {
        return Ok(String::new());
    }
    if cursor.remaining() < len {
        return Err(Error::InvalidMessage(format!(
            "truncated string data: need {} bytes, have {}",
            len,
            cursor.remaining()
        )));
    }
    // Read string bytes (excluding null terminator).
    // len > 0 is guaranteed here because len == 0 returns early above.
    let str_len = len - 1;
    let s = String::from_utf8_lossy(&cursor[..str_len]).to_string();
    cursor.advance(len); // advance past string + null
    Ok(s)
}

/// Parse column metadata entries from a cursor.
///
/// Reads `column_count` column metadata structures from the byte stream.
///
/// # Parameters
///
/// - `cursor`: Byte stream positioned at the first column metadata entry
/// - `column_count`: Number of column metadata entries to parse
/// - `protocol_version`: Negotiated wire protocol version. The 0x80 bit
///   collection type encoding in the legacy type byte is only used for
///   PROTOCOL_V7 and above. For V6 and below, the byte is the raw type code
///   with no collection encoding.
pub fn parse_column_metadata(
    cursor: &mut &[u8],
    column_count: i32,
    protocol_version: u8,
) -> Result<Vec<ColumnMetadata>, Error> {
    if column_count < 0 {
        return Err(Error::InvalidMessage(format!(
            "negative column count: {}",
            column_count
        )));
    }
    let mut columns = Vec::with_capacity(column_count as usize);

    // The 0x80 bit encoding for collection types was introduced in
    // PROTOCOL_V7. For earlier versions, the legacy type byte is
    // the raw CubridDataType code without any collection encoding.
    let supports_collection_encoding = protocol_version >= crate::PROTOCOL_V7;

    for _ in 0..column_count {
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated column metadata".to_string(),
            ));
        }

        // Read column type byte
        let legacy_type = cursor.get_u8();
        let (column_type, collection_element_type) =
            if supports_collection_encoding && legacy_type & 0x80 != 0 {
                // High bit set: this is a collection type (V7+ only).
                // The actual element type follows in the next byte.
                if cursor.remaining() < 1 {
                    return Err(Error::InvalidMessage(
                        "truncated collection element type".to_string(),
                    ));
                }
                let element_type_byte = cursor.get_u8();
                let element_type = CubridDataType::try_from(element_type_byte)?;

                // Determine collection type from bits 5-6.
                // When bits 5-6 are 0x00, the high bit simply indicates
                // that an element type byte follows, but the column itself
                // is a non-collection type — use the element type directly.
                let collection_code = legacy_type & 0x60;
                match collection_code {
                    0x20 => (CubridDataType::Set, Some(element_type)),
                    0x40 => (CubridDataType::MultiSet, Some(element_type)),
                    0x60 => (CubridDataType::Sequence, Some(element_type)),
                    _ => (element_type, None),
                }
            } else {
                let col_type = CubridDataType::try_from(legacy_type)?;
                (col_type, None)
            };

        // Scale (2 bytes)
        if cursor.remaining() < 2 {
            return Err(Error::InvalidMessage("truncated scale".to_string()));
        }
        let scale = cursor.get_i16();

        // Precision (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage("truncated precision".to_string()));
        }
        let precision = cursor.get_i32();

        // Column name, real name, table name
        let name = read_string(cursor)?;
        let real_name = read_string(cursor)?;
        let table_name = read_string(cursor)?;

        // Boolean flags (1 byte each)
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated nullable flag".to_string(),
            ));
        }
        let is_nullable = cursor.get_u8() == 1;

        // Default value (as string)
        let default_value_str = read_string(cursor)?;
        let default_value = if default_value_str.is_empty() {
            None
        } else {
            Some(default_value_str)
        };

        // Remaining boolean flags
        if cursor.remaining() < 7 {
            return Err(Error::InvalidMessage(
                "truncated column flags".to_string(),
            ));
        }
        let is_auto_increment = cursor.get_u8() == 1;
        let is_unique_key = cursor.get_u8() == 1;
        let is_primary_key = cursor.get_u8() == 1;
        let is_reverse_index = cursor.get_u8() == 1;
        let is_reverse_unique = cursor.get_u8() == 1;
        let is_foreign_key = cursor.get_u8() == 1;
        let is_shared = cursor.get_u8() == 1;

        columns.push(ColumnMetadata {
            column_type,
            collection_element_type,
            scale,
            precision,
            name,
            real_name,
            table_name,
            is_nullable,
            default_value,
            is_auto_increment,
            is_unique_key,
            is_primary_key,
            is_reverse_index,
            is_reverse_unique,
            is_foreign_key,
            is_shared,
        });
    }

    Ok(columns)
}

/// Parse minimal column metadata used by SCHEMA_INFO responses.
///
/// Unlike [`parse_column_metadata`] (used by PREPARE), schema info
/// column metadata contains only: type byte(s), scale, precision, and
/// name. It does **not** include `realName`, `tableName`, nullable,
/// `defaultValue`, or the 7 flag bytes.
pub fn parse_schema_column_metadata(
    cursor: &mut &[u8],
    column_count: i32,
    protocol_version: u8,
) -> Result<Vec<ColumnMetadata>, Error> {
    if column_count < 0 {
        return Err(Error::InvalidMessage(format!(
            "negative column count: {}",
            column_count
        )));
    }
    let mut columns = Vec::with_capacity(column_count as usize);
    let supports_collection_encoding = protocol_version >= crate::PROTOCOL_V7;

    for _ in 0..column_count {
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated schema column metadata".to_string(),
            ));
        }

        // Read column type byte (same encoding as parse_column_metadata)
        let legacy_type = cursor.get_u8();
        let (column_type, collection_element_type) =
            if supports_collection_encoding && legacy_type & 0x80 != 0 {
                if cursor.remaining() < 1 {
                    return Err(Error::InvalidMessage(
                        "truncated collection element type".to_string(),
                    ));
                }
                let element_type_byte = cursor.get_u8();
                let element_type = CubridDataType::try_from(element_type_byte)?;

                let collection_code = legacy_type & 0x60;
                match collection_code {
                    0x20 => (CubridDataType::Set, Some(element_type)),
                    0x40 => (CubridDataType::MultiSet, Some(element_type)),
                    0x60 => (CubridDataType::Sequence, Some(element_type)),
                    _ => (element_type, None),
                }
            } else {
                let col_type = CubridDataType::try_from(legacy_type)?;
                (col_type, None)
            };

        // Scale (2 bytes)
        if cursor.remaining() < 2 {
            return Err(Error::InvalidMessage("truncated scale".to_string()));
        }
        let scale = cursor.get_i16();

        // Precision (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage("truncated precision".to_string()));
        }
        let precision = cursor.get_i32();

        // Column name (only field — no realName, tableName, or flags)
        let name = read_string(cursor)?;

        columns.push(ColumnMetadata {
            column_type,
            collection_element_type,
            scale,
            precision,
            name: name.clone(),
            real_name: name,
            table_name: String::new(),
            is_nullable: true,
            default_value: None,
            is_auto_increment: false,
            is_unique_key: false,
            is_primary_key: false,
            is_reverse_index: false,
            is_reverse_unique: false,
            is_foreign_key: false,
            is_shared: false,
        });
    }

    Ok(columns)
}

// ---------------------------------------------------------------------------
// PrepareResponse
// ---------------------------------------------------------------------------

/// Parsed response from a PREPARE request.
#[derive(Debug, Clone)]
pub struct PrepareResponse {
    /// The query handle to use with EXECUTE and FETCH.
    pub query_handle: i32,
    /// Server-side cache lifetime for this statement (seconds).
    pub cache_lifetime: i32,
    /// The type of SQL statement (SELECT, INSERT, etc.).
    pub statement_type: StatementType,
    /// Number of bind parameter placeholders (`?`) in the statement.
    pub bind_count: i32,
    /// Whether the result set is updatable.
    pub is_updatable: bool,
    /// Column metadata for the result set.
    pub columns: Vec<ColumnMetadata>,
}

impl PrepareResponse {
    /// Parse a prepare response from a [`ResponseFrame`].
    ///
    /// The `response_code` of a successful prepare is the query handle.
    ///
    /// # Parameters
    ///
    /// - `frame`: The response frame to parse
    /// - `protocol_version`: Negotiated wire protocol version, passed to
    ///   [`parse_column_metadata`] for version-dependent type encoding
    pub fn parse(frame: &ResponseFrame, protocol_version: u8) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        let query_handle = frame.response_code;
        let mut cursor = &frame.payload[..];

        // Cache lifetime (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated cache lifetime".to_string(),
            ));
        }
        let cache_lifetime = cursor.get_i32();

        // Statement type (1 byte)
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated statement type".to_string(),
            ));
        }
        let stmt_type_byte = cursor.get_u8();
        let statement_type = StatementType::try_from(stmt_type_byte)?;

        // Bind count (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated bind count".to_string(),
            ));
        }
        let bind_count = cursor.get_i32();

        // Is updatable (1 byte)
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated updatable flag".to_string(),
            ));
        }
        let is_updatable = cursor.get_u8() == 1;

        // Column count (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated column count".to_string(),
            ));
        }
        let column_count = cursor.get_i32();

        // Column metadata
        let columns = parse_column_metadata(&mut cursor, column_count, protocol_version)?;

        Ok(PrepareResponse {
            query_handle,
            cache_lifetime,
            statement_type,
            bind_count,
            is_updatable,
            columns,
        })
    }
}

// ---------------------------------------------------------------------------
// SchemaInfoResponse
// ---------------------------------------------------------------------------

/// Parsed response from a SCHEMA_INFO request.
///
/// Unlike [`PrepareResponse`], the SCHEMA_INFO response does not include
/// `cache_lifetime`, `statement_type`, `bind_count`, or `is_updatable`.
/// The payload layout is: `num_tuple` (4 bytes) + `column_count` (4 bytes)
/// + column metadata. The query handle is in `response_code`.
#[derive(Debug, Clone)]
pub struct SchemaInfoResponse {
    /// The query handle to use with FETCH for retrieving schema rows.
    pub query_handle: i32,
    /// Total number of result tuples available.
    pub num_tuple: i32,
    /// Column metadata describing the schema result columns.
    pub columns: Vec<ColumnMetadata>,
}

impl SchemaInfoResponse {
    /// Parse a schema info response from a [`ResponseFrame`].
    ///
    /// # Parameters
    ///
    /// - `frame`: The response frame to parse
    /// - `protocol_version`: Negotiated wire protocol version
    pub fn parse(frame: &ResponseFrame, protocol_version: u8) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        let query_handle = frame.response_code;
        let mut cursor = &frame.payload[..];

        // Number of tuples (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated num_tuple in schema info response".to_string(),
            ));
        }
        let num_tuple = cursor.get_i32();
        if num_tuple < 0 {
            return Err(Error::InvalidMessage(format!(
                "negative num_tuple in schema info response: {}",
                num_tuple
            )));
        }

        // Column count (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated column count in schema info response".to_string(),
            ));
        }
        let column_count = cursor.get_i32();

        // Column metadata (minimal format — no realName, tableName, or flags)
        let columns = parse_schema_column_metadata(&mut cursor, column_count, protocol_version)?;

        Ok(SchemaInfoResponse {
            query_handle,
            num_tuple,
            columns,
        })
    }
}

// ---------------------------------------------------------------------------
// ExecuteResponse
// ---------------------------------------------------------------------------

/// Result info for a single result set in an EXECUTE response.
#[derive(Debug, Clone)]
pub struct ResultInfo {
    /// The type of statement that produced this result.
    pub stmt_type: StatementType,
    /// Number of affected rows (for DML) or fetched rows (for SELECT).
    pub result_count: i32,
    /// Object identifier (8 bytes).
    pub oid: [u8; 8],
    /// Cache time in seconds.
    pub cache_time_sec: i32,
    /// Cache time in microseconds.
    pub cache_time_usec: i32,
}

/// A single column value from a fetched row.
///
/// Column values are kept as raw bytes with a type tag. Actual conversion
/// to Rust types happens in the `cubrid-types` crate, maintaining clean
/// layer separation.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    /// The column value is NULL.
    Null,
    /// The column value with its type and raw data.
    Data {
        /// The CUBRID data type of this value.
        data_type: CubridDataType,
        /// Raw bytes of the value (without length prefix).
        data: Bytes,
    },
}

/// A single row (tuple) from a fetched result set.
#[derive(Debug, Clone)]
pub struct Tuple {
    /// Row index within the result set.
    pub index: i32,
    /// Object identifier.
    pub oid: [u8; 8],
    /// Column values in this row.
    pub values: Vec<ColumnValue>,
}

/// Parsed fetch result containing rows from a result set.
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Number of tuples in this batch.
    pub tuple_count: i32,
    /// The parsed rows.
    pub tuples: Vec<Tuple>,
}

/// Parse fetch result tuples from a cursor.
///
/// This is shared between EXECUTE (inline fetch) and FETCH responses.
///
/// # Parameters
///
/// - `cursor`: Byte stream positioned at the start of tuple data
/// - `tuple_count`: Number of tuples to read
/// - `column_count`: Number of columns per tuple
/// - `column_types`: Column type for each column (from prepare metadata).
///   If `col_idx >= column_types.len()`, the type falls back to
///   [`CubridDataType::Null`], which causes the type byte to be read
///   inline from the data. This occurs for 0-column result sets, CALL
///   statements, and EVALUATE statements where column metadata is not
///   available at prepare time.
pub fn parse_tuples(
    cursor: &mut &[u8],
    tuple_count: i32,
    column_count: usize,
    column_types: &[CubridDataType],
) -> Result<Vec<Tuple>, Error> {
    if tuple_count < 0 {
        return Err(Error::InvalidMessage(format!(
            "negative tuple count: {}",
            tuple_count
        )));
    }
    let mut tuples = Vec::with_capacity(tuple_count as usize);

    for _ in 0..tuple_count {
        // Row index (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage("truncated tuple index".to_string()));
        }
        let index = cursor.get_i32();

        // OID (8 bytes)
        if cursor.remaining() < 8 {
            return Err(Error::InvalidMessage("truncated tuple OID".to_string()));
        }
        let mut oid = [0u8; 8];
        oid.copy_from_slice(&cursor[..8]);
        cursor.advance(8);

        // Column values
        let mut values = Vec::with_capacity(column_count);
        for col_idx in 0..column_count {
            if cursor.remaining() < 4 {
                return Err(Error::InvalidMessage(
                    "truncated column value size".to_string(),
                ));
            }
            let size = cursor.get_i32();

            if size <= 0 {
                // NULL value
                values.push(ColumnValue::Null);
            } else {
                let size = size as usize;
                if cursor.remaining() < size {
                    return Err(Error::InvalidMessage(format!(
                        "truncated column value data: need {}, have {}",
                        size,
                        cursor.remaining()
                    )));
                }

                // Determine the type: for CALL/EVALUATE statements or NULL
                // column types, the type byte is embedded in the data
                let col_type = if col_idx < column_types.len() {
                    column_types[col_idx]
                } else {
                    CubridDataType::Null
                };

                let (data_type, data) = if col_type == CubridDataType::Null {
                    // Type is embedded as the first byte of the data.
                    // Need at least 1 byte for the type code.
                    if size < 1 {
                        return Err(Error::InvalidMessage(
                            "embedded type code requires at least 1 byte".to_string(),
                        ));
                    }
                    let type_byte = cursor[0];
                    let dt = CubridDataType::try_from(type_byte)?;
                    cursor.advance(1);
                    let value_len = size - 1;
                    let value_data = Bytes::copy_from_slice(&cursor[..value_len]);
                    cursor.advance(value_len);
                    (dt, value_data)
                } else {
                    let value_data = Bytes::copy_from_slice(&cursor[..size]);
                    cursor.advance(size);
                    (col_type, value_data)
                };

                values.push(ColumnValue::Data { data_type, data });
            }
        }

        tuples.push(Tuple {
            index,
            oid,
            values,
        });
    }

    Ok(tuples)
}

/// Parsed response from an EXECUTE request.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    /// Total number of tuples affected or available.
    pub total_tuple_count: i32,
    /// Whether cached results can be reused.
    pub cache_reusable: bool,
    /// Result info for each result set.
    pub results: Vec<ResultInfo>,
    /// Shard ID (PROTOCOL_V5+, `None` for earlier versions).
    pub shard_id: Option<i32>,
    /// Inline fetch result for SELECT statements.
    pub fetch_result: Option<FetchResult>,
}

impl ExecuteResponse {
    /// Parse an execute response from a [`ResponseFrame`].
    ///
    /// # Parameters
    ///
    /// - `frame`: The response frame to parse
    /// - `protocol_version`: Negotiated protocol version for version-dependent fields
    /// - `column_types`: Column types from the prepare response (needed for tuple parsing)
    pub fn parse(
        frame: &ResponseFrame,
        protocol_version: u8,
        column_types: &[CubridDataType],
    ) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        let total_tuple_count = frame.response_code;
        let mut cursor = &frame.payload[..];

        // Cache reusable flag (1 byte)
        if cursor.remaining() < 1 {
            return Err(Error::InvalidMessage(
                "truncated cache reusable".to_string(),
            ));
        }
        let cache_reusable = cursor.get_u8() != 0;

        // Result count (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated result count".to_string(),
            ));
        }
        let result_count = cursor.get_i32();
        if result_count < 0 {
            return Err(Error::InvalidMessage(format!(
                "negative result count: {}",
                result_count
            )));
        }

        // Result info entries
        let mut results = Vec::with_capacity(result_count as usize);
        for _ in 0..result_count {
            if cursor.remaining() < 1 + 4 + 8 + 4 + 4 {
                return Err(Error::InvalidMessage(
                    "truncated result info".to_string(),
                ));
            }
            let stmt_type = StatementType::try_from(cursor.get_u8())?;
            let result_count_val = cursor.get_i32();
            let mut oid = [0u8; 8];
            oid.copy_from_slice(&cursor[..8]);
            cursor.advance(8);
            let cache_time_sec = cursor.get_i32();
            let cache_time_usec = cursor.get_i32();

            results.push(ResultInfo {
                stmt_type,
                result_count: result_count_val,
                oid,
                cache_time_sec,
                cache_time_usec,
            });
        }

        // Protocol version > 1: includeColumnInfo flag.
        // When set to 1 (multi-query), the server includes full column
        // metadata after the flag. We must consume it to keep the cursor
        // aligned with subsequent fields (shard_id, inline fetch data).
        if protocol_version > 1 && cursor.remaining() >= 1 {
            let include_column_info = cursor.get_u8();
            if include_column_info == 1 && cursor.remaining() >= 9 {
                // result_cache_lifetime(4) + stmt_type(1) + num_markers(4) = 9
                let _result_cache_lifetime = cursor.get_i32();
                let _stmt_type = cursor.get_u8();
                let _num_markers = cursor.get_i32();
                // updatable_flag(1) + column_count(4) + column metadata
                if cursor.remaining() >= 1 {
                    let _updatable_flag = cursor.get_u8();
                }
                if cursor.remaining() >= 4 {
                    let col_count = cursor.get_i32();
                    // Parse and discard the column metadata (uses the same
                    // format as prepare_column_info_set). Propagate errors
                    // so that malformed metadata is not silently ignored.
                    if col_count > 0 {
                        parse_column_metadata(
                            &mut cursor,
                            col_count,
                            protocol_version,
                        )?;
                    }
                }
            }
        }

        // Protocol version > 4: shard ID
        let shard_id = if protocol_version > 4 && cursor.remaining() >= 4 {
            Some(cursor.get_i32())
        } else {
            None
        };

        // For SELECT statements: inline fetch result
        let fetch_result = if !results.is_empty()
            && results[0].stmt_type.has_result_set()
            && cursor.remaining() >= 8
        {
            let _fetch_code = cursor.get_i32();
            let tuple_count = cursor.get_i32();

            if tuple_count > 0 {
                let tuples =
                    parse_tuples(&mut cursor, tuple_count, column_types.len(), column_types)?;
                Some(FetchResult {
                    tuple_count,
                    tuples,
                })
            } else {
                Some(FetchResult {
                    tuple_count: 0,
                    tuples: vec![],
                })
            }
        } else {
            None
        };

        Ok(ExecuteResponse {
            total_tuple_count,
            cache_reusable,
            results,
            shard_id,
            fetch_result,
        })
    }
}

/// Parsed response from a FETCH request.
impl FetchResult {
    /// Parse a fetch response from a [`ResponseFrame`].
    ///
    /// # Parameters
    ///
    /// - `frame`: The response frame
    /// - `column_types`: Column types from the prepare response
    pub fn parse(
        frame: &ResponseFrame,
        column_types: &[CubridDataType],
    ) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        let mut cursor = &frame.payload[..];

        // Tuple count (4 bytes)
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated fetch tuple count".to_string(),
            ));
        }
        let tuple_count = cursor.get_i32();

        if tuple_count < 0 {
            return Err(Error::InvalidMessage(format!(
                "negative tuple count: {}",
                tuple_count
            )));
        }
        if tuple_count == 0 {
            return Ok(FetchResult {
                tuple_count: 0,
                tuples: vec![],
            });
        }

        let tuples = parse_tuples(&mut cursor, tuple_count, column_types.len(), column_types)?;

        Ok(FetchResult {
            tuple_count,
            tuples,
        })
    }
}

// ---------------------------------------------------------------------------
// DbVersionResponse
// ---------------------------------------------------------------------------

/// Parsed response from a GET_DB_VERSION request.
#[derive(Debug, Clone)]
pub struct DbVersionResponse {
    /// The database version string (e.g., "11.4.0.0150").
    pub version: String,
}

impl DbVersionResponse {
    /// Parse a version response from a [`ResponseFrame`].
    pub fn parse(frame: &ResponseFrame) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        // The payload is the version string (null-terminated)
        let msg_bytes: Vec<u8> = frame
            .payload
            .iter()
            .take_while(|&&b| b != 0)
            .copied()
            .collect();
        let version = String::from_utf8_lossy(&msg_bytes).to_string();

        Ok(DbVersionResponse { version })
    }
}

// ---------------------------------------------------------------------------
// Simple response (for END_TRAN, CON_CLOSE, etc.)
// ---------------------------------------------------------------------------

/// Parse a simple success/error response with no additional payload.
///
/// Used for END_TRAN, CON_CLOSE, CLOSE_REQ_HANDLE, END_SESSION, etc.
pub fn parse_simple_response(frame: &ResponseFrame) -> Result<(), Error> {
    if frame.is_error() {
        Err(frame.parse_error())
    } else {
        Ok(())
    }
}

/// Parsed response from a GET_DB_PARAMETER request.
#[derive(Debug, Clone)]
pub struct DbParameterResponse {
    /// The parameter value.
    pub value: i32,
}

impl DbParameterResponse {
    /// Parse a parameter response from a [`ResponseFrame`].
    pub fn parse(frame: &ResponseFrame) -> Result<Self, Error> {
        if frame.is_error() {
            return Err(frame.parse_error());
        }

        let mut cursor = &frame.payload[..];
        if cursor.remaining() < 4 {
            return Err(Error::InvalidMessage(
                "truncated parameter value".to_string(),
            ));
        }
        let value = cursor.get_i32();
        Ok(DbParameterResponse { value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // -----------------------------------------------------------------------
    // Helper: build raw response bytes
    // -----------------------------------------------------------------------

    /// Build a raw response frame (after length prefix is stripped by codec).
    /// Format: [cas_info:4][response_code:4][payload...]
    fn build_frame(cas_info: [u8; 4], response_code: i32, payload: &[u8]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&cas_info);
        buf.extend_from_slice(&response_code.to_be_bytes());
        buf.extend_from_slice(payload);
        buf.freeze()
    }

    // -----------------------------------------------------------------------
    // ResponseFrame tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_response_frame_parse_success() {
        let data = build_frame([0x01, 0x02, 0x03, 0x04], 42, b"hello");
        let frame = ResponseFrame::parse(data).unwrap();

        assert_eq!(frame.cas_info, CasInfo::from_bytes([0x01, 0x02, 0x03, 0x04]));
        assert_eq!(frame.response_code, 42);
        assert_eq!(&frame.payload[..], b"hello");
        assert!(!frame.is_error());
    }

    #[test]
    fn test_response_frame_parse_error_code() {
        let data = build_frame([0, 0, 0, 0], -1, &[]);
        let frame = ResponseFrame::parse(data).unwrap();

        assert!(frame.is_error());
        assert_eq!(frame.response_code, -1);
    }

    #[test]
    fn test_response_frame_parse_too_short() {
        let data = Bytes::from_static(&[0, 0, 0]); // only 3 bytes
        let result = ResponseFrame::parse(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_response_frame_parse_minimal() {
        // Exactly 8 bytes: cas_info(4) + response_code(4), no payload
        let data = build_frame([0, 0, 0, 0], 0, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        assert_eq!(frame.response_code, 0);
        assert!(frame.payload.is_empty());
    }

    // -----------------------------------------------------------------------
    // Error parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_error_cas_renewed() {
        // Renewed format: response_code = indicator (-1 = CAS),
        // payload = error_code + message (no indicator in payload).
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1007_i32).to_be_bytes()); // error code
        payload.extend_from_slice(b"bind mismatch\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Cas { code, message } => {
                assert_eq!(code, -1007);
                assert_eq!(message, "bind mismatch");
            }
            other => panic!("expected Cas error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error_dbms_renewed() {
        // Renewed format: response_code = indicator (-2 = DBMS),
        // payload = error_code + message.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-493_i32).to_be_bytes());
        payload.extend_from_slice(b"table not found\0");

        let data = build_frame([0, 0, 0, 0], -2, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Dbms { code, message } => {
                assert_eq!(code, -493);
                assert_eq!(message, "table not found");
            }
            other => panic!("expected Dbms error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error_legacy_format() {
        // Legacy format: response_code = generic negative (not -1/-2),
        // payload = indicator + error_code + message.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-2_i32).to_be_bytes()); // DBMS indicator
        payload.extend_from_slice(&(-493_i32).to_be_bytes());
        payload.extend_from_slice(b"table not found\0");

        let data = build_frame([0, 0, 0, 0], -99, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Dbms { code, message } => {
                assert_eq!(code, -493);
                assert_eq!(message, "table not found");
            }
            other => panic!("expected Dbms error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error_no_payload() {
        // Renewed format with empty payload: response_code=-1 (CAS indicator),
        // no error_code in payload → code defaults to 0.
        let data = build_frame([0, 0, 0, 0], -1, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Cas { code, message } => {
                assert_eq!(code, 0);
                assert!(message.is_empty());
            }
            other => panic!("expected Cas error, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Prepare response tests
    // -----------------------------------------------------------------------

    /// Build a minimal prepare response payload.
    fn build_prepare_payload(
        cache_lifetime: i32,
        stmt_type: u8,
        bind_count: i32,
        is_updatable: bool,
        column_count: i32,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&cache_lifetime.to_be_bytes());
        buf.push(stmt_type);
        buf.extend_from_slice(&bind_count.to_be_bytes());
        buf.push(if is_updatable { 1 } else { 0 });
        buf.extend_from_slice(&column_count.to_be_bytes());
        buf
    }

    /// Build column metadata bytes for a simple VARCHAR column.
    fn build_varchar_column(name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        // Column type = String (2), no collection
        buf.push(2);
        // Scale = 0
        buf.extend_from_slice(&0_i16.to_be_bytes());
        // Precision = 255
        buf.extend_from_slice(&255_i32.to_be_bytes());
        // Name (null-terminated with length prefix)
        assert!(
            name.len() < i32::MAX as usize,
            "column name exceeds CUBRID protocol maximum length"
        );
        let name_len = name.len() as i32 + 1;
        buf.extend_from_slice(&name_len.to_be_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        // Real name (same)
        buf.extend_from_slice(&name_len.to_be_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        // Table name = "t1"
        buf.extend_from_slice(&3_i32.to_be_bytes());
        buf.extend_from_slice(b"t1\0");
        // is_nullable = true
        buf.push(1);
        // default_value = empty
        buf.extend_from_slice(&0_i32.to_be_bytes());
        // Boolean flags: all false
        buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
        buf
    }

    #[test]
    fn test_prepare_response_no_columns() {
        let payload = build_prepare_payload(0, 20, 1, false, 0); // INSERT, 1 bind, 0 columns
        let data = build_frame([1, 2, 3, 4], 7, &payload); // handle = 7
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = PrepareResponse::parse(&frame, crate::PROTOCOL_V12).unwrap();

        assert_eq!(resp.query_handle, 7);
        assert_eq!(resp.cache_lifetime, 0);
        assert_eq!(resp.statement_type, StatementType::Insert);
        assert_eq!(resp.bind_count, 1);
        assert!(!resp.is_updatable);
        assert!(resp.columns.is_empty());
    }

    #[test]
    fn test_prepare_response_with_columns() {
        let mut payload = build_prepare_payload(60, 21, 0, false, 1); // SELECT, 0 bind, 1 column
        payload.extend_from_slice(&build_varchar_column("col1"));

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = PrepareResponse::parse(&frame, crate::PROTOCOL_V12).unwrap();

        assert_eq!(resp.statement_type, StatementType::Select);
        assert_eq!(resp.columns.len(), 1);
        assert_eq!(resp.columns[0].name, "col1");
        assert_eq!(resp.columns[0].column_type, CubridDataType::String);
        assert_eq!(resp.columns[0].precision, 255);
        assert!(resp.columns[0].is_nullable);
        assert_eq!(resp.columns[0].table_name, "t1");
        assert!(resp.columns[0].default_value.is_none());
    }

    #[test]
    fn test_prepare_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1001_i32).to_be_bytes());
        payload.extend_from_slice(b"syntax error\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
    }

    #[test]
    fn test_prepare_response_collection_column() {
        let mut payload = build_prepare_payload(0, 21, 0, false, 1);

        // Collection column: SET of INT
        // Legacy type byte: 0x80 | CCI_CODE_SET(0x20) = 0xA0
        payload.push(0xA0);
        // Element type: INT = 8
        payload.push(8);
        // Scale, precision
        payload.extend_from_slice(&0_i16.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());
        // Names
        payload.extend_from_slice(&4_i32.to_be_bytes());
        payload.extend_from_slice(b"ids\0");
        payload.extend_from_slice(&4_i32.to_be_bytes());
        payload.extend_from_slice(b"ids\0");
        payload.extend_from_slice(&3_i32.to_be_bytes());
        payload.extend_from_slice(b"t1\0");
        // Flags
        payload.push(0); // nullable
        payload.extend_from_slice(&0_i32.to_be_bytes()); // no default
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]); // bool flags

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = PrepareResponse::parse(&frame, crate::PROTOCOL_V12).unwrap();

        assert_eq!(resp.columns[0].column_type, CubridDataType::Set);
        assert_eq!(
            resp.columns[0].collection_element_type,
            Some(CubridDataType::Int)
        );
    }

    // -----------------------------------------------------------------------
    // Execute response tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_response_dml() {
        let mut payload = Vec::new();
        // cache_reusable = 0
        payload.push(0);
        // result_count = 1
        payload.extend_from_slice(&1_i32.to_be_bytes());
        // ResultInfo: stmt_type=INSERT(20), result_count=1, oid(8), cache(8)
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes());
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache sec
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache usec

        let data = build_frame([0, 0, 0, 0], 1, &payload); // 1 row affected
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 0, &[]).unwrap();

        assert_eq!(resp.total_tuple_count, 1);
        assert!(!resp.cache_reusable);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].stmt_type, StatementType::Insert);
        assert_eq!(resp.results[0].result_count, 1);
        assert!(resp.shard_id.is_none());
        assert!(resp.fetch_result.is_none());
    }

    #[test]
    fn test_execute_response_select_with_fetch() {
        let mut payload = Vec::new();
        // cache_reusable
        payload.push(0);
        // result_count = 1
        payload.extend_from_slice(&1_i32.to_be_bytes());
        // ResultInfo: SELECT
        payload.push(21);
        payload.extend_from_slice(&2_i32.to_be_bytes()); // 2 rows
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());

        // includeColumnInfo (proto > 1)
        payload.push(0);

        // Inline fetch: fetch_code=0, tuple_count=1
        payload.extend_from_slice(&0_i32.to_be_bytes()); // fetch_code
        payload.extend_from_slice(&1_i32.to_be_bytes()); // tuple_count

        // One tuple with one INT column
        payload.extend_from_slice(&0_i32.to_be_bytes()); // index
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&4_i32.to_be_bytes()); // column size = 4
        payload.extend_from_slice(&42_i32.to_be_bytes()); // value = 42

        let column_types = vec![CubridDataType::Int];
        let data = build_frame([0, 0, 0, 0], 2, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 2, &column_types).unwrap();

        assert_eq!(resp.total_tuple_count, 2);
        let fetch = resp.fetch_result.as_ref().unwrap();
        assert_eq!(fetch.tuple_count, 1);
        assert_eq!(fetch.tuples.len(), 1);

        match &fetch.tuples[0].values[0] {
            ColumnValue::Data { data_type, data } => {
                assert_eq!(*data_type, CubridDataType::Int);
                assert_eq!(&data[..], &42_i32.to_be_bytes());
            }
            ColumnValue::Null => panic!("expected data, got null"),
        }
    }

    #[test]
    fn test_execute_response_with_shard_id() {
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes());
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.push(0); // includeColumnInfo (proto > 1)
        payload.extend_from_slice(&7_i32.to_be_bytes()); // shard_id = 7

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 5, &[]).unwrap();

        assert_eq!(resp.shard_id, Some(7));
    }

    #[test]
    fn test_execute_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-2_i32).to_be_bytes());
        payload.extend_from_slice(&(-100_i32).to_be_bytes());
        payload.extend_from_slice(b"constraint violation\0");

        let data = build_frame([0, 0, 0, 0], -2, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = ExecuteResponse::parse(&frame, 12, &[]);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Fetch response tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_response_empty() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // tuple_count = 0

        let data = build_frame([0, 0, 0, 0], 0, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &[CubridDataType::Int]).unwrap();

        assert_eq!(result.tuple_count, 0);
        assert!(result.tuples.is_empty());
    }

    #[test]
    fn test_fetch_response_with_null_values() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_i32.to_be_bytes()); // 1 tuple

        // Tuple: index, OID, then columns
        payload.extend_from_slice(&0_i32.to_be_bytes()); // index
        payload.extend_from_slice(&[0u8; 8]); // OID
        // Column 1: NULL (size = 0)
        payload.extend_from_slice(&0_i32.to_be_bytes());
        // Column 2: VARCHAR "hello"
        let hello = b"hello\0";
        payload.extend_from_slice(&(hello.len() as i32).to_be_bytes());
        payload.extend_from_slice(hello);

        let column_types = vec![CubridDataType::Int, CubridDataType::String];
        let data = build_frame([0, 0, 0, 0], 0, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &column_types).unwrap();

        assert_eq!(result.tuples.len(), 1);
        assert!(matches!(result.tuples[0].values[0], ColumnValue::Null));
        match &result.tuples[0].values[1] {
            ColumnValue::Data { data_type, data } => {
                assert_eq!(*data_type, CubridDataType::String);
                assert_eq!(&data[..], hello);
            }
            ColumnValue::Null => panic!("expected data"),
        }
    }

    #[test]
    fn test_fetch_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1012_i32).to_be_bytes());
        payload.extend_from_slice(b"no more data\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &[]);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // DbVersionResponse tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_db_version_response() {
        let version = b"11.4.0.0150\0";
        let data = build_frame([0, 0, 0, 0], 0, version);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = DbVersionResponse::parse(&frame).unwrap();

        assert_eq!(resp.version, "11.4.0.0150");
    }

    #[test]
    fn test_db_version_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1016_i32).to_be_bytes());
        payload.extend_from_slice(b"version mismatch\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = DbVersionResponse::parse(&frame);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Simple response tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_simple_response_success() {
        let data = build_frame([0, 0, 0, 0], 0, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        assert!(parse_simple_response(&frame).is_ok());
    }

    #[test]
    fn test_simple_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1003_i32).to_be_bytes());
        payload.extend_from_slice(b"comm error\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = parse_simple_response(&frame);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // DbParameterResponse tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_db_parameter_response() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&4_i32.to_be_bytes()); // isolation level = 4

        let data = build_frame([0, 0, 0, 0], 0, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = DbParameterResponse::parse(&frame).unwrap();

        assert_eq!(resp.value, 4);
    }

    #[test]
    fn test_db_parameter_response_truncated() {
        let data = build_frame([0, 0, 0, 0], 0, &[0, 0]); // only 2 bytes payload
        let frame = ResponseFrame::parse(data).unwrap();
        let result = DbParameterResponse::parse(&frame);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // read_string tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_string_normal() {
        let mut data = Vec::new();
        data.extend_from_slice(&6_i32.to_be_bytes()); // length = 6 (5 chars + null)
        data.extend_from_slice(b"hello\0");

        let mut cursor = &data[..];
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, "hello");
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_read_string_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // length = 0

        let mut cursor = &data[..];
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn test_read_string_truncated_length() {
        let data = [0u8; 2]; // not enough for length
        let mut cursor = &data[..];
        assert!(read_string(&mut cursor).is_err());
    }

    #[test]
    fn test_read_string_truncated_data() {
        let mut data = Vec::new();
        data.extend_from_slice(&10_i32.to_be_bytes()); // length = 10
        data.extend_from_slice(b"hi"); // only 2 bytes

        let mut cursor = &data[..];
        assert!(read_string(&mut cursor).is_err());
    }

    // -----------------------------------------------------------------------
    // C13: read_string negative length
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_string_negative_length() {
        let mut data = Vec::new();
        data.extend_from_slice(&(-1_i32).to_be_bytes()); // negative length

        let mut cursor = &data[..];
        let result = read_string(&mut cursor);
        assert!(result.is_err(), "negative length must be rejected");
    }

    #[test]
    fn test_read_string_large_negative_length() {
        let mut data = Vec::new();
        data.extend_from_slice(&(i32::MIN).to_be_bytes());

        let mut cursor = &data[..];
        let result = read_string(&mut cursor);
        assert!(result.is_err(), "i32::MIN length must be rejected");
    }

    // -----------------------------------------------------------------------
    // C14: Vec::with_capacity negative count
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_column_metadata_negative_count() {
        let data: Vec<u8> = vec![];
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, -1, crate::PROTOCOL_V12);
        assert!(result.is_err(), "negative column_count must be rejected");
    }

    #[test]
    fn test_parse_tuples_negative_count() {
        let data: Vec<u8> = vec![];
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, -1, 0, &[]);
        assert!(result.is_err(), "negative tuple_count must be rejected");
    }

    #[test]
    fn test_execute_response_negative_result_count() {
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&(-1_i32).to_be_bytes()); // result_count = -1

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = ExecuteResponse::parse(&frame, 0, &[]);
        assert!(result.is_err(), "negative result_count must be rejected");
    }

    #[test]
    fn test_prepare_response_negative_column_count() {
        let payload = build_prepare_payload(0, 21, 0, false, -1); // column_count = -1

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err(), "negative column_count must be rejected");
    }

    // -----------------------------------------------------------------------
    // C15: build_varchar_column overflow guard (test helper)
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_varchar_column_normal_name() {
        // Just ensure the helper works for reasonable names
        let col = build_varchar_column("test_col");
        assert!(!col.is_empty());
    }

    // -----------------------------------------------------------------------
    // M29: ExecuteResponse protocol_version branch coverage
    // -----------------------------------------------------------------------

    /// Build a DML execute response payload with configurable protocol fields.
    fn build_dml_execute_payload(
        include_column_info: bool,
        shard_id: Option<i32>,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count = 1
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache sec
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache usec
        if include_column_info {
            payload.push(0);
        }
        if let Some(sid) = shard_id {
            payload.extend_from_slice(&sid.to_be_bytes());
        }
        payload
    }

    #[test]
    fn test_execute_response_protocol_v0_no_include_column_info() {
        let payload = build_dml_execute_payload(false, None);
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 0, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
        assert_eq!(resp.results.len(), 1);
        assert!(resp.shard_id.is_none());
    }

    #[test]
    fn test_execute_response_protocol_v1_boundary() {
        let payload = build_dml_execute_payload(false, None);
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 1, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
        assert!(resp.shard_id.is_none());
    }

    #[test]
    fn test_execute_response_protocol_v2_has_include_column_info() {
        let payload = build_dml_execute_payload(true, None);
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 2, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
        assert!(resp.shard_id.is_none());
    }

    #[test]
    fn test_execute_response_protocol_v4_no_shard_id() {
        let payload = build_dml_execute_payload(true, None);
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 4, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
        assert!(resp.shard_id.is_none());
    }

    #[test]
    fn test_execute_response_protocol_v5_has_shard_id() {
        let payload = build_dml_execute_payload(true, Some(42));
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 5, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
        assert_eq!(resp.shard_id, Some(42));
    }

    // -----------------------------------------------------------------------
    // M1: V6 column metadata parsing (no collection encoding)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_column_metadata_v6_no_collection_encoding() {
        // For PROTOCOL_V6 and below, the legacy type byte is the raw type
        // code. The 0x80 bit collection encoding should NOT be applied.
        let col_bytes = build_varchar_column("v6_col");
        let mut cursor = &col_bytes[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V6).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_type, CubridDataType::String);
        assert!(result[0].collection_element_type.is_none());
        assert_eq!(result[0].name, "v6_col");
    }

    #[test]
    fn test_parse_column_metadata_v7_with_collection_encoding() {
        // For V7+, the 0x80 bit indicates a collection type.
        // Build a SET of INT column: 0xA0 = 0x80 | 0x20(SET), element=INT(8)
        let mut col_bytes = Vec::new();
        col_bytes.push(0xA0); // collection flag + SET
        col_bytes.push(8);    // element type: INT
        col_bytes.extend_from_slice(&0_i16.to_be_bytes()); // scale
        col_bytes.extend_from_slice(&0_i32.to_be_bytes()); // precision
        // name
        col_bytes.extend_from_slice(&4_i32.to_be_bytes());
        col_bytes.extend_from_slice(b"ids\0");
        // real_name
        col_bytes.extend_from_slice(&4_i32.to_be_bytes());
        col_bytes.extend_from_slice(b"ids\0");
        // table_name
        col_bytes.extend_from_slice(&3_i32.to_be_bytes());
        col_bytes.extend_from_slice(b"t1\0");
        // flags
        col_bytes.push(0); // nullable
        col_bytes.extend_from_slice(&0_i32.to_be_bytes()); // no default
        col_bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]); // bool flags

        let mut cursor = &col_bytes[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::Set);
        assert_eq!(result[0].collection_element_type, Some(CubridDataType::Int));
    }

    // -----------------------------------------------------------------------
    // M2: FetchResult negative tuple_count must error
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_response_negative_tuple_count_errors() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-3_i32).to_be_bytes()); // tuple_count = -3

        let data = build_frame([0, 0, 0, 0], 0, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &[CubridDataType::Int]);
        assert!(result.is_err(), "negative tuple_count must return an error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("negative tuple count"),
            "error message should mention 'negative tuple count', got: {}",
            err_msg
        );
    }

    #[test]
    fn test_fetch_response_zero_tuple_count_is_valid() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // tuple_count = 0

        let data = build_frame([0, 0, 0, 0], 0, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &[CubridDataType::Int]).unwrap();
        assert_eq!(result.tuple_count, 0);
        assert!(result.tuples.is_empty());
    }

    // -----------------------------------------------------------------------
    // parse_error: legacy format edge cases (lines 141-154)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_error_legacy_empty_payload() {
        // Legacy format (response_code not -1 or -2), payload too short for
        // the indicator field. Should return Cas error with response_code.
        let data = build_frame([0, 0, 0, 0], -99, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Cas { code, message } => {
                assert_eq!(code, -99);
                assert!(message.is_empty());
            }
            other => panic!("expected Cas error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error_legacy_truncated_before_error_code() {
        // Legacy format: payload has the indicator (4 bytes) but not enough
        // bytes for the error_code. Should fall back to response_code.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes()); // indicator (CAS)
        // No error_code bytes follow

        let data = build_frame([0, 0, 0, 0], -99, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Cas { code, message } => {
                assert_eq!(code, -99); // falls back to response_code
                assert!(message.is_empty());
            }
            other => panic!("expected Cas error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error_legacy_truncated_partial_payload() {
        // Legacy format: payload has indicator (4 bytes) + only 2 bytes
        // (not enough for error_code). Falls back to response_code for
        // the error code, and the 2 leftover bytes are read as message.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-2_i32).to_be_bytes()); // indicator (DBMS)
        payload.extend_from_slice(&[0xFF, 0xFF]); // incomplete error_code

        let data = build_frame([0, 0, 0, 0], -50, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let err = frame.parse_error();

        match err {
            Error::Dbms { code, .. } => {
                assert_eq!(code, -50); // falls back to response_code
            }
            other => panic!("expected Dbms error, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // parse_column_metadata: truncated payloads (lines 296-367)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_column_metadata_truncated_at_type_byte() {
        // Empty cursor when column_count > 0 triggers "truncated column metadata"
        let data: Vec<u8> = vec![];
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated column metadata"));
    }

    #[test]
    fn test_parse_column_metadata_truncated_collection_element_type() {
        // V7+: type byte with 0x80 set but no element type byte following
        let data: Vec<u8> = vec![0xA0]; // collection flag + SET, but no element byte
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated collection element type"));
    }

    #[test]
    fn test_parse_column_metadata_truncated_scale() {
        // Type byte present but no scale bytes
        let data: Vec<u8> = vec![8]; // INT type, then nothing
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated scale"));
    }

    #[test]
    fn test_parse_column_metadata_truncated_precision() {
        // Type byte + scale present but no precision bytes
        let mut data = Vec::new();
        data.push(8); // INT type
        data.extend_from_slice(&0_i16.to_be_bytes()); // scale
        // No precision bytes
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated precision"));
    }

    #[test]
    fn test_parse_column_metadata_truncated_nullable_flag() {
        // type + scale + precision + 3 strings, but no nullable byte
        let mut data = Vec::new();
        data.push(8); // INT
        data.extend_from_slice(&0_i16.to_be_bytes()); // scale
        data.extend_from_slice(&0_i32.to_be_bytes()); // precision
        // name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"a\0");
        // real_name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"a\0");
        // table_name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"t\0");
        // No nullable flag
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated nullable flag"));
    }

    #[test]
    fn test_parse_column_metadata_truncated_column_flags() {
        // type + scale + precision + 3 strings + nullable + default_value,
        // but missing the 7 boolean flag bytes
        let mut data = Vec::new();
        data.push(8); // INT
        data.extend_from_slice(&0_i16.to_be_bytes()); // scale
        data.extend_from_slice(&0_i32.to_be_bytes()); // precision
        // name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"a\0");
        // real_name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"a\0");
        // table_name
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"t\0");
        // nullable
        data.push(1);
        // default_value (empty)
        data.extend_from_slice(&0_i32.to_be_bytes());
        // Only 3 of the required 7 flag bytes
        data.extend_from_slice(&[0, 0, 0]);
        let mut cursor = &data[..];
        let result = parse_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated column flags"));
    }

    // -----------------------------------------------------------------------
    // parse_schema_column_metadata: truncated payloads (lines 412-454)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_schema_column_metadata_negative_count() {
        let data: Vec<u8> = vec![];
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, -1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative column count"));
    }

    #[test]
    fn test_parse_schema_column_metadata_truncated_at_type_byte() {
        let data: Vec<u8> = vec![];
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated schema column metadata"));
    }

    #[test]
    fn test_parse_schema_column_metadata_truncated_collection_element() {
        // V7+: type byte with 0x80 set but no element type byte
        let data: Vec<u8> = vec![0xA0];
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated collection element type"));
    }

    #[test]
    fn test_parse_schema_column_metadata_truncated_scale() {
        // Type byte present but no scale
        let data: Vec<u8> = vec![8]; // INT
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated scale"));
    }

    #[test]
    fn test_parse_schema_column_metadata_truncated_precision() {
        let mut data = Vec::new();
        data.push(8); // INT
        data.extend_from_slice(&0_i16.to_be_bytes()); // scale
        // No precision
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated precision"));
    }

    #[test]
    fn test_parse_schema_column_metadata_collection_set() {
        // V7+: SET of INT — collection_code 0x20
        let mut data = Vec::new();
        data.push(0xA0); // 0x80 | 0x20 (SET)
        data.push(8);    // element: INT
        data.extend_from_slice(&0_i16.to_be_bytes()); // scale
        data.extend_from_slice(&0_i32.to_be_bytes()); // precision
        data.extend_from_slice(&2_i32.to_be_bytes()); // name len
        data.extend_from_slice(b"s\0");
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::Set);
        assert_eq!(result[0].collection_element_type, Some(CubridDataType::Int));
    }

    #[test]
    fn test_parse_schema_column_metadata_collection_multiset() {
        // V7+: MULTISET of String — collection_code 0x40
        let mut data = Vec::new();
        data.push(0xC0); // 0x80 | 0x40 (MULTISET)
        data.push(2);    // element: String
        data.extend_from_slice(&0_i16.to_be_bytes());
        data.extend_from_slice(&0_i32.to_be_bytes());
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"m\0");
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::MultiSet);
        assert_eq!(result[0].collection_element_type, Some(CubridDataType::String));
    }

    #[test]
    fn test_parse_schema_column_metadata_collection_sequence() {
        // V7+: SEQUENCE of Double — collection_code 0x60
        let mut data = Vec::new();
        data.push(0xE0); // 0x80 | 0x60 (SEQUENCE)
        data.push(12);   // element: Double
        data.extend_from_slice(&0_i16.to_be_bytes());
        data.extend_from_slice(&0_i32.to_be_bytes());
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"q\0");
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::Sequence);
        assert_eq!(result[0].collection_element_type, Some(CubridDataType::Double));
    }

    #[test]
    fn test_parse_schema_column_metadata_collection_bare_element() {
        // V7+: 0x80 set but collection_code bits are 0x00 — no collection,
        // just use the element type directly.
        let mut data = Vec::new();
        data.push(0x80); // 0x80 | 0x00 (no collection bits)
        data.push(8);    // element: INT
        data.extend_from_slice(&0_i16.to_be_bytes());
        data.extend_from_slice(&0_i32.to_be_bytes());
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"x\0");
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V7).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::Int);
        assert!(result[0].collection_element_type.is_none());
    }

    #[test]
    fn test_parse_schema_column_metadata_v6_no_collection() {
        // V6: no collection encoding, raw type code
        let mut data = Vec::new();
        data.push(2); // String
        data.extend_from_slice(&0_i16.to_be_bytes());
        data.extend_from_slice(&255_i32.to_be_bytes());
        data.extend_from_slice(&2_i32.to_be_bytes());
        data.extend_from_slice(b"n\0");
        let mut cursor = &data[..];
        let result = parse_schema_column_metadata(&mut cursor, 1, crate::PROTOCOL_V6).unwrap();
        assert_eq!(result[0].column_type, CubridDataType::String);
        assert!(result[0].collection_element_type.is_none());
    }

    // -----------------------------------------------------------------------
    // PrepareResponse: truncated payload at each field boundary (lines 531-565)
    // -----------------------------------------------------------------------

    #[test]
    fn test_prepare_response_truncated_cache_lifetime() {
        // Empty payload: not enough bytes for cache_lifetime (4 bytes)
        let data = build_frame([0, 0, 0, 0], 1, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated cache lifetime"));
    }

    #[test]
    fn test_prepare_response_truncated_statement_type() {
        // Only cache_lifetime (4 bytes), no statement type byte
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache_lifetime
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated statement type"));
    }

    #[test]
    fn test_prepare_response_truncated_bind_count() {
        // cache_lifetime + stmt_type but no bind_count
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache_lifetime
        payload.push(21); // SELECT
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated bind count"));
    }

    #[test]
    fn test_prepare_response_truncated_updatable_flag() {
        // cache_lifetime + stmt_type + bind_count but no updatable flag
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache_lifetime
        payload.push(21); // SELECT
        payload.extend_from_slice(&0_i32.to_be_bytes()); // bind_count
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated updatable flag"));
    }

    #[test]
    fn test_prepare_response_truncated_column_count() {
        // cache_lifetime + stmt_type + bind_count + updatable but no column_count
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache_lifetime
        payload.push(21); // SELECT
        payload.extend_from_slice(&0_i32.to_be_bytes()); // bind_count
        payload.push(0); // is_updatable
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = PrepareResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated column count"));
    }

    // -----------------------------------------------------------------------
    // SchemaInfoResponse: error, truncated, and negative paths (lines 613-636)
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_info_response_error() {
        // Error frame should propagate error
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1001_i32).to_be_bytes());
        payload.extend_from_slice(b"internal error\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = SchemaInfoResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_info_response_truncated_num_tuple() {
        // Empty payload: not enough bytes for num_tuple
        let data = build_frame([0, 0, 0, 0], 1, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = SchemaInfoResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated num_tuple"));
    }

    #[test]
    fn test_schema_info_response_negative_num_tuple() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-5_i32).to_be_bytes()); // negative num_tuple
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = SchemaInfoResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative num_tuple"));
    }

    #[test]
    fn test_schema_info_response_truncated_column_count() {
        // num_tuple present but no column_count
        let mut payload = Vec::new();
        payload.extend_from_slice(&3_i32.to_be_bytes()); // num_tuple = 3
        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = SchemaInfoResponse::parse(&frame, crate::PROTOCOL_V12);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated column count"));
    }

    #[test]
    fn test_schema_info_response_success() {
        // Valid schema info response with 1 column
        let mut payload = Vec::new();
        payload.extend_from_slice(&2_i32.to_be_bytes()); // num_tuple = 2
        payload.extend_from_slice(&1_i32.to_be_bytes()); // column_count = 1
        // Schema column metadata (minimal): type(1) + scale(2) + precision(4) + name
        payload.push(2); // String
        payload.extend_from_slice(&0_i16.to_be_bytes()); // scale
        payload.extend_from_slice(&255_i32.to_be_bytes()); // precision
        payload.extend_from_slice(&5_i32.to_be_bytes()); // name length
        payload.extend_from_slice(b"Name\0");

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = SchemaInfoResponse::parse(&frame, crate::PROTOCOL_V12).unwrap();
        assert_eq!(resp.query_handle, 1);
        assert_eq!(resp.num_tuple, 2);
        assert_eq!(resp.columns.len(), 1);
        assert_eq!(resp.columns[0].name, "Name");
    }

    // -----------------------------------------------------------------------
    // parse_tuples: truncated paths (lines 741-798)
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_tuples_truncated_tuple_index() {
        // tuple_count=1, but not enough data for the 4-byte index
        let data: Vec<u8> = vec![0, 0]; // only 2 bytes
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Int]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated tuple index"));
    }

    #[test]
    fn test_parse_tuples_truncated_tuple_oid() {
        // tuple_count=1, index present but OID truncated
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 4]); // only 4 of 8 OID bytes
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Int]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated tuple OID"));
    }

    #[test]
    fn test_parse_tuples_truncated_column_value_size() {
        // tuple_count=1, index + OID present but column value size truncated
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 8]); // OID
        data.extend_from_slice(&[0, 0]); // only 2 bytes, need 4 for column size
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Int]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated column value size"));
    }

    #[test]
    fn test_parse_tuples_truncated_column_value_data() {
        // tuple_count=1, column value size says 10 bytes but only 3 available
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 8]); // OID
        data.extend_from_slice(&10_i32.to_be_bytes()); // column size = 10
        data.extend_from_slice(&[1, 2, 3]); // only 3 bytes available
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Int]);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("truncated column value data"));
    }

    #[test]
    fn test_parse_tuples_col_idx_beyond_column_types_defaults_to_null() {
        // When col_idx >= column_types.len(), the type defaults to Null,
        // triggering the embedded type code path (lines 781, 792-798).
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 8]); // OID
        // Column value: size=5, first byte is type code (INT=8), then 4 bytes of data
        data.extend_from_slice(&5_i32.to_be_bytes()); // size = 5
        data.push(8); // embedded type: INT
        data.extend_from_slice(&99_i32.to_be_bytes()); // value = 99

        // column_count=1 but empty column_types to force col_idx >= column_types.len()
        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[]).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0].values[0] {
            ColumnValue::Data { data_type, data } => {
                assert_eq!(*data_type, CubridDataType::Int);
                assert_eq!(&data[..], &99_i32.to_be_bytes());
            }
            ColumnValue::Null => panic!("expected data, got null"),
        }
    }

    #[test]
    fn test_parse_tuples_null_column_type_embedded_type_code() {
        // When column_types contains Null, the type code is embedded in the data
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 8]); // OID
        // Column value with embedded type: size=3, type=String(2), then "ab"
        data.extend_from_slice(&3_i32.to_be_bytes()); // size = 3
        data.push(2); // embedded type: String
        data.extend_from_slice(b"ab"); // 2 bytes of string data

        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Null]).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0].values[0] {
            ColumnValue::Data { data_type, data } => {
                assert_eq!(*data_type, CubridDataType::String);
                assert_eq!(&data[..], b"ab");
            }
            ColumnValue::Null => panic!("expected data, got null"),
        }
    }

    #[test]
    fn test_parse_tuples_embedded_type_code_size_too_small() {
        // When embedded type code path has size < 1, it should error
        // (lines 787-790). However, size <= 0 maps to Null, so we need
        // size == 0 with column type Null. Actually size <= 0 gives Null.
        // The embedded path only runs for size > 0. So we need size=1 but
        // that means 0 bytes of actual data after the type code -- which
        // is technically valid (empty data). Let's test that case.
        let mut data = Vec::new();
        data.extend_from_slice(&0_i32.to_be_bytes()); // index
        data.extend_from_slice(&[0u8; 8]); // OID
        // size=1: just the type code, no actual data
        data.extend_from_slice(&1_i32.to_be_bytes()); // size = 1
        data.push(8); // embedded type: INT

        let mut cursor = &data[..];
        let result = parse_tuples(&mut cursor, 1, 1, &[CubridDataType::Null]).unwrap();
        match &result[0].values[0] {
            ColumnValue::Data { data_type, data } => {
                assert_eq!(*data_type, CubridDataType::Int);
                assert!(data.is_empty()); // 0 bytes of value data
            }
            ColumnValue::Null => panic!("expected data, got null"),
        }
    }

    // -----------------------------------------------------------------------
    // ExecuteResponse: truncated paths (lines 856-881)
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_response_truncated_cache_reusable() {
        // Empty payload: not enough for cache_reusable (1 byte)
        let data = build_frame([0, 0, 0, 0], 1, &[]);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = ExecuteResponse::parse(&frame, 0, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated cache reusable"));
    }

    #[test]
    fn test_execute_response_truncated_result_count() {
        // Only cache_reusable, no result_count
        let data = build_frame([0, 0, 0, 0], 1, &[0]); // 1 byte: cache_reusable
        let frame = ResponseFrame::parse(data).unwrap();
        let result = ExecuteResponse::parse(&frame, 0, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated result count"));
    }

    #[test]
    fn test_execute_response_truncated_result_info() {
        // cache_reusable + result_count=1, but not enough bytes for ResultInfo
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count = 1
        // ResultInfo needs 1+4+8+4+4 = 21 bytes; give only 5
        payload.extend_from_slice(&[0u8; 5]);

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = ExecuteResponse::parse(&frame, 0, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated result info"));
    }

    // -----------------------------------------------------------------------
    // ExecuteResponse: include_column_info=1 multi-query path (lines 909-921)
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_response_include_column_info_multi_query() {
        // Protocol V2+: includeColumnInfo=1 triggers parsing of inline
        // column metadata (lines 908-927).
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count = 1
        // ResultInfo for INSERT (no result set, so no inline fetch)
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache sec
        payload.extend_from_slice(&0_i32.to_be_bytes()); // cache usec

        // includeColumnInfo = 1 (multi-query)
        payload.push(1);

        // result_cache_lifetime(4) + stmt_type(1) + num_markers(4) = 9
        payload.extend_from_slice(&0_i32.to_be_bytes()); // result_cache_lifetime
        payload.push(21); // stmt_type (SELECT)
        payload.extend_from_slice(&0_i32.to_be_bytes()); // num_markers

        // updatable_flag(1) + column_count(4) + column metadata
        payload.push(0); // updatable_flag
        payload.extend_from_slice(&1_i32.to_be_bytes()); // column_count = 1

        // One VARCHAR column metadata (same format as prepare)
        payload.extend_from_slice(&build_varchar_column("multi_col"));

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 2, &[]).unwrap();

        assert_eq!(resp.total_tuple_count, 1);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].stmt_type, StatementType::Insert);
    }

    #[test]
    fn test_execute_response_include_column_info_multi_query_zero_columns() {
        // include_column_info=1 but col_count=0 (no column metadata to parse)
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count = 1
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes());
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());

        payload.push(1); // includeColumnInfo = 1
        payload.extend_from_slice(&0_i32.to_be_bytes()); // result_cache_lifetime
        payload.push(20); // stmt_type
        payload.extend_from_slice(&0_i32.to_be_bytes()); // num_markers
        payload.push(0); // updatable_flag
        payload.extend_from_slice(&0_i32.to_be_bytes()); // column_count = 0

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 2, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
    }

    #[test]
    fn test_execute_response_include_column_info_partial_header() {
        // include_column_info=1 but not enough data after the 9-byte header
        // for updatable_flag. Should still succeed (graceful handling).
        let mut payload = Vec::new();
        payload.push(0); // cache_reusable
        payload.extend_from_slice(&1_i32.to_be_bytes()); // result_count = 1
        payload.push(20); // INSERT
        payload.extend_from_slice(&1_i32.to_be_bytes());
        payload.extend_from_slice(&[0u8; 8]); // OID
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());

        payload.push(1); // includeColumnInfo = 1
        payload.extend_from_slice(&0_i32.to_be_bytes()); // result_cache_lifetime
        payload.push(20); // stmt_type
        payload.extend_from_slice(&0_i32.to_be_bytes()); // num_markers
        // No updatable_flag or column_count follows

        let data = build_frame([0, 0, 0, 0], 1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let resp = ExecuteResponse::parse(&frame, 2, &[]).unwrap();
        assert_eq!(resp.total_tuple_count, 1);
    }

    // -----------------------------------------------------------------------
    // FetchResult: truncated tuple count (lines 994-995)
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_result_truncated_tuple_count() {
        // Empty payload: not enough for 4-byte tuple_count
        let data = build_frame([0, 0, 0, 0], 0, &[0, 0]); // only 2 bytes
        let frame = ResponseFrame::parse(data).unwrap();
        let result = FetchResult::parse(&frame, &[CubridDataType::Int]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated fetch tuple count"));
    }

    // -----------------------------------------------------------------------
    // DbParameterResponse: error path (line 1079)
    // -----------------------------------------------------------------------

    #[test]
    fn test_db_parameter_response_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&(-1001_i32).to_be_bytes());
        payload.extend_from_slice(b"internal error\0");

        let data = build_frame([0, 0, 0, 0], -1, &payload);
        let frame = ResponseFrame::parse(data).unwrap();
        let result = DbParameterResponse::parse(&frame);
        assert!(result.is_err());
    }
}
