//! Wire protocol type codes, function codes, and statement types.
//!
//! This module defines the enumerations that correspond to constants in the
//! CUBRID CAS protocol (`cas_protocol.h`). All values match the on-the-wire
//! byte representations used in the binary protocol.

use crate::error::Error;

// ---------------------------------------------------------------------------
// Collection type bitmask constants
// ---------------------------------------------------------------------------

/// Bitmask for SET collection type encoding.
pub const CCI_CODE_SET: u8 = 0x20;

/// Bitmask for MULTISET collection type encoding.
pub const CCI_CODE_MULTISET: u8 = 0x40;

/// Bitmask for SEQUENCE (LIST) collection type encoding.
pub const CCI_CODE_SEQUENCE: u8 = 0x60;

/// Bitmask to extract the collection code from a column type byte.
pub const CCI_CODE_COLLECTION_MASK: u8 = 0x60;

// ---------------------------------------------------------------------------
// CubridDataType
// ---------------------------------------------------------------------------

/// CUBRID data type codes as transmitted over the wire.
///
/// These values are defined in `cas_protocol.h` as `T_CCI_U_TYPE` and
/// represent the type of a column value or bind parameter in the binary
/// protocol. The discriminant values match the wire encoding exactly.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CubridDataType {
    /// NULL or unknown type.
    Null = 0,
    /// Fixed-length character string (CHAR).
    Char = 1,
    /// Variable-length character string (VARCHAR / STRING).
    String = 2,
    /// Fixed-length national character string (NCHAR).
    NChar = 3,
    /// Variable-length national character string (VARNCHAR).
    VarNChar = 4,
    /// Fixed-length binary (BIT).
    Bit = 5,
    /// Variable-length binary (VARBIT).
    VarBit = 6,
    /// Arbitrary precision numeric (NUMERIC / DECIMAL).
    Numeric = 7,
    /// 32-bit signed integer.
    Int = 8,
    /// 16-bit signed integer (SMALLINT).
    Short = 9,
    /// Monetary value (encoded as DOUBLE).
    Monetary = 10,
    /// 32-bit IEEE 754 floating point.
    Float = 11,
    /// 64-bit IEEE 754 floating point.
    Double = 12,
    /// Calendar date (year, month, day).
    Date = 13,
    /// Time of day (hour, minute, second).
    Time = 14,
    /// Date and time without fractional seconds.
    Timestamp = 15,
    /// Unordered collection of unique elements.
    Set = 16,
    /// Unordered collection allowing duplicates.
    MultiSet = 17,
    /// Ordered collection (list).
    Sequence = 18,
    /// Object identifier (OID).
    Object = 19,
    /// Nested result set handle.
    ResultSet = 20,
    /// 64-bit signed integer.
    BigInt = 21,
    /// Date and time with millisecond precision.
    DateTime = 22,
    /// Binary large object.
    Blob = 23,
    /// Character large object.
    Clob = 24,
    /// Enumeration type.
    Enum = 25,
    /// 16-bit unsigned integer (CUBRID 10.0+).
    UShort = 26,
    /// 32-bit unsigned integer (CUBRID 10.0+).
    UInt = 27,
    /// 64-bit unsigned integer (CUBRID 10.0+).
    UBigInt = 28,
    /// Timestamp with timezone (CUBRID 10.0+, PROTOCOL_V7).
    TimestampTz = 29,
    /// Timestamp with local timezone (CUBRID 10.0+, PROTOCOL_V7).
    TimestampLtz = 30,
    /// DateTime with timezone (CUBRID 10.0+, PROTOCOL_V7).
    DateTimeTz = 31,
    /// DateTime with local timezone (CUBRID 10.0+, PROTOCOL_V7).
    DateTimeLtz = 32,
    /// JSON document type (CUBRID 11.2+, PROTOCOL_V8).
    Json = 34,
}

impl CubridDataType {
    /// The highest valid type code.
    pub const MAX_VALUE: u8 = 34;

    /// Returns `true` if this type represents a LOB (BLOB or CLOB).
    pub fn is_lob(self) -> bool {
        matches!(self, CubridDataType::Blob | CubridDataType::Clob)
    }

    /// Returns `true` if this type represents a collection (SET, MULTISET, SEQUENCE).
    pub fn is_collection(self) -> bool {
        matches!(
            self,
            CubridDataType::Set | CubridDataType::MultiSet | CubridDataType::Sequence
        )
    }

    /// Returns `true` if this type represents a date/time variant.
    pub fn is_temporal(self) -> bool {
        matches!(
            self,
            CubridDataType::Date
                | CubridDataType::Time
                | CubridDataType::Timestamp
                | CubridDataType::DateTime
                | CubridDataType::TimestampTz
                | CubridDataType::TimestampLtz
                | CubridDataType::DateTimeTz
                | CubridDataType::DateTimeLtz
        )
    }

    /// Returns `true` if this type represents a string variant.
    pub fn is_string(self) -> bool {
        matches!(
            self,
            CubridDataType::Char
                | CubridDataType::String
                | CubridDataType::NChar
                | CubridDataType::VarNChar
        )
    }

    /// Returns `true` if this type is an integer variant (signed or unsigned).
    pub fn is_integer(self) -> bool {
        matches!(
            self,
            CubridDataType::Short
                | CubridDataType::Int
                | CubridDataType::BigInt
                | CubridDataType::UShort
                | CubridDataType::UInt
                | CubridDataType::UBigInt
        )
    }
}

impl TryFrom<u8> for CubridDataType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CubridDataType::Null),
            1 => Ok(CubridDataType::Char),
            2 => Ok(CubridDataType::String),
            3 => Ok(CubridDataType::NChar),
            4 => Ok(CubridDataType::VarNChar),
            5 => Ok(CubridDataType::Bit),
            6 => Ok(CubridDataType::VarBit),
            7 => Ok(CubridDataType::Numeric),
            8 => Ok(CubridDataType::Int),
            9 => Ok(CubridDataType::Short),
            10 => Ok(CubridDataType::Monetary),
            11 => Ok(CubridDataType::Float),
            12 => Ok(CubridDataType::Double),
            13 => Ok(CubridDataType::Date),
            14 => Ok(CubridDataType::Time),
            15 => Ok(CubridDataType::Timestamp),
            16 => Ok(CubridDataType::Set),
            17 => Ok(CubridDataType::MultiSet),
            18 => Ok(CubridDataType::Sequence),
            19 => Ok(CubridDataType::Object),
            20 => Ok(CubridDataType::ResultSet),
            21 => Ok(CubridDataType::BigInt),
            22 => Ok(CubridDataType::DateTime),
            23 => Ok(CubridDataType::Blob),
            24 => Ok(CubridDataType::Clob),
            25 => Ok(CubridDataType::Enum),
            26 => Ok(CubridDataType::UShort),
            27 => Ok(CubridDataType::UInt),
            28 => Ok(CubridDataType::UBigInt),
            29 => Ok(CubridDataType::TimestampTz),
            30 => Ok(CubridDataType::TimestampLtz),
            31 => Ok(CubridDataType::DateTimeTz),
            32 => Ok(CubridDataType::DateTimeLtz),
            // Note: 33 is reserved (TimeTz, internal use only)
            34 => Ok(CubridDataType::Json),
            _ => Err(Error::UnknownDataType(value)),
        }
    }
}

impl From<CubridDataType> for u8 {
    fn from(dt: CubridDataType) -> u8 {
        dt as u8
    }
}

// ---------------------------------------------------------------------------
// FunctionCode
// ---------------------------------------------------------------------------

/// CAS function codes identifying request message types.
///
/// Each request to the CAS server begins with a single-byte function code
/// that tells the server which operation to perform. These values are
/// defined in `cas_protocol.h` as `t_cas_func_code`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionCode {
    /// Commit or rollback a transaction.
    EndTran = 1,
    /// Prepare a SQL statement for execution.
    Prepare = 2,
    /// Execute a previously prepared statement.
    Execute = 3,
    /// Retrieve a database parameter value.
    GetDbParameter = 4,
    /// Set a database parameter value.
    SetDbParameter = 5,
    /// Release a prepared statement handle.
    CloseReqHandle = 6,
    /// Move the cursor position within a result set.
    Cursor = 7,
    /// Fetch rows from an open result set.
    Fetch = 8,
    /// Query database schema metadata.
    SchemaInfo = 9,
    /// Retrieve an object by OID.
    OidGet = 10,
    /// Update an object by OID.
    OidPut = 11,
    /// Retrieve the database server version string.
    GetDbVersion = 15,
    /// Retrieve the number of objects in a class.
    GetClassNumObjs = 16,
    /// Execute an OID-related command.
    OidCmd = 17,
    /// Manipulate a collection (SET/MULTISET/SEQUENCE) column.
    Collection = 18,
    /// Advance to the next result set in a multi-result query.
    NextResult = 19,
    /// Execute multiple SQL statements in a batch.
    ExecuteBatch = 20,
    /// Execute an array of parameter sets against a prepared statement.
    ExecuteArray = 21,
    /// Update a row through a cursor.
    CursorUpdate = 22,
    /// Retrieve the attribute type string for a class.
    GetAttrTypeStr = 23,
    /// Retrieve query execution plan information.
    GetQueryInfo = 24,
    /// Create or release a transaction savepoint.
    Savepoint = 26,
    /// Retrieve parameter metadata for a prepared statement.
    ParameterInfo = 27,
    /// XA distributed transaction: prepare phase.
    XaPrepare = 28,
    /// XA distributed transaction: recover pending transactions.
    XaRecover = 29,
    /// XA distributed transaction: end (commit/rollback).
    XaEndTran = 30,
    /// Close the CAS connection.
    ConClose = 31,
    /// Check if the CAS process is alive (health check).
    CheckCas = 32,
    /// Create an out-of-band result set (PROTOCOL_V11+).
    MakeOutRs = 33,
    /// Retrieve auto-generated keys from the last INSERT.
    GetGeneratedKeys = 34,
    /// Create a new LOB (large object) handle.
    LobNew = 35,
    /// Write data to a LOB.
    LobWrite = 36,
    /// Read data from a LOB.
    LobRead = 37,
    /// End the current database session.
    EndSession = 38,
    /// Retrieve the number of affected rows from the last operation.
    GetRowCount = 39,
    /// Retrieve the last auto-generated insert ID.
    GetLastInsertId = 40,
    /// Prepare and execute a SQL statement in a single round trip.
    PrepareAndExecute = 41,
    /// Close a server-side cursor.
    CursorClose = 42,
    /// Retrieve shard routing information (PROTOCOL_V5+).
    GetShardInfo = 43,
    /// Switch the CAS between transaction and query mode.
    CasChangeMode = 44,
}

impl TryFrom<u8> for FunctionCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(FunctionCode::EndTran),
            2 => Ok(FunctionCode::Prepare),
            3 => Ok(FunctionCode::Execute),
            4 => Ok(FunctionCode::GetDbParameter),
            5 => Ok(FunctionCode::SetDbParameter),
            6 => Ok(FunctionCode::CloseReqHandle),
            7 => Ok(FunctionCode::Cursor),
            8 => Ok(FunctionCode::Fetch),
            9 => Ok(FunctionCode::SchemaInfo),
            10 => Ok(FunctionCode::OidGet),
            11 => Ok(FunctionCode::OidPut),
            15 => Ok(FunctionCode::GetDbVersion),
            16 => Ok(FunctionCode::GetClassNumObjs),
            17 => Ok(FunctionCode::OidCmd),
            18 => Ok(FunctionCode::Collection),
            19 => Ok(FunctionCode::NextResult),
            20 => Ok(FunctionCode::ExecuteBatch),
            21 => Ok(FunctionCode::ExecuteArray),
            22 => Ok(FunctionCode::CursorUpdate),
            23 => Ok(FunctionCode::GetAttrTypeStr),
            24 => Ok(FunctionCode::GetQueryInfo),
            26 => Ok(FunctionCode::Savepoint),
            27 => Ok(FunctionCode::ParameterInfo),
            28 => Ok(FunctionCode::XaPrepare),
            29 => Ok(FunctionCode::XaRecover),
            30 => Ok(FunctionCode::XaEndTran),
            31 => Ok(FunctionCode::ConClose),
            32 => Ok(FunctionCode::CheckCas),
            33 => Ok(FunctionCode::MakeOutRs),
            34 => Ok(FunctionCode::GetGeneratedKeys),
            35 => Ok(FunctionCode::LobNew),
            36 => Ok(FunctionCode::LobWrite),
            37 => Ok(FunctionCode::LobRead),
            38 => Ok(FunctionCode::EndSession),
            39 => Ok(FunctionCode::GetRowCount),
            40 => Ok(FunctionCode::GetLastInsertId),
            41 => Ok(FunctionCode::PrepareAndExecute),
            42 => Ok(FunctionCode::CursorClose),
            43 => Ok(FunctionCode::GetShardInfo),
            44 => Ok(FunctionCode::CasChangeMode),
            _ => Err(Error::UnknownFunctionCode(value)),
        }
    }
}

impl From<FunctionCode> for u8 {
    fn from(fc: FunctionCode) -> u8 {
        fc as u8
    }
}

// ---------------------------------------------------------------------------
// StatementType
// ---------------------------------------------------------------------------

/// Types of SQL statements as reported by the PREPARE response.
///
/// The CAS server classifies each prepared statement into one of these
/// categories, which determines how the client should handle the result
/// (e.g., whether to expect rows or an affected count).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementType {
    /// ALTER CLASS/TABLE statement.
    AlterClass = 0,
    /// ALTER SERIAL statement.
    AlterSerial = 1,
    /// COMMIT WORK statement.
    CommitWork = 2,
    /// REGISTER DATABASE statement.
    RegisterDatabase = 3,
    /// CREATE CLASS/TABLE statement.
    CreateClass = 4,
    /// CREATE INDEX statement.
    CreateIndex = 5,
    /// CREATE TRIGGER statement.
    CreateTrigger = 6,
    /// CREATE SERIAL statement.
    CreateSerial = 7,
    /// DROP DATABASE statement.
    DropDatabase = 8,
    /// DROP CLASS/TABLE statement.
    DropClass = 9,
    /// DROP INDEX statement.
    DropIndex = 10,
    /// DROP LABEL statement.
    DropLabel = 11,
    /// DROP TRIGGER statement.
    DropTrigger = 12,
    /// DROP SERIAL statement.
    DropSerial = 13,
    /// EVALUATE statement.
    Evaluate = 14,
    /// RENAME CLASS/TABLE statement.
    RenameClass = 15,
    /// ROLLBACK WORK statement.
    RollbackWork = 16,
    /// GRANT statement.
    Grant = 17,
    /// REVOKE statement.
    Revoke = 18,
    /// UPDATE STATISTICS statement.
    UpdateStats = 19,
    /// INSERT statement.
    Insert = 20,
    /// SELECT statement.
    Select = 21,
    /// UPDATE statement.
    Update = 22,
    /// DELETE statement.
    Delete = 23,
    /// CALL stored procedure.
    Call = 24,
    /// GET ISOLATION LEVEL statement.
    GetIsoLvl = 25,
    /// GET TIMEOUT statement.
    GetTimeout = 26,
    /// GET OPTIMIZATION LEVEL statement.
    GetOptLvl = 27,
    /// SET OPTIMIZATION LEVEL statement.
    SetOptLvl = 28,
    /// SCOPE statement.
    Scope = 29,
    /// GET TRIGGER statement.
    GetTrigger = 30,
    /// SET TRIGGER statement.
    SetTrigger = 31,
    /// SAVEPOINT statement.
    Savepoint = 32,
    /// PREPARE statement (server-side).
    Prepare = 33,
    /// ATTACH statement.
    Attach = 34,
    /// USE statement.
    Use = 35,
    /// REMOVE TRIGGER statement.
    RemoveTrigger = 36,
    /// RENAME TRIGGER statement.
    RenameTrigger = 37,
    /// ON LDB statement.
    OnLdb = 38,
    /// GET LDB statement.
    GetLdb = 39,
    /// SET LDB statement.
    SetLdb = 40,
    /// GET STATISTICS statement.
    GetStats = 41,
    /// CREATE USER statement.
    CreateUser = 42,
    /// DROP USER statement.
    DropUser = 43,
    /// ALTER USER statement.
    AlterUser = 44,
    /// SET SYSTEM PARAMETERS statement.
    SetSysParams = 45,
    /// ALTER INDEX statement.
    AlterIndex = 46,
    /// CREATE STORED PROCEDURE statement.
    CreateStoredProcedure = 47,
    /// DROP STORED PROCEDURE statement.
    DropStoredProcedure = 48,
    /// PREPARE STATEMENT (server-side dynamic SQL).
    PrepareStatement = 49,
    /// EXECUTE PREPARE (server-side dynamic SQL).
    ExecutePrepare = 50,
    /// DEALLOCATE PREPARE (server-side dynamic SQL).
    DeallocatePrepare = 51,
    /// TRUNCATE TABLE statement.
    Truncate = 52,
    /// DO statement (execute expression without result).
    Do = 53,
    /// SELECT ... FOR UPDATE statement.
    SelectUpdate = 54,
    /// SET SESSION VARIABLES statement.
    SetSessionVariables = 55,
    /// DROP SESSION VARIABLES statement.
    DropSessionVariables = 56,
    /// MERGE INTO ... USING ... ON ... statement (CUBRID-specific).
    Merge = 57,
    /// SET NAMES statement (character set).
    SetNames = 58,
    /// ALTER STORED PROCEDURE / ALTER STORED PROCEDURE OWNER statement.
    AlterStoredProcedure = 59,
    /// KILL statement (terminate a query or connection).
    Kill = 60,
    /// VACUUM statement (reclaim storage).
    Vacuum = 61,
    /// SET TIMEZONE statement.
    SetTimezone = 62,
    /// CALL stored procedure (extended, 0x7e).
    CallSp = 0x7e,
    /// Unknown statement type (0x7f).
    Unknown = 0x7f,
}

impl StatementType {
    /// Returns `true` if this statement type produces a row-based result set.
    pub fn has_result_set(self) -> bool {
        matches!(
            self,
            StatementType::Select
                | StatementType::Call
                | StatementType::CallSp
                | StatementType::Evaluate
                | StatementType::SelectUpdate
                | StatementType::GetStats
                | StatementType::GetIsoLvl
                | StatementType::GetTimeout
                | StatementType::GetOptLvl
                | StatementType::GetTrigger
                | StatementType::GetLdb
        )
    }

    /// Returns `true` if this is a DML statement that modifies data.
    pub fn is_dml(self) -> bool {
        matches!(
            self,
            StatementType::Insert
                | StatementType::Update
                | StatementType::Delete
                | StatementType::Merge
        )
    }

    /// Returns `true` if this is a DDL statement that modifies schema.
    pub fn is_ddl(self) -> bool {
        matches!(
            self,
            StatementType::AlterClass
                | StatementType::AlterSerial
                | StatementType::AlterIndex
                | StatementType::CreateClass
                | StatementType::CreateIndex
                | StatementType::CreateTrigger
                | StatementType::CreateSerial
                | StatementType::CreateUser
                | StatementType::CreateStoredProcedure
                | StatementType::DropDatabase
                | StatementType::DropClass
                | StatementType::DropIndex
                | StatementType::DropTrigger
                | StatementType::DropSerial
                | StatementType::DropUser
                | StatementType::DropStoredProcedure
                | StatementType::RenameClass
                | StatementType::Truncate
                | StatementType::Grant
                | StatementType::Revoke
        )
    }
}

impl TryFrom<u8> for StatementType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StatementType::AlterClass),
            1 => Ok(StatementType::AlterSerial),
            2 => Ok(StatementType::CommitWork),
            3 => Ok(StatementType::RegisterDatabase),
            4 => Ok(StatementType::CreateClass),
            5 => Ok(StatementType::CreateIndex),
            6 => Ok(StatementType::CreateTrigger),
            7 => Ok(StatementType::CreateSerial),
            8 => Ok(StatementType::DropDatabase),
            9 => Ok(StatementType::DropClass),
            10 => Ok(StatementType::DropIndex),
            11 => Ok(StatementType::DropLabel),
            12 => Ok(StatementType::DropTrigger),
            13 => Ok(StatementType::DropSerial),
            14 => Ok(StatementType::Evaluate),
            15 => Ok(StatementType::RenameClass),
            16 => Ok(StatementType::RollbackWork),
            17 => Ok(StatementType::Grant),
            18 => Ok(StatementType::Revoke),
            19 => Ok(StatementType::UpdateStats),
            20 => Ok(StatementType::Insert),
            21 => Ok(StatementType::Select),
            22 => Ok(StatementType::Update),
            23 => Ok(StatementType::Delete),
            24 => Ok(StatementType::Call),
            25 => Ok(StatementType::GetIsoLvl),
            26 => Ok(StatementType::GetTimeout),
            27 => Ok(StatementType::GetOptLvl),
            28 => Ok(StatementType::SetOptLvl),
            29 => Ok(StatementType::Scope),
            30 => Ok(StatementType::GetTrigger),
            31 => Ok(StatementType::SetTrigger),
            32 => Ok(StatementType::Savepoint),
            33 => Ok(StatementType::Prepare),
            34 => Ok(StatementType::Attach),
            35 => Ok(StatementType::Use),
            36 => Ok(StatementType::RemoveTrigger),
            37 => Ok(StatementType::RenameTrigger),
            38 => Ok(StatementType::OnLdb),
            39 => Ok(StatementType::GetLdb),
            40 => Ok(StatementType::SetLdb),
            41 => Ok(StatementType::GetStats),
            42 => Ok(StatementType::CreateUser),
            43 => Ok(StatementType::DropUser),
            44 => Ok(StatementType::AlterUser),
            45 => Ok(StatementType::SetSysParams),
            46 => Ok(StatementType::AlterIndex),
            47 => Ok(StatementType::CreateStoredProcedure),
            48 => Ok(StatementType::DropStoredProcedure),
            49 => Ok(StatementType::PrepareStatement),
            50 => Ok(StatementType::ExecutePrepare),
            51 => Ok(StatementType::DeallocatePrepare),
            52 => Ok(StatementType::Truncate),
            53 => Ok(StatementType::Do),
            54 => Ok(StatementType::SelectUpdate),
            55 => Ok(StatementType::SetSessionVariables),
            56 => Ok(StatementType::DropSessionVariables),
            57 => Ok(StatementType::Merge),
            58 => Ok(StatementType::SetNames),
            59 => Ok(StatementType::AlterStoredProcedure),
            60 => Ok(StatementType::Kill),
            61 => Ok(StatementType::Vacuum),
            62 => Ok(StatementType::SetTimezone),
            0x7e => Ok(StatementType::CallSp),
            0x7f => Ok(StatementType::Unknown),
            _ => Err(Error::InvalidMessage(format!(
                "unknown statement type: {}",
                value
            ))),
        }
    }
}

impl From<StatementType> for u8 {
    fn from(st: StatementType) -> u8 {
        st as u8
    }
}

// ---------------------------------------------------------------------------
// TransactionOp
// ---------------------------------------------------------------------------

/// Transaction operations for the END_TRAN function code.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionOp {
    /// Commit the current transaction.
    Commit = 1,
    /// Rollback the current transaction.
    Rollback = 2,
}

impl TryFrom<u8> for TransactionOp {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TransactionOp::Commit),
            2 => Ok(TransactionOp::Rollback),
            _ => Err(Error::InvalidMessage(format!(
                "unknown transaction op: {}",
                value
            ))),
        }
    }
}

impl From<TransactionOp> for u8 {
    fn from(op: TransactionOp) -> u8 {
        op as u8
    }
}

// ---------------------------------------------------------------------------
// SchemaType
// ---------------------------------------------------------------------------

/// Schema information request types for the SCHEMA_INFO function code.
///
/// Used to query different categories of database metadata (tables, columns,
/// constraints, etc.).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaType {
    /// List all tables (classes).
    Class = 1,
    /// List all views (virtual classes).
    VClass = 2,
    /// List column attributes for a table.
    Attribute = 4,
    /// List constraints on a table.
    Constraint = 11,
    /// List primary key columns.
    PrimaryKey = 16,
    /// List imported (referenced) foreign keys.
    ImportedKeys = 17,
    /// List exported (referencing) foreign keys.
    ExportedKeys = 18,
}

impl TryFrom<u8> for SchemaType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(SchemaType::Class),
            2 => Ok(SchemaType::VClass),
            4 => Ok(SchemaType::Attribute),
            11 => Ok(SchemaType::Constraint),
            16 => Ok(SchemaType::PrimaryKey),
            17 => Ok(SchemaType::ImportedKeys),
            18 => Ok(SchemaType::ExportedKeys),
            _ => Err(Error::InvalidMessage(format!(
                "unknown schema type: {}",
                value
            ))),
        }
    }
}

impl From<SchemaType> for u8 {
    fn from(st: SchemaType) -> u8 {
        st as u8
    }
}

// ---------------------------------------------------------------------------
// DbParameter
// ---------------------------------------------------------------------------

/// Database parameter identifiers for GET_DB_PARAMETER and SET_DB_PARAMETER.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbParameter {
    /// Transaction isolation level.
    IsolationLevel = 1,
    /// Lock wait timeout in seconds.
    LockTimeout = 2,
    /// Maximum string length for result values.
    MaxStringLength = 3,
    /// Auto-commit mode (0 = off, 1 = on).
    AutoCommit = 4,
}

impl TryFrom<i32> for DbParameter {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(DbParameter::IsolationLevel),
            2 => Ok(DbParameter::LockTimeout),
            3 => Ok(DbParameter::MaxStringLength),
            4 => Ok(DbParameter::AutoCommit),
            _ => Err(Error::InvalidMessage(format!(
                "unknown db parameter: {}",
                value
            ))),
        }
    }
}

impl From<DbParameter> for i32 {
    fn from(p: DbParameter) -> i32 {
        p as i32
    }
}

// ---------------------------------------------------------------------------
// PrepareFlag
// ---------------------------------------------------------------------------

/// Flags that modify the behavior of a PREPARE request.
///
/// Multiple flags can be combined using bitwise OR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareFlag(u8);

impl PrepareFlag {
    /// Normal prepare with no special options.
    pub const NORMAL: Self = PrepareFlag(0x00);
    /// Include OID information in the result set.
    pub const INCLUDE_OID: Self = PrepareFlag(0x01);
    /// Allow updates through the result set cursor.
    pub const UPDATABLE: Self = PrepareFlag(0x02);
    /// Return query plan information.
    pub const QUERY_INFO: Self = PrepareFlag(0x04);
    /// Keep the result set cursor open across transactions.
    pub const HOLDABLE: Self = PrepareFlag(0x08);
    /// Pin XASL cache entry for retry.
    pub const XASL_CACHE_PINNED: Self = PrepareFlag(0x10);
    /// Prepare as a stored procedure call.
    pub const CALL: Self = PrepareFlag(0x40);

    /// Create a flag set from a raw byte value.
    pub const fn from_raw(value: u8) -> Self {
        PrepareFlag(value)
    }

    /// Get the raw byte value.
    pub const fn as_raw(self) -> u8 {
        self.0
    }

    /// Combine two flag sets using bitwise OR.
    pub const fn union(self, other: Self) -> Self {
        PrepareFlag(self.0 | other.0)
    }

    /// Check if a specific flag is set.
    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) == flag.0
    }
}

// ---------------------------------------------------------------------------
// ExecuteFlag
// ---------------------------------------------------------------------------

/// Flags that modify the behavior of an EXECUTE request.
///
/// Multiple flags can be combined using bitwise OR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecuteFlag(u8);

impl ExecuteFlag {
    /// Normal execution.
    pub const NORMAL: Self = ExecuteFlag(0x00);
    /// Asynchronous execution (deprecated, kept for protocol compatibility).
    pub const ASYNC: Self = ExecuteFlag(0x01);
    /// Execute and fetch all result rows in one round trip.
    pub const QUERY_ALL: Self = ExecuteFlag(0x02);
    /// Return query plan information.
    pub const QUERY_INFO: Self = ExecuteFlag(0x04);
    /// Return only the query execution plan, do not execute.
    pub const ONLY_QUERY_PLAN: Self = ExecuteFlag(0x08);
    /// Execute in a separate thread (server-side).
    pub const THREAD: Self = ExecuteFlag(0x10);
    /// Keep the result set holdable across transactions.
    pub const HOLDABLE: Self = ExecuteFlag(0x20);
    /// Return auto-generated keys after INSERT.
    pub const RETURN_GENERATED_KEYS: Self = ExecuteFlag(0x40);

    /// Create a flag set from a raw byte value.
    pub const fn from_raw(value: u8) -> Self {
        ExecuteFlag(value)
    }

    /// Get the raw byte value.
    pub const fn as_raw(self) -> u8 {
        self.0
    }

    /// Combine two flag sets using bitwise OR.
    pub const fn union(self, other: Self) -> Self {
        ExecuteFlag(self.0 | other.0)
    }

    /// Check if a specific flag is set.
    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) == flag.0
    }
}

// ---------------------------------------------------------------------------
// ClientType
// ---------------------------------------------------------------------------

/// CAS client type identifiers sent during the handshake.
///
/// The client type tells the broker which driver is connecting, allowing
/// the server to adjust behavior accordingly.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientType {
    /// No specific client type.
    None = 0,
    /// CCI (C Client Interface) client.
    Cci = 1,
    /// ODBC driver client.
    Odbc = 2,
    /// JDBC driver client.
    Jdbc = 3,
    /// PHP driver client.
    Php = 4,
    /// OLEDB driver client.
    OleDb = 5,
}

impl TryFrom<u8> for ClientType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ClientType::None),
            1 => Ok(ClientType::Cci),
            2 => Ok(ClientType::Odbc),
            3 => Ok(ClientType::Jdbc),
            4 => Ok(ClientType::Php),
            5 => Ok(ClientType::OleDb),
            _ => Err(Error::InvalidMessage(format!(
                "unknown client type: {}",
                value
            ))),
        }
    }
}

impl From<ClientType> for u8 {
    fn from(ct: ClientType) -> u8 {
        ct as u8
    }
}

// ---------------------------------------------------------------------------
// DbmsType
// ---------------------------------------------------------------------------

/// DBMS type identifiers returned in the broker info during handshake.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbmsType {
    /// No DBMS type specified.
    None = 0,
    /// CUBRID database.
    Cubrid = 1,
    /// MySQL (via gateway).
    Mysql = 2,
    /// Oracle (via gateway).
    Oracle = 3,
}

impl TryFrom<u8> for DbmsType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DbmsType::None),
            1 => Ok(DbmsType::Cubrid),
            2 => Ok(DbmsType::Mysql),
            3 => Ok(DbmsType::Oracle),
            _ => Err(Error::InvalidMessage(format!(
                "unknown DBMS type: {}",
                value
            ))),
        }
    }
}

impl From<DbmsType> for u8 {
    fn from(dt: DbmsType) -> u8 {
        dt as u8
    }
}

// ---------------------------------------------------------------------------
// XaOp
// ---------------------------------------------------------------------------

/// XA end transaction operations for the XA_END_TRAN function code.
///
/// Used to commit or rollback a prepared XA distributed transaction
/// during the second phase of two-phase commit (2PC).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XaOp {
    /// Commit a prepared XA transaction.
    Commit = 1,
    /// Rollback a prepared XA transaction.
    Rollback = 2,
}

impl TryFrom<u8> for XaOp {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(XaOp::Commit),
            2 => Ok(XaOp::Rollback),
            _ => Err(Error::InvalidMessage(format!(
                "unknown XA op: {}",
                value
            ))),
        }
    }
}

impl From<XaOp> for u8 {
    fn from(op: XaOp) -> u8 {
        op as u8
    }
}

// ---------------------------------------------------------------------------
// Xid (XA Transaction Identifier)
// ---------------------------------------------------------------------------

/// An XA transaction identifier following the X/Open XA specification.
///
/// Used with [`xa_prepare`](crate::message::frontend::xa_prepare),
/// [`xa_recover`](crate::message::frontend::xa_recover), and
/// [`xa_end_tran`](crate::message::frontend::xa_end_tran) for distributed
/// transaction management.
///
/// # Wire format
///
/// ```text
/// [4 bytes] format_id              (big-endian i32)
/// [4 bytes] gtrid_length           (big-endian i32)
/// [4 bytes] bqual_length           (big-endian i32)
/// [N bytes] global_transaction_id  (gtrid_length bytes)
/// [M bytes] branch_qualifier       (bqual_length bytes)
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Xid {
    /// Identifies the format of the global transaction ID and branch qualifier.
    pub format_id: i32,
    /// The global transaction identifier (max 64 bytes per X/Open spec).
    pub global_transaction_id: Vec<u8>,
    /// The branch qualifier (max 64 bytes per X/Open spec).
    pub branch_qualifier: Vec<u8>,
}

impl Xid {
    /// Create a new XA transaction identifier.
    pub fn new(format_id: i32, gtrid: Vec<u8>, bqual: Vec<u8>) -> Self {
        Self {
            format_id,
            global_transaction_id: gtrid,
            branch_qualifier: bqual,
        }
    }

    /// Serialize this XID into the CUBRID wire format.
    pub fn encode(&self) -> Vec<u8> {
        let total = 12 + self.global_transaction_id.len() + self.branch_qualifier.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&self.format_id.to_be_bytes());
        buf.extend_from_slice(&(self.global_transaction_id.len() as i32).to_be_bytes());
        buf.extend_from_slice(&(self.branch_qualifier.len() as i32).to_be_bytes());
        buf.extend_from_slice(&self.global_transaction_id);
        buf.extend_from_slice(&self.branch_qualifier);
        buf
    }

    /// Deserialize an XID from the CUBRID wire format.
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 12 {
            return Err(Error::InvalidMessage(format!(
                "XID too short: {} bytes (need at least 12)",
                data.len()
            )));
        }
        let format_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let gtrid_len = i32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let bqual_len = i32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let expected = 12 + gtrid_len + bqual_len;
        if data.len() < expected {
            return Err(Error::InvalidMessage(format!(
                "XID truncated: {} bytes (expected {})",
                data.len(),
                expected
            )));
        }
        let gtrid = data[12..12 + gtrid_len].to_vec();
        let bqual = data[12 + gtrid_len..12 + gtrid_len + bqual_len].to_vec();
        Ok(Xid {
            format_id,
            global_transaction_id: gtrid,
            branch_qualifier: bqual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // CubridDataType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_data_type_round_trip_all_valid() {
        // All valid type codes should round-trip through u8 conversion.
        let valid_codes: &[u8] = &[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34,
        ];
        for &code in valid_codes {
            let dt = CubridDataType::try_from(code).unwrap();
            assert_eq!(u8::from(dt), code, "round-trip failed for code {}", code);
        }
    }

    #[test]
    fn test_data_type_invalid_codes() {
        // Code 33 (TimeTz, reserved internal) should fail
        assert!(CubridDataType::try_from(33).is_err());
        // Codes above 34 should fail
        assert!(CubridDataType::try_from(35).is_err());
        assert!(CubridDataType::try_from(255).is_err());
    }

    #[test]
    fn test_data_type_is_lob() {
        assert!(CubridDataType::Blob.is_lob());
        assert!(CubridDataType::Clob.is_lob());
        assert!(!CubridDataType::String.is_lob());
        assert!(!CubridDataType::Int.is_lob());
    }

    #[test]
    fn test_data_type_is_collection() {
        assert!(CubridDataType::Set.is_collection());
        assert!(CubridDataType::MultiSet.is_collection());
        assert!(CubridDataType::Sequence.is_collection());
        assert!(!CubridDataType::String.is_collection());
    }

    #[test]
    fn test_data_type_is_temporal() {
        let temporal = [
            CubridDataType::Date,
            CubridDataType::Time,
            CubridDataType::Timestamp,
            CubridDataType::DateTime,
            CubridDataType::TimestampTz,
            CubridDataType::TimestampLtz,
            CubridDataType::DateTimeTz,
            CubridDataType::DateTimeLtz,
        ];
        for dt in &temporal {
            assert!(dt.is_temporal(), "{:?} should be temporal", dt);
        }
        assert!(!CubridDataType::Int.is_temporal());
        assert!(!CubridDataType::String.is_temporal());
    }

    #[test]
    fn test_data_type_is_string() {
        assert!(CubridDataType::Char.is_string());
        assert!(CubridDataType::String.is_string());
        assert!(CubridDataType::NChar.is_string());
        assert!(CubridDataType::VarNChar.is_string());
        assert!(!CubridDataType::Int.is_string());
        assert!(!CubridDataType::Blob.is_string());
    }

    #[test]
    fn test_data_type_is_integer() {
        let integers = [
            CubridDataType::Short,
            CubridDataType::Int,
            CubridDataType::BigInt,
            CubridDataType::UShort,
            CubridDataType::UInt,
            CubridDataType::UBigInt,
        ];
        for dt in &integers {
            assert!(dt.is_integer(), "{:?} should be integer", dt);
        }
        assert!(!CubridDataType::Float.is_integer());
        assert!(!CubridDataType::Double.is_integer());
        assert!(!CubridDataType::String.is_integer());
    }

    // -----------------------------------------------------------------------
    // FunctionCode tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_function_code_round_trip() {
        let valid_codes: &[u8] = &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26,
            27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
        ];
        for &code in valid_codes {
            let fc = FunctionCode::try_from(code).unwrap();
            assert_eq!(u8::from(fc), code, "round-trip failed for code {}", code);
        }
    }

    #[test]
    fn test_function_code_invalid() {
        assert!(FunctionCode::try_from(0).is_err());
        assert!(FunctionCode::try_from(12).is_err()); // deprecated
        assert!(FunctionCode::try_from(13).is_err()); // deprecated
        assert!(FunctionCode::try_from(14).is_err()); // deprecated
        assert!(FunctionCode::try_from(25).is_err()); // deprecated
        assert!(FunctionCode::try_from(45).is_err()); // beyond max
        assert!(FunctionCode::try_from(255).is_err());
    }

    #[test]
    fn test_function_code_cubrid_specific() {
        // OID operations (CUBRID object-relational)
        assert_eq!(FunctionCode::OidGet as u8, 10);
        assert_eq!(FunctionCode::OidPut as u8, 11);
        assert_eq!(FunctionCode::OidCmd as u8, 17);
        // Collection manipulation
        assert_eq!(FunctionCode::Collection as u8, 18);
        // XA distributed transactions
        assert_eq!(FunctionCode::XaPrepare as u8, 28);
        assert_eq!(FunctionCode::XaRecover as u8, 29);
        assert_eq!(FunctionCode::XaEndTran as u8, 30);
        // Generated keys (AUTO_INCREMENT)
        assert_eq!(FunctionCode::GetGeneratedKeys as u8, 34);
        // Shard
        assert_eq!(FunctionCode::GetShardInfo as u8, 43);
    }

    // -----------------------------------------------------------------------
    // StatementType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_statement_type_round_trip() {
        // All codes 0-62 should be valid, plus 0x7e and 0x7f
        for code in 0..=62_u8 {
            let st = StatementType::try_from(code).unwrap();
            assert_eq!(u8::from(st), code, "round-trip failed for code {}", code);
        }
        assert_eq!(
            u8::from(StatementType::try_from(0x7e).unwrap()),
            0x7e
        );
        assert_eq!(
            u8::from(StatementType::try_from(0x7f).unwrap()),
            0x7f
        );
    }

    #[test]
    fn test_statement_type_invalid() {
        assert!(StatementType::try_from(63).is_err());
        assert!(StatementType::try_from(100).is_err());
        assert!(StatementType::try_from(0x7d).is_err());
    }

    #[test]
    fn test_statement_type_has_result_set() {
        assert!(StatementType::Select.has_result_set());
        assert!(StatementType::Call.has_result_set());
        assert!(StatementType::CallSp.has_result_set());
        assert!(StatementType::SelectUpdate.has_result_set());
        assert!(StatementType::Evaluate.has_result_set());
        assert!(!StatementType::Insert.has_result_set());
        assert!(!StatementType::Update.has_result_set());
        assert!(!StatementType::Delete.has_result_set());
        assert!(!StatementType::CreateClass.has_result_set());
    }

    #[test]
    fn test_statement_type_is_dml() {
        assert!(StatementType::Insert.is_dml());
        assert!(StatementType::Update.is_dml());
        assert!(StatementType::Delete.is_dml());
        assert!(StatementType::Merge.is_dml());
        assert!(!StatementType::Select.is_dml());
        assert!(!StatementType::CreateClass.is_dml());
    }

    #[test]
    fn test_statement_type_is_ddl() {
        assert!(StatementType::CreateClass.is_ddl());
        assert!(StatementType::CreateIndex.is_ddl());
        assert!(StatementType::DropClass.is_ddl());
        assert!(StatementType::AlterClass.is_ddl());
        assert!(StatementType::Truncate.is_ddl());
        assert!(StatementType::Grant.is_ddl());
        assert!(!StatementType::Insert.is_ddl());
        assert!(!StatementType::Select.is_ddl());
    }

    #[test]
    fn test_statement_type_cubrid_specific() {
        // CUBRID object-relational features
        assert_eq!(StatementType::Merge as u8, 57);
        assert_eq!(StatementType::CreateSerial as u8, 7);
        assert_eq!(StatementType::DropSerial as u8, 13);
        assert_eq!(StatementType::CreateStoredProcedure as u8, 47);
        assert_eq!(StatementType::Kill as u8, 60);
        assert_eq!(StatementType::Vacuum as u8, 61);
        assert_eq!(StatementType::SetTimezone as u8, 62);
    }

    // -----------------------------------------------------------------------
    // TransactionOp tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_transaction_op_round_trip() {
        assert_eq!(
            u8::from(TransactionOp::try_from(1u8).unwrap()),
            1
        );
        assert_eq!(
            u8::from(TransactionOp::try_from(2u8).unwrap()),
            2
        );
    }

    #[test]
    fn test_transaction_op_invalid() {
        assert!(TransactionOp::try_from(0u8).is_err());
        assert!(TransactionOp::try_from(3u8).is_err());
    }

    // -----------------------------------------------------------------------
    // SchemaType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_type_round_trip() {
        let valid_codes: &[u8] = &[1, 2, 4, 11, 16, 17, 18];
        for &code in valid_codes {
            let st = SchemaType::try_from(code).unwrap();
            assert_eq!(u8::from(st), code);
        }
    }

    #[test]
    fn test_schema_type_invalid() {
        assert!(SchemaType::try_from(0).is_err());
        assert!(SchemaType::try_from(3).is_err());
        assert!(SchemaType::try_from(5).is_err());
    }

    // -----------------------------------------------------------------------
    // DbParameter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_db_parameter_round_trip() {
        let valid_values: &[i32] = &[1, 2, 3, 4];
        for &val in valid_values {
            let p = DbParameter::try_from(val).unwrap();
            assert_eq!(i32::from(p), val);
        }
    }

    #[test]
    fn test_db_parameter_invalid() {
        assert!(DbParameter::try_from(0).is_err());
        assert!(DbParameter::try_from(5).is_err());
    }

    // -----------------------------------------------------------------------
    // PrepareFlag tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_prepare_flag_normal() {
        assert_eq!(PrepareFlag::NORMAL.as_raw(), 0x00);
    }

    #[test]
    fn test_prepare_flag_union() {
        let combined = PrepareFlag::INCLUDE_OID.union(PrepareFlag::HOLDABLE);
        assert_eq!(combined.as_raw(), 0x01 | 0x08);
        assert!(combined.contains(PrepareFlag::INCLUDE_OID));
        assert!(combined.contains(PrepareFlag::HOLDABLE));
        assert!(!combined.contains(PrepareFlag::UPDATABLE));
    }

    #[test]
    fn test_prepare_flag_from_raw() {
        let flag = PrepareFlag::from_raw(0x42);
        assert!(flag.contains(PrepareFlag::UPDATABLE));
        assert!(flag.contains(PrepareFlag::CALL));
    }

    // -----------------------------------------------------------------------
    // ExecuteFlag tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_flag_normal() {
        assert_eq!(ExecuteFlag::NORMAL.as_raw(), 0x00);
    }

    #[test]
    fn test_execute_flag_union() {
        let combined = ExecuteFlag::QUERY_ALL.union(ExecuteFlag::HOLDABLE);
        assert_eq!(combined.as_raw(), 0x02 | 0x20);
        assert!(combined.contains(ExecuteFlag::QUERY_ALL));
        assert!(combined.contains(ExecuteFlag::HOLDABLE));
        assert!(!combined.contains(ExecuteFlag::RETURN_GENERATED_KEYS));
    }

    // -----------------------------------------------------------------------
    // ClientType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_client_type_round_trip() {
        let valid_codes: &[u8] = &[0, 1, 2, 3, 4, 5];
        for &code in valid_codes {
            let ct = ClientType::try_from(code).unwrap();
            assert_eq!(u8::from(ct), code);
        }
    }

    #[test]
    fn test_client_type_invalid() {
        assert!(ClientType::try_from(6).is_err());
        assert!(ClientType::try_from(255).is_err());
    }

    // -----------------------------------------------------------------------
    // DbmsType tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_dbms_type_round_trip() {
        let valid_codes: &[u8] = &[0, 1, 2, 3];
        for &code in valid_codes {
            let dt = DbmsType::try_from(code).unwrap();
            assert_eq!(u8::from(dt), code);
        }
    }

    #[test]
    fn test_dbms_type_invalid() {
        assert!(DbmsType::try_from(4).is_err());
        assert!(DbmsType::try_from(255).is_err());
    }

    // -----------------------------------------------------------------------
    // XaOp tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_xa_op_round_trip() {
        assert_eq!(u8::from(XaOp::try_from(1u8).unwrap()), 1);
        assert_eq!(u8::from(XaOp::try_from(2u8).unwrap()), 2);
    }

    #[test]
    fn test_xa_op_invalid() {
        assert!(XaOp::try_from(0u8).is_err());
        assert!(XaOp::try_from(3u8).is_err());
    }

    // -----------------------------------------------------------------------
    // Xid tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_xid_encode_decode_round_trip() {
        let xid = Xid::new(1, b"global-tx-1".to_vec(), b"branch-1".to_vec());
        let encoded = xid.encode();
        let decoded = Xid::decode(&encoded).unwrap();
        assert_eq!(xid, decoded);
    }

    #[test]
    fn test_xid_encode_empty_qualifiers() {
        let xid = Xid::new(0, vec![], vec![]);
        let encoded = xid.encode();
        assert_eq!(encoded.len(), 12); // header only
        let decoded = Xid::decode(&encoded).unwrap();
        assert_eq!(xid, decoded);
    }

    #[test]
    fn test_xid_decode_too_short() {
        assert!(Xid::decode(&[0; 11]).is_err());
    }

    #[test]
    fn test_xid_decode_truncated_data() {
        let xid = Xid::new(1, b"abcd".to_vec(), b"ef".to_vec());
        let encoded = xid.encode();
        // Truncate before branch qualifier
        assert!(Xid::decode(&encoded[..15]).is_err());
    }

    // -----------------------------------------------------------------------
    // Collection code constants
    // -----------------------------------------------------------------------

    #[test]
    fn test_collection_code_constants() {
        assert_eq!(CCI_CODE_SET, 0x20);
        assert_eq!(CCI_CODE_MULTISET, 0x40);
        assert_eq!(CCI_CODE_SEQUENCE, 0x60);
        assert_eq!(CCI_CODE_COLLECTION_MASK, 0x60);
    }
}
